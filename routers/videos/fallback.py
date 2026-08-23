import asyncio
import json
import re
from typing import Any

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from core import get_client, proxy_parallel

router = APIRouter()

# Piped is kept as a secondary provider. The primary fallback below talks to
# YouTube's public InnerTube endpoints directly, so the app no longer depends
# on choco-youtube-js for video metadata/related videos.
PIPED_APIS = [
    "https://pipedapi.kavin.rocks",
    "https://pipedapi.leptons.xyz",
    "https://pipedapi.adminforge.de",
    "https://api.piped.yt",
    "https://pipedapi.privacy.com.de",
]

_YT_HOME = "https://www.youtube.com/"
_YT_API = "https://www.youtube.com/youtubei/v1"
_YT_CONFIG_TTL = 1800
_yt_config: dict[str, Any] = {"key": "", "version": "", "expires": 0.0}
_yt_config_lock = asyncio.Lock()


def _text(v: Any) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        if isinstance(v.get("simpleText"), str):
            return v["simpleText"]
        runs = v.get("runs")
        if isinstance(runs, list):
            return "".join(_text(x) for x in runs)
        for key in ("text", "content", "title", "headline"):
            if key in v:
                value = _text(v[key])
                if value:
                    return value
        return ""
    if isinstance(v, list):
        return "".join(_text(x) for x in v)
    return str(v or "")


def _id(v: dict) -> str:
    return str(v.get("videoId") or v.get("video_id") or v.get("id") or v.get("contentId") or "")


def _thumbs(v: dict) -> list:
    t = v.get("videoThumbnails") or v.get("thumbnails") or v.get("thumbnail") or []
    if isinstance(t, dict):
        t = t.get("thumbnails") or t.get("sources") or []
    if isinstance(t, str):
        return [{"url": t}]
    return t if isinstance(t, list) else []


def _duration_seconds(v: dict) -> int:
    for key in ("lengthSeconds", "duration", "length"):
        value = v.get(key)
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
    # YouTube UI often supplies a durationText such as 12:34.
    text = _text(v.get("lengthText") or v.get("durationText"))
    if text and re.fullmatch(r"\d+(?::\d{2}){1,2}", text):
        parts = [int(x) for x in text.split(":")]
        total = 0
        for p in parts:
            total = total * 60 + p
        return total
    return 0


def _normalize_related(items: Any) -> list:
    if isinstance(items, dict):
        items = (
            items.get("relatedStreams")
            or items.get("relatedVideos")
            or items.get("recommendedVideos")
            or items.get("videos")
            or items.get("items")
            or []
        )
    if not isinstance(items, list):
        return []
    out = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        vid = _id(item)
        if not vid or vid in seen:
            continue
        seen.add(vid)
        thumbs = _thumbs(item)
        thumb = item.get("thumbnail")
        if thumb and not thumbs:
            thumbs = [{"url": thumb}]
        author = item.get("author") or item.get("owner") or item.get("uploaderName")
        author_id = item.get("authorId") or item.get("channelId") or item.get("uploaderUrl") or ""
        out.append({
            "videoId": vid,
            "title": _text(item.get("title") or item.get("headline")),
            "author": _text(author),
            "authorId": author_id,
            "lengthSeconds": _duration_seconds(item),
            "viewCount": item.get("viewCount") or item.get("views") or 0,
            "publishedText": _text(item.get("publishedText") or item.get("published") or item.get("uploadedDate")),
            "authorThumbnails": item.get("authorThumbnails") or [],
            "videoThumbnails": thumbs,
        })
    return out


def _parse_renderer_video(node: dict) -> dict | None:
    """Convert YouTube renderer/lockup variants to the site's card contract."""
    r = node.get("compactVideoRenderer") or node.get("videoRenderer") or node.get("gridVideoRenderer")
    if isinstance(r, dict):
        vid = _id(r)
        if not vid:
            return None
        return {
            "videoId": vid,
            "title": _text(r.get("title")),
            "author": _text(r.get("ownerText") or r.get("longBylineText") or r.get("shortBylineText")),
            "authorId": _extract_channel_id(r),
            "lengthSeconds": _duration_seconds(r),
            "viewCount": _parse_number_text(r.get("viewCountText")),
            "publishedText": _text(r.get("publishedTimeText")),
            "authorThumbnails": _extract_thumbnails(r.get("channelThumbnailSupportedRenderers") or r.get("channelThumbnail")),
            "videoThumbnails": r.get("thumbnail", {}).get("thumbnails", []) if isinstance(r.get("thumbnail"), dict) else [],
        }

    lock = node.get("lockupViewModel")
    if isinstance(lock, dict):
        vid = str(lock.get("contentId") or "")
        if not vid:
            return None
        meta = lock.get("metadata", {}) if isinstance(lock.get("metadata"), dict) else {}
        title = _text(meta.get("lockupMetadataViewModel", {}).get("title")) if isinstance(meta.get("lockupMetadataViewModel"), dict) else ""
        if not title:
            title = _text(lock.get("title") or lock.get("headline"))
        return {
            "videoId": vid,
            "title": title,
            "author": _text(lock.get("ownerText") or lock.get("byline")),
            "authorId": _extract_channel_id(lock),
            "lengthSeconds": _duration_seconds(lock),
            "viewCount": _parse_number_text(lock.get("viewCountText")),
            "publishedText": _text(lock.get("publishedTimeText")),
            "authorThumbnails": [],
            "videoThumbnails": _extract_thumbnails(lock.get("contentImage")),
        }
    return None


def _extract_channel_id(node: dict) -> str:
    for key in ("channelId", "authorId"):
        if node.get(key):
            return str(node[key])
    for key in ("longBylineText", "shortBylineText", "ownerText"):
        obj = node.get(key)
        if isinstance(obj, dict):
            for run in obj.get("runs", []):
                ep = run.get("navigationEndpoint", {}) if isinstance(run, dict) else {}
                browse = ep.get("browseEndpoint", {}) if isinstance(ep, dict) else {}
                if browse.get("browseId"):
                    return str(browse["browseId"])
    return ""


def _extract_thumbnails(node: Any) -> list:
    if isinstance(node, dict):
        if isinstance(node.get("thumbnails"), list):
            return node["thumbnails"]
        for value in node.values():
            result = _extract_thumbnails(value)
            if result:
                return result
    elif isinstance(node, list):
        for value in node:
            result = _extract_thumbnails(value)
            if result:
                return result
    return []


def _parse_number_text(value: Any) -> int:
    text = _text(value).lower().replace(",", "").strip()
    if not text:
        return 0
    m = re.match(r"([0-9.]+)\s*([kmb])?", text)
    if not m:
        return 0
    number = float(m.group(1))
    mult = {"k": 1000, "m": 1_000_000, "b": 1_000_000_000}.get(m.group(2) or "", 1)
    return int(number * mult)


def _walk_related(obj: Any, out: list, seen: set[str], current_id: str = "") -> None:
    if isinstance(obj, dict):
        parsed = _parse_renderer_video(obj)
        if parsed and parsed["videoId"] != current_id and parsed["videoId"] not in seen:
            if parsed.get("title"):
                seen.add(parsed["videoId"])
                out.append(parsed)
        for value in obj.values():
            _walk_related(value, out, seen, current_id)
    elif isinstance(obj, list):
        for value in obj:
            _walk_related(value, out, seen, current_id)


def _find_video_details(player: dict, video_id: str) -> dict:
    details = player.get("videoDetails") if isinstance(player, dict) else None
    if not isinstance(details, dict):
        return {}
    thumbs = details.get("thumbnail", {}).get("thumbnails", []) if isinstance(details.get("thumbnail"), dict) else []
    return {
        "videoId": video_id,
        "title": _text(details.get("title")),
        "author": _text(details.get("author")),
        "authorId": details.get("channelId") or "",
        "viewCount": int(details.get("viewCount") or 0),
        "lengthSeconds": int(details.get("lengthSeconds") or 0),
        "description": details.get("shortDescription") or "",
        "videoThumbnails": thumbs,
        "authorThumbnails": [],
        "publishedText": "",
        "authorVerified": False,
    }


async def _yt_config() -> tuple[str, str]:
    import time
    now = time.time()
    if _yt_config["key"] and _yt_config["expires"] > now:
        return _yt_config["key"], _yt_config["version"]
    async with _yt_config_lock:
        if _yt_config["key"] and _yt_config["expires"] > time.time():
            return _yt_config["key"], _yt_config["version"]
        client = await get_client()
        r = await client.get(_YT_HOME, timeout=12)
        r.raise_for_status()
        html = r.text
        key_match = re.search(r'"INNERTUBE_API_KEY"\s*:\s*"([^"]+)"', html)
        version_match = re.search(r'"INNERTUBE_CLIENT_VERSION"\s*:\s*"([^"]+)"', html)
        if not key_match:
            raise RuntimeError("YouTube InnerTube API key was not found")
        key = key_match.group(1)
        version = version_match.group(1) if version_match else "2.20260820.01.00"
        _yt_config.update({"key": key, "version": version, "expires": time.time() + _YT_CONFIG_TTL})
        return key, version


async def _yt_post(endpoint: str, body: dict) -> dict:
    key, version = await _yt_config()
    client = await get_client()
    context = {
        "client": {
            "clientName": "WEB",
            "clientVersion": version,
            "hl": "ja",
            "gl": "JP",
        }
    }
    payload = {"context": context, **body}
    r = await client.post(f"{_YT_API}/{endpoint}?key={key}", json=payload, timeout=18)
    r.raise_for_status()
    return r.json()


async def _innertube_video(video_id: str) -> dict | None:
    """Fetch player + next directly from YouTube, like wkt's getInfo()."""
    try:
        player, nxt = await asyncio.gather(
            _yt_post("player", {"videoId": video_id, "contentCheckOk": True, "racyCheckOk": True}),
            _yt_post("next", {"videoId": video_id}),
        )
        details = _find_video_details(player, video_id)
        if not details.get("title"):
            return None
        related: list = []
        _walk_related(nxt, related, set(), video_id)
        # Some YouTube responses wrap the same cards in several containers.
        # De-duplicate while preserving the order returned by YouTube.
        dedup = []
        seen = set()
        for item in related:
            if item["videoId"] in seen:
                continue
            seen.add(item["videoId"])
            dedup.append(item)
        details["recommendedVideos"] = dedup[:50]
        details["_source"] = "youtube-innertube"
        return details
    except Exception:
        return None


async def _innertube_related(video_id: str) -> list:
    try:
        data = await _yt_post("next", {"videoId": video_id})
        related: list = []
        _walk_related(data, related, set(), video_id)
        return related[:50]
    except Exception:
        return []


async def _innertube_home() -> list:
    try:
        data = await _yt_post("browse", {"browseId": "FEwhat_to_watch"})
        related: list = []
        _walk_related(data, related, set())
        return related[:80]
    except Exception:
        return []


async def _piped_get(path: str, timeout: float = 15.0):
    client = await get_client()
    last = None
    for base in PIPED_APIS:
        try:
            r = await client.get(f"{base}{path}", timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if data:
                return data
        except Exception as exc:
            last = exc
    if last:
        raise last
    return None


async def _piped_video(video_id: str) -> dict | None:
    try:
        data = await _piped_get(f"/streams/{video_id}")
        if not isinstance(data, dict):
            return None
        related = _normalize_related(data.get("relatedStreams") or [])
        return {
            "videoId": video_id,
            "title": _text(data.get("title")),
            "author": _text(data.get("uploader")),
            "authorId": data.get("uploaderUrl") or data.get("channelId") or "",
            "viewCount": data.get("views") or 0,
            "lengthSeconds": data.get("duration") or 0,
            "description": data.get("description") or "",
            "videoThumbnails": ([{"url": data.get("thumbnailUrl")}] if data.get("thumbnailUrl") else []),
            "authorThumbnails": ([{"url": data.get("uploaderAvatar")}] if data.get("uploaderAvatar") else []),
            "publishedText": _text(data.get("uploadDate")),
            "recommendedVideos": related,
            "_source": "piped-fallback",
        }
    except Exception:
        return None


async def _piped_channel(channel_id: str) -> dict | None:
    try:
        data = await _piped_get(f"/channel/{channel_id}", timeout=18.0)
        if not isinstance(data, dict):
            return None
        return {
            "author": {
                "author": data.get("name") or "",
                "authorId": data.get("id") or channel_id,
                "authorThumbnails": ([{"url": data.get("avatarUrl")}] if data.get("avatarUrl") else []),
                "authorBanner": data.get("bannerUrl") or "",
                "description": data.get("description") or "",
                "subCount": data.get("subscriberCount") or 0,
                "authorVerified": data.get("verified") or False,
            },
            "videos": _normalize_related(data.get("relatedStreams") or []),
            "_source": "piped-fallback",
        }
    except Exception:
        return None


@router.get("/api/videoinfo/{video_id}")
async def fallback_videoinfo(video_id: str, nocache: bool = False):
    # 1) Existing Invidious path, 2) direct YouTube InnerTube (wkt-style),
    # 3) Piped. InnerTube is deliberately independent of choco-youtube-js.
    async def inv():
        try:
            r = await proxy_parallel("video", f"/api/v1/videos/{video_id}")
            return r.get("data")
        except Exception:
            return None

    inv_result, yt_result, piped_result = await asyncio.gather(
        inv(), _innertube_video(video_id), _piped_video(video_id), return_exceptions=True
    )

    candidates = [yt_result, inv_result, piped_result]
    # Prefer the direct InnerTube result when it has related videos, because it
    # is the same underlying watch-next data that wkt consumes.
    candidates.sort(key=lambda x: 2 if isinstance(x, dict) and x.get("_source") == "youtube-innertube" and x.get("recommendedVideos") else (1 if isinstance(x, dict) else 0), reverse=True)

    for result in candidates:
        if not isinstance(result, dict):
            continue
        if not result.get("title") and not result.get("videoId"):
            continue
        if not result.get("recommendedVideos"):
            result["recommendedVideos"] = await _innertube_related(video_id)
        if not result.get("recommendedVideos") and isinstance(piped_result, dict):
            result["recommendedVideos"] = piped_result.get("recommendedVideos", [])
        result["videoId"] = result.get("videoId") or video_id
        return JSONResponse(result)

    related = await _innertube_related(video_id)
    if not related and isinstance(piped_result, dict):
        related = piped_result.get("recommendedVideos", [])
    if related:
        return JSONResponse({
            "videoId": video_id,
            "title": "",
            "recommendedVideos": related,
            "_source": "related-fallback",
        })
    return JSONResponse({"error": "動画情報の取得に失敗しました"}, status_code=502)


@router.get("/api/channel-home/{channel_id}")
async def fallback_channel_home(channel_id: str):
    # Direct InnerTube browse is the first choice, avoiding the old helper API.
    try:
        data = await _yt_post("browse", {"browseId": channel_id})
        videos: list = []
        _walk_related(data, videos, set())
        if videos:
            return JSONResponse({"videos": videos, "_source": "youtube-innertube"})
    except Exception:
        pass

    piped = await _piped_channel(channel_id)
    if piped:
        return JSONResponse(piped)

    try:
        channel_task = asyncio.create_task(proxy_parallel("channel", f"/api/v1/channels/{channel_id}"))
        videos_task = asyncio.create_task(proxy_parallel("channel_latest", f"/api/v1/channels/{channel_id}/videos?sort_by=newest"))
        channel_result, videos_result = await asyncio.gather(channel_task, videos_task, return_exceptions=True)
        channel = channel_result.get("data", {}) if isinstance(channel_result, dict) else {}
        videos = videos_result.get("data", {}) if isinstance(videos_result, dict) else []
        if isinstance(videos, dict):
            videos = videos.get("videos", [])
        return JSONResponse({"author": channel, "videos": videos if isinstance(videos, list) else [], "_source": "invidious-fallback"})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=502)


@router.get("/api/home")
async def fallback_home():
    videos = await _innertube_home()
    if videos:
        return JSONResponse(videos, headers={"X-Source": "youtube-innertube"})
    try:
        data = await _piped_get("/trending?region=JP", timeout=18.0)
        videos = _normalize_related(data)
        if videos:
            return JSONResponse(videos, headers={"X-Source": "piped"})
    except Exception:
        pass
    try:
        r = await proxy_parallel("popular", "/api/v1/popular")
        return JSONResponse(r.get("data", []), headers={"X-Source": "invidious"})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=502)
