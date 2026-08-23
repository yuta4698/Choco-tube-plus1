import asyncio
from typing import Any

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from core import INNERTUBE_BASE, get_client, proxy_parallel

router = APIRouter()

# Piped publishes a live public-instance list, but keeping a small set of
# known API endpoints here prevents one dead instance from breaking playback.
PIPED_APIS = [
    "https://pipedapi.kavin.rocks",
    "https://pipedapi.leptons.xyz",
    "https://pipedapi.adminforge.de",
    "https://api.piped.yt",
    "https://pipedapi.privacy.com.de",
]


def _text(v: Any) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        return v.get("text") or v.get("simpleText") or ""
    if isinstance(v, list):
        return "".join(_text(x) for x in v)
    return str(v or "")


def _id(v: dict) -> str:
    return str(v.get("videoId") or v.get("video_id") or v.get("id") or "")


def _thumbs(v: dict) -> list:
    t = v.get("videoThumbnails") or v.get("thumbnails") or v.get("thumbnail") or []
    if isinstance(t, dict):
        t = t.get("thumbnails") or []
    if isinstance(t, str):
        return [{"url": t}]
    return t if isinstance(t, list) else []


def normalize_related(items: Any) -> list:
    if isinstance(items, dict):
        items = items.get("relatedStreams") or items.get("relatedVideos") or items.get("recommendedVideos") or items.get("videos") or items.get("items") or []
    if not isinstance(items, list):
        return []
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        vid = _id(item)
        if not vid:
            url = item.get("url") or ""
            if "?v=" in url:
                vid = url.split("?v=", 1)[1].split("&", 1)[0]
            elif "/watch/" in url:
                vid = url.rsplit("/watch/", 1)[1].split("?", 1)[0]
        if not vid:
            continue
        thumb = item.get("thumbnail")
        thumbs = _thumbs(item)
        if thumb and not thumbs:
            thumbs = [{"url": thumb}]
        out.append({
            "videoId": vid,
            "title": _text(item.get("title")),
            "author": _text(item.get("uploaderName") or item.get("author") or item.get("owner")),
            "authorId": item.get("uploaderUrl") or item.get("authorId") or item.get("channelId") or "",
            "lengthSeconds": item.get("duration") or item.get("lengthSeconds") or 0,
            "viewCount": item.get("views") or item.get("viewCount") or 0,
            "publishedText": _text(item.get("uploadedDate") or item.get("publishedText")),
            "authorThumbnails": ([{"url": item.get("uploaderAvatar")}] if item.get("uploaderAvatar") else []),
            "videoThumbnails": thumbs,
        })
    return out


def normalize_video(raw: Any) -> dict | None:
    if not isinstance(raw, dict):
        return None
    for key in ("video", "data", "result"):
        if isinstance(raw.get(key), dict) and (raw[key].get("videoId") or raw[key].get("id") or raw[key].get("title")):
            raw = raw[key]
            break

    related = normalize_related(raw)
    vid = _id(raw)
    title = _text(raw.get("title"))
    if not vid and not title:
        return None
    return {
        "videoId": vid,
        "title": title,
        "author": _text(raw.get("author") or raw.get("uploaderName") or raw.get("uploader") or raw.get("owner")),
        "authorId": raw.get("authorId") or raw.get("channelId") or raw.get("uploaderUrl") or "",
        "viewCount": raw.get("viewCount") or raw.get("views") or 0,
        "likeCount": raw.get("likeCount") or raw.get("likes") or 0,
        "publishedText": _text(raw.get("publishedText") or raw.get("uploadedDate") or raw.get("published")),
        "description": raw.get("description") or "",
        "descriptionHtml": raw.get("descriptionHtml") or raw.get("description") or "",
        "lengthSeconds": raw.get("lengthSeconds") or raw.get("duration") or 0,
        "subCount": raw.get("subCount") or raw.get("subscriberCount") or 0,
        "subCountText": raw.get("subCountText") or "",
        "authorVerified": raw.get("authorVerified") or raw.get("verified") or False,
        "authorThumbnails": raw.get("authorThumbnails") or [],
        "videoThumbnails": _thumbs(raw),
        "recommendedVideos": related,
        "_source": "piped-fallback",
    }


async def _helper_get(path: str, timeout: float = 18.0):
    client = await get_client()
    r = await client.get(f"{INNERTUBE_BASE}{path}", timeout=timeout)
    r.raise_for_status()
    return r.json()


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
            continue
    if last:
        raise last
    return None


async def _helper_video(video_id: str) -> dict | None:
    for path in (f"/video/{video_id}", f"/video/{video_id}/info"):
        try:
            data = await _helper_get(path)
            result = normalize_video(data)
            if result:
                return result
        except Exception:
            continue
    return None


async def _helper_related(video_id: str) -> list:
    try:
        data = await _helper_get(f"/video/{video_id}/related")
        return normalize_related(data)
    except Exception:
        return []


async def _piped_video(video_id: str) -> dict | None:
    try:
        data = await _piped_get(f"/streams/{video_id}")
        result = normalize_video(data)
        if result:
            return result
    except Exception:
        pass
    return None


async def _piped_channel(channel_id: str) -> dict | None:
    try:
        data = await _piped_get(f"/channel/{channel_id}", timeout=18.0)
        if not isinstance(data, dict):
            return None
        videos = normalize_related(data.get("relatedStreams") or [])
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
            "videos": videos,
            "_source": "piped-fallback",
        }
    except Exception:
        return None


async def _piped_trending():
    try:
        data = await _piped_get("/trending?region=JP", timeout=18.0)
        return normalize_related(data)
    except Exception:
        return []


@router.get("/api/videoinfo/{video_id}")
async def fallback_videoinfo(video_id: str, nocache: bool = False):
    async def inv():
        try:
            r = await proxy_parallel("video", f"/api/v1/videos/{video_id}")
            return r.get("data")
        except Exception:
            return None

    # The companion Innertube service can be cold/unavailable. Piped is an
    # independent provider and its /streams endpoint also includes relatedStreams.
    results = await asyncio.gather(inv(), _helper_video(video_id), _piped_video(video_id), return_exceptions=True)
    for result in results:
        if isinstance(result, dict) and not result.get("error"):
            normalized = result if result.get("_source") else normalize_video(result)
            if normalized:
                if not normalized.get("recommendedVideos"):
                    normalized["recommendedVideos"] = await _helper_related(video_id)
                if not normalized.get("recommendedVideos"):
                    piped = await _piped_video(video_id)
                    if piped:
                        normalized["recommendedVideos"] = piped.get("recommendedVideos", [])
                return JSONResponse(normalized)

    related = await _helper_related(video_id)
    if not related:
        piped = await _piped_video(video_id)
        related = (piped or {}).get("recommendedVideos", [])
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
    try:
        data = await _helper_get(f"/channel/{channel_id}", timeout=20)
        if isinstance(data, dict):
            return JSONResponse(data)
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
        return JSONResponse({
            "author": channel,
            "videos": videos if isinstance(videos, list) else [],
            "_source": "invidious-fallback",
        })
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=502)


@router.get("/api/home")
async def fallback_home():
    videos = await _piped_trending()
    if videos:
        return JSONResponse(videos, headers={"X-Source": "piped"})
    try:
        r = await proxy_parallel("popular", "/api/v1/popular")
        return JSONResponse(r.get("data", []), headers={"X-Source": "invidious"})
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=502)
