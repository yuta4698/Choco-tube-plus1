import asyncio
import re
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from core import get_client, proxy_parallel

router = APIRouter()
YT_HOME = "https://www.youtube.com/"
YT_API = "https://www.youtube.com/youtubei/v1"
_config: dict[str, Any] = {"key": "", "version": "", "expires": 0.0}
_config_lock = asyncio.Lock()


def text(v: Any) -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, dict):
        if isinstance(v.get("simpleText"), str):
            return v["simpleText"]
        if isinstance(v.get("runs"), list):
            return "".join(text(x) for x in v["runs"])
        for k in ("text", "content", "title", "headline"):
            if k in v:
                s = text(v[k])
                if s:
                    return s
    if isinstance(v, list):
        return "".join(text(x) for x in v)
    return str(v or "")


def duration(v: dict) -> int:
    for k in ("lengthSeconds", "duration"):
        x = v.get(k)
        if isinstance(x, (int, float)):
            return int(x)
        if isinstance(x, str) and x.isdigit():
            return int(x)
    s = text(v.get("lengthText") or v.get("durationText"))
    if re.fullmatch(r"\d+(?::\d{2}){1,2}", s):
        n = 0
        for p in s.split(":"):
            n = n * 60 + int(p)
        return n
    return 0


def channel_id(v: dict) -> str:
    for k in ("channelId", "authorId"):
        if v.get(k):
            return str(v[k])
    for k in ("ownerText", "longBylineText", "shortBylineText"):
        obj = v.get(k)
        if isinstance(obj, dict):
            for run in obj.get("runs", []):
                ep = run.get("navigationEndpoint", {}) if isinstance(run, dict) else {}
                b = ep.get("browseEndpoint", {}) if isinstance(ep, dict) else {}
                if b.get("browseId"):
                    return str(b["browseId"])
    return ""


def renderer(v: dict) -> dict | None:
    r = v.get("compactVideoRenderer") or v.get("videoRenderer") or v.get("gridVideoRenderer")
    if isinstance(r, dict):
        vid = str(r.get("videoId") or "")
        if not vid:
            return None
        return {
            "videoId": vid,
            "title": text(r.get("title")),
            "author": text(r.get("ownerText") or r.get("longBylineText") or r.get("shortBylineText")),
            "authorId": channel_id(r),
            "lengthSeconds": duration(r),
            "viewCount": 0,
            "publishedText": text(r.get("publishedTimeText")),
            "authorThumbnails": [],
            "videoThumbnails": r.get("thumbnail", {}).get("thumbnails", []) if isinstance(r.get("thumbnail"), dict) else [],
        }
    l = v.get("lockupViewModel")
    if isinstance(l, dict):
        vid = str(l.get("contentId") or "")
        if not vid:
            return None
        m = l.get("metadata", {})
        lm = m.get("lockupMetadataViewModel", {}) if isinstance(m, dict) else {}
        return {
            "videoId": vid,
            "title": text(lm.get("title") if isinstance(lm, dict) else l.get("title")),
            "author": text(l.get("ownerText") or l.get("byline")),
            "authorId": channel_id(l),
            "lengthSeconds": duration(l),
            "viewCount": 0,
            "publishedText": text(l.get("publishedTimeText")),
            "authorThumbnails": [],
            "videoThumbnails": [],
        }
    return None


def walk(obj: Any, out: list, seen: set, current: str = ""):
    if isinstance(obj, dict):
        x = renderer(obj)
        if x and x["videoId"] != current and x["videoId"] not in seen and x.get("title"):
            seen.add(x["videoId"])
            out.append(x)
        for v in obj.values():
            walk(v, out, seen, current)
    elif isinstance(obj, list):
        for v in obj:
            walk(v, out, seen, current)


async def config():
    import time
    if _config["key"] and _config["expires"] > time.time():
        return _config["key"], _config["version"]
    async with _config_lock:
        if _config["key"] and _config["expires"] > time.time():
            return _config["key"], _config["version"]
        c = await get_client()
        r = await c.get(YT_HOME, timeout=12)
        r.raise_for_status()
        h = r.text
        km = re.search(r'"INNERTUBE_API_KEY"\s*:\s*"([^\"]+)"', h)
        vm = re.search(r'"INNERTUBE_CLIENT_VERSION"\s*:\s*"([^\"]+)"', h)
        if not km:
            raise RuntimeError("InnerTube API key not found")
        _config.update(
            key=km.group(1),
            version=vm.group(1) if vm else "2.20260820.01.00",
            expires=time.time() + 1800,
        )
        return _config["key"], _config["version"]


async def post(endpoint: str, body: dict) -> dict:
    key, ver = await config()
    c = await get_client()
    payload = {
        "context": {
            "client": {
                "clientName": "WEB",
                "clientVersion": ver,
                "hl": "ja",
                "gl": "JP",
            }
        },
        **body,
    }
    r = await c.post(f"{YT_API}/{endpoint}?key={key}", json=payload, timeout=18)
    r.raise_for_status()
    return r.json()


async def get_related(video_id: str) -> list:
    try:
        data = await post("next", {"videoId": video_id})
        out = []
        walk(data, out, set(), video_id)
        return out[:50]
    except Exception:
        return []


async def get_info(video_id: str) -> dict | None:
    try:
        player, nxt = await asyncio.gather(
            post("player", {"videoId": video_id, "contentCheckOk": True, "racyCheckOk": True}),
            post("next", {"videoId": video_id}),
        )
        d = player.get("videoDetails", {})
        if not d.get("title"):
            return None
        out = {
            "videoId": video_id,
            "title": d.get("title", ""),
            "author": d.get("author", ""),
            "authorId": d.get("channelId", ""),
            "viewCount": int(d.get("viewCount") or 0),
            "lengthSeconds": int(d.get("lengthSeconds") or 0),
            "description": d.get("shortDescription", ""),
            "videoThumbnails": d.get("thumbnail", {}).get("thumbnails", []) if isinstance(d.get("thumbnail"), dict) else [],
            "recommendedVideos": [],
            "_source": "youtube-innertube-direct",
        }
        walk(nxt, out["recommendedVideos"], set(), video_id)
        return out
    except Exception:
        return None


@router.get("/api/videoinfo/{video_id}")
async def videoinfo(video_id: str, nocache: bool = False):
    yt = await get_info(video_id)
    if yt:
        return JSONResponse(yt)
    try:
        r = await proxy_parallel("video", f"/api/v1/videos/{video_id}")
        data = r.get("data")
        if isinstance(data, dict):
            if not data.get("recommendedVideos"):
                data["recommendedVideos"] = await get_related(video_id)
            return JSONResponse(data)
    except Exception:
        pass
    rel = await get_related(video_id)
    if rel:
        return JSONResponse({"videoId": video_id, "recommendedVideos": rel, "_source": "youtube-next-direct"})
    return JSONResponse({"error": "動画情報の取得に失敗しました"}, status_code=502)


@router.get("/api/home-direct")
async def home_direct():
    try:
        data = await post("browse", {"browseId": "FEwhat_to_watch"})
        out = []
        walk(data, out, set())
        return JSONResponse(out[:80])
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
