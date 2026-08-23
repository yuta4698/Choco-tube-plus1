import asyncio
from typing import Any

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from core import INNERTUBE_BASE, get_client, proxy_parallel

router = APIRouter()


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
    return t if isinstance(t, list) else []


def normalize_video(raw: Any) -> dict | None:
    if not isinstance(raw, dict):
        return None
    # Some helper versions wrap the actual object.
    for key in ("video", "data", "result"):
        if isinstance(raw.get(key), dict) and (raw[key].get("videoId") or raw[key].get("id") or raw[key].get("title")):
            raw = raw[key]
            break

    related_raw = raw.get("recommendedVideos") or raw.get("relatedVideos") or raw.get("related") or raw.get("relatedStreams") or []
    related = []
    for item in related_raw:
        if not isinstance(item, dict):
            continue
        vid = _id(item)
        if not vid:
            url = item.get("url") or ""
            if "?v=" in url:
                vid = url.split("?v=", 1)[1].split("&", 1)[0]
        if not vid:
            continue
        related.append({
            "videoId": vid,
            "title": _text(item.get("title") or item.get("headline")),
            "author": _text(item.get("author") or item.get("uploaderName") or item.get("owner")),
            "authorId": item.get("authorId") or item.get("channelId") or "",
            "lengthSeconds": item.get("lengthSeconds") or item.get("duration") or 0,
            "viewCount": item.get("viewCount") or item.get("views") or 0,
            "publishedText": _text(item.get("publishedText") or item.get("uploadedDate") or item.get("published")),
            "authorThumbnails": item.get("authorThumbnails") or [],
            "videoThumbnails": _thumbs(item),
        })

    vid = _id(raw)
    title = _text(raw.get("title"))
    if not vid and not title:
        return None
    return {
        "videoId": vid,
        "title": title,
        "author": _text(raw.get("author") or raw.get("uploader") or raw.get("owner")),
        "authorId": raw.get("authorId") or raw.get("channelId") or "",
        "viewCount": raw.get("viewCount") or raw.get("views") or 0,
        "likeCount": raw.get("likeCount") or raw.get("likes") or 0,
        "publishedText": _text(raw.get("publishedText") or raw.get("published")),
        "description": raw.get("description") or "",
        "descriptionHtml": raw.get("descriptionHtml") or raw.get("description") or "",
        "lengthSeconds": raw.get("lengthSeconds") or raw.get("duration") or 0,
        "subCount": raw.get("subCount") or raw.get("subscriberCount") or 0,
        "subCountText": raw.get("subCountText") or "",
        "authorVerified": raw.get("authorVerified") or raw.get("verified") or False,
        "authorThumbnails": raw.get("authorThumbnails") or [],
        "videoThumbnails": _thumbs(raw),
        "recommendedVideos": related,
        "_source": "innertube-fallback",
    }


async def _helper_get(path: str, timeout: float = 18.0):
    client = await get_client()
    r = await client.get(f"{INNERTUBE_BASE}{path}", timeout=timeout)
    r.raise_for_status()
    return r.json()


async def _helper_video(video_id: str) -> dict | None:
    # Keep the primary contract used by the companion Innertube service.
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
        if isinstance(data, dict):
            data = data.get("relatedVideos") or data.get("recommendedVideos") or data.get("related") or data.get("videos") or data.get("items") or []
        if not isinstance(data, list):
            return []
        dummy = normalize_video({"videoId": video_id, "title": "", "recommendedVideos": data})
        return (dummy or {}).get("recommendedVideos", [])
    except Exception:
        return []


@router.get("/api/videoinfo/{video_id}")
async def fallback_videoinfo(video_id: str, nocache: bool = False):
    # Use all independent providers concurrently.  The old route depended on
    # Invidious/Piped only, which is why both video info and related videos
    # collapsed to 502 when those providers were unavailable.
    async def inv():
        try:
            r = await proxy_parallel("video", f"/api/v1/videos/{video_id}")
            return r.get("data")
        except Exception:
            return None

    async def helper():
        return await _helper_video(video_id)

    results = await asyncio.gather(inv(), helper(), return_exceptions=True)
    for result in results:
        if isinstance(result, dict) and not result.get("error"):
            normalized = result if result.get("_source") else normalize_video(result)
            if normalized:
                if not normalized.get("recommendedVideos"):
                    normalized["recommendedVideos"] = await _helper_related(video_id)
                return JSONResponse(normalized)

    # If the full helper video endpoint is unavailable, related can still be
    # useful to the client and gives a graceful response instead of 502.
    related = await _helper_related(video_id)
    if related:
        return JSONResponse({
            "videoId": video_id,
            "title": "",
            "recommendedVideos": related,
            "_source": "innertube-related-fallback",
        })
    return JSONResponse({"error": "動画情報の取得に失敗しました"}, status_code=502)


@router.get("/api/channel-home/{channel_id}")
async def fallback_channel_home(channel_id: str):
    # The companion service is still preferred because it returns the exact
    # channel-home shape expected by the existing UI.
    try:
        data = await _helper_get(f"/channel/{channel_id}", timeout=20)
        if isinstance(data, dict):
            return JSONResponse(data)
    except Exception:
        pass

    # Graceful local fallback: return channel metadata plus the latest videos.
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
