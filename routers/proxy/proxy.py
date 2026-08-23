import asyncio
from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from core import (
    CHANNEL_VIDEO_CATEGORIES, INNERTUBE_BASE, _CH_TAB_RE,
    _apply_enrichment, _innertube_cont_get, _innertube_cont_set,
    _innertube_to_invidious, fetch_innertube_continuation,
    fetch_innertube_videos, get_instances, get_video_back_instances,
    get_client, map_path, proxy_parallel,
)

router = APIRouter()


def _text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return value.get("text") or value.get("simpleText") or ""
    if isinstance(value, list):
        return "".join(_text(x) for x in value)
    return str(value or "")


def _innertube_related_to_invidious(raw):
    if isinstance(raw, dict):
        items = raw.get("related") or raw.get("videos") or raw.get("items") or []
    else:
        items = raw if isinstance(raw, list) else []
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        vid = item.get("videoId") or item.get("video_id") or item.get("id")
        if not vid:
            continue
        thumbs = item.get("thumbnails") or item.get("thumbnail") or []
        if isinstance(thumbs, dict):
            thumbs = thumbs.get("thumbnails") or []
        out.append({
            "videoId": str(vid),
            "title": _text(item.get("title") or item.get("headline")),
            "author": _text(item.get("author") or item.get("owner")),
            "authorId": item.get("authorId") or item.get("author_id") or item.get("channelId") or "",
            "lengthSeconds": item.get("lengthSeconds") or item.get("duration") or 0,
            "viewCount": item.get("viewCount") or item.get("view_count") or 0,
            "publishedText": _text(item.get("publishedText") or item.get("published")),
            "authorThumbnails": [],
            "videoThumbnails": thumbs if isinstance(thumbs, list) else [],
        })
    return out


async def _fetch_innertube_related(video_id: str):
    try:
        client = await get_client()
        resp = await client.get(f"{INNERTUBE_BASE}/video/{video_id}/related", timeout=12)
        resp.raise_for_status()
        return _innertube_related_to_invidious(resp.json())
    except Exception:
        return []


async def _fetch_innertube_home():
    try:
        client = await get_client()
        resp = await client.get(f"{INNERTUBE_BASE}/home", timeout=15)
        resp.raise_for_status()
        raw = resp.json()
        # youtubei.js HomeFeed の items/videos から動画カードだけを抽出
        items = raw.get("videos") or raw.get("items") or [] if isinstance(raw, dict) else []
        return _innertube_related_to_invidious(items)
    except Exception:
        return []


@router.get("/proxy/main/api/home")
async def proxy_home():
    """Home feed: Innertubeを優先し、YouTubeホームが空でも動画カードを返す。"""
    data = await _fetch_innertube_home()
    if data:
        return JSONResponse(data, headers={"X-Source": "innertube"})
    try:
        result = await proxy_parallel("popular", "/api/v1/popular")
        return JSONResponse(result["data"], headers={"X-Source": "invidious"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


@router.get("/proxy/main/{path:path}")
async def proxy_main(path: str, request: Request):
    qs = request.url.query
    app_path = "/" + path + ("?" + qs if qs else "")
    category, invidious_path = map_path(app_path)

    ch_match = _CH_TAB_RE.match(app_path) if category in CHANNEL_VIDEO_CATEGORIES else None
    channel_id = ch_match.group(1) if ch_match else None
    tab = ch_match.group(2) if ch_match else None

    if ch_match:
        inv_cont_in = request.query_params.get("continuation")
        innertube_cont_key = _innertube_cont_get(channel_id, tab, inv_cont_in) if inv_cont_in else None
        invidious_task = asyncio.create_task(proxy_parallel(category, invidious_path, prefer_valid_videos=True))
        if innertube_cont_key:
            innertube_task = asyncio.create_task(fetch_innertube_continuation(innertube_cont_key))
        else:
            innertube_task = asyncio.create_task(fetch_innertube_videos(channel_id, tab))
        innertube_result = ([], None)
        try:
            innertube_result = await asyncio.wait_for(innertube_task, timeout=8.0)
        except (asyncio.TimeoutError, Exception):
            innertube_result = ([], None)
        try:
            inv_result = await invidious_task
        except Exception as e:
            inv_result = e
        innertube_items, new_innertube_cont_key = innertube_result if isinstance(innertube_result, tuple) else (innertube_result, None)
        if isinstance(inv_result, Exception):
            if innertube_items:
                fallback = _innertube_to_invidious(innertube_items, channel_id)
                return JSONResponse(fallback if tab == "latest" else {"videos": fallback, "continuation": None})
            return JSONResponse({"error": str(inv_result)}, status_code=502)
        if new_innertube_cont_key:
            inv_data = inv_result["data"]
            inv_cont_out = inv_data.get("continuation") if isinstance(inv_data, dict) else None
            if inv_cont_out:
                _innertube_cont_set(channel_id, tab, inv_cont_out, new_innertube_cont_key)
        return JSONResponse(_apply_enrichment(inv_result["data"], innertube_items, channel_id))

    try:
        result = await proxy_parallel(category, invidious_path)
        data = result["data"]
        if category == "video" and isinstance(data, dict):
            related = data.get("recommendedVideos") or data.get("relatedVideos") or []
            if not related:
                video_id = app_path.split("/api/stream/", 1)[-1].split("?", 1)[0]
                if video_id:
                    fallback = await _fetch_innertube_related(video_id)
                    if fallback:
                        data["recommendedVideos"] = fallback
                        data["_relatedSource"] = "innertube"
        return JSONResponse(data)
    except Exception as e:
        if category == "video":
            video_id = app_path.split("/api/stream/", 1)[-1].split("?", 1)[0]
            fallback = await _fetch_innertube_related(video_id) if video_id else []
            if fallback:
                return JSONResponse({"recommendedVideos": fallback, "_source": "innertube"})
        return JSONResponse({"error": str(e)}, status_code=502)


@router.get("/proxy/stream/{path:path}")
async def proxy_stream(path: str, request: Request):
    try:
        params = dict(request.query_params)
        exclude_list = [s.strip() for s in params.pop("exclude", "").split(",") if s.strip()]
        qs = urlencode(params) if params else ""
        app_path = "/" + path + ("?" + qs if qs else "")
        _, invidious_path = map_path(app_path)
        try:
            vb_instances = await get_video_back_instances()
        except Exception:
            vb_instances = []
        if not vb_instances:
            vb_instances = await get_instances("video")
        result = await proxy_parallel("video", invidious_path, exclude_list, prefer_valid_stream=True, override_instances=vb_instances)
        headers = {}
        if result.get("used_instance"):
            headers["X-Instance-Used"] = result["used_instance"]
        return JSONResponse(result["data"], headers=headers)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)
