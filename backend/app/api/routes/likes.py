"""Video likes routes using Supabase REST API - Instagram-style likes."""
from fastapi import APIRouter, HTTPException, Query
from datetime import datetime

from app.core.supabase_rest_client import get_supabase_rest

router = APIRouter(prefix="/likes", tags=["likes"])


@router.get("/batch")
async def get_like_status_batch(
    codes: str = Query(..., description="Comma-separated video codes"),
    user_id: str = Query(...)
):
    """
    Get like status and count for multiple videos in one request.
    Optimized for performance - reduces API calls.
    
    Returns: { results: [{ code, liked, like_count }, ...] }
    """
    client = get_supabase_rest()
    
    # Parse codes
    video_codes = [code.strip() for code in codes.split(',') if code.strip()]
    
    if not video_codes:
        return {"results": []}
    
    # Limit to prevent abuse
    video_codes = video_codes[:50]
    
    # Get all user likes for these videos in one query
    codes_filter = ','.join(f'"{code}"' for code in video_codes)
    user_likes = await client.get(
        "video_likes",
        filters={
            "video_code": f"in.({codes_filter})",
            "user_id": f"eq.{user_id}"
        }
    )
    
    # Create set of liked codes for fast lookup
    liked_codes = set(like["video_code"] for like in user_likes) if user_likes else set()
    
    # Get like counts for all videos
    # Note: This could be optimized further with a custom SQL query
    results = []
    for code in video_codes:
        like_count = await client.count(
            "video_likes",
            filters={"video_code": f"eq.{code}"}
        )
        
        results.append({
            "code": code,
            "liked": code in liked_codes,
            "like_count": like_count
        })
    
    return {"results": results}


@router.get("/{video_code}")
async def get_like_status(
    video_code: str,
    user_id: str = Query(...)
):
    """
    Get like status and count for a video.
    Returns: { liked: bool, like_count: int }
    """
    client = get_supabase_rest()
    
    # Check if user liked this video
    user_like = await client.get(
        "video_likes",
        filters={
            "video_code": f"eq.{video_code}",
            "user_id": f"eq.{user_id}"
        }
    )
    
    # Get total like count
    like_count = await client.count(
        "video_likes",
        filters={"video_code": f"eq.{video_code}"}
    )
    
    return {
        "liked": len(user_like) > 0 if user_like else False,
        "like_count": like_count
    }


@router.post("/{video_code}")
async def toggle_like(
    video_code: str,
    user_id: str = Query(...)
):
    """
    Toggle like status for a video (like/unlike).
    Returns: { liked: bool, like_count: int }
    """
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID required")
    
    client = get_supabase_rest()
    
    # Check if already liked
    existing = await client.get(
        "video_likes",
        filters={
            "video_code": f"eq.{video_code}",
            "user_id": f"eq.{user_id}"
        }
    )
    
    if existing and len(existing) > 0:
        # Unlike - delete the like
        await client.delete(
            "video_likes",
            filters={
                "video_code": f"eq.{video_code}",
                "user_id": f"eq.{user_id}"
            },
            use_admin=True
        )
        liked = False
    else:
        # Like - create new like
        await client.insert(
            "video_likes",
            data={
                "video_code": video_code,
                "user_id": user_id,
                "created_at": datetime.utcnow().isoformat()
            },
            use_admin=True
        )
        liked = True
    
    # Get updated like count
    like_count = await client.count(
        "video_likes",
        filters={"video_code": f"eq.{video_code}"}
    )
    
    return {
        "liked": liked,
        "like_count": like_count
    }


@router.get("/user/liked-videos")
async def get_liked_videos(
    user_id: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=50)
):
    """Get videos liked by user with pagination."""
    client = get_supabase_rest()
    
    offset = (page - 1) * page_size
    
    # Get user's likes with video details
    likes = await client.get(
        "video_likes",
        filters={"user_id": f"eq.{user_id}"},
        order="created_at.desc",
        limit=page_size,
        offset=offset
    )
    
    if not likes or len(likes) == 0:
        return {
            "items": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "total_pages": 0
        }
    
    # Get video codes
    video_codes = [like["video_code"] for like in likes]
    
    # Get video details (properly quote codes for in filter)
    codes_filter = ','.join(f'"{code}"' for code in video_codes)
    videos = await client.get(
        "videos",
        filters={"code": f"in.({codes_filter})"}
    )
    
    # Get total count
    total = await client.count(
        "video_likes",
        filters={"user_id": f"eq.{user_id}"}
    )
    
    # Map videos to maintain order
    video_map = {v["code"]: v for v in videos} if videos else {}
    items = [video_map[code] for code in video_codes if code in video_map]
    
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 0
    }
