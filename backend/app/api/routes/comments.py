"""Comment routes using Supabase REST API - Reddit-style threaded comments."""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

from app.core.supabase_rest_client import get_supabase_rest

router = APIRouter(prefix="/comments", tags=["comments"])


class CreateCommentRequest(BaseModel):
    content: str
    parent_id: Optional[int] = None


class UpdateCommentRequest(BaseModel):
    content: str


async def _get_comments_tree(video_code: str, user_id: str = None, sort: str = "best"):
    """Get threaded comments for a video."""
    client = get_supabase_rest()
    
    # Get all comments for this video
    order = "created_at.desc" if sort == "new" else "created_at.asc" if sort == "old" else "score.desc"
    
    comments = await client.get(
        "video_comments",
        filters={"video_code": f"eq.{video_code}", "is_deleted": "eq.false"},
        order=order
    )
    
    if not comments:
        return []
    
    # Build tree structure
    comment_map = {}
    root_comments = []
    
    for c in comments:
        comment_data = {
            "id": c["id"],
            "content": c["content"],
            "user_id": c["user_id"],
            "username": c.get("username") or f"User_{c['user_id'][:8]}",
            "created_at": c["created_at"],
            "updated_at": c.get("updated_at"),
            "score": c.get("score", 0),
            "parent_id": c.get("parent_id"),
            "replies": [],
            "user_vote": 0
        }
        comment_map[c["id"]] = comment_data
    
    # Get user votes if user_id provided
    if user_id:
        votes = await client.get(
            "comment_votes",
            filters={"user_id": f"eq.{user_id}"}
        )
        vote_map = {v["comment_id"]: v["vote"] for v in votes}
        for cid, comment in comment_map.items():
            comment["user_vote"] = vote_map.get(cid, 0)
    
    # Build tree
    for c in comments:
        comment = comment_map[c["id"]]
        parent_id = c.get("parent_id")
        if parent_id and parent_id in comment_map:
            comment_map[parent_id]["replies"].append(comment)
        else:
            root_comments.append(comment)
    
    return root_comments


@router.get("/{video_code}")
async def get_comments(
    video_code: str,
    user_id: str = Query(None, description="User ID for vote status"),
    sort: str = Query("best", description="Sort: best, new, old, controversial")
):
    """Get threaded comments for a video."""
    comments = await _get_comments_tree(video_code, user_id, sort)
    client = get_supabase_rest()
    count = await client.count("video_comments", filters={"video_code": f"eq.{video_code}", "is_deleted": "eq.false"})
    return {"comments": comments, "count": count}


@router.post("/{video_code}")
async def create_comment(
    video_code: str,
    request: CreateCommentRequest,
    user_id: str = Query(..., description="User ID"),
    username: str = Query(None, description="Display name")
):
    """Create a new comment or reply."""
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID is required")
    
    if not request.content or not request.content.strip():
        raise HTTPException(status_code=400, detail="Comment cannot be empty")
    
    if len(request.content) > 10000:
        raise HTTPException(status_code=400, detail="Comment too long (max 10000 chars)")
    
    sanitized_username = None
    if username and username.strip():
        sanitized_username = username.strip()[:100]
    
    client = get_supabase_rest()
    
    # Check if parent exists if parent_id provided
    if request.parent_id:
        parent = await client.get(
            "video_comments",
            filters={"id": f"eq.{request.parent_id}"},
            single=True
        )
        if not parent:
            raise HTTPException(status_code=400, detail="Parent comment not found")
    
    result = await client.insert(
        "video_comments",
        {
            "video_code": video_code,
            "user_id": user_id.strip(),
            "username": sanitized_username,
            "content": request.content.strip(),
            "parent_id": request.parent_id,
            "score": 0,
            "is_deleted": False,
            "created_at": datetime.utcnow().isoformat()
        },
        use_admin=True
    )
    
    if not result:
        raise HTTPException(status_code=500, detail="Failed to create comment")
    
    return {
        "id": result["id"],
        "content": result["content"],
        "user_id": result["user_id"],
        "username": result.get("username") or f"User_{user_id[:8]}",
        "created_at": result["created_at"],
        "score": 0,
        "parent_id": request.parent_id,
        "replies": [],
        "user_vote": 0
    }


@router.put("/{comment_id}")
async def update_comment(
    comment_id: int,
    request: UpdateCommentRequest,
    user_id: str = Query(..., description="User ID")
):
    """Update a comment (owner only)."""
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID is required")
    
    if not request.content or not request.content.strip():
        raise HTTPException(status_code=400, detail="Comment cannot be empty")
    
    client = get_supabase_rest()
    
    # Check ownership
    comment = await client.get(
        "video_comments",
        filters={"id": f"eq.{comment_id}"},
        single=True
    )
    
    if not comment:
        raise HTTPException(status_code=404, detail="Comment not found")
    
    if comment["user_id"] != user_id.strip():
        raise HTTPException(status_code=403, detail="Not authorized to edit this comment")
    
    result = await client.update(
        "video_comments",
        {
            "content": request.content.strip(),
            "updated_at": datetime.utcnow().isoformat()
        },
        filters={"id": f"eq.{comment_id}"},
        use_admin=True
    )
    
    return {
        "id": comment_id,
        "content": request.content.strip(),
        "updated_at": datetime.utcnow().isoformat()
    }


@router.delete("/{comment_id}")
async def delete_comment(
    comment_id: int,
    user_id: str = Query(..., description="User ID")
):
    """Delete a comment (owner only)."""
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID is required")
    
    client = get_supabase_rest()
    
    # Check ownership
    comment = await client.get(
        "video_comments",
        filters={"id": f"eq.{comment_id}"},
        single=True
    )
    
    if not comment:
        raise HTTPException(status_code=404, detail="Comment not found")
    
    if comment["user_id"] != user_id.strip():
        raise HTTPException(status_code=403, detail="Not authorized to delete this comment")
    
    # Soft delete
    await client.update(
        "video_comments",
        {"is_deleted": True, "content": "[deleted]"},
        filters={"id": f"eq.{comment_id}"},
        use_admin=True
    )
    
    return {"success": True}


@router.post("/{comment_id}/vote")
async def vote_comment(
    comment_id: int,
    vote: int = Query(..., ge=-1, le=1, description="Vote: 1 (up), -1 (down), 0 (remove)"),
    user_id: str = Query(..., description="User ID")
):
    """Vote on a comment."""
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID is required")
    
    client = get_supabase_rest()
    
    # Get comment
    comment = await client.get(
        "video_comments",
        filters={"id": f"eq.{comment_id}"},
        single=True
    )
    
    if not comment:
        raise HTTPException(status_code=404, detail="Comment not found")
    
    # Get existing vote
    existing_vote = await client.get(
        "comment_votes",
        filters={"comment_id": f"eq.{comment_id}", "user_id": f"eq.{user_id}"},
        single=True
    )
    
    old_vote = existing_vote["vote"] if existing_vote else 0
    score_change = vote - old_vote
    
    if vote == 0:
        # Remove vote
        if existing_vote:
            await client.delete(
                "comment_votes",
                filters={"comment_id": f"eq.{comment_id}", "user_id": f"eq.{user_id}"},
                use_admin=True
            )
    else:
        # Upsert vote
        await client.insert(
            "comment_votes",
            {
                "comment_id": comment_id,
                "user_id": user_id,
                "vote": vote
            },
            upsert=True,
            use_admin=True
        )
    
    # Update comment score
    new_score = (comment.get("score", 0) or 0) + score_change
    await client.update(
        "video_comments",
        {"score": new_score},
        filters={"id": f"eq.{comment_id}"},
        use_admin=True
    )
    
    return {"score": new_score, "user_vote": vote}
