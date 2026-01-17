"""Comment routes - Reddit-style threaded comments."""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session

from backend.app.api.deps import get_db
from backend.app.services import comment_service

router = APIRouter(prefix="/comments", tags=["comments"])


class CreateCommentRequest(BaseModel):
    content: str
    parent_id: Optional[int] = None


class UpdateCommentRequest(BaseModel):
    content: str


@router.get("/{video_code}")
def get_comments(
    video_code: str,
    user_id: str = Query(None, description="User ID for vote status"),
    sort: str = Query("best", description="Sort: best, new, old, controversial"),
    db: Session = Depends(get_db)
):
    """Get threaded comments for a video."""
    comments = comment_service.get_comments(db, video_code, user_id, sort)
    count = comment_service.get_comment_count(db, video_code)
    return {"comments": comments, "count": count}


@router.post("/{video_code}")
def create_comment(
    video_code: str,
    request: CreateCommentRequest,
    user_id: str = Query(..., description="User ID"),
    username: str = Query(None, description="Display name"),
    db: Session = Depends(get_db)
):
    """Create a new comment or reply."""
    # Validate user_id
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID is required")
    
    if not request.content or not request.content.strip():
        raise HTTPException(status_code=400, detail="Comment cannot be empty")
    
    if len(request.content) > 10000:
        raise HTTPException(status_code=400, detail="Comment too long (max 10000 chars)")
    
    # Sanitize username - prevent impersonation
    sanitized_username = None
    if username and username.strip():
        sanitized_username = username.strip()[:100]  # Limit username length
    
    try:
        return comment_service.create_comment(
            db, video_code, user_id.strip(), request.content, sanitized_username, request.parent_id
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{comment_id}")
def update_comment(
    comment_id: int,
    request: UpdateCommentRequest,
    user_id: str = Query(..., description="User ID"),
    db: Session = Depends(get_db)
):
    """Update a comment (owner only)."""
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID is required")
    
    if not request.content or not request.content.strip():
        raise HTTPException(status_code=400, detail="Comment cannot be empty")
    
    try:
        return comment_service.update_comment(db, comment_id, user_id.strip(), request.content)
    except ValueError as e:
        error_msg = str(e)
        if "Not authorized" in error_msg:
            raise HTTPException(status_code=403, detail=error_msg)
        raise HTTPException(status_code=400, detail=error_msg)


@router.delete("/{comment_id}")
def delete_comment(
    comment_id: int,
    user_id: str = Query(..., description="User ID"),
    db: Session = Depends(get_db)
):
    """Delete a comment (owner only)."""
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID is required")
    
    try:
        success = comment_service.delete_comment(db, comment_id, user_id.strip())
        if not success:
            raise HTTPException(status_code=404, detail="Comment not found")
        return {"success": True}
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))


@router.post("/{comment_id}/vote")
def vote_comment(
    comment_id: int,
    vote: int = Query(..., ge=-1, le=1, description="Vote: 1 (up), -1 (down), 0 (remove)"),
    user_id: str = Query(..., description="User ID"),
    db: Session = Depends(get_db)
):
    """Vote on a comment."""
    # Validate user_id
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=400, detail="User ID is required")
    
    try:
        return comment_service.vote_comment(db, comment_id, user_id.strip(), vote)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
