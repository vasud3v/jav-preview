"""Comment service - Reddit-style threaded comments with voting."""
from datetime import datetime
from typing import List, Optional, Dict
from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from app.models import Comment, CommentVote, Video


def get_comments(
    db: Session,
    video_code: str,
    user_id: Optional[str] = None,
    sort_by: str = "best"  # best, new, old, controversial
) -> List[dict]:
    """Get all comments for a video as a threaded tree."""
    # Get all comments for this video
    comments = db.query(Comment).filter(
        Comment.video_code == video_code
    ).all()
    
    if not comments:
        return []
    
    comment_ids = [c.id for c in comments]
    
    # Get vote counts for all comments
    vote_counts = db.query(
        CommentVote.comment_id,
        func.sum(CommentVote.vote).label('score'),
        func.count(CommentVote.id).label('vote_count')
    ).filter(
        CommentVote.comment_id.in_(comment_ids)
    ).group_by(CommentVote.comment_id).all()
    
    vote_map = {v.comment_id: {'score': int(v.score or 0), 'vote_count': v.vote_count} for v in vote_counts}
    
    # Get user's votes if logged in
    user_votes = {}
    if user_id:
        votes = db.query(CommentVote).filter(
            CommentVote.comment_id.in_(comment_ids),
            CommentVote.user_id == user_id
        ).all()
        user_votes = {v.comment_id: v.vote for v in votes}
    
    # Build comment dict
    comment_dict = {}
    for c in comments:
        votes = vote_map.get(c.id, {'score': 0, 'vote_count': 0})
        is_deleted = bool(c.is_deleted)
        comment_dict[c.id] = {
            'id': c.id,
            'video_code': c.video_code,
            'user_id': '' if is_deleted else c.user_id,  # Hide user_id for deleted comments
            'username': '[deleted]' if is_deleted else (c.username or 'Anonymous'),
            'parent_id': c.parent_id,
            'content': '[deleted]' if is_deleted else c.content,
            'created_at': c.created_at.isoformat(),
            'updated_at': c.updated_at.isoformat() if c.updated_at else None,
            'is_deleted': is_deleted,
            'score': votes['score'],
            'vote_count': votes['vote_count'],
            'user_vote': user_votes.get(c.id, 0),
            'replies': []
        }
    
    # Build tree structure
    root_comments = []
    for comment_id, comment in comment_dict.items():
        parent_id = comment['parent_id']
        if parent_id and parent_id in comment_dict:
            comment_dict[parent_id]['replies'].append(comment)
        else:
            root_comments.append(comment)
    
    # Sort function
    def sort_comments(comments_list: List[dict]) -> List[dict]:
        if sort_by == "new":
            comments_list.sort(key=lambda x: x['created_at'], reverse=True)
        elif sort_by == "old":
            comments_list.sort(key=lambda x: x['created_at'])
        elif sort_by == "controversial":
            # High vote count but low score
            comments_list.sort(key=lambda x: (x['vote_count'], -abs(x['score'])), reverse=True)
        else:  # best (default) - Wilson score approximation
            comments_list.sort(key=lambda x: _wilson_score(x['score'], x['vote_count']), reverse=True)
        
        # Recursively sort replies
        for comment in comments_list:
            if comment['replies']:
                comment['replies'] = sort_comments(comment['replies'])
        
        return comments_list
    
    return sort_comments(root_comments)


def _wilson_score(score: int, n: int) -> float:
    """Wilson score for ranking (Reddit's algorithm)."""
    if n == 0:
        return 0
    
    # Convert score to positive/negative
    # Assuming score = upvotes - downvotes, n = total votes
    # p = (score + n) / (2 * n) gives us the positive ratio
    z = 1.96  # 95% confidence
    p = (score + n) / (2 * n)
    
    # Clamp p to valid range [0, 1]
    p = max(0, min(1, p))
    
    try:
        left = p + z * z / (2 * n)
        right = z * ((p * (1 - p) + z * z / (4 * n)) / n) ** 0.5
        under = 1 + z * z / n
        return (left - right) / under
    except (ZeroDivisionError, ValueError):
        return 0


def create_comment(
    db: Session,
    video_code: str,
    user_id: str,
    content: str,
    username: Optional[str] = None,
    parent_id: Optional[int] = None
) -> dict:
    """Create a new comment or reply."""
    # Validate user_id
    if not user_id or not user_id.strip():
        raise ValueError("User ID is required")
    user_id = user_id.strip()
    
    # Validate video_code
    if not video_code or not video_code.strip():
        raise ValueError("Video code is required")
    video_code = video_code.strip()
    
    # Validate and sanitize content
    content = content.strip() if content else ""
    if not content:
        raise ValueError("Comment cannot be empty")
    
    if len(content) > 10000:
        raise ValueError("Comment too long (max 10000 characters)")
    
    # Sanitize username
    if username:
        username = username.strip()[:100] if username.strip() else None
    
    # Verify video exists
    video = db.query(Video).filter(Video.code == video_code).first()
    if not video:
        raise ValueError("Video not found")
    
    # Verify parent exists if replying
    if parent_id:
        parent = db.query(Comment).filter(Comment.id == parent_id).first()
        if not parent:
            raise ValueError("Parent comment not found")
        if parent.video_code != video_code:
            raise ValueError("Parent comment is from different video")
        if parent.is_deleted:
            raise ValueError("Cannot reply to deleted comment")
        
        # Check reply depth (max 6 levels)
        depth = 0
        current = parent
        while current.parent_id and depth < 10:  # Safety limit
            current = db.query(Comment).filter(Comment.id == current.parent_id).first()
            if not current:
                break
            depth += 1
        if depth >= 6:
            raise ValueError("Maximum reply depth reached")
    
    # Create comment
    comment = Comment(
        video_code=video_code,
        user_id=user_id,
        username=username,
        parent_id=parent_id,
        content=content
    )
    db.add(comment)
    db.commit()
    db.refresh(comment)
    
    return {
        'id': comment.id,
        'video_code': comment.video_code,
        'user_id': comment.user_id,
        'username': comment.username or 'Anonymous',
        'parent_id': comment.parent_id,
        'content': comment.content,
        'created_at': comment.created_at.isoformat(),
        'updated_at': None,
        'is_deleted': False,
        'score': 0,
        'vote_count': 0,
        'user_vote': 0,
        'replies': []
    }


def update_comment(
    db: Session,
    comment_id: int,
    user_id: str,
    content: str
) -> dict:
    """Update a comment (only by owner)."""
    # Validate user_id
    if not user_id or not user_id.strip():
        raise ValueError("User ID is required")
    user_id = user_id.strip()
    
    # Validate content
    content = content.strip() if content else ""
    if not content:
        raise ValueError("Comment cannot be empty")
    
    if len(content) > 10000:
        raise ValueError("Comment too long (max 10000 characters)")
    
    comment = db.query(Comment).filter(Comment.id == comment_id).first()
    if not comment:
        raise ValueError("Comment not found")
    
    # Strict ownership check
    if not comment.user_id or comment.user_id.strip() != user_id:
        raise ValueError("Not authorized to edit this comment")
    
    if comment.is_deleted:
        raise ValueError("Cannot edit deleted comment")
    
    comment.content = content
    comment.updated_at = datetime.utcnow()
    db.commit()
    
    # Get vote info
    vote_info = db.query(
        func.sum(CommentVote.vote).label('score'),
        func.count(CommentVote.id).label('vote_count')
    ).filter(CommentVote.comment_id == comment_id).first()
    
    return {
        'id': comment.id,
        'video_code': comment.video_code,
        'user_id': comment.user_id,
        'username': comment.username or 'Anonymous',
        'parent_id': comment.parent_id,
        'content': comment.content,
        'created_at': comment.created_at.isoformat(),
        'updated_at': comment.updated_at.isoformat(),
        'is_deleted': False,
        'score': int(vote_info.score or 0) if vote_info else 0,
        'vote_count': vote_info.vote_count if vote_info else 0,
        'user_vote': 0,
        'replies': []
    }


def delete_comment(db: Session, comment_id: int, user_id: str) -> bool:
    """Soft delete a comment (preserves thread structure)."""
    # Validate user_id
    if not user_id or not user_id.strip():
        raise ValueError("User ID is required")
    user_id = user_id.strip()
    
    comment = db.query(Comment).filter(Comment.id == comment_id).first()
    if not comment:
        return False
    
    # Strict ownership check
    if not comment.user_id or comment.user_id.strip() != user_id:
        raise ValueError("Not authorized to delete this comment")
    
    # Check if has replies - soft delete to preserve thread
    has_replies = db.query(Comment).filter(Comment.parent_id == comment_id).first()
    
    if has_replies:
        comment.is_deleted = 1
        comment.content = ""
        db.commit()
    else:
        # No replies, can hard delete
        # Also delete votes
        db.query(CommentVote).filter(CommentVote.comment_id == comment_id).delete()
        db.delete(comment)
        db.commit()
    
    return True


def vote_comment(
    db: Session,
    comment_id: int,
    user_id: str,
    vote: int  # 1, -1, or 0 (remove vote)
) -> dict:
    """Vote on a comment."""
    # Validate user_id
    if not user_id or not user_id.strip():
        raise ValueError("User ID is required")
    user_id = user_id.strip()
    
    if vote not in [-1, 0, 1]:
        raise ValueError("Vote must be -1, 0, or 1")
    
    comment = db.query(Comment).filter(Comment.id == comment_id).first()
    if not comment:
        raise ValueError("Comment not found")
    
    # Don't allow voting on deleted comments
    if comment.is_deleted:
        raise ValueError("Cannot vote on deleted comment")
    
    # Don't allow voting on own comments
    if comment.user_id and comment.user_id.strip() == user_id:
        raise ValueError("Cannot vote on your own comment")
    
    existing = db.query(CommentVote).filter(
        CommentVote.comment_id == comment_id,
        CommentVote.user_id == user_id
    ).first()
    
    if vote == 0:
        # Remove vote
        if existing:
            db.delete(existing)
            db.commit()
    elif existing:
        # Update vote
        existing.vote = vote
        db.commit()
    else:
        # New vote
        new_vote = CommentVote(
            comment_id=comment_id,
            user_id=user_id,
            vote=vote
        )
        db.add(new_vote)
        db.commit()
    
    # Get updated score
    vote_info = db.query(
        func.sum(CommentVote.vote).label('score'),
        func.count(CommentVote.id).label('vote_count')
    ).filter(CommentVote.comment_id == comment_id).first()
    
    return {
        'comment_id': comment_id,
        'score': int(vote_info.score or 0) if vote_info else 0,
        'vote_count': vote_info.vote_count if vote_info else 0,
        'user_vote': vote
    }


def get_comment_count(db: Session, video_code: str) -> int:
    """Get total comment count for a video."""
    return db.query(func.count(Comment.id)).filter(
        Comment.video_code == video_code,
        Comment.is_deleted == 0
    ).scalar() or 0
