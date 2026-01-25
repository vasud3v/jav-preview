"""File upload routes."""
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from app.core.supabase import get_supabase, get_supabase_admin
from app.core.config import settings
from app.core.auth import require_auth
import uuid

router = APIRouter(prefix="/upload", tags=["upload"])

MAX_SIZE = 5 * 1024 * 1024  # 5MB


def validate_file_content(content: bytes) -> tuple[str, str]:
    """
    Validate file content using magic numbers.
    Returns (content_type, extension) if valid.
    Raises ValueError if invalid.
    """
    if len(content) < 12:  # Minimum needed for WebP check
        raise ValueError("File too small")

    # JPEG: FF D8 FF
    if content.startswith(b'\xFF\xD8\xFF'):
        return "image/jpeg", "jpg"

    # PNG: 89 50 4E 47 0D 0A 1A 0A
    if content.startswith(b'\x89PNG\r\n\x1a\n'):
        return "image/png", "png"

    # GIF: GIF87a or GIF89a
    if content.startswith(b'GIF87a') or content.startswith(b'GIF89a'):
        return "image/gif", "gif"

    # WebP: RIFF....WEBP
    if content.startswith(b'RIFF') and content[8:12] == b'WEBP':
        return "image/webp", "webp"

    raise ValueError("Invalid file signature. Only JPEG, PNG, GIF, and WebP are allowed.")


def get_storage_client():
    """Get Supabase client for storage operations. Uses service key if available."""
    if settings.supabase_service_key:
        return get_supabase_admin()
    return get_supabase()


@router.post("/avatar")
async def upload_avatar(
    file: UploadFile = File(...),
    user: dict = Depends(require_auth)
):
    """Upload user avatar to Supabase storage."""
    
    # Read file content
    content = await file.read()
    
    # Validate file size
    if len(content) > MAX_SIZE:
        raise HTTPException(status_code=400, detail="File too large. Maximum size is 5MB.")

    # Validate file content and get real type
    try:
        content_type, ext = validate_file_content(content)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid file content. Must be a valid image (JPEG, PNG, GIF, WebP).")
    
    # Generate unique filename using validated extension
    filename = f"{user['id']}/{uuid.uuid4()}.{ext}"
    
    try:
        supabase = get_storage_client()
        
        # Delete old avatars for this user
        try:
            old_files = supabase.storage.from_("avatars").list(user["id"])
            if old_files:
                paths_to_delete = [f"{user['id']}/{f['name']}" for f in old_files]
                if paths_to_delete:
                    supabase.storage.from_("avatars").remove(paths_to_delete)
        except Exception as e:
            print(f"Error deleting old avatars: {e}")
        
        # Upload new avatar
        result = supabase.storage.from_("avatars").upload(
            filename,
            content,
            {"content-type": content_type}
        )
        
        # Get public URL
        public_url = supabase.storage.from_("avatars").get_public_url(filename)
        
        # Update user profile
        from datetime import datetime, timezone
        supabase.table("profiles").update({
            "avatar_url": public_url,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", user["id"]).execute()
        
        return {"avatar_url": public_url}
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        error_msg = str(e)
        if "row-level security" in error_msg.lower() or "rls" in error_msg.lower():
            raise HTTPException(status_code=500, detail="Storage permission denied. Please configure SUPABASE_SERVICE_KEY in backend/.env")
        raise HTTPException(status_code=500, detail=f"Upload failed: {error_msg}")


@router.delete("/avatar")
async def delete_avatar(user: dict = Depends(require_auth)):
    """Delete user avatar."""
    try:
        supabase = get_storage_client()
        
        # Delete all avatars for this user
        try:
            old_files = supabase.storage.from_("avatars").list(user["id"])
            if old_files:
                paths_to_delete = [f"{user['id']}/{f['name']}" for f in old_files]
                if paths_to_delete:
                    supabase.storage.from_("avatars").remove(paths_to_delete)
        except Exception as e:
            print(f"Error deleting avatars: {e}")
        
        # Clear avatar URL in profile
        from datetime import datetime, timezone
        supabase.table("profiles").update({
            "avatar_url": None,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", user["id"]).execute()
        
        return {"message": "Avatar deleted"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")
