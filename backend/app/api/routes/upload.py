"""File upload routes."""
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from backend.app.core.supabase import get_supabase, get_supabase_admin
from backend.app.core.config import settings
from backend.app.core.auth import require_auth
import uuid

router = APIRouter(prefix="/upload", tags=["upload"])

ALLOWED_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}
MAX_SIZE = 5 * 1024 * 1024  # 5MB


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
    
    # Validate file type
    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(status_code=400, detail="Invalid file type. Use JPEG, PNG, GIF, or WebP.")
    
    # Read file content
    content = await file.read()
    
    # Validate file size
    if len(content) > MAX_SIZE:
        raise HTTPException(status_code=400, detail="File too large. Maximum size is 5MB.")
    
    # Generate unique filename
    ext = file.filename.split(".")[-1] if "." in file.filename else "jpg"
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
            {"content-type": file.content_type}
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
