"""Auth routes using Supabase."""
from fastapi import APIRouter, HTTPException, Depends
from app.core.supabase import get_supabase
from app.core.auth import require_auth, get_current_user
from app.schemas.auth import (
    SignUpRequest, SignInRequest, AuthResponse, UserResponse, RefreshRequest,
    ForgotPasswordRequest, ResetPasswordRequest, UpdatePasswordRequest,
    UpdateProfileRequest, MessageResponse
)

router = APIRouter(prefix="/auth", tags=["auth"])


def get_user_profile(supabase, user_id: str) -> dict:
    """Fetch user profile from profiles table."""
    try:
        result = supabase.table("profiles").select("*").eq("id", user_id).execute()
        return result.data[0] if result.data else {}
    except:
        return {}


def build_user_response(user, profile: dict = None) -> dict:
    """Build user response with profile data."""
    return {
        "id": user.id if hasattr(user, 'id') else user.get("id"),
        "email": user.email if hasattr(user, 'email') else user.get("email"),
        "username": profile.get("username") if profile else None,
        "avatar_url": profile.get("avatar_url") if profile else None,
        "created_at": str(user.created_at) if hasattr(user, 'created_at') and user.created_at else user.get("created_at"),
    }


@router.post("/signup", response_model=MessageResponse)
async def sign_up(request: SignUpRequest):
    """Register a new user."""
    try:
        supabase = get_supabase()
        
        # Check if username is taken
        if request.username:
            existing = supabase.table("profiles").select("id").eq("username", request.username).execute()
            if existing.data:
                raise HTTPException(status_code=400, detail="Username already taken")
        
        response = supabase.auth.sign_up({
            "email": request.email,
            "password": request.password,
        })

        if not response.user:
            raise HTTPException(status_code=400, detail="Sign up failed")

        # Update username if provided
        if request.username:
            supabase.table("profiles").update({
                "username": request.username
            }).eq("id", response.user.id).execute()

        # Sign out immediately so user has to log in manually
        supabase.auth.sign_out()

        return MessageResponse(message="Account created successfully. Please sign in.")
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        if "already registered" in error_msg.lower():
            raise HTTPException(status_code=400, detail="Email already registered")
        raise HTTPException(status_code=400, detail=error_msg)


@router.post("/signin", response_model=AuthResponse)
async def sign_in(request: SignInRequest):
    """Sign in with email and password."""
    try:
        supabase = get_supabase()
        response = supabase.auth.sign_in_with_password({
            "email": request.email,
            "password": request.password,
        })

        if not response.session or not response.user:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        profile = get_user_profile(supabase, response.user.id)

        return AuthResponse(
            access_token=response.session.access_token,
            refresh_token=response.session.refresh_token,
            user=build_user_response(response.user, profile),
        )
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e).lower()
        if "invalid" in error_msg or "credentials" in error_msg:
            raise HTTPException(status_code=401, detail="Invalid email or password")
        if "not confirmed" in error_msg:
            raise HTTPException(status_code=401, detail="Please confirm your email first")
        raise HTTPException(status_code=401, detail="Invalid credentials")


@router.post("/refresh", response_model=AuthResponse)
async def refresh_token(request: RefreshRequest):
    """Refresh access token."""
    try:
        supabase = get_supabase()
        response = supabase.auth.refresh_session(request.refresh_token)

        if not response.session or not response.user:
            raise HTTPException(status_code=401, detail="Invalid refresh token")

        profile = get_user_profile(supabase, response.user.id)

        return AuthResponse(
            access_token=response.session.access_token,
            refresh_token=response.session.refresh_token,
            user=build_user_response(response.user, profile),
        )
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid refresh token")


@router.post("/signout", response_model=MessageResponse)
async def sign_out(user: dict = Depends(require_auth)):
    """Sign out current user."""
    try:
        supabase = get_supabase()
        supabase.auth.sign_out()
    except:
        pass
    return MessageResponse(message="Signed out successfully")


@router.get("/me", response_model=UserResponse)
async def get_me(user: dict = Depends(require_auth)):
    """Get current user info with profile."""
    supabase = get_supabase()
    profile = get_user_profile(supabase, user["id"])
    
    return UserResponse(
        id=user["id"],
        email=user.get("email", ""),
        username=profile.get("username"),
        avatar_url=profile.get("avatar_url"),
        created_at=user.get("created_at"),
    )


@router.post("/forgot-password", response_model=MessageResponse)
async def forgot_password(request: ForgotPasswordRequest):
    """Send password reset email."""
    try:
        supabase = get_supabase()
        supabase.auth.reset_password_email(request.email)
        return MessageResponse(message="If an account exists, a reset link has been sent")
    except Exception:
        # Don't reveal if email exists
        return MessageResponse(message="If an account exists, a reset link has been sent")


@router.post("/reset-password", response_model=MessageResponse)
async def reset_password(request: ResetPasswordRequest, user: dict = Depends(require_auth)):
    """Reset password with token (user must be authenticated via reset link)."""
    try:
        supabase = get_supabase()
        supabase.auth.update_user({"password": request.password})
        return MessageResponse(message="Password updated successfully")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/update-password", response_model=MessageResponse)
async def update_password(request: UpdatePasswordRequest, user: dict = Depends(require_auth)):
    """Update password for authenticated user."""
    try:
        supabase = get_supabase()
        
        # Verify current password by attempting sign in
        try:
            supabase.auth.sign_in_with_password({
                "email": user["email"],
                "password": request.current_password,
            })
        except:
            raise HTTPException(status_code=400, detail="Current password is incorrect")
        
        # Update to new password
        supabase.auth.update_user({"password": request.new_password})
        return MessageResponse(message="Password updated successfully")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/profile", response_model=UserResponse)
async def update_profile(request: UpdateProfileRequest, user: dict = Depends(require_auth)):
    """Update user profile."""
    try:
        supabase = get_supabase()
        
        update_data = {}
        if request.username is not None:
            # Validate username length if not empty
            if request.username and len(request.username) < 3:
                raise HTTPException(status_code=400, detail="Username must be at least 3 characters")
            
            # Check if username is taken by another user (only if not empty)
            if request.username:
                existing = supabase.table("profiles").select("id").eq("username", request.username).neq("id", user["id"]).execute()
                if existing.data:
                    raise HTTPException(status_code=400, detail="Username already taken")
            update_data["username"] = request.username if request.username else None
        
        if request.avatar_url is not None:
            update_data["avatar_url"] = request.avatar_url
        
        if update_data:
            from datetime import datetime, timezone
            update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
            supabase.table("profiles").update(update_data).eq("id", user["id"]).execute()
        
        profile = get_user_profile(supabase, user["id"])
        
        return UserResponse(
            id=user["id"],
            email=user.get("email", ""),
            username=profile.get("username"),
            avatar_url=profile.get("avatar_url"),
            created_at=user.get("created_at"),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/account", response_model=MessageResponse)
async def delete_account(user: dict = Depends(require_auth)):
    """Delete user account."""
    try:
        from app.core.supabase import get_supabase_admin
        admin = get_supabase_admin()
        admin.auth.admin.delete_user(user["id"])
        return MessageResponse(message="Account deleted successfully")
    except Exception as e:
        raise HTTPException(status_code=400, detail="Failed to delete account")
