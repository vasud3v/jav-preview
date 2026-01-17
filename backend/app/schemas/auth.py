"""Auth schemas."""
from datetime import datetime
from typing import Optional, Union
from pydantic import BaseModel, EmailStr, Field


class SignUpRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=6)
    username: Optional[str] = Field(None, min_length=3, max_length=30)


class SignInRequest(BaseModel):
    email: EmailStr
    password: str


class AuthResponse(BaseModel):
    access_token: str
    refresh_token: str
    user: dict


class UserResponse(BaseModel):
    id: str
    email: str
    username: Optional[str] = None
    avatar_url: Optional[str] = None
    created_at: Optional[Union[str, datetime]] = None


class RefreshRequest(BaseModel):
    refresh_token: str


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    password: str = Field(..., min_length=6)


class UpdatePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=6)


class UpdateProfileRequest(BaseModel):
    username: Optional[str] = Field(None, max_length=30)
    avatar_url: Optional[str] = None


class MessageResponse(BaseModel):
    message: str
