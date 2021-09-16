"""Profile operations."""

from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel

from iris.api.dependencies import get_database, settings
from iris.api.security import (
    authenticate_user,
    create_access_token,
    get_current_active_user,
)
from iris.commons.database import Database
from iris.commons.schemas import public

router = APIRouter()


class Token(BaseModel):
    access_token: str
    token_type: str


@router.post(
    "/token",
    response_model=Token,
    summary="Authenticate and get a JWT token.",
)
async def get_token(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
    database: Database = Depends(get_database),
):
    user = await authenticate_user(
        request, database, form_data.username, form_data.password
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(days=settings.API_TOKEN_EXPIRATION_TIME)
    access_token = create_access_token(
        request, data={"sub": user.username}, expires_delta=access_token_expires
    )

    return {"access_token": access_token, "token_type": "bearer"}


@router.get(
    "/",
    response_model=public.Profile,
    summary="Get current user profile.",
)
async def get_profile(
    request: Request,
    user: public.Profile = Depends(get_current_active_user),
):
    return user
