"""Profile operations."""

from datetime import timedelta
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel

from iris.api.schemas import (
    ProfileGetResponse,
    ProfileRIPEPutBody,
    ProfileRIPEPutResponse,
)
from iris.api.security import (
    authenticate_user,
    create_access_token,
    get_current_active_user,
)
from iris.commons.database import Users

router = APIRouter()


class Token(BaseModel):
    access_token: str
    token_type: str


@router.post(
    "/token",
    response_model=Token,
    summary="Get JWT token.",
)
async def get_token(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends(),
):
    user = await authenticate_user(request, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(
        days=request.app.settings.API_TOKEN_EXPIRATION_TIME
    )
    access_token = create_access_token(
        request, data={"sub": user["username"]}, expires_delta=access_token_expires
    )

    return {"access_token": access_token, "token_type": "bearer"}


@router.get(
    "/",
    response_model=ProfileGetResponse,
    summary="Get profile information.",
)
async def get_profile(
    request: Request,
    user: Dict = Depends(get_current_active_user),
):
    """Get profile information."""
    user["ripe"] = {
        "account": user["ripe_account"],
        "key": user["ripe_key"],
    }
    return user


@router.put(
    "/ripe",
    status_code=status.HTTP_201_CREATED,
    response_model=ProfileRIPEPutResponse,
    summary="Put RIPE profile information.",
)
async def put_ripe_profile(
    request: Request,
    ripe_info: ProfileRIPEPutBody,
    user: Dict = Depends(get_current_active_user),
):
    users_database = Users(request.app.settings, request.app.logger)

    if not isinstance(ripe_info.account, type(ripe_info.key)):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="`account` and `key` must have the same type",
        )

    await users_database.register_ripe(
        user["username"], ripe_info.account, ripe_info.key
    )
    return {"account": ripe_info.account, "key": ripe_info.key}
