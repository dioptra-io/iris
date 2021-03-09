"""Profile operations."""

from fastapi import APIRouter, Depends, HTTPException, Request, status

from iris.api.schemas import (
    ProfileGetResponse,
    ProfileRIPEPutBody,
    ProfileRIPEPutResponse,
)
from iris.api.security import authenticate
from iris.commons.database import DatabaseUsers, get_session

router = APIRouter()


@router.get(
    "/",
    response_model=ProfileGetResponse,
    summary="Get profile information",
)
async def get_profile(
    request: Request,
    username: str = Depends(authenticate),
):
    """Get profile information."""
    session = get_session(request.app.settings)
    users_database = DatabaseUsers(session, request.app.settings, request.app.logger)

    profile_info = await users_database.get(username)
    profile_info["ripe"] = {
        "account": profile_info["ripe_account"],
        "key": profile_info["ripe_key"],
    }
    return profile_info


@router.put(
    "/ripe",
    status_code=status.HTTP_201_CREATED,
    response_model=ProfileRIPEPutResponse,
    summary="Put RIPE profile information",
)
async def put_ripe_profile(
    request: Request,
    ripe_info: ProfileRIPEPutBody,
    username: str = Depends(authenticate),
):
    session = get_session(request.app.settings)
    users_database = DatabaseUsers(session, request.app.settings, request.app.logger)

    if not isinstance(ripe_info.account, type(ripe_info.key)):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="`account` and `key` must have the same type",
        )

    await users_database.register_ripe(username, ripe_info.account, ripe_info.key)
    return {"account": ripe_info.account, "key": ripe_info.key}
