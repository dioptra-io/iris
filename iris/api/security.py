"""Authentication and security handlers."""

import warnings
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer

with warnings.catch_warnings():
    # NOTE: https://github.com/mpdavis/python-jose/issues/208
    warnings.filterwarnings("ignore", message="int_from_bytes is deprecated")
    from jose import JWTError, jwt

from passlib.context import CryptContext

from iris.commons.database import DatabaseUsers, get_session

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="profile/token")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(
    request: Request, data: dict, expires_delta: Optional[timedelta] = None
):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(
        to_encode,
        request.app.settings.API_TOKEN_SECRET_KEY,
        algorithm=request.app.settings.API_TOKEN_ALGORITHM,
    )
    return encoded_jwt


async def get_user(request: Request, username: str):
    session = get_session(request.app.settings)
    database = DatabaseUsers(session, request.app.settings, request.app.logger)
    return await database.get(username)


async def authenticate_user(request: Request, username: str, password: str):
    user = await get_user(request, username)

    if not user:
        return False
    if not user["is_active"]:
        return False
    if not verify_password(password, user["hashed_password"]):
        return False

    return user


async def get_current_user(request: Request, token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(
            token,
            request.app.settings.API_TOKEN_SECRET_KEY,
            algorithms=[request.app.settings.API_TOKEN_ALGORITHM],
        )
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = await get_user(request, username=username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(current_user=Depends(get_current_user)):
    if not current_user["is_active"]:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
