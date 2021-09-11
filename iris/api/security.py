"""Authentication and security handlers."""

from datetime import datetime, timedelta
from typing import Optional

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext

from iris.api.dependencies import get_database, settings
from iris.commons.database import Database, Users
from iris.commons.schemas import public

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
        settings.API_TOKEN_SECRET_KEY,
        algorithm=settings.API_TOKEN_ALGORITHM,
    )
    return encoded_jwt


async def get_user(
    request: Request, database: Database, username: str
) -> Optional[public.Profile]:
    return await Users(database).get(username)


async def authenticate_user(
    request: Request, database: Database, username: str, password: str
) -> Optional[public.Profile]:
    user = await get_user(request, database, username)
    if not user:
        return None
    if not user.is_active:
        return None
    if not verify_password(password, user._hashed_password):
        return None
    return user


async def get_current_user(
    request: Request,
    database: Database = Depends(get_database),
    token: str = Depends(oauth2_scheme),
) -> public.Profile:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(
            token,
            settings.API_TOKEN_SECRET_KEY,
            algorithms=[settings.API_TOKEN_ALGORITHM],
        )
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    user = await get_user(request, database, username=username)
    if not user:
        raise credentials_exception
    return user


async def get_current_active_user(
    current_user: public.Profile = Depends(get_current_user),
):
    if not current_user.is_active:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
