"""Authentication and security handlers."""


from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from passlib.context import CryptContext

from iris.commons.database import DatabaseUsers, get_session

security = HTTPBasic()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


async def authenticate(
    request: Request, credentials: HTTPBasicCredentials = Depends(security)
):
    session = get_session(request.app.settings)
    database = DatabaseUsers(session, request.app.settings, request.app.logger)
    user = await database.get(credentials.username)

    is_verified = False
    if user is not None:
        is_verified = pwd_context.verify(credentials.password, user["hashed_password"])

    if not is_verified:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username
