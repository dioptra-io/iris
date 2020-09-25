"""Authentication and security handlers."""


from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from iris.commons.database import get_session, DatabaseUsers
from iris.api.settings import APISettings
from passlib.context import CryptContext

settings = APISettings()
security = HTTPBasic()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


async def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    session = get_session()
    database = DatabaseUsers(session)
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
