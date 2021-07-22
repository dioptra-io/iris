from pydantic import BaseModel


class GenericException(BaseModel):
    """Generic exception (Response)."""

    detail: str
