"""Authentication operations.."""

from fastapi import APIRouter, HTTPException

router = APIRouter()


@router.post("/", summary="Authenticate to the API")
def post_authentication():
    """Authenticate to the API."""
    raise HTTPException(501, detail="Not implemented")
