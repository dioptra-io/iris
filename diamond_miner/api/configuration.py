"""Configuration."""

from fastapi import APIRouter, HTTPException


router = APIRouter()


@router.get("/", summary="Get the configuration")
def get_configuration():
    """Get the configuration."""
    raise HTTPException(501, detail="Not implemented")


@router.put("/", summary="Change the configuration")
def put_configuration():
    """Change the configuration."""
    raise HTTPException(501, detail="Not implemented")
