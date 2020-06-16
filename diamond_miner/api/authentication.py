"""Authentication operations.."""

from fastapi import APIRouter

router = APIRouter()


@router.post("/")
def post_authentication():
    return {}
