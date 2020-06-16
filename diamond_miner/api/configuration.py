"""Configuration."""

from fastapi import APIRouter


router = APIRouter()


@router.get("/")
def get_configuration():
    return {}


@router.put("/")
def put_configuration():
    return {}
