from typing import Callable, TypeVar

from pydantic import parse_obj_as

T = TypeVar("T")


class APIResponseError(Exception):
    pass


def assert_response(response, expected):
    actual = cast_response(response, expected.__class__)
    assert actual == expected


def assert_status_code(response, expected):
    if response.status_code != expected:
        raise APIResponseError(response.content)


def cast_response(response, type_: Callable[..., T]) -> T:
    try:
        return parse_obj_as(type_, response.json())
    except Exception as e:
        raise APIResponseError(response.content) from e
