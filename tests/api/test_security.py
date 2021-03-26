from iris.api.security import verify_password


def test_verify_password():
    """Test the hash verification"""

    assert verify_password(
        "test", "$2y$12$seiW.kzNc9NFRlpQpyeKie.PUJGhAtxn6oGPB.XfgnmTKx8Y9XCve"
    )
