import sys

from gunicorn.app.wsgiapp import run

if __name__ == "__main__":
    # Equivalent to `python -m gunicorn --worker-class iris.api.uvicorn iris.api.main:app`.
    sys.argv.extend(
        ["--worker-class", "iris.api.uvicorn.Worker", "iris.api.main:make_app"]
    )
    run()
