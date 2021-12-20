import sys

from gunicorn.app.wsgiapp import run

if __name__ == "__main__":
    # Equivalent to `python -m gunicorn --worker-class uvicorn.workers.UvicornWorker iris.api.main:app`.
    sys.argv.extend(
        ["--worker-class", "uvicorn.workers.UvicornWorker", "iris.api.main:app"]
    )
    run()
