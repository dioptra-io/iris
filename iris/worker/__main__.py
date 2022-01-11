import sys

from dramatiq.cli import main

if __name__ == "__main__":
    # Equivalent to `python -m dramatiq iris.worker.watch`.
    sys.argv.append("iris.worker.watch")
    sys.exit(main())
