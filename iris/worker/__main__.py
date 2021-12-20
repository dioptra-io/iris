import sys

from dramatiq.cli import main

if __name__ == "__main__":
    # Equivalent to `python -m dramatiq iris.worker.hook`.
    sys.argv.append("iris.worker.hook")
    sys.exit(main())
