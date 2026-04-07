"""Thin wrapper to dispatch watchdog entrypoints as spark_python_task.

Usage (from DABs job):
    spark_python_task:
      python_file: ../src/run_task.py
      parameters: [crawl, --catalog, main, --schema, watchdog, ...]

First positional arg selects the entrypoint; remaining args are forwarded.
"""

import os
import sys

# Add the src/ directory to sys.path so `import watchdog.*` works.
# In serverless environments __file__ is not defined, so we fall back
# to scanning sys.path for the deployed bundle files location.
try:
    src_dir = str(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    # Serverless: find the bundle files path from sys.path or CWD
    src_dir = None
    for p in sys.path:
        if "/files/src" in p or p.endswith("/src"):
            src_dir = p
            break
    if src_dir is None:
        # Last resort: look relative to CWD
        cwd = os.getcwd()
        candidate = os.path.join(cwd, "src")
        src_dir = candidate if os.path.isdir(candidate) else cwd

if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

ENTRYPOINTS = {
    "crawl":    "watchdog.entrypoints:crawl",
    "evaluate": "watchdog.entrypoints:evaluate",
    "notify":   "watchdog.entrypoints:notify",
    "adhoc":    "watchdog.entrypoints:adhoc",
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ENTRYPOINTS:
        print(f"Usage: run_task.py <{'|'.join(ENTRYPOINTS)}> [args...]")
        sys.exit(1)

    command = sys.argv[1]
    # Remove the command from argv so argparse in entrypoints sees only its args
    sys.argv = [sys.argv[0]] + sys.argv[2:]

    module_path, func_name = ENTRYPOINTS[command].split(":")
    module = __import__(module_path, fromlist=[func_name])
    fn = getattr(module, func_name)
    fn()


if __name__ == "__main__":
    main()
