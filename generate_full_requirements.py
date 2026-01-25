import argparse
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parent
REQ_DIR = ROOT / "requirements"
FULL_PATH = REQ_DIR / "full.txt"


def run_resolver(action: str):
    result = subprocess.run(
        [sys.executable, str(ROOT / "resolve_deps.py"), "--action", action],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    outputs = {}
    for line in result.stdout.strip().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            outputs[k.strip()] = v.strip()
    packages = set()
    if outputs.get("use_full") == "true":
        return packages, True
    if "packages" in outputs and outputs["packages"]:
        packages.update(outputs["packages"].split())
    return packages, False


def main():
    parser = argparse.ArgumentParser(description="Generate requirements/full.txt from resolver.")
    parser.add_argument("--write", action="store_true", help="Write full.txt (default).")
    parser.add_argument("--check", action="store_true", help="Check full.txt is up-to-date.")
    parser.add_argument("--render", action="store_true", help="Print generated full.txt to stdout.")
    args = parser.parse_args()

    if sum(bool(x) for x in (args.check, args.write, args.render)) > 1:
        print("Choose only one of --check, --write, or --render.")
        return 2

    actions = ["refresh", "midday", "eod"]
    all_packages = set()
    for action in actions:
        packages, used_full = run_resolver(action)
        if used_full:
            print(f"Resolver fell back to full for action={action}; cannot generate.")
            return 1
        all_packages.update(packages)

    content = "\n".join(sorted(all_packages)) + "\n"

    if args.render:
        print(content, end="")
        return 0

    if args.check:
        if not FULL_PATH.exists():
            print("requirements/full.txt is missing.")
            return 1
        existing = FULL_PATH.read_text(encoding="utf-8")
        if existing.strip() != content.strip():
            print("requirements/full.txt is out of date.")
            return 1
        print("requirements/full.txt is up to date.")
        return 0

    FULL_PATH.write_text(content, encoding="utf-8")
    print(f"Wrote {FULL_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
