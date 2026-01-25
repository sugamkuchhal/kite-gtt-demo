import argparse
import json
import os
import shlex
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REQ_DIR = ROOT / "requirements"
SCRIPT_DEPS_PATH = REQ_DIR / "script_deps.json"
ACTION_SCRIPTS_PATH = REQ_DIR / "action_scripts.json"


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def parse_script_name(command: str) -> str:
    if not command:
        return ""
    parts = shlex.split(command)
    if not parts:
        return ""
    first = parts[0]
    if first in ("python", "python3"):
        if len(parts) < 2:
            return ""
        first = parts[1]
    name = os.path.basename(first)
    if name.endswith(".py"):
        name = name[:-3]
    return name


def main():
    parser = argparse.ArgumentParser(description="Resolve dependency groups for an action/script.")
    parser.add_argument("--action", required=True, help="Action: script|refresh|midday|eod")
    parser.add_argument("--command", default="", help="Command for script mode (may include args)")
    parser.add_argument("--need-login", default="false", help="true if login is required")
    args = parser.parse_args()

    script_deps = load_json(SCRIPT_DEPS_PATH)
    action_scripts = load_json(ACTION_SCRIPTS_PATH)

    action = args.action.strip().lower()
    need_login = str(args.need_login).lower() in ("1", "true", "yes")

    scripts = []
    if action == "script":
        script_name = parse_script_name(args.command)
        if script_name:
            scripts = [script_name]
    else:
        scripts = action_scripts.get(action, [])

    groups = set()
    unknown = []
    for script in scripts:
        deps = script_deps.get(script)
        if deps is None:
            unknown.append(script)
        else:
            groups.update(deps)

    # ensure_token always needs base
    if action in ("script", "refresh", "midday", "eod"):
        groups.add("base")

    if need_login:
        groups.add("selenium")

    use_full = False
    if unknown:
        use_full = True

    if use_full:
        req_files = [str(REQ_DIR / "full.txt")]
    else:
        req_files = [str(REQ_DIR / f"{g}.txt") for g in sorted(groups)]

    print(f"req_files={' '.join(req_files)}")
    print(f"use_full={'true' if use_full else 'false'}")
    if unknown:
        print(f"unknown_scripts={','.join(unknown)}")


if __name__ == "__main__":
    main()
