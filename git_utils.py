"""
git_utils.py — Shared git commit helper for scripts that write files.

Used by:
  - refresh_ref_sheets_json.py  (commits ref_sheets.json)
  - db/backfill_market_data.py  (commits db/trading.db)
  - db/fetch_market_data.py     (commits db/trading.db)

Commit only happens if the file has actually changed.
Push uses --rebase to handle concurrent workflow runs safely.
"""

from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

GIT_USER_NAME  = "github-actions[bot]"
GIT_USER_EMAIL = "41898282+github-actions[bot]@users.noreply.github.com"


def _get_authenticated_remote(repo_root_path: Path) -> str | None:
    """
    Returns an authenticated remote URL using GH_PAT or GITHUB_TOKEN env vars.
    Returns None if no token is available (local dev — uses existing remote).
    """
    token = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")
    if not token:
        return None

    # Get current remote URL and inject token
    result = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=str(repo_root_path),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    if result.returncode != 0:
        return None

    url = result.stdout.strip()
    # https://github.com/user/repo.git → https://token@github.com/user/repo.git
    if url.startswith("https://"):
        url = url.replace("https://", f"https://x-access-token:{token}@")
    return url


def commit_file_if_changed(
    filepath: str | Path,
    message: str,
    repo_root: Path | None = None,
) -> bool:
    """
    Stages, commits, and pushes a single file if it has changed.

    Args:
        filepath:  Path to the file to commit (absolute or relative to repo root).
        message:   Commit message.
        repo_root: Repo root directory. Defaults to cwd.

    Returns:
        True if a commit was made, False if nothing changed.
    """
    cwd          = str(repo_root) if repo_root else None
    repo_path    = Path(repo_root) if repo_root else Path.cwd()
    auth_remote  = _get_authenticated_remote(repo_path)

    def _run(cmd: list[str], env=None) -> subprocess.CompletedProcess:
        return subprocess.run(cmd, check=True, cwd=cwd,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, env=env)

    try:
        # Configure git identity
        _run(["git", "config", "user.name",  GIT_USER_NAME])
        _run(["git", "config", "user.email", GIT_USER_EMAIL])

        # Stage the file
        _run(["git", "add", str(filepath)])

        # Check if anything is staged
        result = subprocess.run(
            ["git", "diff", "--staged", "--quiet"],
            cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if result.returncode == 0:
            log.info(f"No changes in {filepath} — nothing to commit.")
            return False

        # Commit
        _run(["git", "commit", "-m", message])
        log.info(f"Committed: {message}")

        # Pull rebase then push — use authenticated remote if available
        if auth_remote:
            _run(["git", "pull", "--rebase", auth_remote, "main"])
            _run(["git", "push", auth_remote, "main"])
        else:
            _run(["git", "pull", "--rebase", "origin", "main"])
            _run(["git", "push", "origin", "main"])

        log.info(f"Pushed to origin/main.")
        return True

    except subprocess.CalledProcessError as e:
        log.error(f"Git operation failed: {e.stderr.strip()}")
        return False
