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
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

GIT_USER_NAME  = "github-actions[bot]"
GIT_USER_EMAIL = "41898282+github-actions[bot]@users.noreply.github.com"


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
    cwd = str(repo_root) if repo_root else None

    def _run(cmd: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(cmd, check=True, cwd=cwd,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

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

        # Pull rebase then push
        _run(["git", "pull", "--rebase", "origin", "main"])
        _run(["git", "push", "origin", "main"])
        log.info(f"Pushed to origin/main.")
        return True

    except subprocess.CalledProcessError as e:
        log.error(f"Git operation failed: {e.stderr.strip()}")
        return False
