"""~15 dakikada bir commit; push `LONGJOB_GIT_PUSH` ortam degiskeni
arkasinda, yerelde varsayilan kapali (plan onayi maddesi 5).

Push aciksa: push oncesi `pull --rebase`, cakismada tekrar dener
(CLAUDE.md git davranisi). Actions'ta actions/checkout'un standart
token'i yeterli sayiliyor -- ayri bir sir kurulumu bu kodun
sorumlulugunda degil.
"""

import os
import subprocess
from pathlib import Path
from typing import Optional

PUSH_ENV_VAR = "LONGJOB_GIT_PUSH"


class GitCommitError(Exception):
    pass


def _run(args, cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True)


def push_enabled(env: Optional[dict] = None) -> bool:
    env = env if env is not None else os.environ
    return env.get(PUSH_ENV_VAR, "") == "1"


def commit_paths(paths, *, message: str, repo_dir: Path) -> bool:
    """Verilen yollari stage'ler, degisiklik varsa commit atar.

    Returns: commit atildiysa True, atilacak bir degisiklik yoksa False.
    """
    str_paths = [str(p) for p in paths]
    add_result = _run(["git", "add", *str_paths], cwd=repo_dir)
    if add_result.returncode != 0:
        raise GitCommitError(f"git add basarisiz: {add_result.stderr}")

    status_result = _run(["git", "status", "--porcelain", *str_paths], cwd=repo_dir)
    if not status_result.stdout.strip():
        return False

    commit_result = _run(["git", "commit", "-m", message], cwd=repo_dir)
    if commit_result.returncode != 0:
        raise GitCommitError(f"git commit basarisiz: {commit_result.stderr}")
    return True


def push_with_retry(
    *,
    repo_dir: Path,
    branch: Optional[str] = None,
    max_attempts: int = 3,
    env: Optional[dict] = None,
) -> bool:
    """`pull --rebase` + `push`. Cakismada tekrar dener.

    Push kapaliysa (varsayilan) hicbir sey yapmaz, False doner -- veri
    o zaman yalnizca yerel commit'te kalir.
    """
    if not push_enabled(env):
        return False

    last_pull_error = None
    last_push_error = None
    for _attempt in range(max_attempts):
        pull_args = ["git", "pull", "--rebase"]
        if branch:
            pull_args += ["origin", branch]
        pull_result = _run(pull_args, cwd=repo_dir)
        if pull_result.returncode != 0:
            last_pull_error = pull_result.stderr
            continue

        push_args = ["git", "push"]
        if branch:
            push_args += ["-u", "origin", branch]
        push_result = _run(push_args, cwd=repo_dir)
        if push_result.returncode == 0:
            return True
        last_push_error = push_result.stderr

    raise GitCommitError(
        f"git push {max_attempts} denemede basarisiz. "
        f"son pull hatasi: {last_pull_error!r}, son push hatasi: {last_push_error!r}"
    )
