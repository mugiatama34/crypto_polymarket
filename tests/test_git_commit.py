import subprocess

from collector.git_commit import commit_paths, push_enabled, push_with_retry


def _git(args, cwd):
    result = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return result


def _init_repo_with_remote(tmp_path):
    remote_dir = tmp_path / "remote.git"
    remote_dir.mkdir()
    _git(["init", "--bare"], cwd=remote_dir)

    work_dir = tmp_path / "work"
    work_dir.mkdir()
    _git(["init", "-b", "main"], cwd=work_dir)
    _git(["config", "user.email", "test@example.com"], cwd=work_dir)
    _git(["config", "user.name", "Test"], cwd=work_dir)
    (work_dir / "README.md").write_text("init\n", encoding="utf-8")
    _git(["add", "README.md"], cwd=work_dir)
    _git(["commit", "-m", "init"], cwd=work_dir)
    _git(["remote", "add", "origin", str(remote_dir)], cwd=work_dir)
    _git(["push", "-u", "origin", "main"], cwd=work_dir)
    return work_dir, remote_dir


def test_commit_paths_creates_commit_when_changed(tmp_path):
    work_dir, _ = _init_repo_with_remote(tmp_path)
    (work_dir / "data.txt").write_text("hello\n", encoding="utf-8")

    committed = commit_paths([work_dir / "data.txt"], message="add data", repo_dir=work_dir)

    assert committed is True
    log = _git(["log", "--oneline", "-1"], cwd=work_dir).stdout
    assert "add data" in log


def test_commit_paths_no_changes_returns_false(tmp_path):
    work_dir, _ = _init_repo_with_remote(tmp_path)
    committed = commit_paths([work_dir / "README.md"], message="noop", repo_dir=work_dir)
    assert committed is False


def test_push_enabled_reads_env_var():
    assert push_enabled({"LONGJOB_GIT_PUSH": "1"}) is True
    assert push_enabled({"LONGJOB_GIT_PUSH": "0"}) is False
    assert push_enabled({}) is False


def test_push_with_retry_disabled_by_default_does_nothing(tmp_path):
    work_dir, _ = _init_repo_with_remote(tmp_path)
    result = push_with_retry(repo_dir=work_dir, branch="main", env={})
    assert result is False


def test_push_with_retry_enabled_pushes_to_origin(tmp_path):
    work_dir, remote_dir = _init_repo_with_remote(tmp_path)
    (work_dir / "data.txt").write_text("hello\n", encoding="utf-8")
    commit_paths([work_dir / "data.txt"], message="add data", repo_dir=work_dir)

    result = push_with_retry(repo_dir=work_dir, branch="main", env={"LONGJOB_GIT_PUSH": "1"})

    assert result is True
    clone_dir = tmp_path / "verify_clone"
    _git(["clone", str(remote_dir), str(clone_dir)], cwd=tmp_path)
    log = _git(["log", "--oneline", "-1", "origin/main"], cwd=clone_dir).stdout
    assert "add data" in log


def test_push_with_retry_rebases_past_remote_changes(tmp_path):
    work_dir, remote_dir = _init_repo_with_remote(tmp_path)

    other_dir = tmp_path / "other"
    _git(["clone", str(remote_dir), str(other_dir)], cwd=tmp_path)
    _git(["checkout", "-b", "main", "origin/main"], cwd=other_dir)
    _git(["config", "user.email", "other@example.com"], cwd=other_dir)
    _git(["config", "user.name", "Other"], cwd=other_dir)
    (other_dir / "other.txt").write_text("other\n", encoding="utf-8")
    _git(["add", "other.txt"], cwd=other_dir)
    _git(["commit", "-m", "other change"], cwd=other_dir)
    _git(["push", "origin", "main"], cwd=other_dir)

    (work_dir / "mine.txt").write_text("mine\n", encoding="utf-8")
    commit_paths([work_dir / "mine.txt"], message="mine change", repo_dir=work_dir)

    result = push_with_retry(repo_dir=work_dir, branch="main", env={"LONGJOB_GIT_PUSH": "1"})

    assert result is True
    assert (work_dir / "other.txt").exists()  # rebase remote degisikligini getirdi
    clone_dir = tmp_path / "verify_clone2"
    _git(["clone", str(remote_dir), str(clone_dir)], cwd=tmp_path)
    log = _git(["log", "--oneline", "origin/main"], cwd=clone_dir).stdout
    assert "mine change" in log
    assert "other change" in log
