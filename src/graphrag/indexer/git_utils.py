from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional

from git import Commit, Repo


def open_repo(path: Path) -> Repo:
    return Repo(str(path))


def get_branch_head(repo: Repo, branch: str) -> Commit:
    ref = getattr(repo.heads, branch, None)
    if ref is None:
        raise ValueError(f"Branch {branch} not found in repository")
    return ref.commit


def commits_since(repo: Repo, last_hash: Optional[str], branch: str) -> List[Commit]:
    rev = f"{last_hash}..{branch}" if last_hash else branch
    commits = list(repo.iter_commits(rev))
    commits.reverse()
    return commits


def changed_swift_files(commit: Commit) -> List[str]:
    files: set[str] = set()
    if commit.parents:
        parent = commit.parents[0]
        diff_index = commit.diff(parent, paths=None)
        for diff in diff_index:
            path = (diff.b_path or diff.a_path) or ""
            if path.endswith(".swift"):
                files.add(path)
    else:
        for blob in commit.tree.traverse():
            if blob.type == "blob" and blob.path.endswith(".swift"):
                files.add(blob.path)
    return sorted(files)


def file_content_at_commit(repo: Repo, commit: Commit, path: str) -> Optional[str]:
    try:
        blob = commit.tree / path
    except KeyError:
        return None
    data = blob.data_stream.read()
    return data.decode("utf-8", errors="ignore")

