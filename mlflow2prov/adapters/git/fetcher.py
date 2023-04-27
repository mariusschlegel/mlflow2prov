from collections.abc import Iterator
from dataclasses import dataclass
from itertools import zip_longest
from pathlib import Path

import git
import git.repo

from mlflow2prov.domain.constants import ChangeType, ProvRole
from mlflow2prov.domain.model import Commit, File, FileRevision, User


@dataclass
class GitFetcher:
    path: Path | None = None
    repo: git.repo.Repo | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.repo:
            self.repo.close()
            self.repo = None

    def get_from_local_path(self, path: Path) -> None:
        self.path = path
        self.repo = git.repo.Repo(path)

    def fetch_all(self) -> Iterator[Commit | File | FileRevision]:
        if self.repo:
            yield from extract_commits(self.repo)
            yield from extract_files(self.repo)
            yield from extract_revisions(self.repo)


def get_author(commit: git.Commit) -> User:
    return User(
        name="None" if commit.author.name is None else commit.author.name,
        email="None" if commit.author.email is None else commit.author.email,
        prov_role=ProvRole.AUTHOR,
    )


def get_committer(commit: git.Commit) -> User:
    return User(
        name="None" if commit.committer.name is None else commit.committer.name,
        email="None" if commit.committer.email is None else commit.committer.email,
        prov_role=ProvRole.COMMITTER,
    )


def parse_log(log: str):
    # split at line breaks, strip whitespace, remove empty lines
    lines = [line.strip() for line in log.split("\n") if line]

    # every second line contains the SHA1 of a commit
    hexshas = lines[::2]

    # every other line contains a type, aswell as a file path
    types = [line.split()[0][0] for line in lines[1::2]]
    paths = [line.split()[1][:] for line in lines[1::2]]

    # zip all three together
    return zip(paths, hexshas, types)


def extract_commits(repo: git.repo.Repo) -> Iterator[Commit]:
    commit: git.Commit
    for commit in repo.iter_commits("--all"):
        yield Commit(
            sha=commit.hexsha,
            title=commit.summary,  # type:ignore
            message=commit.message,  # type:ignore
            author=get_author(commit),
            committer=get_committer(commit),
            parents=[parent.hexsha for parent in commit.parents],
            authored_at=commit.authored_datetime,
            committed_at=commit.committed_datetime,
        )


def extract_files(repo: git.repo.Repo) -> Iterator[File]:
    for commit in repo.iter_commits("--all"):
        # choose the parent commit to diff against
        # use *magic* empty tree sha for commits without parents
        parent = (
            commit.parents[0]
            if commit.parents
            else "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
        )

        # diff against parent
        diff = commit.diff(parent, R=True)

        # only consider files that have been added to the repository
        # disregard modifications and deletions
        for diff_item in diff.iter_change_type(ChangeType.ADDED):
            # path for new files is stored in diff b_path
            yield File(
                name=Path(diff_item.b_path).name,
                path=diff_item.b_path,
                commit=commit.hexsha,
            )


def extract_revisions(repo: git.repo.Repo) -> Iterator[FileRevision]:
    for file in extract_files(repo):
        revs = []

        for path, sha, status in parse_log(
            repo.git.log(
                "--all",
                "--follow",
                "--name-status",
                "--pretty=format:%H",
                "--",
                file.path,
            )
        ):
            revs.append(
                FileRevision(
                    name=Path(path).name,
                    path=path,
                    commit=sha,
                    status=status,
                    file=file,
                )
            )

        # revisions remeber their predecessor (previous revision)
        for rev, prev in zip_longest(revs, revs[1:]):
            rev.previous = prev
            yield rev
