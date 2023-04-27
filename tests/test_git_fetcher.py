import os
import pathlib

import git.repo

from mlflow2prov.adapters.git.fetcher import (
    GitFetcher,
    extract_commits,
    extract_files,
    extract_revisions,
)

path_testproject_git_repo = pathlib.Path(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "resources/testproject/project/.git",
    )
)


class TestGitFetcher:
    def test_enter_exit(self):
        with GitFetcher() as fetcher:
            fetcher.get_from_local_path(path_testproject_git_repo)

        assert fetcher.repo == None

    def test_get_from_local_path(self):
        fetcher = GitFetcher()
        fetcher.get_from_local_path(path_testproject_git_repo)

        assert fetcher.path == path_testproject_git_repo
        assert fetcher.repo == git.repo.Repo(path_testproject_git_repo)

    def test_extract_commits(self):
        fetcher = GitFetcher()

        for c in extract_commits(git.repo.Repo(path_testproject_git_repo)):
            pass

    def test_extract_files(self):
        fetcher = GitFetcher()

        for f in extract_files(git.repo.Repo(path_testproject_git_repo)):
            pass

    def test_extract_revisions(self):
        fetcher = GitFetcher()

        for r in extract_revisions(git.repo.Repo(path_testproject_git_repo)):
            pass

    def test_fetch_all(self):
        fetcher = GitFetcher()
        fetcher.get_from_local_path(path_testproject_git_repo)

        for resource in fetcher.fetch_all():
            pass
