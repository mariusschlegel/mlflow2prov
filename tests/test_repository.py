from mlflow2prov.adapters.repository import InMemoryRepository
from mlflow2prov.domain.model import User


class TestInMemoryRepository:
    def test_add_and_get(self):
        repo = InMemoryRepository()

        u1 = User(name="u1", email="u1@domain.com", prov_role="r1")
        u2 = User(name="u2", email="u2@domain.com", prov_role="r2")

        repo.add(u1)
        repo.add(u2)

        assert repo.get(User, name="u1") == u1
        assert repo.get(User, name="u2") == u2

    def test_get_returns_none_if_repository_empty(self):
        repo = InMemoryRepository()

        assert repo.get(User, name="u") == None

    def test_list_all(self):
        repo = InMemoryRepository()

        u1 = User(name="u1", email="u1@domain.com", prov_role="r1")
        u2 = User(name="u2", email="u2@domain.com", prov_role="r1")

        repo.add(u1)
        repo.add(u2)

        assert repo.list_all(resource_type=User, name="u1") == [u1]
        assert repo.list_all(resource_type=User, name="u2") == [u2]
        assert repo.list_all(resource_type=User, prov_role="r1") == [u1, u2]

    def test_list_all_returns_empty_list_if_repository_empty(self):
        repo = InMemoryRepository()

        assert repo.list_all(resource_type=User, name="u") == []

    def test_eq(self):
        repo1 = InMemoryRepository()
        repo2 = InMemoryRepository()

        repo1_u1 = User(name="u1", email="u1@domain.com", prov_role="r1")
        repo1_u2 = User(name="u2", email="u2@domain.com", prov_role="r2")

        repo1.add(repo1_u1)
        repo1.add(repo1_u2)

        repo2_u1 = User(name="u1", email="u1@domain.com", prov_role="r1")
        repo2_u2 = User(name="u2", email="u2@domain.com", prov_role="r2")

        repo2.add(repo2_u1)
        repo2.add(repo2_u2)

        assert repo1 == repo2

        repo3 = InMemoryRepository()

        repo3_u1 = repo1_u1
        repo3_u2 = User(name="u2", email="u2@domain.com", prov_role="r1")

        repo3.add(repo3_u1)
        repo3.add(repo3_u2)

        assert not repo1 == repo3

        assert not repo1 == [repo1_u1, repo1_u2]

    def test_eq_if_repositories_empty(self):
        repo1 = InMemoryRepository()
        repo2 = InMemoryRepository()

        assert repo1 == repo2
