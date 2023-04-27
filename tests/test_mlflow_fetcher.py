import os

import mlflow
import mlflow.exceptions

from mlflow2prov.adapters.git.fetcher import GitFetcher
from mlflow2prov.adapters.mlflow.fetcher import MLflowFetcher, log


class TestMLflowFetcher:
    def test_post_init(self):
        fetcher = MLflowFetcher(tracking_uri="http://localhost:6000")

        assert os.environ.get("MLFLOW_TRACKING_URI") is None
        assert fetcher.tracking_uri == "http://localhost:6000"
        assert mlflow.get_tracking_uri() == "http://localhost:6000"

        os.environ["MLFLOW_TRACKING_URI"] = "http://localhost:6000"
        fetcher = MLflowFetcher()
        assert fetcher.tracking_uri == "http://localhost:6000"

        os.environ.pop("MLFLOW_TRACKING_URI", None)

    def test_eq(self):
        fetcher1 = MLflowFetcher()
        fetcher2 = MLflowFetcher()

        assert fetcher1 == fetcher2
        assert not fetcher1 == MLflowFetcher(tracking_uri="http://localhost:6000")
        assert not fetcher1 == GitFetcher()

    def test_log_error(self, caplog):
        fetcher = MLflowFetcher()
        fetcher.log_error(
            log, mlflow.exceptions.MlflowException("error"), "experiments"
        )

        assert "failed to fetch" in caplog.text
        assert "error" in caplog.text

    def test_fetch_experiments(self):
        fetcher = MLflowFetcher()
        for e in fetcher.fetch_experiments():
            pass

    def test_fetch_runs(self):
        fetcher = MLflowFetcher()
        for r in fetcher.fetch_runs():
            pass

    def test_fetch_registered_models(self):
        fetcher = MLflowFetcher()
        for m in fetcher.fetch_models():
            pass

    def test_fetch_all(self):
        fetcher = MLflowFetcher()
        for resource in fetcher.fetch_all():
            pass
