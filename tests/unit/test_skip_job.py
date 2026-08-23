"""SkipJob from any hook drops the job: no destination, no records, no failure."""

import pytest

from reflowfy import SkipJob
from reflowfy.execution.job_runner import run_job_records


class _StaticSource:
    def __init__(self, records):
        self._records = records

    def fetch(self, runtime_params):
        return self._records


class _Drop:
    name = "drop"

    def apply(self, records, runtime_params):
        raise SkipJob("nothing useful")


class _Pipeline:
    name = "p"

    def __init__(self, where):
        self.where = where

    def define_transformations(self, records, runtime_params):
        return [_Drop()] if self.where == "transformation" else []

    def define_destination(self, records, runtime_params):
        if self.where == "destination":
            raise SkipJob("nothing ready")
        raise AssertionError("destination must not be resolved in this test")


@pytest.mark.parametrize("where", ["transformation", "destination"])
def test_skip_job_drops_the_job(where):
    runtime_params = {}
    records, transformed, applied, destination = run_job_records(
        _StaticSource([{"id": 1}]), _Pipeline(where), runtime_params
    )
    assert (records, transformed, applied, destination) == ([], [], [], None)
    # The reason is what the worker/local executor log and store in job stats.
    assert runtime_params["skip_reason"] in ("nothing useful", "nothing ready")


def test_skipped_job_is_its_own_metric_status():
    """A dropped job must not be counted as an ordinary completed one."""
    from prometheus_client import REGISTRY

    from reflowfy.worker.executor import JobStats, WorkerExecutor

    stats = JobStats()
    stats.success = True
    stats.skip_reason = "nothing ready"
    assert stats.to_dict()["skip_reason"] == "nothing ready"

    def count(status):
        return (
            REGISTRY.get_sample_value(
                "reflowfy_jobs_processed_total",
                {"pipeline": "skip-metrics", "status": status},
            )
            or 0
        )

    before_skipped, before_completed = count("skipped"), count("completed")
    # record_job_metrics touches no instance state, so no DB connection is needed.
    WorkerExecutor.record_job_metrics(
        None,
        pipeline="skip-metrics",
        success=stats.success,
        deduplicated=stats.deduplicated,
        skipped=stats.skip_reason is not None,
        error_type=None,
        duration=0.1,
        records=0,
    )
    assert count("skipped") == before_skipped + 1
    assert count("completed") == before_completed
