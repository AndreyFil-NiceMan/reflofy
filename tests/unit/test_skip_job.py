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
    records, transformed, applied, destination = run_job_records(
        _StaticSource([{"id": 1}]), _Pipeline(where), {}
    )
    assert (records, transformed, applied, destination) == ([], [], [], None)
