import copy
import unittest

from run_benchmarks import validate_report


class ReportValidation(unittest.TestCase):
    def test_expected_rollbacks_are_not_failures(self):
        report = {"total": {"failed": 0, "committed": 10, "expected_rollbacks": 3},
                  "consistency_checks": {str(i): "pass" for i in range(15)},
                  "reopen_consistency_checks": {str(i): "pass" for i in range(15)}}
        validate_report("tpcc", report)
        for change in ("failure", "empty", "missing_check", "reopen_failure"):
            broken = copy.deepcopy(report)
            if change == "failure":
                broken["total"]["failed"] = 1
            elif change == "empty":
                broken["total"]["committed"] = 0
            elif change == "missing_check":
                broken["consistency_checks"].pop("1")
            else:
                broken["reopen_consistency_checks"]["1"] = "mismatch"
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_report("tpcc", broken)

    def test_timeouts_and_incomplete_tpch_runs_fail(self):
        report = {"failed_executions": 0, "timed_out_executions": 0,
                  "successful_executions": 66, "counts_verified_after_reopen": True,
                  "config": {"runs": 3}}
        validate_report("tpch", report)
        for key, value in (("failed_executions", 1), ("timed_out_executions", 1),
                           ("successful_executions", 65), ("counts_verified_after_reopen", False)):
            broken = copy.deepcopy(report)
            broken[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_report("tpch", broken)


if __name__ == "__main__":
    unittest.main()
