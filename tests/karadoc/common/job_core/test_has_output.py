from unittest import TestCase

from karadoc.common.job_core import has_output
from karadoc.common.job_core.has_output import HasOutput


class TestHasOutput(TestCase):
    def test_output_partition_to_dynamic_partitions_list(self):
        actual = has_output._output_partition_to_dynamic_partitions(None)
        self.assertEqual(actual, [])

        actual = has_output._output_partition_to_dynamic_partitions([])
        self.assertEqual(actual, [])

        actual = has_output._output_partition_to_dynamic_partitions([("k1", "v1")])
        self.assertEqual(actual, [])

        actual = has_output._output_partition_to_dynamic_partitions(["k0", ("k1", "v1"), "k2"])
        self.assertEqual(actual, ["k0", "k2"])

    def test_output_partitioning_type(self):
        job = HasOutput()
        job.output_partition = [("day", "2020-10-10")]
        self.assertEqual(job.output_partitioning_type, "static")
        job.output_partition = ["day"]
        self.assertEqual(job.output_partitioning_type, "dynamic")

    def test_output_partition_names(self):
        job = HasOutput()
        job.output_partition = [("part1_name", "part1_value"), ("part2_name", "part2_value")]
        self.assertEqual(job.output_partition_names, ["part1_name", "part2_name"])
        job.output_partition = ["p1", "p2", "p3"]
        self.assertEqual(job.output_partition_names, ["p1", "p2", "p3"])
