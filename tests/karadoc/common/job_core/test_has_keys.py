from unittest import TestCase

from karadoc.spark.batch.spark_batch_job import HasKeys


class TestHasKeys(TestCase):
    def test_setting_primary_keys_multiple_times(self):
        job = HasKeys()

        job.primary_key = ("column_1", "column_2")

        self.assertEqual(job.primary_key, ("column_1", "column_2"))

        job.primary_key = "column_3"

        self.assertEqual(job.primary_key, "column_3")

    def test_setting_secondary_keys_multiple_times(self):
        job = HasKeys()

        job.secondary_keys = [("column_1", "column_2"), "column_1"]

        self.assertListEqual(job.secondary_keys, [("column_1", "column_2"), "column_1"])

        job.secondary_keys = [("column_1", "column_2"), "column_3"]

        self.assertListEqual(job.secondary_keys, [("column_1", "column_2"), "column_3"])

    def test_primary_key_setter(self):
        job = HasKeys()

        job.primary_key = ("column_1", "column_2")
        expected_standardized_key = ("column_1", "column_2")

        self.assertEqual(job.standardized_primary_key, expected_standardized_key)

    def test_primary_key_setter_with_type_error(self):
        """
        GIVEN a job that has keys
        WHEN we set it's primary key with an incorrect type
        THEN a TypeError should be raised
        """
        job = HasKeys()

        with self.assertRaises(TypeError):
            job.primary_key = ["column_1", "column_2"]

    def test_secondary_key_setter(self):
        job = HasKeys()

        job.secondary_keys = [("column_1", "column_2"), "column_3"]
        expected_standardized_keys = [("column_1", "column_2"), ("column_3",)]

        self.assertEqual(job.standardized_secondary_keys, expected_standardized_keys)

    def test_secondary_key_setter_with_type_error(self):
        """
        GIVEN a job that has keys
        WHEN we set it's primary key with an incorrect type
        THEN a TypeError should be raised
        """
        job = HasKeys()

        with self.assertRaises(TypeError):
            job.primary_key = {"column_1", "column_2"}
