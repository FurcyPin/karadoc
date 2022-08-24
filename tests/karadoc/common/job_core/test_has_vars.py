from unittest import TestCase

from karadoc.common.run.spark_batch_job import HasVars


class TestHasVars(TestCase):
    def test_setting_vars_multiple_times(self):
        job = HasVars()

        job.vars = {"day": "2020-01-01"}

        self.assertDictEqual(job.vars, {"day": "2020-01-01"})

        job.vars = {"day": "2020-01-02"}

        self.assertDictEqual(job.vars, {"day": "2020-01-02"})
