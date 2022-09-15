from unittest import TestCase

from karadoc.common.job_core.has_batch_inputs import HasBatchInputs


class TestHasBatchInputs(TestCase):
    def test_inputs_setter_wrong_type(self):
        """
        GIVEN ha job that has inputs
        WHEN we declare an input with a wrong type
        THEN a TypeError should be raised
        """
        job = HasBatchInputs()
        with self.assertRaises(TypeError):
            job.inputs = {"alias": 1}

    def test_inputs_setter_wrong_dict(self):
        """
        GIVEN ha job that has inputs
        WHEN we declare an input as a dict without specifying the table name
        THEN a TypeError should be raised
        """
        job = HasBatchInputs()
        with self.assertRaises(TypeError):
            job.inputs = {"alias": {"format": "parquet"}}
