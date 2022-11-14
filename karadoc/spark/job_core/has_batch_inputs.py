from karadoc.spark.job_core.has_inputs import HasInputs


class HasBatchInputs(HasInputs):
    def __init__(self) -> None:
        super().__init__()

    def get_reader(self):
        return self.spark.read
