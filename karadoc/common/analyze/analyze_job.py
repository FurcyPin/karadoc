from karadoc.common.job_core.has_batch_inputs import HasBatchInputs
from karadoc.common.job_core.has_spark import HasSpark
from karadoc.common.job_core.has_vars import HasVars


class AnalyzeJob(HasBatchInputs, HasVars, HasSpark):
    _action_file_name_conf_key = "spark.analyze_timeline"
    _run_method_name = "analyze"

    def __init__(self) -> None:
        HasSpark.__init__(self)
        HasBatchInputs.__init__(self)
        HasVars.__init__(self)

        # Attributes that the user may change
        self.reference_time_col = "application_date"
        self.cohorts = ["cohort"]
        self.nb_buckets = 5
        self.analyze = None
