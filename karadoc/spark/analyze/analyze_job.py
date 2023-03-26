from karadoc.common.job_core.has_vars import HasVars
from karadoc.common.job_core.package import RequiredMethod
from karadoc.spark.job_core.has_batch_inputs import HasBatchInputs
from karadoc.spark.job_core.has_spark import HasSpark


class AnalyzeJob(HasBatchInputs, HasVars, HasSpark):
    _action_file_name_conf_key = "spark.analyze_timeline"

    def __init__(self) -> None:
        HasSpark.__init__(self)
        HasBatchInputs.__init__(self)
        HasVars.__init__(self)

        # Attributes that the user may change
        self.reference_time_col = "application_date"
        self.cohorts = ["cohort"]
        self.nb_buckets = 5

        self.analyze = RequiredMethod("analyze")
