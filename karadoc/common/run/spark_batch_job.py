from karadoc.common.job_core.has_disable import HasDisable
from karadoc.common.job_core.has_external_inputs import HasExternalInputs
from karadoc.common.job_core.has_external_outputs import HasExternalOutputs
from karadoc.common.job_core.has_inputs import HasInputs
from karadoc.common.job_core.has_keys import HasKeys
from karadoc.common.job_core.has_output import HasOutput
from karadoc.common.job_core.has_spark import HasSpark
from karadoc.common.job_core.has_vars import HasVars
from karadoc.common.job_core.job_base import JobBase


class SparkBatchJob(
    HasInputs, HasOutput, HasVars, HasExternalInputs, HasExternalOutputs, HasKeys, HasDisable, HasSpark, JobBase
):
    _action_file_name_conf_key = "spark.batch"
    _run_method_name = "run"

    def __init__(self) -> None:
        JobBase.__init__(self)
        HasSpark.__init__(self)
        HasInputs.__init__(self)
        HasOutput.__init__(self)
        HasVars.__init__(self)
        HasExternalInputs.__init__(self)
        HasExternalOutputs.__init__(self)
        HasKeys.__init__(self)
        HasDisable.__init__(self)

        self.run = None
