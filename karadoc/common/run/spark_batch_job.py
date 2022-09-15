from karadoc.common.job_core.has_batch_inputs import HasBatchInputs
from karadoc.common.job_core.has_batch_output import HasBatchOutput
from karadoc.common.job_core.has_disable import HasDisable
from karadoc.common.job_core.has_external_inputs import HasExternalInputs
from karadoc.common.job_core.has_external_outputs import HasExternalOutputs
from karadoc.common.job_core.has_keys import HasKeys
from karadoc.common.job_core.has_spark import HasSpark
from karadoc.common.job_core.has_vars import HasVars


class SparkBatchJob(
    HasBatchInputs,
    HasBatchOutput,
    HasVars,
    HasExternalInputs,
    HasExternalOutputs,
    HasKeys,
    HasDisable,
    HasSpark,
):
    _action_file_name_conf_key = "spark.batch"
    _run_method_name = "run"

    def __init__(self) -> None:
        HasSpark.__init__(self)
        HasBatchInputs.__init__(self)
        HasBatchOutput.__init__(self)
        HasVars.__init__(self)
        HasExternalInputs.__init__(self)
        HasExternalOutputs.__init__(self)
        HasKeys.__init__(self)
        HasDisable.__init__(self)

        self.run = None
