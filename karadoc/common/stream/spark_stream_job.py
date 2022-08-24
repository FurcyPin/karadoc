from karadoc.common.job_core.has_disable import HasDisable
from karadoc.common.job_core.has_spark import HasSpark
from karadoc.common.job_core.has_stream_external_inputs import HasStreamExternalInputs
from karadoc.common.job_core.has_stream_external_output import HasStreamExternalOutput
from karadoc.common.job_core.has_vars import HasVars
from karadoc.common.job_core.job_base import JobBase


class SparkStreamJob(HasVars, HasStreamExternalInputs, HasStreamExternalOutput, HasDisable, HasSpark, JobBase):
    _action_file_name_conf_key = "spark.stream"
    _run_method_name = "stream"

    def __init__(self) -> None:
        JobBase.__init__(self)
        HasSpark.__init__(self)
        HasVars.__init__(self)
        HasStreamExternalInputs.__init__(self)
        HasStreamExternalOutput.__init__(self)
        HasDisable.__init__(self)
        self.stream = None
