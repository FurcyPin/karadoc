from typing import List

from karadoc.common.job_core.has_batch_inputs import HasBatchInputs
from karadoc.common.job_core.has_before_after import HasBeforeAfter
from karadoc.common.job_core.has_disable import HasDisable
from karadoc.common.job_core.has_external_inputs import HasExternalInputs
from karadoc.common.job_core.has_spark import HasSpark
from karadoc.common.job_core.has_vars import HasVars
from karadoc.common.quality.checks import Alert, Metric
from karadoc.common.utils.assert_utils import assert_true


class QualityCheckJob(HasBatchInputs, HasVars, HasBeforeAfter, HasExternalInputs, HasDisable, HasSpark):
    _action_file_name_conf_key = "spark.quality_check"

    def __init__(self) -> None:
        HasSpark.__init__(self)
        HasBatchInputs.__init__(self)
        HasVars.__init__(self)
        HasBeforeAfter.__init__(self)
        HasExternalInputs.__init__(self)
        HasDisable.__init__(self)

        self.__alerts: List[Alert] = []
        self.__metrics: List[Metric] = []

    @property
    def alerts(self) -> List[Alert]:
        return sorted(list(self.__alerts), key=lambda check: check._creation_rank)

    def add_alert(self, alert: Alert) -> None:
        assert_true(isinstance(alert, Alert))
        self.__alerts.append(alert)

    @property
    def metrics(self) -> List[Metric]:
        return sorted(list(self.__metrics), key=lambda check: check._creation_rank)

    def add_metric(self, metric: Metric) -> None:
        assert_true(isinstance(metric, Metric))
        self.__metrics.append(metric)
