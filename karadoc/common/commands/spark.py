from typing import Dict, Optional

from karadoc.common import conf
from karadoc.spark.job_core.has_spark import HasSpark


def init_job(job: HasSpark, raw_args: str, spark_conf: Optional[Dict] = None):
    job.init(app_name=f"{conf.APPLICATION_NAME.lower()} {raw_args}", spark_conf=spark_conf)
