from typing import Dict, Optional

from karadoc.common.conf.conf_box import ConfBox
from karadoc.common.conf.package import _get_settings_for_env

SPARK_CONF_GROUP = "spark.conf"


def get_spark_conf(env: Optional[str] = None) -> Dict[str, object]:
    """Return the configuration for the given connection in the specified environment.
    If no environment is specified, use the current environment.

    :param env:
    :return:
    """
    dynabox = _get_settings_for_env(env).get(SPARK_CONF_GROUP)
    if dynabox is None:
        return {}
    else:
        return ConfBox(dynabox).to_flat_dict()
