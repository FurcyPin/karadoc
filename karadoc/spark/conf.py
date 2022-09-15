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


def get_batch_default_output_format(env: Optional[str] = None) -> str:
    return (
        _get_settings_for_env(env)
        .get("spark", {})
        .get("write", {})
        .get("batch", {})
        .get("default_format", default="parquet")
    )


def get_stream_default_output_format(env: Optional[str] = None) -> str:
    return (
        _get_settings_for_env(env)
        .get("spark", {})
        .get("write", {})
        .get("stream", {})
        .get("default_format", default="parquet")
    )


def get_write_options_for_format(output_format: str, env: Optional[str] = None) -> Dict:
    """Return the configured write options for the given format

    Example:

    In your settings.toml:

    .. code-block:: python

        [default.spark.write.options.text]
            compression = "gzip"

        or

        [default.spark.write.options]
          text.compression = "gzip"

    In your code:

    compression = get_write_options_for_format("text")
    """
    format_write_options = (
        _get_settings_for_env(env).get("spark", {}).get("write", {}).get("options", {}).get(output_format)
    )
    if format_write_options is not None:
        return format_write_options.to_dict()
    else:
        return {}
