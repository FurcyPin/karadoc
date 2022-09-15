from typing import List, Optional

import dynaconf
from dynaconf.utils.boxing import DynaBox

from karadoc.common.conf.conf_box import ConfBox
from karadoc.common.exceptions import MissingConfigurationException
from karadoc.common.utils.assert_utils import assert_true

APPLICATION_NAME = "KARADOC"
APPLICATION_DESCRIPTION = "karadoc: a development environment for PySpark."
CONNECTION_GROUP = "connection"
SECRET_STORE_GROUP = "secret"  # nosec B105
REMOTE_ENV_GROUP = "remote_env"
CUSTOM_GROUP = "custom"
OBSERVABILITY_GROUP = "observability"


def get_local_data_folder_location() -> str:
    return "data"


def get_model_folder_location() -> str:
    return dynaconf.settings.get("model_dir", default="model")


def get_conf_folder_location() -> str:
    return dynaconf.settings.get("conf_dir", default="conf")


def get_libs_folder_location() -> str:
    return dynaconf.settings.get("libs", default="libs")


def get_template_package_name() -> str:
    return dynaconf.settings.get("template_package", default="model_lib.templates")


def get_warehouse_folder_location() -> str:
    return dynaconf.settings.get("warehouse_dir", default="data/hive/warehouse")


def get_analyze_timeline_dir_location() -> str:
    return dynaconf.settings.get("analyze_timeline_dir", default="data/analyze_timeline")


def get_spark_stream_dir() -> str:
    return dynaconf.settings.get("spark_stream_dir", default="data/spark/stream")


def get_streaming_checkpoint_folder_location() -> str:
    spark_stream_dir = get_spark_stream_dir()
    return f"{spark_stream_dir}/checkpoint"


def get_spark_stream_tmp_dir() -> str:
    spark_stream_dir = get_spark_stream_dir()
    return f"{spark_stream_dir}/tmp"  # nosec B108


def get_custom_command_packages() -> List[str]:
    value = dynaconf.settings.get("custom_command_packages", default=[])
    assert_true(isinstance(value, list), "Setting custom_command_packages should be a List[str]")
    return value


def get_env() -> str:
    return dynaconf.settings.current_env.lower()


def is_dev_env() -> bool:
    return get_env() == "development" or get_env() == "testing"


def enable_file_index_cache() -> bool:
    return dynaconf.settings.get("enable_file_index_cache", default=True)


def allow_missing_vars() -> bool:
    """When a job with declared variables (job.vars) is run, the values of the declared variables
    must be passed via the --vars option. This security can be disabled on an environment by setting
    allow_missing_vars = true, in which case the default values will be used when missing, instead of raising an error.

    It can be useful for development environment, but it is recommended to keep this to false in production.
    """
    return dynaconf.settings.get("allow_missing_vars", default=False)


def list_connection_names(env: Optional[str] = None) -> List[str]:
    """Return the list of all connection items for the specified environment if given,
    for the current environment otherwise."""
    connections = _get_settings_for_env(env).get(CONNECTION_GROUP)
    if connections:
        return [name for name, params in connections.items() if isinstance(params, DynaBox)]
    else:
        return []


def get_conn_conf_id(conn_name: str, group: str, env: str = None) -> str:
    """Builds the id of a connection:

    >>> get_conn_conf_id(conn_name="dummy", group=CONNECTION_GROUP, env="IC")
    'IC.connection.dummy'

    >>> get_conn_conf_id(conn_name="keyvault", group=SECRET_STORE_GROUP, env="PP")
    'PP.secret.keyvault'

    :param conn_name: Name of the connection
    :param env: environment on which to get the connection conf, use current environment if None
    :param group: configuration group
    :return:
    """
    if env is None:
        env = get_env()
    return f"{env}.{group}.{conn_name}"


def _get_settings_for_env(env: Optional[str]) -> DynaBox:
    if env is None:
        return dynaconf.settings
    else:
        return dynaconf.settings.from_env(env)


def __get_connection_conf(conn_name: str, group: str, env: Optional[str] = None) -> ConfBox:
    connections = _get_settings_for_env(env).get(group)
    if connections:
        connection_conf = connections.get(conn_name)
        if connection_conf:
            return ConfBox(connection_conf, env)

    conn_conf_id = get_conn_conf_id(conn_name, group=group, env=env)
    raise MissingConfigurationException(conf_id=conn_conf_id)


def get_connection_conf(connection_name: str, env: Optional[str] = None) -> ConfBox:
    """Return the configuration for the given connection in the specified environment.
    If no environment is specified, use the current environment.
    """
    return __get_connection_conf(connection_name, CONNECTION_GROUP, env)


def get_vault_conf(vault_name: str, env: Optional[str] = None) -> ConfBox:
    """Return the configuration for the given secret vault."""
    return __get_connection_conf(vault_name, group=SECRET_STORE_GROUP, env=env)


def get_remote_env_conf(remote_env_name: str, env: Optional[str] = None) -> ConfBox:
    """Return the configuration for the given remote environment."""
    return __get_connection_conf(remote_env_name, group=REMOTE_ENV_GROUP, env=env)


def get_custom_settings(env: Optional[str] = None) -> ConfBox:
    """Return the whole configuration object of custom configuration.
    Secret stores are supported as well.

    Example:

    In your settings.toml:

    .. code-block:: python

        [development]
        custom.my_conf_variable = "hello"
        custom.my_secret_variable.secret.keyvault = "secret-name-in-keyvault"

    In your code:

    my_variable = get_custom_conf().get("my_variable")
    my_secret_variable = get_custom_conf().get("my_variable")

    """
    custom_conf = _get_settings_for_env(env).get(CUSTOM_GROUP)
    if custom_conf is None:
        if env is None:
            env = get_env()
        raise MissingConfigurationException(conf_id=f"{env}.{CUSTOM_GROUP}")
    return ConfBox(custom_conf)


def get_action_file_name(conf_key: str) -> str:
    """Return the whole configuration object of action_file name configuration.

    Example:

    In your settings.toml:

    .. code-block:: python

        [development]
        action_file_name.spark.batch = "BATCH"

    In your code:

    my_variable = get_action_file_name("spark.batch")
    """
    action_file_name_default = {
        "spark.batch": "POPULATE",
        "spark.stream": "STREAM",
        "spark.quality_check": "QUALITY_CHECK",
        "spark.analyze_timeline": "ANALYZE",
    }
    action_file_name_conf = dynaconf.settings.get("action_file_name", default=DynaBox(action_file_name_default))
    if action_file_name_conf:
        return ConfBox(action_file_name_conf).get(conf_key)
    raise MissingConfigurationException(conf_id=f"{get_env()}.get_action_file_name")


def get_observability_conf() -> Optional[ConfBox]:
    """Return the configuration for the given remote environment."""
    settings = _get_settings_for_env(env=None)
    observability_conf = settings.get(OBSERVABILITY_GROUP)

    if observability_conf:
        return ConfBox(observability_conf)
