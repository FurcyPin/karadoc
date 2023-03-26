import importlib.machinery
import inspect
import sys
from os.path import isfile, splitext
from pathlib import Path
from types import FunctionType, ModuleType
from typing import Dict, Optional, Type, TypeVar, Union, cast

from karadoc.common import conf
from karadoc.common.exceptions import ActionFileLoadingError, ForbiddenActionError
from karadoc.common.job_core.has_vars import HasVars
from karadoc.common.job_core.job_base import JobBase
from karadoc.common.job_core.package import ActionFileMethod
from karadoc.common.model import file_index
from karadoc.common.table_utils import parse_table_name
from karadoc.common.utils.assert_utils import assert_true
from karadoc.spark.job_core.has_spark import HasSpark
from karadoc.spark.quality.checks import Alert, Metric

Job = TypeVar("Job", JobBase, JobBase)
A = TypeVar("A")
B = TypeVar("B")


def load_non_runnable_action_file(full_table_name: str, job_type: Type[Job]) -> Job:
    """Return a non-runnable version of the `job` object declared in the ACTION_FILE.py file of the given table
    with its default variables.

    This method is useful to load any ACTION_FILE for inspecting its metadata (input, output, output_partitions, etc.)
    even when no variable is defined, by using its default values. The `run/stream/analyze` method is removed to prevent
    any misuse where one would call the ACTION_FILE without setting the variables properly.

    If you want to execute the `run/stream/analyze` method use the `load_runnable_action_file` method instead.

    :param full_table_name: Full name of the table (schema_name.table_name)
    :param job_type: job type defined in the action file. (e.g. SparkBathJob, QualityCheckJob ...)
    :return: the SparkBatchJob object defined in the ACTION_FILE.
    """
    passed_vars = None

    job = __load_action_file(job_type, full_table_name, passed_vars)
    __unset_all_action_methods(job)
    return job


def load_runnable_action_file(full_table_name: str, job_type: Type[Job], passed_vars: Dict[str, str]) -> Job:
    """Return the `job` object declared in the ACTION_FILE.py file of the given table and override its variables,
    checking that they match the variables declared in the ACTION_FILE.py file.

    Explanation:

    When you call a command with the '--vars' option, this method will be called and the `vars` passed to
     the ACTION_FILE file in the job.vars dict.

    :param full_table_name: Full name of the table (schema_name.table_name)
    :param job_type: job type defined in the action file. (e.g. SparkBathJob, QualityCheckJob ...)
    :param passed_vars: Set the variables of the job and check that they match the ones declared
    :return: the ActionFileJob object defined in the ACTION_FILE.py file
    """
    if passed_vars is None:
        passed_vars = dict()
    return __load_action_file(job_type, full_table_name, passed_vars)


def __load_action_file(job_type: Type[Job], full_table_name: str, passed_vars: Optional[Dict[str, str]]) -> Job:
    assert_true(issubclass(job_type, JobBase))
    (schema_name, table_name, _) = parse_table_name(full_table_name)
    file_path = file_index.get_action_file(schema_name, table_name, job_type)
    if file_path is None:
        expected_path = Path(f"**/{schema_name}.db") / table_name / f"{job_type.get_action_file_name()}"
        raise FileNotFoundError(
            f"Found no file matching the path '{expected_path}' for the table {full_table_name} "
            f"in the directory '{conf.get_model_folder_location()}'"
        )
    file_path = cast(str, file_path)
    job = __load_file(schema_name, table_name, file_path, job_type.get_action_file_name(), passed_vars, job_type)
    return job


def check_method_signatures(method_name: str, actual: FunctionType, expected: FunctionType) -> None:
    """Ensures that the signature of a given method matches the expected signature taken from another method.

    :param method_name: name of the method to check
    :param actual: function to check
    :param expected: control function with the expected signature
    :return:
    """
    if not isinstance(actual, FunctionType):
        raise TypeError("%s is not a function" % method_name)
    actual_args = inspect.getfullargspec(actual)
    expected_args = inspect.getfullargspec(expected)
    if actual_args != expected_args:
        expected_signature = inspect.getsource(expected).split("\n")[0]
        raise TypeError(f'The method {method_name} should have the following signature\n"{expected_signature}"')


def __set_job_attr_if_exists(mod: ModuleType, attr_name: str) -> None:
    if hasattr(mod, attr_name):
        setattr(mod.job, attr_name, getattr(mod, attr_name))


def __delete_module_if_exists(module_name: str) -> None:
    if module_name in sys.modules:
        del sys.modules[module_name]


def __delete_template_modules() -> None:
    """
    Delete all template modules to avoid caching templates.
    Doing so will ensure the execution of the template content every load.
    """
    from karadoc.common import conf

    template_package_name = conf.get_template_package_name()
    template_modules = [m for m in sys.modules if m.startswith(template_package_name)]
    for m in template_modules:
        del sys.modules[m]


def __load_module_file(module_name: str, module_path: Union[str, Path]) -> ModuleType:
    """Loads an action file.

    Implementation details
    ----------------------

    If an __init__.py file is located in the same folder, we load the __init__.py file first.
    This allows relative imports inside action files.

    If a module with the same name was already loaded, we must unload it first.
    This does not happen in regular uses, but it may happen while running unit test suites

    :param module_name: Name of the module that will be loaded. It must be unique. Dots will be replaced by colons.
    :param module_path: Path of the action file to load
    :return: a Python module
    """
    # The module_name must not contain dots, this tends to cause errors in relative imports
    module_name = module_name.replace(".", ":")
    if isinstance(module_path, str):
        module_path = Path(module_path)
    file_name = splitext(module_path.name)[0]
    init_file = module_path.parent / "__init__.py"
    if isfile(init_file):
        __delete_module_if_exists(module_name)
        importlib.machinery.SourceFileLoader(module_name, str(init_file)).load_module()
    submodule_name = module_name + "." + file_name
    __delete_module_if_exists(submodule_name)
    mod = importlib.machinery.SourceFileLoader(submodule_name, str(module_path)).load_module()
    return mod


def __load_action_methods(mod: ModuleType) -> None:
    action_methods = [method for name, method in inspect.getmembers(mod.job) if isinstance(method, ActionFileMethod)]
    for method in action_methods:
        if hasattr(mod, method.name):
            method_defined_in_file = getattr(mod, method.name)
            method.set_method(method_defined_in_file)


def __unset_action_method(job: Job, method: ActionFileMethod):
    def empty_run() -> None:
        raise ForbiddenActionError(
            f"The {method.name} method of a job returned by the `load_non_runnable_action_file` "
            "method cannot be called. Use load_runnable_action_file instead."
        )

    job.__setattr__(method.name, empty_run)


def __unset_all_action_methods(job: Job) -> None:
    action_methods = [method for name, method in inspect.getmembers(job) if isinstance(method, ActionFileMethod)]

    for method in action_methods:
        __unset_action_method(job, method)


def _set_quality_check_job(mod: ModuleType) -> None:
    from karadoc.spark.quality.quality_check_job import QualityCheckJob

    if isinstance(mod.job, QualityCheckJob):
        found_alerts = [obj for name, obj in inspect.getmembers(mod) if isinstance(obj, Alert)]
        for alert in found_alerts:
            mod.job.add_alert(alert)
        found_metrics = [obj for name, obj in inspect.getmembers(mod) if isinstance(obj, Metric)]
        for metric in found_metrics:
            mod.job.add_metric(metric)


def __set_job_from_module(mod: ModuleType) -> JobBase:
    __load_action_methods(mod)
    _set_quality_check_job(mod)
    return mod.job


def __load_job_module(module_name: str, module_path: Union[str, Path]) -> JobBase:
    __delete_template_modules()
    mod = __load_module_file(module_name, module_path)
    job = __set_job_from_module(mod)
    return job


def __load_file(
    schema_name: str,
    table_name: str,
    file_path: str,
    file_type: str,
    passed_vars: Optional[Dict[str, str]],
    job_type: Type[Job],
) -> Job:
    full_table_name = schema_name + "." + table_name
    if file_path is None:
        raise ActionFileLoadingError(f"Could not find a {file_type} for table {full_table_name}")
    try:
        HasVars._global_vars = passed_vars
        HasSpark._global_init_lock = True
        job = __load_job_module(conf.APPLICATION_NAME.upper() + ":" + full_table_name, file_path)
        job.output = full_table_name
    except Exception as e:
        raise ActionFileLoadingError(
            f"Could not load {file_type} file for table {full_table_name} at {file_path}"
        ) from e
    finally:
        HasSpark._global_init_lock = False
        HasVars._global_vars = None
    if not isinstance(job, job_type):
        raise ActionFileLoadingError(
            f"The 'job' object returned by this {file_type} is not of the expected type."
            f"Expected Type: {job_type}, Type Found: {type(job)}"
        )
    return job
