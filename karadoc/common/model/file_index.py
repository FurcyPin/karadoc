import os
from functools import lru_cache
from typing import Dict, Generator, Iterable, Optional, Tuple, Type

from karadoc.common import conf
from karadoc.common.job_core.job_base import JobBase
from karadoc.common.utils.assert_utils import assert_true


def _is_hidden(folder: str):
    """We consider that a folder is hidden if at least one of it's ancestor folder's name starts with a '.' or '_'"""
    normalized_path = os.path.normpath(folder)
    ancestors = normalized_path.split(os.sep)
    for ancestor in ancestors:
        if ancestor.startswith(".") or ancestor.startswith("_"):
            return True
    return False


# We use a cache here to avoid recomputing everything all the time.
@lru_cache(maxsize=100)
def __index_files_with_name_cached(file_name: Optional[str]) -> Dict[str, Dict[str, str]]:
    """Recursively scan the model folder and returns all the relative paths of the files
    indexed by schema and table names.
    If file_name is None, return the path of the model folders that contain at least one file.

    :param file_name: name of the files to match
    :return:
    """
    schemas = {}
    for schema, table, folder in list_schema_table_folders():
        for file in os.listdir(folder):
            if file_name is None:
                schemas.setdefault(schema, {})[table] = folder
                break
            if file == file_name:
                schemas.setdefault(schema, {})[table] = os.path.join(folder, file)

    return schemas


def __index_files_with_name(file_name: Optional[str]) -> Dict[str, Dict[str, str]]:
    """Recursively scan the model folder and returns all the relative paths of the files
    indexed by schema and table names
    :param file_name: name of the files to match
    :return:
    """
    # For unit tests, we disable the cache because the model directory may change between tests.
    if not conf.enable_file_index_cache():
        __index_files_with_name_cached.cache_clear()
    return __index_files_with_name_cached(file_name)


def list_schema_table_folders() -> Generator:
    """Recursively scan the model folder and returns all the triplets
        (schema_name, table_name, table_folder_path)
    :return: A generator with all (schema_name, table_name, table_folder_path)
    """
    model_location = conf.get_model_folder_location()
    for (dir, subdirs, files) in os.walk(model_location):
        if not _is_hidden(os.path.relpath(dir, model_location)) and dir.endswith(".db"):
            for subdir in subdirs:
                if not subdir.startswith(".") and not subdir.startswith("_"):
                    schema_name = os.path.split(dir)[1][:-3]
                    table_name = subdir
                    yield schema_name, table_name, os.path.join(dir, subdir)


def __get_indexed_action_file(action_file_name: Optional[str]) -> Dict[str, Dict[str, str]]:
    """Recursively scan the model folder and returns all ACTION_FILE.py files relative paths
    indexed by schema and table names
    :return: A dict of dict
    """
    return __index_files_with_name(action_file_name)


def list_action_files(job_type: Type[JobBase]) -> Iterable[Tuple[str, str, str]]:
    """List all action files corresponding to jobs of type job_type in the project
    :param job_type: job type defined in the action file. (e.g. SparkBathJob, QualityCheckJob ...)
    :return: A generator with all STREAM.py paths
    """
    assert_true(issubclass(job_type, JobBase))
    for schema, tables in __get_indexed_action_file(job_type.get_action_file_name()).items():
        for table, stream in tables.items():
            yield schema, table, stream


def get_action_file(schema_name: str, table_name: str, job_type: Type[JobBase] = None) -> Optional[str]:
    """Recursively scan the model folder and return the path of the ACTION_FILE.py file for the given table_name
    :param schema_name: name of the schema
    :param table_name: name of the table
    :param job_type: job type defined in the action file. (e.g. SparkBathJob, QualityCheckJob ...)
    :return: A string path
    """
    if job_type is None:
        action_file_name = None
    else:
        assert_true(issubclass(job_type, JobBase))
        action_file_name = job_type.get_action_file_name()
    result = __get_indexed_action_file(action_file_name).get(schema_name, {}).get(table_name)
    if result:
        return result
    else:
        return None
