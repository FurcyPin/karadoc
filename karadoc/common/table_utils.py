import os
import re
from typing import List, Optional, Tuple, Type

from karadoc.common.conf import get_model_folder_location
from karadoc.common.model import file_index


def _model_location():
    return get_model_folder_location()


def get_data_location(table_name: str) -> str:
    (schema_name, table_name, _) = parse_table_name(table_name)
    return "data/hive/warehouse/%s.db/%s" % (schema_name, table_name)


def get_folder_location(table_name: str) -> Optional[str]:
    (schema_name, table_name, _) = parse_table_name(table_name)

    path = file_index.get_action_file(schema_name, table_name)
    if path:
        return os.path.split(path)[0]
    else:
        return None


def parse_partition_name(partitions: Optional[str]) -> Optional[List[Tuple[str, str]]]:
    if partitions:
        res = []
        for p in partitions.split("/"):
            [k, v] = p.split("=")
            res.append((k, v))
        return res
    else:
        return None


def remove_partition_from_name(full_table_name: str) -> str:
    schema, table, _ = parse_table_name(full_table_name)
    return "%s.%s" % (schema, table)


def parse_table_name(table_name: str) -> Tuple[str, str, str]:
    t = """[_A-Za-z0-9]+"""
    table_regex: re.Pattern[str] = re.compile(rf"""\A({t})[.]({t})(?:/(.*))?\Z""")
    matcher = table_regex.match(table_name)
    if matcher:
        [schema_name, table_name, partitions] = matcher.groups()
        return schema_name, table_name, parse_partition_name(partitions)
    else:
        raise ValueError("Incorrect table name : %s" % table_name)


def table_exists(table_name: str) -> bool:
    folder_location = get_folder_location(table_name)
    if folder_location:
        return os.path.isdir(folder_location)
    else:
        return False


def populate_exists(full_table_name: str) -> bool:
    """
    :return: True if the given table has a {spark.batch}.py
    """
    from karadoc.common.run.spark_batch_job import SparkBatchJob

    return __action_file_exists(full_table_name, SparkBatchJob)


def quality_check_exists(full_table_name: str) -> bool:
    """
    :return: True if the given table has a {spark.quality_check}.py
    """
    from karadoc.common.quality.quality_check_job import QualityCheckJob

    return __action_file_exists(full_table_name, QualityCheckJob)


def stream_file_exists(full_table_name: str) -> bool:
    """
    :return: True if the given table has a {stream.batch}.py
    """
    from karadoc.common.stream.spark_stream_job import SparkStreamJob

    return __action_file_exists(full_table_name, SparkStreamJob)


def __action_file_exists(full_table_name: str, job_type: Type) -> bool:
    """
    :return: True if the given table has an ACTION_FILE.py
    """
    (schema_name, table_name, _) = parse_table_name(full_table_name)
    path = file_index.get_action_file(schema_name, table_name, job_type)
    if path is None:
        return False
    else:
        return os.path.isfile(path)
