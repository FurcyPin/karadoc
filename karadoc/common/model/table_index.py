from typing import Dict, List, Optional

from karadoc.common import table_utils
from karadoc.common.model import file_index
from karadoc.common.run import load_populate


class Populate:
    input_tables: List[str] = []
    disabled: bool = False
    partitions: List[str] = []
    external_inputs: Dict = {}
    external_outputs: Dict = {}
    vars: Dict[str, str] = {}
    hide_inputs: bool = False

    def __init__(self, full_table_name):
        job = load_populate(full_table_name)
        self.input_tables = [table_utils.remove_partition_from_name(table) for table in job.list_input_tables()]
        self.disabled = job.disable
        self.partitions = job.output_partition_names
        self.external_inputs = job.external_inputs
        self.external_outputs = job.external_outputs
        self.vars = job.vars
        self.hide_inputs = job.hide_inputs


class Table:
    def __init__(self, full_name):
        self.full_name: str = full_name
        self.populate: Optional[Populate] = None
        if table_utils.populate_exists(full_name):
            self.populate = Populate(full_name)


def build_table_index() -> Dict[str, Table]:
    """Returns a dict (full_table_name -> table information) of all tables declared in Karadoc's model"""
    res = {}
    for (schema, table, path) in file_index.list_schema_table_folders():
        full_table_name = "%s.%s" % (schema, table)
        res[full_table_name] = Table(full_table_name)
    return res
