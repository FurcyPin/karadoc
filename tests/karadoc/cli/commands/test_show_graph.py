import os
import shutil
import unittest
from unittest import mock

from networkx import DiGraph

import karadoc
from karadoc.test_utils.mock_settings import mock_settings_for_test, mock_settings_for_test_class
from tests.karadoc.test_utils import get_resource_folder_path

test_dir = "test_working_dir/show_graph"


def mock_render(*args, **kwargs):
    """This mock prevent us from calling the program 'dot' which is not installed on the CI hosts."""
    pass


@mock_settings_for_test_class()
@mock.patch("graphviz.Digraph.render", mock_render)
class TestShowGraph(unittest.TestCase):
    def setUp(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)
        os.makedirs(test_dir, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(test_dir, ignore_errors=True)

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": get_resource_folder_path(__name__) + "/models",
        }
    )
    def test_show_graph_without_tables_option(self):
        """
        GIVEN a model
        WHEN we run the "show_graph" command without specifying the --models option
        THEN all tables should be displayed
        """

        def inspect(graph: DiGraph):
            assert sorted(list(graph.nodes)) == ["schema.t0", "schema.t1", "schema.t12", "schema.t2"]
            assert sorted(list(graph.edges)) == [
                ("schema.t0", "schema.t1"),
                ("schema.t0", "schema.t2"),
                ("schema.t1", "schema.t12"),
                ("schema.t2", "schema.t12"),
            ]

        with mock.patch("karadoc.cli.commands.show_graph.inspect", side_effect=inspect) as mocked_inspect:
            karadoc.cli.run_command("show_graph")
        mocked_inspect.assert_called_once()

    @mock_settings_for_test(
        {
            "enable_file_index_cache": False,
            "model_dir": get_resource_folder_path(__name__) + "/models",
        }
    )
    def test_show_graph_with_upstream_and_downstream(self):
        """
        GIVEN a model
        WHEN we run the "show_graph" command by specifying a table upstream and downstream
        THEN only the tables upstream and downstream should be displayed
        """

        def inspect(graph: DiGraph):
            assert sorted(list(graph.nodes)) == ["schema.t0", "schema.t1", "schema.t12"]
            assert sorted(list(graph.edges)) == [("schema.t0", "schema.t1"), ("schema.t1", "schema.t12")]

        with mock.patch("karadoc.cli.commands.show_graph.inspect", side_effect=inspect) as mocked_inspect:
            karadoc.cli.run_command("show_graph --models +schema.t1+")
        mocked_inspect.assert_called_once()
