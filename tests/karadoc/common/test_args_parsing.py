import unittest
from argparse import ArgumentParser

from karadoc.cli.commands.run import RunCommand
from karadoc.common.commands.exec import add_commands_to_parser


class ArgsParsing(unittest.TestCase):
    def test_sub_commands(self):
        parser = ArgumentParser()
        add_commands_to_parser(parser, {"run": RunCommand}, command_depth=0)
        args = parser.parse_args(["run", "--dry", "--models", "schema.table1", "schema.table2"])
        self.assertEqual(args.command_0, "run")
        self.assertEqual(args.models, ["schema.table1", "schema.table2"])

    def test_variable_parsing(self):
        parser = ArgumentParser()
        add_commands_to_parser(parser, {"run": RunCommand}, command_depth=0)
        args = parser.parse_args(["run", "--vars", "k1=v1", "k2=v2", "--models", "schema.table1", "schema.table2"])
        self.assertEqual(args.command_0, "run")
        self.assertEqual(args.vars, {"k1": "v1", "k2": "v2"})
        self.assertEqual(args.models, ["schema.table1", "schema.table2"])
