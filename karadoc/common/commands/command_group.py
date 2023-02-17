from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace
from typing import Dict, Optional, Type

from karadoc.common.commands.command import Command
from karadoc.common.commands.exec import add_commands_to_parser, do_command, load_commands_from_package
from karadoc.common.commands.return_code import ReturnCode


class CommandGroup(Command, ABC):
    """A group of commands.

    Command groups are defined via a python module with the following structure:
    - An __init__.py file containing an implementation of CommandGroup
    - One python file per command in the group each one containing an implementation of Command

    A command group can also contain other command groups.

    A CommandGroup implementation must implement the following element:

    - description: str.

    Example of command group structure
    ----------------------------------

    The example file structure below will define the following commands
    - karadoc build validate
    - karadoc build package

        # commands/build/__init__.py :
        class BuildCommandGroup(CommandGroup):
            description = "set of commands used for code validation and packaging"

        # commands/build/validate.py :
        class ValidateCommand(Command):
            description = "apply validation checks on all tables and connections referenced in the project"

            @staticmethod
            def add_arguments(parser: ArgumentParser):
                ...

            @staticmethod
            def do_command(args: Namespace) -> Optional[ReturnCode]:
                ...

        # commands/build/package.py :
        class PackageCommand(Command):
            description = "create a deployable package of the project"

            @staticmethod
            def add_arguments(parser: ArgumentParser):
                ...

            @staticmethod
            def do_command(args: Namespace) -> Optional[ReturnCode]:
                ...
    """

    parser: ArgumentParser
    sub_commands_by_name: Dict[str, Type[Command]]

    @property
    @abstractmethod
    def description(self) -> str:
        """Description of the command group which will be displayed in the help"""
        pass

    @classmethod
    def add_arguments(cls, parser: ArgumentParser) -> None:
        cls.parser = parser
        cls.sub_commands_by_name = load_commands_from_package(
            cls._get_command_module(), command_depth=cls.command_depth + 1
        )
        add_commands_to_parser(parser, cls.sub_commands_by_name, command_depth=cls.command_depth + 1)

    @classmethod
    def do_command(cls, args: Namespace) -> Optional[ReturnCode]:
        return do_command(args, cls.parser, cls.sub_commands_by_name, command_depth=cls.command_depth + 1)
