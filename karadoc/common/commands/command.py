from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace
from typing import Optional

from karadoc.common.commands.return_code import ReturnCode


class Command(ABC):
    """Implement this class to define new karadoc commands.

    Each command class must be implemented in a separate python file.

    A Command implementation must implement the following elements:

    - description: str.
    - add_arguments(parser: ArgumentParser).
    - do_command(args: Namespace) -> Optional[ReturnCode]:
    """

    command_depth: int
    """Depth of the command, set by the command loader.
      (e.g. in "karadoc build package", 'build' has depth 0 and 'package' has depth 1)
    """

    @classmethod
    def _get_command_module(cls) -> str:
        """Get the name of the module containing the current command"""
        return cls.__module__

    @classmethod
    def _get_command_name(cls) -> str:
        """Get the name of the module containing the current command"""
        return cls._get_command_module().split(".")[-1]

    @property
    @abstractmethod
    def description(self) -> str:
        """Description of the command which will be displayed in the help"""
        pass

    @staticmethod
    @abstractmethod
    def add_arguments(parser: ArgumentParser) -> None:
        """Method used to set the options and arguments of the command via parser.add_argument()"""
        pass

    @staticmethod
    @abstractmethod
    def do_command(args: Namespace) -> Optional[ReturnCode]:
        """Entrypoint of the command.
        This method can return nothing if its only way of failing is raising an Exception.
        Commands that can fail without raising exceptions (e.g. a validation command) should return a ReturnCode.
        """
        pass
