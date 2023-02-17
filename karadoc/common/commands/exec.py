import glob
import importlib
import logging as _logging
import pkgutil
import re
import sys
import traceback
from argparse import ArgumentParser, Namespace
from os.path import basename, dirname, isfile
from typing import Dict, List, Optional, Sequence, Type, Union

from karadoc.common import conf
from karadoc.common.class_utils import find_class_from_module
from karadoc.common.commands.autocomplete.custom_completion_finder import CustomCompletionFinder
from karadoc.common.commands.command import Command
from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.conf import APPLICATION_DESCRIPTION, APPLICATION_NAME
from karadoc.common.exceptions import CommandValidationError, LoggedException
from karadoc.common.observability import LogEvent, get_logging_context
from karadoc.common.observability.conf import setup_observability
from karadoc.common.observability.console_event import ConsoleEvent
from karadoc.common.utils.assert_utils import assert_true
from karadoc.common.utils.stopwatch import Stopwatch

COMMAND_ROOT_DEPTH = 0
BUILTIN_COMMAND_PACKAGE = "karadoc.cli.commands"
modules = glob.glob(dirname(__file__) + "/*.py")
__all__ = [basename(f)[:-3] for f in modules if isfile(f) and not f.endswith("__init__.py")]
LOG = _logging.getLogger(__name__)


def __full_name(command: Type[Command]) -> str:
    return f"{command.__module__}.{command.__name__}"


def _check_not_abstract(command: Type[Command]):
    """Raise an exception if the class is abstract"""
    abstract_methods = set(getattr(command, "__abstractmethods__"))
    if len(abstract_methods) > 0:
        raise CommandValidationError(
            f"The class {__full_name(command)} should implement the following methods: {abstract_methods}"
        )


def _print_command_override_warning(
    command_name: str, overridden_command_full_name: str, overriding_command_full_name: str
) -> None:
    print(
        f"WARNING: command '{command_name}', "
        f"initially defined as {overridden_command_full_name}, "
        f"has been replaced with {overriding_command_full_name}"
    )


def _merge_command_packages(command_packages_with_names: List[Dict[str, Type[Command]]]) -> Dict[str, Type[Command]]:
    """Merge together several command packages, and print warnings whenever a command would be
    overridden by another one."""
    assert_true(len(command_packages_with_names) > 0, "No command package found")
    result = command_packages_with_names[0]
    for command_package in command_packages_with_names[1:]:
        for command_name, command in command_package.items():
            if command_name in result:
                _print_command_override_warning(
                    command_name=command_name,
                    overridden_command_full_name=__full_name(result[command_name]),
                    overriding_command_full_name=__full_name(command),
                )
            result[command_name] = command
    return result


def load_commands() -> Dict[str, Type[Command]]:
    """Load all commands from built-in and custom packages"""
    packages = [BUILTIN_COMMAND_PACKAGE] + conf.get_custom_command_packages()
    command_packages = [load_commands_from_package(package, command_depth=COMMAND_ROOT_DEPTH) for package in packages]
    merged_command_package = _merge_command_packages(command_packages)
    return merged_command_package


def load_commands_from_package(package_name: str, command_depth: int) -> Dict[str, Type[Command]]:
    """Automagically load all commands that are in specified module

    :param package_name: path to the python package or module containing the command
    :param command_depth: depth of the command
      (e.g. in "karadoc build package", 'build' has depth 0 and 'package' has depth 1)
    :return: a Dict(command_name, command)
    """

    def _load_commands():
        commands = importlib.import_module(package_name)
        for importer, modname, ispkg in pkgutil.iter_modules(commands.__path__):
            module_full_path = f"{package_name}.{modname}"
            module = importlib.import_module(module_full_path)
            command_class = find_class_from_module(module, Command)
            command_class.command_depth = command_depth
            yield command_class

    return {command._get_command_name(): command for command in _load_commands()}


def _dest(command_depth: int) -> str:
    """Return the name of the "args" attribute in which the command or subcommand will be stored, depending on its
    depth level.

    Explanation
    -----------

    As shown in the example below, the 'dest' attribute is set to command_0 for the command group and command_1 for
    the subcommand. They have to be distinct because they are both added to the flat `args` object returned by
    `parser.parse_args`. We need to use the `command_depth` to avoid collision between commands, subcommands,
    subsubcommands, etc.

    >>> parser = ArgumentParser()
    >>> sub_parsers = parser.add_subparsers(title='commands', dest='command_0')
    >>> sub_parser = sub_parsers.add_parser("custom_command_group")
    >>> sub_sub_parsers = sub_parser.add_subparsers(title='commands', dest='command_1')
    >>> sub_sub_parser = sub_sub_parsers.add_parser("sub_command")
    >>> args = parser.parse_args(["custom_command_group", "sub_command"])
    >>> args.command_0
    'custom_command_group'
    >>> args.command_1
    'sub_command'
    """
    return f"command_{command_depth}"


def add_commands_to_parser(parser: ArgumentParser, commands: Dict[str, Type[Command]], command_depth: int) -> None:
    dest = _dest(command_depth)
    subparsers = parser.add_subparsers(title="commands", metavar="", prog=APPLICATION_NAME.lower(), dest=dest)
    for command_name, command in commands.items():
        _check_not_abstract(command)
        subparser = subparsers.add_parser(command_name, help=command.description)
        subparser.description = command.description
        command.add_arguments(subparser)


def _split_command_line(command_line: str) -> List[str]:
    """Split a command string by whitespace characters

    :param command_line:
    :return:
    """
    return re.compile("\\s+").split(command_line)


def do_command(
    args: Namespace, parser: ArgumentParser, commands_by_name: Dict[str, Type[Command]], command_depth: int
) -> Optional[ReturnCode]:
    command_to_run = getattr(args, _dest(command_depth))
    if command_to_run is not None:
        assert_true(
            (command_to_run in commands_by_name),
            f"Probable Bug: command {command_to_run} was found by arg_parser but is not listed in commands_by_name",
        )
        return commands_by_name[command_to_run].do_command(args)
    else:
        parser.print_help()


def get_argument_parser(commands_by_name):
    parser = ArgumentParser(description=APPLICATION_DESCRIPTION, prog=APPLICATION_NAME.lower())
    parser.add_argument("-v", "--verbose", default=False, action="store_true", help="Activate verbose mode")
    add_commands_to_parser(parser, commands_by_name, command_depth=COMMAND_ROOT_DEPTH)
    CustomCompletionFinder()(parser, exit_method=sys.exit)
    return parser


def __run_command(argv: List[str]) -> ReturnCode:
    commands_by_name = load_commands()
    parser = get_argument_parser(commands_by_name)
    args = parser.parse_args(argv)
    args.raw_args = " ".join(argv)

    return_value = do_command(args, parser, commands_by_name, command_depth=COMMAND_ROOT_DEPTH)
    if return_value is not None:
        return return_value
    else:
        return ReturnCode.Success


def _get_command_end_extra_info(return_code: ReturnCode, stopwatch: Stopwatch) -> Dict[str, str]:
    return {"return_code": str(return_code), "duration_in_seconds": stopwatch.duration_in_seconds}


def _run_command_unsafe(command_line: Union[str, Sequence[str]]) -> ReturnCode:
    return_code = ReturnCode.Error
    stopwatch = Stopwatch()
    argv = _split_command_line(command_line) if isinstance(command_line, str) else command_line
    command = " ".join(argv)
    # Workaround: we detect the --verbose option as soon as possible before starting to log events.
    if "-v" in argv or "--verbose" in argv:
        verbose = True
    else:
        verbose = False
    setup_observability(reset=True, verbose=verbose)
    get_logging_context().command = command
    try:
        LOG.info(LogEvent(f"Command started: {command}", event_type="command_start"))
        return_code = __run_command(argv)
        extra = _get_command_end_extra_info(return_code, stopwatch)
        LOG.info(LogEvent(f"Command ended: {command}", event_type="command_end", extra=extra))
    except LoggedException as err:
        # If the exception has already been logged, we don't send a ConsoleEvent.
        extra = _get_command_end_extra_info(return_code, stopwatch)
        LOG.error(LogEvent(f"Command failed: {command}", event_type="command_end", extra=extra), exc_info=err)
        raise err
    except Exception as err:
        extra = _get_command_end_extra_info(return_code, stopwatch)
        LOG.error(ConsoleEvent(f"Command failed: {command}", event_type="command_end", extra=extra), exc_info=err)
        raise LoggedException() from err
    return return_code


def run_command(command_line: Union[str, Sequence[str]]) -> ReturnCode:
    """Execute a karadoc command line

    Examples:

    - run_command("run --tables s.t1 s.t2")
    - run_command(["run", "--tables", "s.t1", "s.t2"])

    :param command_line: A plain string command, or a list of arguments
    """
    try:
        return _run_command_unsafe(command_line)
    except LoggedException as err:
        raise err.__cause__
    except Exception as err:
        print("ERROR: An exception occurred before logging could be configured", file=sys.stderr)
        traceback.print_exc()
        raise err
