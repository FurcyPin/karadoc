import os
import sys
from argparse import ArgumentParser, Namespace
from typing import Dict

from karadoc.common.commands.command import Command
from karadoc.common.commands.return_code import ReturnCode


def calc_container(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def get_imported_modules_with_size() -> Dict[str, int]:
    res = dict()
    for key in sorted(sys.modules.keys()):
        module = sys.modules[key]
        if hasattr(module, "__file__"):
            file = sys.modules[key].__file__
            if file:
                main_module = key.split(".")[0]
                module_size = os.path.getsize(file)
                if main_module not in res:
                    res[main_module] = module_size
                else:
                    res[main_module] += module_size
    return res


class NothingCommand(Command):
    description = "list all tables"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--fail-if-modules",
            dest="fail_if_modules",
            type=str,
            nargs="*",
            metavar="MODULE",
            help="return an error status if one of the specified package is found",
        )

    @staticmethod
    def do_command(args: Namespace) -> ReturnCode:
        modules_with_size = get_imported_modules_with_size()
        if args.fail_if_modules:
            failing_modules = [module for module in args.fail_if_modules if module in modules_with_size]
            if len(failing_modules) > 0:
                print(f"Found the following modules: {failing_modules}", file=sys.stderr)
                return ReturnCode.Error
            else:
                return ReturnCode.Success
        else:
            modules_sorted_by_size = {k: v for k, v in sorted(modules_with_size.items(), key=lambda item: item[1])}
            for module, size in modules_sorted_by_size.items():
                print(size, module)
            return ReturnCode.Success
