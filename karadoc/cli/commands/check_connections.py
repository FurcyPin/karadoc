import traceback
from argparse import ArgumentParser, Namespace

from termcolor import colored

from karadoc.common.commands.command import Command
from karadoc.common.commands.return_code import ReturnCode
from karadoc.common.commands.spark import init_job
from karadoc.common.conf import CONNECTION_GROUP, get_conn_conf_id
from karadoc.common.connector import load_connector


def check_connection(conn_name, job):
    return_code = ReturnCode.Success
    conn_conf_id = get_conn_conf_id(conn_name, group=CONNECTION_GROUP)
    try:
        connector = load_connector(conn_name, job.spark)
        disabled = connector.conf.get("disable", False)
        if disabled:
            print(colored(f"{conn_conf_id} ({connector.conf.type}): disabled", "white"))
        else:
            test_result = connector.test_connection()
            if test_result == NotImplemented:
                print(colored(f"{conn_conf_id} ({connector.conf.type}): not implemented", "yellow"))
            elif test_result:
                print(colored(f"{conn_conf_id} ({connector.conf.type}): ok", "green"))
            else:
                print(colored(f"{conn_conf_id} ({connector.conf.type}): not ok", "red"))
                return_code = ReturnCode.Error
    except Exception:
        traceback.print_exc()
        return_code = ReturnCode.Error
        print(colored(f"{conn_conf_id}: not ok", "red"))
    return return_code


def check_connections(args) -> ReturnCode:
    from karadoc.common import Job, conf

    job = Job()
    init_job(job=job, raw_args=args.raw_args)
    return_code = ReturnCode.Success
    for conn_name in sorted(conf.list_connection_names()):
        if not args.connections or conn_name in args.connections:
            return_code |= check_connection(conn_name, job)

    return return_code


class CheckConnectionsCommand(Command):
    description = "check all jdbc and mssql connections"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--connections",
            dest="connections",
            metavar="connections",
            type=str,
            nargs="*",
            help="If passed, only check the specified connections",
        )

    @staticmethod
    def do_command(args: Namespace):
        check_connections(args)
