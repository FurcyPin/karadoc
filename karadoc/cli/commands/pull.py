import os
import shutil
from argparse import ArgumentParser, Namespace
from pathlib import Path

from karadoc.common import conf
from karadoc.common.commands.command import Command
from karadoc.common.utils.assert_utils import assert_true


def __pull_from_azure_blob(args, remote_env, schema_name, table_name):
    from azure.storage.blob import BlockBlobService
    from azure.storage.blob.models import Blob

    # tqdm is a nice library that wraps a list and automagically displays a progress bar
    from tqdm import tqdm

    storage_account = remote_env["storage_account"]
    storage_key = remote_env["storage_key"]
    container_name = remote_env["container_name"]
    warehouse_dir = remote_env["warehouse_dir"]

    blob_service = BlockBlobService(account_name=storage_account, account_key=storage_key)

    source_prefix = warehouse_dir
    table_relative_path = "%s.db/%s" % (schema_name, table_name)
    source_full_path = "%s/%s" % (source_prefix, table_relative_path)
    local_warehouse = conf.get_warehouse_folder_location()
    output_folder = "%s/%s" % (local_warehouse, table_relative_path)

    print(
        "input: %s" % ("wasbs://%s@%s.blob.core.windows.net/%s" % (container_name, storage_account, source_full_path))
    )
    print("output: %s" % output_folder)

    all_blobs = blob_service.list_blobs(container_name=container_name, prefix=source_full_path)
    file_blobs = [f for f in all_blobs if f.properties.content_length > 0]
    nb_files = len(file_blobs)

    if args.dry:
        print("dry run: would copy %s files" % nb_files)
    else:
        # we delete the destination folder first
        if os.path.exists(output_folder) and os.path.isdir(output_folder):
            shutil.rmtree(output_folder)

        f: Blob
        for f in tqdm(file_blobs):
            relative_path = f.name[len(source_prefix):]  # fmt: skip
            dest = "%s/%s" % (local_warehouse, relative_path)

            # create the destination directory first if it does not exist
            if not os.path.exists(str(Path(dest).parent)):
                os.makedirs(str(Path(dest).parent))

            # download the file
            blob_service.get_blob_to_path(container_name=container_name, blob_name=f.name, file_path=dest)

        print("copied %s files" % len(file_blobs))


def pull_table(args, table):
    print("pulling table %s from %s" % (table, args.from_env))
    a = table.split(".")
    schema_name = a[0]
    table_name = a[1] if len(a) > 1 else ""
    remote_env = conf.get_connection_conf(args.from_env)
    assert_true(
        (remote_env["type"] == "wasbs"),
        f"Remote environment {args.from_env} is of type {remote_env['type']}, which is not supported yet",
    )
    __pull_from_azure_blob(args, remote_env, schema_name, table_name)


class PullCommand(Command):
    description = "pull data from a remote environment to the local environment"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.add_argument("--dry", dest="dry", default=False, action="store_true", help="perform a dry-run")
        parser.add_argument("--tables", metavar="table", type=str, nargs="+", help="Names of the tables to pull")
        parser.add_argument(
            "--from",
            metavar="env",
            dest="from_env",
            required=True,
            type=str,
            help="Name of the remote environment to pull from",
        )

    @staticmethod
    def do_command(args: Namespace):
        for table in args.models:
            pull_table(args, table)
