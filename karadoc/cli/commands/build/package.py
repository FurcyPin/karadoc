import os
import zipfile
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import List, Union

import karadoc
from karadoc.common import conf
from karadoc.common.commands.command import Command

CONF_TARGET = "conf"
LIBS_TARGET = "libs"
MODEL_TARGET = "model"


def __write_to_zip(zip_file, source_path, dest_path, logger):
    """Add the file at the specified 'path' to the specified 'zip_file'"""
    zip_file.write(source_path, dest_path)
    if logger is not None:
        logger.info(f"adding {source_path} as {dest_path}")


def _add_folder_to_zip_file(input_dir: str, output_dir: str, zip_file: zipfile.ZipFile, logger=None):
    """Add the content of 'input_dir' to the 'zip_file' archive and ignore '__pycache__' folders"""
    for dirpath, dirnames, filenames in os.walk(input_dir):
        relpath = os.path.relpath(dirpath, input_dir)
        for name in sorted(dirnames):
            source_path = os.path.normpath(os.path.join(dirpath, name))
            dest_path = os.path.normpath(os.path.join(output_dir, relpath, name))
            if "__pycache__" not in name:
                __write_to_zip(zip_file, source_path, dest_path, logger)
        if "__pycache__" not in relpath:
            for name in filenames:
                source_path = os.path.normpath(os.path.join(dirpath, name))
                dest_path = os.path.normpath(os.path.join(output_dir, relpath, name))
                if os.path.isfile(source_path):
                    __write_to_zip(zip_file, source_path, dest_path, logger)


def _add_folder_to_python_zip_file(input_dir, zip_file: zipfile.PyZipFile, logger=None):
    """Add the content of 'input_dir' to the 'zip_file' archive."""
    zip_file.writepy(input_dir)
    if logger is not None:
        logger.info("adding '%s' to '%s'", input_dir, zip_file)


def make_zipfile(output_base_name: str,
                 input_dirs: List[str],
                 keep_root_folder: bool,
                 dry_run=False,
                 logger=None):
    """Create a zip file from all the files in the 'input_dirs'.

    The output zip file will be named 'output_base_name' + ".zip".
    Return the name of the output zip file.

    :param output_base_name:
    :param input_dirs:
    :param keep_root_folder: set to True if you want to keep the root folders in the zip file
    :param dry_run: set to True to perform a dry-run
    :param logger: pass logger
    :return:
    """

    zip_filename = output_base_name + ".zip"
    archive_dir = os.path.dirname(output_base_name)

    if archive_dir and not os.path.exists(archive_dir):
        if logger is not None:
            logger.info(f"creating {archive_dir}")
        if not dry_run:
            os.makedirs(archive_dir)

    if logger is not None:
        logger.info(f"creating '{zip_filename}' and adding '{input_dirs}' to it")

    zip_file_type = zipfile.ZipFile

    if not dry_run:
        with zip_file_type(zip_filename, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for input_dir in input_dirs:
                if keep_root_folder:
                    output_dir = str(Path(input_dir).name)
                else:
                    output_dir = ""
                _add_folder_to_zip_file(input_dir, output_dir, zf, logger)

    return zip_filename


class PackageCommand(Command):
    description = "(Experimental) create deployable archives in the target/ folder"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        parser.add_argument(
            '--target',
            dest='target',
            type=str,
            default='target',
            metavar='FOLDER',
            help='Write archives in target FOLDER'
        )

    @staticmethod
    def do_command(args: Namespace):
        target_folder = Path(args.target)

        karadoc_code_dir = str(Path(karadoc.__file__).parent)
        libs_folder = conf.get_libs_folder_location()
        libs = [str(Path(libs_folder) / f) for f in os.listdir(libs_folder)]
        input_dirs = [karadoc_code_dir] + libs
        PackageCommand.zip_folders(input_dirs=input_dirs,
                                   target_folder=target_folder / LIBS_TARGET,
                                   keep_root_folder=True)

        PackageCommand.zip_folders(input_dirs=conf.get_model_folder_location(),
                                   target_folder=target_folder / MODEL_TARGET,
                                   keep_root_folder=False)

        PackageCommand.zip_folders(input_dirs=conf.get_conf_folder_location(),
                                   target_folder=target_folder / CONF_TARGET,
                                   keep_root_folder=False)

    @staticmethod
    def zip_folders(input_dirs: Union[str, List[str]], target_folder: Union[str, Path], keep_root_folder: bool):
        """Create a zip file from all the files in the 'input_dirs'.

        Example
        let's concider the folder input_dir :
                input_dir/
                └── sub_dir
             zip_folders('input_dir', 'target', keep_root_folder=True)
             generates a target.zip containing :
                input_dir/
                   └── sub_dir
             zip_folders('input_dir', 'target', keep_root_folder=False)
             generates target.zip containing :
                sub_dir

        :param target_folder: the output zip file will be named 'target_folder' + ".zip".
        :param input_dirs: folders to zip
        :param keep_root_folder: set to True if you want to keep the root folders in the zip file
        """
        if isinstance(input_dirs, str):
            input_dirs = [input_dirs]
        output_zip = make_zipfile(output_base_name=str(target_folder),
                                  input_dirs=input_dirs,
                                  keep_root_folder=keep_root_folder)
        modules = ", ".join(input_dirs)
        print(f"Created '{output_zip}' containing the following modules: {modules}")
