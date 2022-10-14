import os
from argparse import ArgumentParser, Namespace
from pathlib import Path

from karadoc.common import conf
from karadoc.common.commands.command import Command

bash_script = f"""
# This script is based from a script generated with https://github.com/kislyuk/argcomplete

# Run something, muting output or redirecting it to the debug stream
# depending on the value of _ARC_DEBUG.
# If ARGCOMPLETE_USE_TEMPFILES is set, use tempfiles for IPC.
__python_argcomplete_run() {{
    if [[ -z "${{ARGCOMPLETE_USE_TEMPFILES-}}" ]]; then
        __python_argcomplete_run_inner "$@"
        return
    fi
    local tmpfile="$(mktemp)"
    _ARGCOMPLETE_STDOUT_FILENAME="$tmpfile" __python_argcomplete_run_inner "$@"
    local code=$?
    rm "$tmpfile"
    return $code
}}

__python_argcomplete_run_inner() {{
    if [[ -z "${{_ARC_DEBUG-}}" ]]; then
        "$@" 8>&1 9>&2 1>/dev/null 2>&1
    else
        "$@" 8>&1 9>&2 1>&9 2>&1
    fi
}}

_python_argcomplete() {{
    local IFS=$'\\013'
    local SUPPRESS_SPACE=1
    COMPREPLY=( $(IFS="$IFS" \\
                  COMP_LINE="$COMP_LINE" \\
                  COMP_POINT="$COMP_POINT" \\
                  COMP_TYPE="$COMP_TYPE" \\
                  _ARGCOMPLETE_COMP_WORDBREAKS="$COMP_WORDBREAKS" \\
                  _ARGCOMPLETE=1 \\
                  _ARGCOMPLETE_SUPPRESS_SPACE=$SUPPRESS_SPACE \\
                  __python_argcomplete_run "$1") )
    if [[ $? != 0 ]]; then
        unset COMPREPLY
    fi
}}
complete -o nospace -o default -o bashdefault -F _python_argcomplete {conf.APPLICATION_NAME}
"""


class InstallAutocompleteCommand(Command):
    description = "install autocomplete. Only bash is supported at the moment"

    @staticmethod
    def add_arguments(parser: ArgumentParser):
        # This command takes no arguments
        pass

    @staticmethod
    def do_command(args: Namespace):  # pragma: no cover
        if "bash" not in os.environ["SHELL"]:
            raise NotImplementedError("Autocomplete installation is only supported for bash at the moment")
        home = Path.home()
        bash_completion_dir = home / ".bash_completion.d"
        bash_completion_dir.mkdir(parents=True, exist_ok=True)
        bash_completion_script_path = bash_completion_dir / conf.APPLICATION_NAME
        with open(bash_completion_script_path, "w") as file:
            file.write(bash_script)
        bash_rc_file = home / ".bashrc"
        source_string = f"source {bash_completion_script_path.absolute()}"
        with open(bash_rc_file, "r") as r_file, open(bash_rc_file, "a") as a_file:
            if source_string not in r_file.read():
                a_file.write(source_string + "\n")
