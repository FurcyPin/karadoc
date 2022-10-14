import sys
from typing import List

from karadoc.common.commands.exec import run_command
from karadoc.common.commands.return_code import ReturnCode


def main(argv: List[str] = None):
    if argv is None:
        argv = sys.argv[1:]
    return_code = ReturnCode.Error
    try:
        return_code = run_command(argv)
        sys.exit(return_code)
    except Exception:
        sys.exit(return_code)
    finally:
        # This if statement prevent loading pyspark when we don't use it
        if "pyspark" in sys.modules:
            from pyspark.sql import SparkSession

            if SparkSession._instantiatedSession is not None:
                SparkSession._instantiatedSession.stop()


if __name__ == "__main__":
    main()
