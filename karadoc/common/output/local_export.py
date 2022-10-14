from io import StringIO
from typing import TYPE_CHECKING, Dict, Optional, Union

from pyspark.sql import DataFrame

if TYPE_CHECKING:
    import pandas as pd

default_encoding = "UTF-8"


def write_pandas_dataframes_to_xlsx(
    dfs: Union["pd.DataFrame", Dict[str, "pd.DataFrame"]], output: str, **options
) -> None:
    """Writes one or multiple DataFrames (as dicts) in an Excel file (with one sheet per DataFrame if multiple
    DataFrames were given).

    Extra options will be passed through to the
    `pandas.DataFrame.to_excel()
    <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_excel.html>`_ method

    Code has been shamelessly pumped from `StackOverflow
    <https://stackoverflow.com/questions/17326973/is-there-a-way-to-auto-adjust-excel-column-widths-with-pandas-excelwriter>`_

    Known Limitations
    -----------------
    When exporting multiple DataFrames, the sheet name length must not exceed 31 characters.

    :param dfs: A Pandas DataFrame or a Dict of multiple (sheet_name, DataFrame)
    :param output: the path of the output Excel file
    :param options: extra options to pass to `pandas.DataFrame.to_excel()`
    :return:
    """
    import pandas as pd

    if isinstance(dfs, pd.DataFrame):
        dfs = {"Sheet1": dfs}
    writer = pd.ExcelWriter(output, engine="xlsxwriter")
    for sheetname, df in dfs.items():  # loop through `dict` of dataframes
        df.to_excel(writer, sheet_name=sheetname, **options)  # send df to writer
        worksheet = writer.sheets[sheetname]  # pull worksheet object
        for idx, col in enumerate(df):  # loop through all columns
            series = df[col]
            max_len = (
                max(
                    (
                        series.astype(str).map(len).max(),  # len of largest item
                        len(str(series.name)),  # len of column name/header
                    )
                )
                + 1
            )  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width
    writer.save()


def __export_string(string: str, output: Optional[str]) -> None:
    if output is None:
        print(string)
    else:
        with open(output, "w") as f:
            f.write(string)


def local_export_dataframe(df: DataFrame, output: Optional[str], format: str) -> None:
    """Export a Spark DataFrame on the local filesystem with the given output and format.

    Supported formats are:
    - markdown
    - csv
    - xlsx

    If no output is specified, the result will be directly printed on stdout, except for binary formats (e.g. xlsx)
    which requires an output path.

    :param df: a Spark DataFrame
    :param output: (Optional) the output file path
    :param format: Either one of ["markdown", "csv", "xlsx"]
    :return:
    """
    pdf = df.toPandas()
    if format == "xlsx":
        if output is None:
            raise ValueError(f"The output file path is required when exporting to binary format {format}")
        write_pandas_dataframes_to_xlsx(pdf, output, index=False, freeze_panes=(1, 0))
        print(f"Output written to file {output}")
    elif format == "csv":
        if output is None:
            buffer = StringIO()
            pdf.to_csv(buffer, index=False)
            print(buffer.getvalue().encode(default_encoding))
        else:
            pdf.to_csv(output, index=False, encoding=default_encoding)
            print(f"Output written to file {output}")
    elif format == "markdown":
        string = pdf.to_markdown(index=False)
        __export_string(string, output)
    elif format == "txt":
        string = "\n".join([row[0] for row in df.collect()])
        __export_string(string, output)
    else:
        raise ValueError(f"Unknown format: {format}")


def local_export_dataframes_xlsx(dataframes: Dict[str, DataFrame], output: str) -> None:
    """Export multiple Spark DataFrames on the local filesystem as an Excel file, with one tab per DataFrame.

    Supported formats are:
    - markdown
    - csv
    - xlsx

    If no output is specified, the result will be directly printed on stdout, except for binary formats (e.g. xlsx)
    which requires an output path.

    :param df: a Spark DataFrame
    :param output: (Optional) the output file path
    :param format: Either one of ["markdown", "csv", "xlsx"]
    :return:
    """
    p_dataframes = {k: v.toPandas() for k, v, in dataframes.items()}
    if output is None:
        raise ValueError("The output file path is required when exporting to binary format xlsx")
    write_pandas_dataframes_to_xlsx(p_dataframes, output, index=False, freeze_panes=(1, 0))
    print(f"Output written to file {output}")
