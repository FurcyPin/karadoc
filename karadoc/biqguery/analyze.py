from typing import Iterable, List, Optional

import pandas as pd
from bigquery_frame import BigQueryBuilder, DataFrame
from bigquery_frame import functions as f
from bigquery_frame.transformations_impl.analyze import analyze
from google.cloud import bigquery
from google.oauth2 import service_account

from karadoc.common import conf
from karadoc.common.output.local_export import write_pandas_dataframes_to_xlsx
from karadoc.common.utils.assert_utils import assert_true


def _generate_empty_line_df(bq: BigQueryBuilder) -> DataFrame:
    df = bq.sql(
        """
        SELECT
            CAST(NULL as STRING) as `table_name`,
            CAST(NULL as STRING) as `column_name`,
            CAST(NULL as STRING) as `column_type`,
            CAST(NULL as INT) as `count`,
            CAST(NULL as INT) as `count_distinct`,
            CAST(NULL as INT) as `count_null`,
            CAST(NULL as STRING) as `min`,
            CAST(NULL as STRING) as `max`,
            CAST(NULL as STRING) as `approx_top_100`,
        """
    )
    return df


def _get_gcp_credentials_path(env: str):
    env_conf = conf.get_connection_conf(env)
    assert_true("bigquery" in env_conf.get("type"))
    json_keyfile = env_conf.get("json_keyfile")
    return json_keyfile


def _get_bq_client(gcp_credentials_path: str) -> bigquery.Client:
    if gcp_credentials_path.endswith(".json"):
        credentials = service_account.Credentials.from_service_account_file(
            gcp_credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        return client
    else:
        raise ValueError(f"Expected a .json file, got : {gcp_credentials_path}")


def _analyze_table(table_name: str, bq: BigQueryBuilder) -> Iterable[DataFrame]:
    print(table_name + ":")
    df = analyze(bq.table(table_name)).persist()
    MAX_CHARS = 20
    df = df.withColumn(
        "approx_top_100",
        f.expr(
            f"""
            ARRAY(
              SELECT
                CONCAT(
                    '(',
                    SUBSTRING(x.value, 0, {MAX_CHARS}),
                    CASE WHEN LENGTH(x.value) > 10 THEN '...' ELSE '' END,
                    ',',
                    CAST(x.count as STRING),
                    ')'
                )
              FROM UNNEST(approx_top_100) x
            )
            """
        ),
        replace=True,
    )
    df = df.withColumn(
        "approx_top_100", f.expr("""CONCAT('[', ARRAY_TO_STRING(approx_top_100, ", ") , ']')"""), replace=True
    )
    yield df.select(f.lit(table_name).alias("table_name"), *df.columns)
    yield _generate_empty_line_df(bq)


def analyze_tables(tables: List[str], env: str, output: Optional[str]):
    json_keyfile = _get_gcp_credentials_path(env)
    client = _get_bq_client(json_keyfile)
    bq = BigQueryBuilder(client)

    pdfs = [df.toPandas() for table in tables for df in _analyze_table(table, bq)]
    if output is None:
        output = "report.xlsx"

    # We perform the union in Pandas because unions with bigquery-frame do not preserve ordering.
    union_pdf = pd.concat(pdfs)
    write_pandas_dataframes_to_xlsx(union_pdf, output, index=False, freeze_panes=(1, 0))
