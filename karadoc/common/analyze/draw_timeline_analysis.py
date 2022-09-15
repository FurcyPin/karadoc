import os
from math import ceil
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

FEATURES = ["val_count", "val_frequency"]
COLOR_PAL = [
    {"sunset_orange": "#ff5050"},
    {"gray": "#828282"},
    {"aqua_forest": "#5FA364"},
    {"blue_bayoux": "#4C5C6F"},
    {"pacific_blue": "#00A6BC"},
    {"flamingo": "#F26533"},
    {"mondo": "#504136"},
    {"malta": "#BCAF9C"},
    {"double_colonial_white": "#EEE8AA"},
    {"orange_peel": "#E69F00"},
    {"cornflower_blue": "#6495ED"},
    {"olive": "#808000"},
]


def __null_fields_to_missing_or_other(df: DataFrame) -> DataFrame:
    """Replace all null values in columns with the string 'MISSING' or 'OTHER'
    depending on the value of 'is_other' boolean.
    :param df: a DataFrame
    :return:
    """
    df_res = df
    data_types = ["string"]
    data_cols = [col_name for col_name, type in df.dtypes if type in data_types]
    for column in data_cols:
        df_res = df_res.withColumn(
            column,
            f.when(((f.col(column).isNull()) & (f.col("is_other") == "false")), f.lit("MISSING"))
            .when(((f.col(column).isNull()) & (f.col("is_other") == "true")), f.lit("OTHER_BUCKET"))
            .otherwise(f.col(column)),
        )
    return df_res


def __configure_draw(df: DataFrame, cols: List[str]):
    """
    Create a dictionary with the constant elements (figure, dataset, status list...) used to draw the timeline
    :param df: A Dataframe
    :param cols: The name of the columns to use as groups for the analysis
    :return: A dictionary with a Pandas dataframe, a figure (Figure object) and its axes (Axes object), the number
        of distinct keys used, a list of distinct status if it exists: [dataset, fig, axs, nb_keys, status_list]
    """
    if cols[1] == "":
        has_status_cohort = False
        dataset = (
            df.withColumnRenamed(cols[0], "cohort")
            .withColumn("status", f.lit(None).cast(StringType()))
            .withColumn("val_frequency", f.round(f.col("val_frequency"), 2))
            .orderBy("cohort", "val_count")
            .toPandas()
        )
        status_list = [""]
        keys = dataset["key"].unique()
        figsize = (3 * len(keys) + 11, 3 * len(keys) + 2)
        return dataset, figsize, keys, status_list, has_status_cohort
    else:
        has_status_cohort = True
        dataset = (
            df.withColumnRenamed(cols[0], "cohort")
            .withColumnRenamed(cols[1], "status")
            .withColumn("val_frequency", f.round(f.col("val_frequency"), 2))
            .orderBy("cohort", "val_count")
            .toPandas()
        )
        status_list = dataset["status"].unique()
        keys = dataset["key"].unique()
        figsize = (80, 3 * len(keys) + 2)
        return dataset, figsize, keys, status_list, has_status_cohort


def __get_sorted_vals(dataset, key):
    vals = dataset[dataset.key == key]["val"].unique()
    sorted_vals = []
    if "MISSING" in vals:
        sorted_vals += ["MISSING"]
    if "OTHER_BUCKET" in vals:
        sorted_vals += ["OTHER_BUCKET"]
    sorted_vals += [i for i in vals if i not in {"MISSING", "OTHER_BUCKET"}]
    return sorted_vals


def draw(df: DataFrame, cohort_cols: List[str], filepath: str, filename: str):  # NOSONAR
    """
    Draw a stackplot with the given dataframe into a .png file with the val_count draw (left) and the frequency draw
    (right) for the given cohorts
    :param df: A Dataframe
    :param cohort_cols: The name of the columns to use as groups for the analysis
    :param filepath: The path of the .png file created
    :param filename: The name of the .png file created
    :return:
    """
    import matplotlib.pyplot as plt
    import numpy as np
    from matplotlib import use
    from pandas.plotting import register_matplotlib_converters

    register_matplotlib_converters()
    use("Agg")

    df_analysed = __null_fields_to_missing_or_other(df)
    dataset, figsize, keys, status_list, has_status_cohort = __configure_draw(df_analysed, cohort_cols)
    nb_keys = len(keys)
    fig, axs = plt.subplots(nb_keys, 2 * len(status_list), figsize=figsize)
    x_axis, legend = None, None
    for pos, key in enumerate(keys):
        nb = 0
        for nb_status, status in enumerate(status_list):
            for nb_tables, feature in enumerate(FEATURES):
                p = []
                labels = []
                nb_tables = nb_tables + nb_status + nb
                sorted_vals = __get_sorted_vals(dataset, key)
                for i, val_local in enumerate(sorted_vals):
                    row_filter = (dataset.val == val_local) & (dataset.key == key)
                    if has_status_cohort:
                        row_filter &= dataset.status == status
                    if i == 0:
                        x_axis = np.array(dataset[row_filter]["cohort"])
                        p.append(x_axis)
                    p.append(np.array(dataset[row_filter][feature]))
                    labels.append(val_local)
                custom_axs = axs[nb_tables] if nb_keys <= 1 else axs[pos][nb_tables]
                custom_axs.stackplot(*p, colors=[list(a.values())[0] for a in COLOR_PAL], alpha=0.6)
                custom_axs.set_xticks(x_axis[:: int(ceil(0.086 * len(x_axis) + 0.091))])
                custom_axs.margins(x=0, y=0)
                plt.setp(custom_axs.xaxis.get_majorticklabels(), rotation=45, fontsize="x-small")
                custom_axs.set_title(feature + " " + key) if cohort_cols[1] == "" else custom_axs.set_title(
                    feature + " " + key + " for " + status
                )
                custom_axs.set_ylabel(feature)
                legend = custom_axs.legend(labels=labels, ncol=1, loc=2, bbox_to_anchor=(1, 1.01), prop={"size": 6.5})
            nb += 1
    plt.tight_layout()
    if not os.path.isdir(filepath):
        os.makedirs(filepath)
    plt.savefig(filepath + "/" + filename + ".png", bbox_extra_artists=(legend,), bbox_inches="tight", dpi=180)
    plt.close()
