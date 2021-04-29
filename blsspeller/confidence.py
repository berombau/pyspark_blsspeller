from math import factorial
from operator import mul
from functools import reduce
from blsspeller.consts import IUPAC, IUPAC_COMPLEMENT
from operator import or_
from functools import reduce
from pyspark.sql.functions import col, lit
from pyspark.sql import functions as F

import pandas as pd
import numpy as np

def get_calc_background_function(n_thresholds, bg_columns):
    def calc_background(pdf):
        group = pdf["group"].iloc[0]
        max_motifs = factorial(len(group)) // reduce(
            mul, [factorial(group.count(l)) for l in set(group)], 1
        )
        half = (max_motifs + 1) / 2
        found_motifs = len(pdf)
        if max_motifs == 1:
            arr = pdf.loc[0, ~pdf.columns.isin(['group', 'motif'])]
        elif found_motifs < half:
            arr = np.zeros(n_thresholds)
        else:
            arr = np.quantile(
                pdf.loc[:, ~pdf.columns.isin(['group', 'motif'])], 1 - half / found_motifs, axis=0, interpolation="lower"
            )
        series = pd.Series(arr, index=bg_columns)
        df = pd.DataFrame([series])
        df.insert(0, "group", group)
        return df
    return calc_background

def get_confidence_score(df, args, n_thresholds, schema_bg, fc_columns, bg_columns, conf_columns):
    calc_background = get_calc_background_function(n_thresholds, bg_columns)

    df_bg = df.groupby("group").applyInPandas(
        calc_background,
        schema=schema_bg,
    )

    df = df.join(df_bg, "group")

    for conf, bg, fc in zip(conf_columns, bg_columns, fc_columns):
        df = df.withColumn(
            conf,
            F.when(col(fc) > col(bg), lit(1.0) - col(bg) / col(fc)).otherwise(lit(0.0)),
        )

    df = df.filter(
        reduce(
            or_,
            (
                (((col)(fc) >= args["fc_cutoff"]) & (col(conf) >= args["conf_cutoff"]))
                for (fc, conf) in zip(fc_columns, conf_columns)
            ),
        )
    )

    df = df.withColumn(
        "motif_rc",
        F.reverse(F.translate(col("motif"), IUPAC, IUPAC_COMPLEMENT)),
    )

    df = df.withColumn(
        "group_sc",
        F.concat_ws(
            "",
            F.sort_array(
                F.split(F.translate(col("group"), IUPAC, IUPAC_COMPLEMENT), "")
            ),
        ),
    )

    df = df.filter(
        (~(col("group") == col("group_sc"))) | (col("motif") <= col("motif_rc"))
    )
    return df