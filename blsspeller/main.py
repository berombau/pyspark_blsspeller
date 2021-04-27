from operator import or_, mul
from functools import reduce
from math import factorial
import argparse
from pathlib import Path
import shutil
import os

# import pandas as pd
# import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
logger = spark._jvm.org.apache.log4j.Logger.getLogger(__name__)


parser = argparse.ArgumentParser(description="BLSSpeller configuration.")
parser.add_argument("--input", type=str, required=True)
parser.add_argument("--output", type=str, required=True)
parser.add_argument("--exec", type=str, required=True)
parser.add_argument("--bls_thresholds", type=str, default="0.15,0.5,0.6,0.7,0.9,0.95")
parser.add_argument("--alphabet", type=int, default=2)
parser.add_argument("--degen", type=int, default=2)
parser.add_argument("--min_len", type=int, default=8)
parser.add_argument("--max_len", type=int, default=9)
parser.add_argument("--conf_cutoff", type=float, default=0.5)
parser.add_argument("--fc_cutoff", type=int, default=1)
parser.add_argument(
    "--limit",
    type=int,
    default=None,
    help="Limit the amount of input files that will be processed",
)
parser.add_argument(
    "--resume",
    type=bool,
    nargs="?",
    const=True,
    default=False,
    help="Skip iteration and reduction when these output folders are present",
)
parser.add_argument(
    "--streaming",
    type=bool,
    nargs="?",
    const=True,
    default=False,
    help="Reduce motifs as soon as a process is finished iterating",
)
parser.add_argument(
    "--keep_tmps",
    type=bool,
    nargs="?",
    const=True,
    default=False,
    help="Keep the temporary iterated motifs, that otherwise get remove when reducing via streaming",
)
parser.add_argument(
    "--trigger",
    type=str,
    default="5 seconds",
    help="Interval for checking new files when streaming. Can also be used to balance iteration and reduction.",
)
parser.add_argument("--alignment_option", type=str, default="AF")

args = vars(parser.parse_args())

logger.info(str(args))

n_thresholds = len(args["bls_thresholds"].split(","))
blsvector_columns = [f"bls{i}" for i in range(n_thresholds)]

newSchema = StructType(
    [
        StructField("group", StringType(), False),
        StructField("motif", StringType(), False),
    ]
    + [StructField(c, IntegerType(), False) for c in blsvector_columns]
)

output = Path(args["output"]).resolve()
exec = Path(args["exec"]).resolve()
output_iterated = output / "iterated"
output_reduction = output / "reduction"

input = Path(args["input"])
assert input.is_dir()

total_families = len(list(input.iterdir()))

limit = args["limit"]
if limit:
    input_families = limit
else:
    input_families = total_families

fc_columns = [f"sum_{c}" for c in blsvector_columns]
exprs = [F.sum(c).alias(c_alias) for c, c_alias in zip(blsvector_columns, fc_columns)]

if not (args["resume"] and output.exists()):
    if output.exists():
        shutil.rmtree(output)
    output.mkdir()

    output_iterated.mkdir()

if output_reduction.exists():
    shutil.rmtree(output_reduction)

output_reduction.mkdir()

def write_df(df):
    (df.repartition(1)
        .write.mode("overwrite")
        .parquet(str(output_reduction))
    )

if args["streaming"]:
    logger.info("Reducing motifs with Spark Streaming")
    stream = (
        spark.readStream.schema(newSchema)
        # DELETE will only delete the input files of the previous batch when a new batch gets started.
        # This means the input files of the final batch will remain in the folder.
        .option("cleanSource", "OFF" if args["keep_tmps"] else "DELETE")
        .parquet(str(output_iterated))
        .groupby("group", "motif")
        .agg(*exprs)  # keeps checkpoints of aggregation​
    )
    stream = (
        stream.writeStream
        # there are no semantics to output only once when stopping, so every batch gets outputted and overwritten by the next batch
        .outputMode("complete")
        # default trigger is 0 seconds, so one batch at a time, every time there is input
        # balance between producing and reducing will differ for more iteration intensive settings
        .trigger(processingTime=args["trigger"])
        .foreachBatch(lambda df, _: write_df(df))
        .start()
    )

if not (args["resume"] and output.exists()):
    families = spark.sparkContext.wholeTextFiles(args["input"])
    families = spark.sparkContext.parallelize(
        families.map(lambda x: x[1]).take(input_families), input_families
    )

    cmd = " ".join(
        [
            str(x)
            for x in [
                exec,
                "-",
                "$OUTPUT",
                args["alignment_option"],
                args["alphabet"],
                args["bls_thresholds"],
                args["degen"],
                args["min_len"],
                args["max_len"],
            ]
        ]
    )
    # atomic mv is needed for Spark streaming
    script = " ".join([
        "/bin/bash -c '",
        r"OUTPUT=`mktemp`.parquet;",
        # r"'eval $(/usr/share/lmod/lmod/libexec/lmod load Arrow/0.17.1-fosscuda-2020b) && eval $(${LMOD_SETTARG_CMD:-:}) && \ ",
        f"{cmd} && mv $OUTPUT {str(output_iterated)}'",
    ])

    env_variables_names = ["LD_LIBRARY_PATH"]
    env_variables = {x: os.environ[x] for x in env_variables_names}
    logger.info(f"env_variables: {env_variables}")

    families.pipe(
        script,
        checkCode=True,
        env=env_variables
        # block until all families are iterated and moved to next folder
    ).collect()

if args["streaming"]:
    stream.processAllAvailable()  # blocks until all Parquets files are reduced
    stream.stop()  # stop stream
    logger.info("Finished reducing motifs with Spark Streaming")
    df = spark.read.schema(newSchema).parquet(str(output_reduction))
else:
    logger.info("Reducing motifs")
    df = (
        spark.read.option("mergeSchema", "true")
        .schema(newSchema)
        .parquet(str(output_iterated))
    )
    # TODO make group optional
    df = df.groupby("group", "motif").agg(*exprs)
    write_df(df)
    logger.info("Finished reducing motifs")

if not args['keep_tmps'] and output_iterated.exists():
    shutil.rmtree(output_iterated)

bg_columns = [f"bg_{c}" for c in blsvector_columns]
conf_columns = [f"conf_{c}" for c in blsvector_columns]

# TODO check where null values are generated so schema nullable can be False
bg_schema = StructType(
    [
        StructField("group", StringType(), True),
    ]
    + [StructField(c, IntegerType(), True) for c in bg_columns]
)


def calc_background(pdf):
    group = pdf["group"].iloc[0]
    max_motifs = factorial(len(group)) // reduce(
        mul, [factorial(group.count(l)) for l in set(group)], 1
    )
    half = (max_motifs + 1) / 2
    found_motifs = len(pdf)
    if max_motifs == 1:
        arr = pdf.iloc[0, 2:]
    elif found_motifs < half:
        arr = np.zeros(n_thresholds)
    else:
        arr = np.quantile(
            pdf.iloc[:, 2:], 1 - half / found_motifs, axis=0, interpolation="lower"
        )
    series = pd.Series(arr, index=bg_columns)
    df = pd.DataFrame([series])
    df.insert(0, "group", group)
    return df


df_bg = df.groupby("group").applyInPandas(
    calc_background,
    schema=bg_schema,
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

iupac = "ACGTNRYSWKMBVDH"
iupac_complement = "TGCANYRSWMKVBHD"

df = df.withColumn(
    "motif_rc",
    F.reverse(F.translate(col("motif"), iupac, iupac_complement)),
)

df = df.withColumn(
    "group_sc",
    F.concat_ws(
        "",
        F.sort_array(F.split(F.translate(col("group"), iupac, iupac_complement), "")),
    ),
)

df = df.filter((~(col("group") == col("group_sc"))) | (col("motif") <= col("motif_rc")))

output_results = output / "results"
output_results.mkdir()

# writes partitions to folder as parquet files
(
    df.select(*(["group", "motif"] + [c for c in fc_columns + conf_columns]))
    .repartition(1)
    .write.mode("overwrite")
    .parquet(str(output_results))
)
