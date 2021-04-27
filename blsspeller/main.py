from blsspeller.schemas import get_schema_bg, get_schema_iterated, get_schema_reduced
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from blsspeller.parser import parser
from blsspeller.iterator import iterate_motifs
from blsspeller.stream import start_stream, stop_stream

spark = SparkSession.builder.getOrCreate()
logger = spark._jvm.org.apache.log4j.Logger.getLogger(__name__)

args = vars(parser.parse_args())
logger.info(str(args))

n_thresholds = len(args["bls_thresholds"].split(","))
blsvector_columns = [f"bls{i}" for i in range(n_thresholds)]

fc_columns = [f"fc_{c}" for c in blsvector_columns]
bg_columns = [f"bg_{c}" for c in blsvector_columns]
conf_columns = [f"conf_{c}" for c in blsvector_columns]

output = Path(args["output"]).resolve()
output_iterated = output / "iterated"
output_reduction = output / "reduction"
output_results = output / "results"

schema_iterated = get_schema_iterated(blsvector_columns)
schema_bg = get_schema_bg(bg_columns)

exprs = [F.sum(c).alias(c_alias) for c, c_alias in zip(blsvector_columns, fc_columns)]

if not (args["resume"] and output.exists()):
    if output.exists():
        shutil.rmtree(output)
    output.mkdir()

    output_iterated.mkdir()

if output_reduction.exists():
    shutil.rmtree(output_reduction)

output_reduction.mkdir()

if args["streaming"]:
    logger.info("Reducing motifs with Spark Streaming")
    stream = start_stream(
        spark,
        args,
        schema_iterated=schema_iterated,
        output_iterated=output_iterated,
        output_reduction=output_reduction,
        exprs=exprs,
    )

if not (args["resume"] and output.exists()):
    iterate_motifs(spark, args, output_iterated)

if args["streaming"]:
    stop_stream(stream)
    logger.info("Finished reducing motifs with Spark Streaming")
else:
    logger.info("Reducing motifs")
    df = (
        spark.read.option("mergeSchema", "true")
        .schema(schema_iterated)
        .parquet(str(output_iterated))
    )
    # TODO make group optional
    df = df.groupby("group", "motif").agg(*exprs)
    df.repartition(1).write.mode("overwrite").parquet(str(output_reduction))
    logger.info("Finished reducing motifs")


if args["reduce_only"]:
    logger.info("Stopping because reduce_only mode.")
else:
    schema_reduced = get_schema_reduced(fc_columns)
    df = spark.read.schema(schema_reduced).parquet(str(output_reduction))
    # only import these now, so previous steps are not dependent on these.
    from blsspeller.confidence import get_confidence_score

    df = get_confidence_score(
        df,
        args,
        n_thresholds=n_thresholds,
        schema_bg=schema_bg,
        fc_columns=fc_columns,
        bg_columns=bg_columns,
        conf_columns=conf_columns,
    )

    output_results.mkdir()

    # writes partitions to folder as parquet files
    (
        df.select(*(["group", "motif"] + [c for c in fc_columns + conf_columns]))
        .repartition(1)
        .write.mode("overwrite")
        .parquet(str(output_results))
    )

if not args["keep_tmps"] and output_iterated.exists():
    # only remove now, because lazy evaluation can still depend on this folder
    # in non-streaming case
    shutil.rmtree(output_iterated)

spark.stop()
