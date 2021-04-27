from pyspark.sql.types import *


def get_schema_iterated(blsvector_columns):
    return StructType(
        [
            StructField("group", StringType(), False),
            StructField("motif", StringType(), False),
        ]
        + [StructField(c, IntegerType(), False) for c in blsvector_columns]
    )


def get_schema_reduced(fc_columns):
    return StructType(
        [
            StructField("group", StringType(), False),
            StructField("motif", StringType(), False),
        ]
        + [StructField(c, LongType(), False) for c in fc_columns]
    )

# TODO check where null values are generated so schema nullable can be False
def get_schema_bg(bg_columns):
    return StructType(
        [
            StructField("group", StringType(), True),
        ]
        + [StructField(c, IntegerType(), True) for c in bg_columns]
    )