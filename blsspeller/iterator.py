from pathlib import Path
from os import getenv

def iterate_motifs(spark, args, output_iterated):
    logger = spark._jvm.org.apache.log4j.Logger.getLogger(__name__)

    input = Path(args["input"])
    assert input.is_dir()

    total_families = len(list(input.iterdir()))

    limit = args["limit"]
    if limit:
        input_families = limit
    else:
        input_families = total_families


    families = spark.sparkContext.wholeTextFiles(args["input"])
    families = spark.sparkContext.parallelize(
        families.map(lambda x: x[1]).take(input_families), input_families
    )

    executable = Path(args["exec"]).resolve()

    cmd = " ".join(
        [
            str(x)
            for x in [
                executable,
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
    script = " ".join(
        [
            "/bin/bash -c '",
            r"OUTPUT=`mktemp`.parquet;",
            # r"'eval $(/usr/share/lmod/lmod/libexec/lmod load Arrow/0.17.1-fosscuda-2020b) && eval $(${LMOD_SETTARG_CMD:-:}) && \ ",
            f"{cmd} && mv $OUTPUT {str(output_iterated)}'",
        ]
    )

    env_variables_names = ["LD_LIBRARY_PATH"]
    env_variables = dict()
    for k in env_variables_names:
        v = getenv(k)
        if v:
            env_variables[k] = v
    logger.info(f"env_variables: {env_variables}")

    families.pipe(
        script,
        checkCode=True,
        env=env_variables
        # block until all families are iterated and moved to next folder
    ).collect()