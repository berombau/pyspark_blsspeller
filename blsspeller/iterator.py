from pathlib import Path
from os import getenv

def parse_lines(lines):
    """
    >>> parse_lines('\\n'.join(2*['name', 'tree', '2', 'id1', 'r1', 'id2', 'r2']))
    ['name\\ntree\\n2\\nid1\\nr1\\nid2\\nr2', 'name\\ntree\\n2\\nid1\\nr1\\nid2\\nr2']
    """
    lines = [s for s in lines.splitlines() if s]
    families = []
    family = []
    i = 0
    while i < len(lines):
        # name
        # tree
        # number of regions
        n = int(lines[i+2])
        lines_in_family = 3+n*2
        family = lines[i:i+lines_in_family]
        families.append("\n".join(family))
        i += lines_in_family
    return families

def iterate_motifs(spark, args, output_iterated):
    logger = spark._jvm.org.apache.log4j.Logger.getLogger(__name__)

    input = Path(args["input"])
    assert input.is_dir()

    input_files = spark.sparkContext.wholeTextFiles(args["input"])
    families = input_files.flatMap(lambda x: parse_lines(x[1]))

    limit = args["limit"]
    if limit:
        families = spark.sparkContext.parallelize(families.take(limit))

    partitions = args["partitions"]
    if not partitions:
        # local driver process uses 1 core 
        partitions = spark.sparkContext.defaultParallelism - 1
    if limit and limit < partitions:
        partitions = limit
    families = families.repartition(partitions)

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
                "counted" if args["counted"] else "not_counted",
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
