import argparse

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
    "--reduce_only",
    type=bool,
    nargs="?",
    const=True,
    default=False,
    help="Stop when all motifs are counted up, resulting in a single compact Parquet file. Handy when HPC does not support newer version of Arrow.",
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
