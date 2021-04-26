# pyspark_blsspeller

Pyspark implementation of BLSSpeller algorithm.

## Usage

```text
usage: main.py [-h] --input INPUT --output OUTPUT --bindir BINDIR [--bls_thresholds BLS_THRESHOLDS] [--alphabet ALPHABET] [--degen DEGEN] [--min_len MIN_LEN] [--max_len MAX_LEN] [--conf_cutoff CONF_CUTOFF] [--fc_cutoff FC_CUTOFF] [--limit LIMIT] [--resume [RESUME]] [--streaming [STREAMING]] [--keep_tmps [KEEP_TMPS]] [--trigger TRIGGER] [--alignment_option ALIGNMENT_OPTION]

BLSSpeller configuration.

optional arguments:
  -h, --help            show this help message and exit
  --input INPUT
  --output OUTPUT
  --bindir BINDIR
  --bls_thresholds BLS_THRESHOLDS
  --alphabet ALPHABET
  --degen DEGEN
  --min_len MIN_LEN
  --max_len MAX_LEN
  --conf_cutoff CONF_CUTOFF
  --fc_cutoff FC_CUTOFF
  --limit LIMIT         Limit the amount of input files that will be processed
  --resume [RESUME]     Skip iteration and reduction when these output folders are present
  --streaming [STREAMING]
                        Reduce motifs as soon as a process is finished iterating
  --keep_tmps [KEEP_TMPS]
                        Keep the temporary iterated motifs, that otherwise get remove when reducing via streaming
  --trigger TRIGGER     Interval for checking new files when streaming. Can also be used to balance iteration and reduction.
  --alignment_option ALIGNMENT_OPTION
```

## Local

```bash
conda env update -f environment.yaml
conda activate pyspark_blsspeller
python setup.py bdist_wheel --universal
bash example_spark-submit.sh
```

## HPC

```bash
module swap cluster/swalot
module load setuptools
module load hod
bash example_spark-submit.sh
```
