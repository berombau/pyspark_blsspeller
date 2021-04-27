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
conda env update -f environment.yml
conda activate pyspark_blsspeller
python setup.py bdist_wheel --universal
bash example_spark-submit.sh
```

## HPC

Note that first usage of conda requires `conda init bash`.

```bash
module swap cluster/swalot
module load Miniconda3/4.9.2
conda env update -f environment.yml
conda activate pyspark_blsspeller
python setup.py bdist_wheel --universal
```

### Compile motifIterator

TODO

```bash
module load intel/2019b gtest/1.10.0-GCCcore-8.3.0 Arrow/0.17.1-fosscuda-2020b
# change to build dir in motifIterator
cmake ..
make -j0
```

### Test on login node

The Spark module will also load an Arrow module, which will change LD_LIBRARY_PATH.
The pyspark code expects this variable in the environment.

```bash
module load Spark/3.1.1-fosscuda-2020b
bash hpc_example_spark-submit.sh
```

### Execute on cluster nodes

Note that `hod` can only run with Python 2, so remove the Python 3 of conda with `conda deactivate`.

```bash
module load hod
hod batch -n 1 --info --label pyspark_test_1 --workdir . --hodconf hod.conf --script hpc_example_spark-submit.sh -l 1 -m e
```
