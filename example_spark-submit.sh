#!/bin/bash

# From http://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html
# export PYSPARK_DRIVER_PYTHON=python # Do not set in cluster modes.
# export PYSPARK_PYTHON=./environment/bin/python
# --archives pyspark_conda_env.tar.gz#environment \

export PYSPARK_DRIVER_PYTHON=~/miniconda3/envs/pyspark_blsspeller/bin/python
export PYSPARK_PYTHON=~/miniconda3/envs/executor_pyspark_blsspeller/bin/python
time spark-submit \
--driver-memory 10G \
--py-files dist/blsspeller-1.0-py2.py3-none-any.whl blsspeller/main.py \
--input ../input_analysis/subset \
--output test_output \
--exec ../suffixtree-motif-speller/motifIterator/build_parquet/motifIterator \
--limit 1 \
|& tee output.log