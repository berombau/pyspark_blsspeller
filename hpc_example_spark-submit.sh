#!/bin/bash

time spark-submit \
--driver-memory 20G \
--py-files dist/blsspeller-1.0-py2.py3-none-any.whl blsspeller/main.py \
--input /scratch/gent/vo/000/gvo00024/vsc40486/bls/input/wheat_100 \
--output test_output \
--exec /scratch/gent/vo/000/gvo00024/vsc40486/bls/bin/motifIteratorParquet \
--streaming --limit 1 \
|& tee output.log