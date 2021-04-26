#!/bin/bash

time spark-submit \
--driver-memory 10G \
--py-files dist/blsspeller-1.0-py2.py3-none-any.whl blsspeller/main.py \
--input ../input_analysis/subset \
--output test_output \
--bindir ../suffixtree-motif-speller/motifIterator/build_parquet \
--streaming --limit 1 \
|& tee output.log