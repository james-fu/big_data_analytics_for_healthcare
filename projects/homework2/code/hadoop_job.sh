#!/usr/bin/env bash

hadoop jar \
/usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-D mapreduce.job.reduces=5 \
-files lr \
-mapper "python lr/mapper.py -n 10 -r 0.4" \
-reducer "python lr/reducer.py -f 3793 -e 0.1 -c 0.8" \
-input hw2/training \
-output hw2/models
