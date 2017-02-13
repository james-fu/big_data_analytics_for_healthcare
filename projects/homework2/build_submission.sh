#!/usr/bin/env bash

dirname='902312911-jmcgehee3-hw2'


rm -r $dirname
rm -r $dirname.tar.gz

mkdir $dirname
mkdir $dirname/code
mkdir $dirname/code/hive
mkdir $dirname/code/lr
mkdir $dirname/code/pig
mkdir $dirname/code/zeppelin

cp code/hive/event_statistics.hql $dirname/code/hive/event_statistics.hql

cp code/lr/lrsgd.py $dirname/code/lr/lrsgd.py
cp code/lr/mapper.py $dirname/code/lr/mapper.py
cp code/lr/testensemble.py $dirname/code/lr/testensemble.py

cp code/pig/etl.pig $dirname/code/pig/etl.pig
cp code/pig/utils.py $dirname/code/pig/utils.py

cp code/zeppelin/bdh_hw2_zeppelin.json $dirname/code/zeppelin/bdh_hw2_zeppelin.json

cp homework2answer.pdf $dirname/homework2answer.pdf

tar -czvf $dirname.tar.gz $dirname

rm -r $dirname



