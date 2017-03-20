#!/usr/bin/env bash

dirname='902312911-jmcgehee3-hw4'


rm -r $dirname
rm -r $dirname.tar.gz

mkdir $dirname

cp -r code/src $dirname/src
cp -r code/sbt $dirname/sbt
cp -r code/project $dirname/project
cp -r code/zeppelin $dirname/zeppelin
cp code/build.sbt $dirname/build.sbt

tar -czvf $dirname.tar.gz $dirname

rm -r $dirname



