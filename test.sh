#!/usr/bin/env bash

lines=$(hadoop fs -ls -d $1/results/1_1600/allValues* | awk '{print $8}')

for line in $lines
do
	echo `hadoop fs -text $line/part* | wc -l`
done


