#!/usr/bin/env bash

lines=$(hadoop fs -ls -d $1* | awk '{print $8}')

for line in $lines
do
	echo `hadoop fs -text $line/part* | wc -l`
done


