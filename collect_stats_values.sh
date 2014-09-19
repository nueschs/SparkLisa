#!/bin/bash
cd temp
hadoop fs -text sparkLisa/results/$1_$2/measuredValuesPositions-$3/part* > mvp
hadoop fs -text sparkLisa/results/$1_$2/finalLisaValues-$3/part* > flv
hadoop fs -text sparkLisa/results/$1_$2/lisaValuesWithRandomNeighbourIds-$3/part* > lvwni
hadoop fs -text sparkLisa/results/$1_$2/allValues-$3/part* > av
hadoop fs -text sparkLisa/results/$1_$2/randomNeighbourSums-$3/part* > rns
cd ..
tar cfz results_$1_$2_$3.tar.gz temp/*
