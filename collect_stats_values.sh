#!/bin/bash
cd temp
hadoop fs -text sparkLisa/results/$1_$2/measuredValuesPositions-$3/part* > mvp
hadoop fs -text sparkLisa/results/$1_$2/finalLisaValues-$3/part* > flv
hadoop fs -text sparkLisa/results/$1_$2/lisaValuesWithRandomNeighbourIds-$3/part* > lvwni
hadoop fs -text sparkLisa/results/$1_$2/allValues-$3/part* > av
hadoop fs -text sparkLisa/results/$1_$2/randomNeighbourSums-$3/part* > rns
hadoop fs -text sparkLisa/results/$1_$2/allLisaValues-$3/part* > alv
hadoop fs -copyToLocal sparkLisa/topology/topology_bare_connected_$2.txt
cd ..
tar cfz results_$1_$2_$3.tar.gz temp/*