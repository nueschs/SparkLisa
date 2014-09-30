set terminal png nocrop enhanced font arial 8 size 1280,800
set output 'test.png'
#set xtics (0,1,2,4,8,16)
#set ytics (0,1,2,4,8,16,32)
#set yrange[0:200]
set xrange[0:17]
set offset 0.5
set lmargin 15
set rmargin 15
set tmargin 7
set bmargin 7
set ylabel 'Batch Duration (seconds)' offset 0
set xlabel '# Cluster Nodes' offset 0,-1
set title 'LISA Statistics with Monte Carlo Simulation'
plot filename using 9:7 with lines title '1 Value per Minute, 1600 Nodes'