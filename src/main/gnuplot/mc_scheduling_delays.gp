set terminal png nocrop enhanced font arial 8 size 1280,1024
set output 'output/mc_scheduling_delays.png'
set xrange[0:17]
set offset 0.5
set lmargin 15
set rmargin 15
set tmargin 7
set bmargin 7
set ylabel 'Scheduling Delays (seconds)' offset 0
set xlabel 'Run Time' offset 0,-1
unset xtics
set title 'Improved Monte Carlo Simulation - Scheduling Delays'
set datafile missing "NaN"
plot 'data/mc_delays.dat' using 1:($2/1000) with lines title '1 Worker', \
    '' u ($6/1000) with lines title '16 Workers', \
    '' u ($3/1000) with lines title '2 Workers', \
    '' u ($4/1000) with lines title '4 Workers', \
    '' u ($5/1000) with lines title '8 Workers'