set terminal png nocrop enhanced font 'Times-Roman bold' 16 size 1280,800
set output 'output/mcn_scheduling_delays.png'
set xrange[0:17]
set yrange[0:700]
set ylabel 'Scheduling Delays (seconds)' offset 0 font 'Times-Roman bold,24'
set xlabel 'Number of Processed Batches' offset 0,-1 font 'Times-Roman bold,24'
set ytics font 'Times-Roman bold,20'
set xtics font 'Times-Roman bold,20' offset 0,-0.5
unset title
set datafile missing "NaN"
set style line 1 lc rgb '#363c9c' lw 3 pt 7 ps 2.0
set style line 2 lc rgb '#365e9c' lw 3 pt 7 ps 2.0
set style line 3 lc rgb '#9c9636' lw 3 pt 7 ps 2.0
set style line 4 lc rgb '#5e9c36' lw 3 pt 7 ps 2.0
set style line 5 lc rgb '#9c363c' lw 3 pt 7 ps 2.0
set key font ",20"
plot 'data/mc_naive_delays.dat' using 1:($2/1000) with lines title '1 Worker', \
    '' u ($3/1000) with lines title '2 Workers', \
    '' u ($4/1000) with lines title '4 Workers', \
    '' u ($5/1000) with lines title '8 Workers', \
    '' u ($6/1000) with lines title '16 Workers'