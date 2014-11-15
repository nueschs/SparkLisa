set terminal png nocrop enhanced font 'Times-Roman bold' 16 size 1280,800
set output 'output/single_runs_spatial_1_16.png'
set ytics font 'Times-Roman bold,20'
set xtics 100 font 'Times-Roman bold,20' offset 0,-0.5
set yrange [0:]
set ylabel 'Batch Duration (seconds)' offset 0 font 'Times-Roman bold,24'
set xlabel 'Run Time (in seconds)' offset 0,-1 font 'Times-Roman bold,24'
unset title
set key font ",20"
set style line 1 lc rgb '#8b1a0e' lw 3 pt 7 ps 2.0
set style line 2 lc rgb '#5e9c36' lw 3 pt 7 ps 2.0
plot 'data/single_runs_spatial_1_16.dat' using ($1*3):2 title '1 Cluster Worker' with lines, \
    '' u ($1*3):3 with lines title '16 Cluster Workers', \