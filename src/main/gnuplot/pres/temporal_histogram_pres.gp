set terminal svg dynamic enhanced fname 'Times-Roman bold' fsize 16 size 1024,800
set output 'output/temporal_histogram.svg'
set yrange [0:1.8]
set style data histogram
set style histogram clustered gap 1
set style fill solid border -1
set ylabel "Average Batch Duration (in s)" font 'Times-Roman bold,24'
set xlabel "Number of Past Values" font 'Times-Roman bold,24'
set boxwidth
set xtics font 'Times-Roman bold,20'
set ytics font 'Times-Roman bold,20'
set style line 1 lc rgb '#ffffcc'
set style line 2 lc rgb '#a1dab4'
set style line 3 lc rgb '#41b6c4'
set style line 4 lc rgb '#2c7fb8'
set style line 5 lc rgb '#253494'
set title "Temporal LISA Calculation Times" font 'Times-Roman bold,24'
plot 'data/temporal_histogram.dat' using 2:xticlabels(1) title "1 Cluster Node", \
    ''  u 3 title "2 Cluster Nodes", \
    ''  u 4 title "4 Cluster Nodes", \
    ''  u 5 title "8 Cluster Nodes", \
    ''  u 6 title "16 Cluster Nodes"
    #'' u 7 with lp lw 2 title "Average Calculation Time"