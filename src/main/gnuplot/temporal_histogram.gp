set terminal png nocrop enhanced font 'Times-Roman bold' 16 size 1280,900
set output 'output/temporal_histogram.png'
set yrange [0.4:1.6]
set style data histogram
set style histogram clustered gap 1
set style fill solid border -1
set ylabel "Average Batch Time (s)" offset 0 font 'Times-Roman bold,24'
set xlabel "Number of Past Values" offset 0,-1 font 'Times-Roman bold,24'
set xtics font 'Times-Roman bold,20' offset 0,-0.5
set ytics font 'Times-Roman bold,20'
unset title
set boxwidth
set style line 1 lc rgb '#ffffcc'
set style line 2 lc rgb '#a1dab4'
set style line 3 lc rgb '#41b6c4'
set style line 4 lc rgb '#2c7fb8'
set style line 5 lc rgb '#253494'
set key font ",20"
plot 'data/temporal_histogram.dat' using 2:xticlabels(1) title "1 Cluster Node", \
    ''  u 3 title "2 Cluster Nodes", \
    ''  u 4 title "4 Cluster Nodes", \
    ''  u 5 title "8 Cluster Nodes", \
    ''  u 6 title "16 Cluster Nodes"