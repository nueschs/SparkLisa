set terminal png nocrop enhanced font arial 8 size 1280,1024
set output 'temporal_histogram.png'
set yrange [0:2.5]
set style data histogram
set style histogram clustered gap 1
set style fill solid border -1
set ylabel "Average Batch Time (s)"
set xlabel "Number of Past Values"
set boxwidth
plot 'temporal_histogram.dat' using 2:xticlabels(1) title "1 Cluster Node" lt rgb "#a6cee3", \
    ''  u 3 lt rgb "#1f78b4" title "16 Cluster Nodes"