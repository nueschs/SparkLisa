set terminal png nocrop enhanced font 'Times-Roman bold' 16 size 1280,800
set output 'output/topologies.png'
set style histogram clustered gap 0.5
set style data histogram
set style fill solid noborder
set boxwidth
set yrange [0:25]
set ylabel 'Batch Duration (seconds)' offset 0 font 'Times-Roman bold,24'
set xlabel '# Cluster Nodes' offset 0,-1 font 'Times-Roman bold,24'
set xtics center nomirror font 'Times-Roman bold,20' offset 0,-0.5
set ytics font 'Times-Roman bold,20'
set style line 2 lc rgb '#41b6c4'
set style line 1 lc rgb '#253494'
unset title
set key font ",20"
plot 'data/topologies_avgs.dat' using 2:xticlabel(1) title '1600 Nodes', \
    'data/topologies_avgs_10000.dat' using 2:xticlabel(1) title '10000 Nodes'