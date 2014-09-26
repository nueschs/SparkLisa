set terminal png nocrop enhanced font arial 8 size 1280,800
set output 'output/topologies.png'
set style histogram clustered gap 0.5
set style data histogram
set style fill solid noborder
set boxwidth
set yrange [0:25]
set lmargin 15
set rmargin 15
set tmargin 7
set bmargin 7
set ylabel 'Batch Duration (seconds)' offset 0
set xlabel '# Cluster Nodes' offset 0,-1
set xtics center nomirror
set style line 2 lc rgb '#41b6c4'
set style line 1 lc rgb '#253494'
set title 'LISA Statistics with Monte Carlo Simulation - Different Topology Types'
plot 'data/topologies_avgs.dat' using 2:xticlabel(1) title '1600 Nodes', \
    'data/topologies_avgs_10000.dat' using 2:xticlabel(1) title '10000 Nodes'