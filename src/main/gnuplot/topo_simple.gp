set terminal png nocrop enhanced font arial 8 size 1280,1024
set output 'test.png'
set style histogram clustered gap 1 title offset 2,0.25
set style data histogram
set style fill solid noborder
set boxwidth 0.95
set yrange [0:25]
set xtics offset graph 0, -0.01
set lmargin 15
set rmargin 15
set tmargin 7
set bmargin 7
set ylabel 'Batch Duration (seconds)' offset 0
set xlabel '# Cluster Nodes' offset 0,-1
set title 'LISA Statistics with Monte Carlo Simulation - Different Topology Types'
plot filename using 2:xticlabel(1) title '1 Value per Minute, 1600 Nodes'