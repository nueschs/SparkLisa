set terminal png nocrop enhanced font arial 8 size 1280,800
set output filename.'.png'
set offset 0.5
set lmargin 15
set rmargin 15
set tmargin 7
set bmargin 7
set ylabel 'Temporal lisa' offset 0
set xlabel '# Cluster Nodes' offset 0,-1
set title 'LISA Statistics with Monte Carlo Simulation'
plot filename.'.dat' using 1:2 with lines title '1 Value per Minute, 1600 Nodes, t='.k