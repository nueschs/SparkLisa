set terminal png nocrop enhanced font arial 8 size 1280,800
set output 'output/spatial_all_lines.png'
set yrange [0:3.5]
set offset 0,0.5,0,0
set lmargin 15
set rmargin 15
set tmargin 7
set bmargin 7
set ylabel 'Batch Duration (seconds)' offset 0
set xlabel 'Number of Workers' offset 0,-1
set title 'Batch Durations for Different Numbers of Nodes'
set key font ",12"
plot 'data/spatial_comb_1600.dat' using 7:xticlabel(9) title '1600 Nodes' with lines, \
    'data/spatial_comb_3600.dat' u 7 title '3600 Nodes' with lines, \
    'data/spatial_comb_10000.dat' u 7 title '10000 Nodes' with lines, \
    'data/spatial_comb_25600.dat' u 7 title '25600 Nodes' with lines