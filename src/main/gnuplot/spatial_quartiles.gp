set terminal png nocrop enhanced font 'Times-Roman,16' size 1280,800
set output 'output/spatial_quartiles.png'
set boxwidth 0.4 absolute
set xrange [ 0.7: ] noreverse nowriteback
set style fill empty
set yrange [ 0 : 4 ]
set offset 0,0.3,0,0
set xtics font 'Times-Roman bold,20' offset 0,-0.5
set ytics font 'Times-Roman bold,20'
unset title
set ylabel "Average Batch Duration (in s)" font 'Times-Roman bold,24'
set xlabel "Number of Cluster Workers" font 'Times-Roman bold,24' offset 0,-1
set style line 2 lc rgb 'black' lt 1 lw 2 pt 7 ps 1.5
set key font ",20"
plot 'data/spatial_comb.dat' using 1:3:2:6:5:xticlabels(9) with candlesticks lt 3 lw 2 title 'Quartiles' whiskerbars, \
    '' u 1:7 with lp lw 2 title "Average Calculation Time"
    #'' using 1:4:4:4:4 with candlesticks lt -1 lw 2 notitle
