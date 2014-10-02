set terminal svg dynamic enhanced fname 'arial bold' fsize 16 size 1024,800
set output 'output/spatial_quartiles_pres.svg'
set key tc rgb "#505050"
set boxwidth 0.4 absolute
set xrange [ 0.7: ] noreverse nowriteback
set style fill empty
set yrange [ 0 : 4 ]
set offset 0,0.3,0,0
set xtics font 'arial bold,20'
set ytics font 'arial bold,20'
set title "LISA Calculation Time" font 'arial bold,24'
set ylabel "Average Batch Calculation Time (s)" font 'arial bold,24'
set xlabel "Number of Cluster Nodes" font 'arial bold,24'
set style line 2 lc rgb 'black' lt 1 lw 2 pt 7 ps 0.8
plot 'data/spatial_comb.dat' using 1:3:2:6:5:xticlabels(9) with candlesticks lt 3 lw 2 title 'Quartiles' whiskerbars, \
    '' u 1:4 with lp lw 2 title "Average Calculation Time"
    #'' using 1:4:4:4:4 with candlesticks lt -1 lw 2 notitle
