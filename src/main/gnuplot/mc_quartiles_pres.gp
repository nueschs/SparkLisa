set terminal svg dynamic enhanced fname 'arial bold' fsize 16 size 1024,600
set output 'output/mc_quartiles_pres.svg'
set boxwidth 0.4 absolute
set xrange [ 0: ] noreverse nowriteback
set yrange [0:200]
set style fill empty
set offset 0,0.5,0,0
set title "LISA with Monte Carlo Simulation" font 'arial bold,24'
set ylabel "Average Batch Duration (in s)" font 'arial bold,24'
set xlabel "Number of Cluster Nodes" font 'arial bold,24'
set xtics font 'arial bold,20'
set ytics font 'arial bold,20'
set style line 2 lc rgb 'black' lt 1 lw 2 pt 7 ps 0.8
plot 'data/mc_comb.dat' using 1:3:2:6:5:xticlabels(9) with candlesticks lt 3 lw 2 title 'Quartiles' whiskerbars, \
    '' u 1:4 with lp lw 2 title "Average Batch Duration"
    #'' using 1:4:4:4:4 with candlesticks lt -1 lw 2 notitle
