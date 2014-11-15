set terminal svg dynamic enhanced fname 'Times-Roman bold' fsize 16 size 1024,600
set output 'output/mcn_quartiles_pres.svg'
set boxwidth 0.4 absolute
set xrange [ 0: ] noreverse nowriteback
set style fill empty
set offset 0,0.5,0,0
set title "LISA with Monte Carlo Simulation (Naive)" font 'Times-Roman bold,24'
set ylabel "Average Batch Duration (in s)" font 'Times-Roman bold,24'
unset xlabel
set xtics font 'Times-Roman bold,24'
set ytics font 'Times-Roman bold,20'
set style line 2 lc rgb 'black' lt 1 lw 2 pt 7 ps 0.8
plot 'data/mcn_comb.dat' using 1:3:2:6:5:xticlabels(9) with candlesticks lt 3 lw 2 title 'Quartiles' whiskerbars, \
    '' u 1:4 with lp lw 2 title "Batch Duration Time"
    #'' using 1:4:4:4:4 with candlesticks lt -1 lw 2 notitle
