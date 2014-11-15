set terminal png nocrop enhanced font 'Times-Roman bold' 16 size 1280,800
set output 'output/mcn_quartiles.png'
set boxwidth 0.4 absolute
set xrange [ 0: ] noreverse nowriteback
set yrange [50:400]
set style fill empty
set offset 0,0.5,0,0
unset title
set ylabel "Calculation Time (in seconds)" font 'Times-Roman bold,24'
set xlabel "Number of Cluster Nodes" font 'Times-Roman bold,24' offset 0,-1
set style line 2 lc rgb 'black' lt 1 lw 2 pt 7 pi -1 ps 1.5
set key font ",20"
set xtics font 'Times-Roman bold,20' offset 0,-0.5
set ytics font 'Times-Roman bold,20'
plot 'data/mcn_comb.dat' using 1:3:2:6:5:xticlabels(9) with candlesticks lt 3 lw 2 title 'Quartiles' whiskerbars, \
    '' u 1:7 with lp lw 2 title "Average Calculation Time"
    #'' using 1:4:4:4:4 with candlesticks lt -1 lw 2 notitle
