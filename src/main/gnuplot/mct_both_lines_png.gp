set terminal png nocrop enhanced font 'Times-Roman bold' 16 size 1280,800
set output 'output/mct_both_lines.png'
set ylabel 'Average Batch Duration (in s)' font 'Times-Roman bold,24'
set xlabel 'Number of Past Values (k)' font 'Times-Roman bold,24' offset 0,-1
unset title
set xtics (1,2,5) font 'Times-Roman bold,20' offset 0,-0.5
set ytics font 'Times-Roman bold,20'
set key font ",20"
set key at 2.0,175,2
set style line 1 lc rgb '#8b1a0e' lw 3 pt 7 ps 2.0
set style line 2 lc rgb '#5e9c36' lw 3 pt 7 ps 2.0
plot 'data/mctc.dat' using 1:2 title 'Local Selection' with lp, \
    '' u 1:3 title 'Global Selection' with lp