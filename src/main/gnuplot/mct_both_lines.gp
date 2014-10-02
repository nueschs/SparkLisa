set terminal svg dynamic enhanced fname 'arial bold' fsize 16 size 1024,600
set output 'output/mct_both_lines.svg'
set ylabel 'Average Batch Duration (in s)' font 'arial bold,24'
set xlabel 'Number of Past Values (k)' font 'arial bold,24'
set title 'Temporal LISA - Global Monte Carlo' font 'arial bold,24'
set xtics font 'arial bold,20'
set ytics font 'arial bold,20'
set key font ",12" rm 10
plot 'data/mctc.dat' using 2:xticlabel(1) title 'Local Selection' with lines, \
    '' u 3 title 'Global Selection' with lines