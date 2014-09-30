set terminal png nocrop enhanced font arial 8 size 1280,800
set output 'simple_runs.png'
unset xtics
set yrange [0:]
set lmargin 15
set rmargin 15
set tmargin 7
set bmargin 7
set ylabel 'Batch Duration (seconds)' offset 0
set xlabel 'Run Time' offset 0,-1
set title 'Batch Durations along Single Runs'
set key font ",12"
plot filename using 2:xticlabel(1) title '1 Base Station' with lines, \
    '' u 3 with lines title '16 Base Stations', \