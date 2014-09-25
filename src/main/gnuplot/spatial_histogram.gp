set output 'averages_'.filename.'.png'
set yrange [0.1:]
set style data histogram
set style histogram clustered gap 1
set style fill solid border -1
set boxwidth
set offset 1,0,0,0
plot 'spatial_all_avgs.dat' using 2:xticlabels(1) lt rgb "#a6cee3", \
    ''  u 3 lt rgb "#1f78b4", \
    '' u 4 lt rgb "#b2df8a"