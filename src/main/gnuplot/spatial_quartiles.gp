set terminal png nocrop enhanced font arial 8 size 1280,1024
set output outfile
set boxwidth 0.4 absolute
set title "box-and-whisker with median bar and whiskerbars"
set xrange [ 0: ] noreverse nowriteback
#set yrange [0.1: 3]
set style fill empty
set offset 0,0.5,0,0
plot filename using 1:3:2:6:5:xticlabels(9) with candlesticks lt 3 lw 2 title 'Quartiles' whiskerbars, \
    ''                 using 1:4:4:4:4 with candlesticks lt -1 lw 2 notitle

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