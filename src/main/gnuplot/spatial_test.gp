set terminal png nocrop enhanced font arial 8 size 1280,1024
set output 'test.png'
set logscale y
plot filename using 9:7 with lines