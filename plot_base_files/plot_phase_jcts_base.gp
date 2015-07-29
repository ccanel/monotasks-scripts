set terminal pdfcairo font 'Times,19' size 5,2.5 linewidth 1 rounded dashlength 1

# Line style for axes
set style line 80 lt 1 lc rgb "#808080"

# Line style for grid
set style line 81 lt 0 # dashed
set style line 81 lt rgb "#808080"  # grey

set grid ytics back linestyle 81
set border 3 back linestyle 80 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
             # Also, put it in grey; no need for so much emphasis on a border.

set xtics nomirror ("1" 1, "2" 2, "4" 3, "8" 4, "16" 5, "32" 6, "Monotasks" 7)
set ytics nomirror

set output "__OUTPUT_FILENAME__"

set boxwidth 0.2

set style fill pattern 1 border -1

set grid ytics

set key right vertical

set xlabel "Number of Concurrent Tasks"
set ylabel "Job Completion Time (seconds)" offset 1

set xrange [0:8]
set yrange [0:__Y_MAX__]

plot "__JCT_DATA_FILENAME__" using ($2):($4):($3):($7):($6) with candlesticks fs pattern 0 lc rgb "white" title "Spark JCT" whiskerbars,\
"__JCT_DATA_FILENAME__" using ($2):($5):($5):($5):($5) with candlesticks notitle
