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

set xtics nomirror ("1" 1, "2" 2, "4" 3, "8" 4, "16" 5, "32" 6)
set ytics nomirror

set output "__OUTPUT_FILENAME__"

set style fill solid border -1
set grid xtics
set grid ytics
set key right vertical

set xlabel "Number of Concurrent Tasks"
set ylabel "Job Completion Time (seconds)" offset 1

set xrange [0:7]
set yrange [0:4000]

# Add the Monotasks JCT.
set arrow from 0,__MONOTASKS_JCT__ to 7,__MONOTASKS_JCT__ nohead lw 2
set label "Monotasks: __MONOTASKS_JCT__ s" left at __MONOTASKS_JCT_DESCRIPTION_POSITION__

# Plot the Spark JCTs.
plot "__SPARK_JCT_DATA_FILENAME__" using ($0+1):1 with points pointtype 6 title "Spark JCT"
