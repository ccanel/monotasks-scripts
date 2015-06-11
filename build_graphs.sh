#!/bin/bash

resultsDirectory=$1
prefix=$2
evalDirectory=$3
testDirectories=(1 2 4 8 16 24)

for testDirectory in ${testDirectories[*]}; do
    completePrefix="$(echo "$prefix _ $testDirectory _monotasks" | tr -d ' ')"
    eventLog="$(echo "$resultsDirectory$testDirectory/monotasks/$completePrefix _event_log" | tr -d ' ')"
    python "$evalDirectory/parse_event_logs.py" $eventLog
done

for testDirectory in ${testDirectories[*]}; do
    completePrefix="$(echo "$prefix _ $testDirectory _spark" | tr -d ' ')"
    eventLog="$(echo "$resultsDirectory$testDirectory/spark/$completePrefix _event_log" | tr -d ' ')"
    python "$evalDirectory/parse_event_logs.py" $eventLog
done

python "$evalDirectory/plot_throughput.py"
