#!/bin/bash
if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters"
else
    total=$1
    node=$2
    step=$((100000/total))
    start=$(($step*$node))
    end=$(($start+$step-1))
    if [ $end -gt 99999 ]; then
        end=99999
    fi
    echo "Step: $step, Start: $start, End: $end"
    generate_data.sh $start $end
fi
