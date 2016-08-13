#!/bin/bash
if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters"
else
    if [ $1 -lt 90 ]; then
        start=$(($1*486))
        end=$((start+486-1))
        echo $1 $start $end 
    else
        start=$(($1*485+90))
        end=$((start+485-1))
        echo $1 $start $end 
    fi
    generate_data.sh $start $end
fi
