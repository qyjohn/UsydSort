#!/bin/bash
for i in ` seq $1 $2 `
do
    start="$i""000000"
    if [ $i -eq 0 ]; then
        echo "Generating input_0000000"
        gensort -b0 1000000 input_0000000
    elif [ $i -lt 10 ]; then
        echo "Generating input_000000$i"
        gensort -b$start 1000000 input_000000$i
    elif [ $i -lt 100 ]; then
        echo "Generating input_00000$i"
        gensort -b$start 1000000 input_00000$i
    elif [ $i -lt 1000 ]; then
        echo "Generating input_0000$i"
        gensort -b$start 1000000 input_0000$i
    elif [ $i -lt 10000 ]; then
        echo "Generating input_000$i"
        gensort -b$start 1000000 input_000$i
    elif [ $i -lt 100000 ]; then
        echo "Generating input_00$i"
        gensort -b$start 1000000 input_00$i
    elif [ $i -lt 1000000 ]; then
        echo "Generating input_0$i"
        gensort -b$start 1000000 input_0$i
    elif [ $i -lt 10000000 ]; then
        echo "Generating input_$i"
        gensort -b$start 1000000 input_$i
    fi
done
