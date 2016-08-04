#!/bin/bash
SortServer 5000 5 0 /data/5001.out > 5000.log &
SortServer 5001 5 1 /data/5001.out /data/Temp/1 > 5001.log &
SortServer 5002 5 1 /data/5002.out /data/Temp/2 > 5002.log &
SortServer 5003 5 1 /data/5003.out /data/Temp/3 > 5003.log &
SortServer 5004 5 1 /data/5004.out /data/Temp/4 > 5004.log &

