#!/bin/bash
SortServer 5003 1 0 /data/5003.out > 5003.log &
SortServer 5004 1 1 /data/5004.out /data/Temp/ > 5004.log &
