#!/bin/bash
SortServer 5001 1 0 /data/5001.out > 5001.log &
SortServer 5002 1 1 /data/5002.out /data/Temp/ > 5002.log &
