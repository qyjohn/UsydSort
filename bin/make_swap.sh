#!/bin/bash
#
# $1 is the device name
#
mkswap $1
swapon $1
swapon -s
