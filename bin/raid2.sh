#!/bin/bash
mdadm --create --verbose /dev/md0 --level=0 --name=John --raid-devices=2 /dev/xvdb /dev/xvdc
mkfs.ext4 /dev/md0
mount /dev/md0 /data
chown -R ubuntu:ubuntu /data
