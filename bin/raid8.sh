#!/bin/bash
mdadm --create --verbose /dev/md0 --level=0 --name=John --raid-devices=8 /dev/xvdb /dev/xvdc /dev/xvdd /dev/xvde /dev/xvdf /dev/xvdg /dev/xvdh /dev/xvdi
mkfs.ext4 /dev/md0
mount /dev/md0 /data
chown -R ec2-user:ec2-user /data

