#!/bin/sh -ue

DEVICE="/dev/mapper/myvg1-testlv"

mkdir -p mnt
sudo ./restore-poc snap1 raw.img diff0.img,diff1.img,diff2.img,diff3.img,diff4.img ${DEVICE}
sudo mount ${DEVICE} mnt
echo "Contents of raw.img:"
ls -lh mnt
sudo umount mnt
sudo lvs

sudo ./restore-poc snap2 raw.img diff0.img,diff1.img,diff2.img,diff3.img,diff4.img ${DEVICE}
sudo mount ${DEVICE} mnt
echo "Contents of raw.img + diff:"
ls -lh mnt
sudo umount mnt
sudo lvs
