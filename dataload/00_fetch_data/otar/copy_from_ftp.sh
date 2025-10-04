#!/bin/bash

rm -rf targets diseases molecule evidence

srun --partition=datamover -t 1:30:00 --mem=5G --pty bash -c "cp -r /nfs/ftp/public/databases/opentargets/platform/25.09/output/* ."

