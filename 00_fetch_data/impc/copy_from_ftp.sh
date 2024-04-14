#!/bin/bash

srun --partition=datamover -t 1:30:00 --mem=5G --pty cp -r /nfs/ftp/public/databases/impc/all-data-releases/release-20.0/impc-kg .
