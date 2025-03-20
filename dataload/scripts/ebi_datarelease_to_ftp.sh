#!/bin/bash

#!/bin/bash

if [ "$SLURM_JOB_PARTITION" != "datamover" ]; then
  echo "Must run on a datamover node"
  exit 1
fi

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <subgraph> <datarelease_path>"
  exit 1
fi

SUBGRAPH=$1
DATARELEASE_PATH=$2
VERSION=$(date +"%Y-%b-%d")

./check_datarelease.sh $SUBGRAPH $DATARELEASE_PATH

FTP_PATH=/nfs/ftp/public/databases/spot/kg/$SUBGRAPH/$VERSION
LATEST_PATH=/nfs/ftp/public/databases/spot/kg/$SUBGRAPH/latest

echo "Copying $DATARELEASE_PATH to $FTP_PATH"

rm -rf $FTP_PATH/*
mkdir -p $FTP_PATH

cp -Lr $DATARELEASE_PATH/* $FTP_PATH/

echo "Copying $FTP_PATH to $LATEST_PATH"
rm -rf $LATEST_PATH
cp -r $FTP_PATH $LATEST_PATH





