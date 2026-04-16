#!/bin/bash

if [ "$SLURM_JOB_PARTITION" != "datamover" ]; then
  echo "Must run on a datamover node"
  exit 1
fi

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <datarelease_path>"
  exit 1
fi

DATARELEASE_PATH=$1
VERSION=$(date +"%Y-%b-%d")

./check_datarelease.sh $DATARELEASE_PATH

FTP_PATH=/nfs/ftp/public/databases/spot/kg/$VERSION
LATEST_PATH=/nfs/ftp/public/databases/spot/kg/latest

echo "Copying archives from $DATARELEASE_PATH to $FTP_PATH"

rm -rf $FTP_PATH
mkdir -p $FTP_PATH

cp -L $DATARELEASE_PATH/*.tgz $FTP_PATH/
cp -L $DATARELEASE_PATH/*_metadata.json $FTP_PATH/
cp -rL $DATARELEASE_PATH/query_results $FTP_PATH/

echo "Copying $FTP_PATH to $LATEST_PATH"
rm -rf $LATEST_PATH
cp -r $FTP_PATH $LATEST_PATH




