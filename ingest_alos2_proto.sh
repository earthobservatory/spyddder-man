#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# export GDAL env variables (adapted from isce.sh)
export LD_LIBRARY_PATH=/usr/local/gdal/lib:$LD_LIBRARY_PATH
export GDAL_DATA=/usr/local/gdal/share/gdal

# wrapper for ingest_alos2_proto.py

#validate input args
if [ -z "${1}" ]
    then
    echo "No download url specified"
    exit 1
fi
if [ -z "${2}" ]
    then
    echo "No filetype specified"
    exit 1
fi

${BASE_PATH}/ingest_alos2_proto.py ${1} ${2}