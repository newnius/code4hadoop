#! /bin/bash

if [ $# -lt 1 ]; then
    echo 'Generate M points'
    echo "Usage: $0 <M>"
    exit 1
fi

POINT_NUM=$1

for i in $( seq 1 $POINT_NUM ) ; do
    id=${i}
    x=$((RANDOM%100))
    y=$((RANDOM%100))
    echo -e "${id}\t${x},${y}"
done
