#! /bin/bash

if [ $# -lt 1 ]; then
    echo 'generate N points within M types as trained points'
    echo "Usage: $0 <N>"
    exit 1
fi

POINT_NUM=$1

for i in $( seq 1 $POINT_NUM ) ; do
    id=${i}
    x=$((RANDOM%100))
    y=$((RANDOM%100))
    type=$((RANDOM%${2}))
    echo -e "${id}\t${x},${y}\t${type}"
done
