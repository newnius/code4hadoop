#! /bin/bash

if [ $# -lt 1 ]; then
	echo 'select N points as initial center of clusters'
	echo "Usage: $0 <N>"
	exit 1
fi

POINT_NUM=$1

for i in $( seq 1 $POINT_NUM ) ; do
	read LINE
	echo -e "${i}\t${i}\t1\t${LINE}"
done
