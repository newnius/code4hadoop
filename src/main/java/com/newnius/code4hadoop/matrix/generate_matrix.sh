#! /bin/bash

if [ $# -lt 2 ]; then
	echo 'Generate a matrix of row, column in form of <i,j\tval>'
	echo "Usage: $0 <row> <column>"
	exit 1
fi

ROW=$1
COLUMN=$2

for i in $( seq 1 $ROW ) ; do
	for j in $( seq 1 $COLUMN ) ; do
		s=$((RANDOM%100))
		echo -e "${i},${j}\t$s"
	done
done
