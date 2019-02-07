#!/bin/sh
for i in $(seq 1 100000)
do
        vsql -w 'password' -o /dev/null -f $1

done

