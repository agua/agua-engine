#!/bin/bash
    
hostname -f 
date
    
SLEEP=$1;

echo "Sleeping $SLEEP seconds"
sleep $SLEEP;

date

echo "Completed"
