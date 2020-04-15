#!/bin/bash
DATA_DIR=$1
echo "Trying to convert $DATA_DIR from hadoop"
hadoop fs -getmerge /user/rc12g2/$DATA_DIR data.csv
echo "Merged result saved into data.csv"
hadoop fs -rm /user/rc12g2/$DATA_DIR/*
echo "TRACK_ID,USER_ID,DOWNLOAD,LIKED,PURCHASE,IMPRESSION,TYPE_CNT,TAG_CNT,N_PRICE,N_DURATION" > result.csv
cat data.csv >> result.csv
echo "Removed small csvs from hadoop"
hadoop fs -put result.csv /user/rc12g2/$DATA_DIR/data.csv
echo "Done"
