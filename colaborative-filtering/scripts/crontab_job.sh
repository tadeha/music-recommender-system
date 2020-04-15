#!/bin/bash

HOME=/home/rc12g2
HADOOP_HOME=/user/rc12g2
source $HOME/.bashrc

echo "spark job -> started"

hadoop fs -rm -r -f $HADOOP_HOME/final-actions-v3
spark-submit $HOME/beeptunes_recsys/colaborative-filtering/final-action-aggregator.py > $HOME/log/spark.log
hadoop fs -getmerge $HADOOP_HOME/user_recs_v3 $HOME/result/user_recs_v3.csv
hadoop fs -rm -r -f $HADOOP_HOME/user_recs_v3
sed -i '1USER_ID,RECOMMENDATION_IDS' $HOME/result/user_recs_v3.csv
mongoimport --port 28018 -u 4max -p HFmk87Q3DgfEKKgC --type csv -d g2recsys -c user_rec --headerline --drop $HOME/result/user_recs_v3.csv

echo "spark run -> complete"