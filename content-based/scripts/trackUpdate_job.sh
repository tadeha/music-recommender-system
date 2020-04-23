#!/bin/bash

HOME=/home/rc12g2

echo "update_track_collection job -> started"

cd $HOME/beeptunes_recsys/jobs

mongoexport --port 28018 -u 4max -p HFmk87Q3DgfEKKgC --type csv -d g2recsys -c tracks --fieldFile tracks_fields.txt  --out ./export.csv > $HOME/log/update_track_collection.log

python update_trackCollection.py

mongo --port 28018 -u 4max -p HFmk87Q3DgfEKKgC g2recsys --eval 'db.tracks.drop()'

rm $HOME/beeptunes_recsys/jobs/export.csv

echo "update_track_collection job -> ended"