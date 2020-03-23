#~api/app.py
from flask import Flask, request, Response, jsonify, render_template
from flask_cors import CORS, cross_origin
from database.db import initialize_db
from database.models import TrackSim, UserRec, MiniActionTime, ActionTime, Trends, Tracks, TrackSimTracks

import random

number_of_actions_to_check = 16

app = Flask(__name__)
cors = CORS(app)

app.config['MONGODB_SETTINGS'] = {
    'host': 'YOUR_HOST'
}
app.config['CORS_HEADERS'] = 'Content-Type'

initialize_db(app)

@app.route('/user/<user_id>/recommend/')
@cross_origin()
def recommendTo(user_id):
    if len(MiniActionTime.objects.filter(USER_ID=user_id)) == 0:
        return jsonify(response='success', \
                recommended_tracks=getTrends(), \
                used_technique='Trends Collection'), 200
    else:
        user_action = MiniActionTime.objects.filter(USER_ID=user_id)[0]
        if user_action.ACTION_COUNTS <= number_of_actions_to_check:
            similar_tracks = []
            user_tracks = TrackSimTracks.objects.filter(TRACK_ID__in=user_action.extract_track_ids())
            
            for user_track in user_tracks:
                similar_tracks.extend(user_track.extract_similarities())
            
            return jsonify(response='success', \
                recommended_tracks=random.choices(list(set(similar_tracks)), k=10), \
                used_technique='Content-Based'), 200
        else:
            user_recs = UserRec.objects.filter(USER_ID=user_id)
            return jsonify(response='success', \
                recommended_tracks=user_recs[0].extract_recommendations(), \
                used_technique='Collaborative Filtering'), 200

@app.route('/track/<track_id>/similars/')
@cross_origin()
def findSimilarsTo(track_id):
    num_of_samples = 10
    num_of_samples = int(request.args.get("num_of_samples",num_of_samples))
    
    tracks = TrackSimTracks.objects().filter(TRACK_ID=track_id)
    
    if len(tracks) == 1:
        return jsonify(response='success', \
               similar_tracks=random.choices(tracks[0].extract_similarities(),k=num_of_samples), \
               used_technique='Content-Based'), 200
    else:
        return jsonify(response='success', \
               similar_tracks=getTrends(n=num_of_samples), \
               used_technique='Trends Collection'), 200

def getTrends(n=10):
    trends = Trends.objects
    track_ids = []
    for trend in trends:
        track_ids.append(trend.TRACK_ID)
        
    return random.choices(track_ids,k=n)

@app.route('/action/', methods=['POST'])
@cross_origin()
def addActionWith(): 
    body = request.get_json()
    action = ActionTime(**body).save()
    user_id = action.USER_ID
    return jsonify('Action Saved for USER_ID: ' + str(user_id)), 201

@app.route('/track/', methods=['POST'])
@cross_origin()
def addTrackWith():
    body = request.get_json()
    track = Tracks(**body).save()
    track_id = track.TRACK_ID
    return jsonify('Track Saved with TRACK_ID: ' + str(track_id)), 201

@app.route('/view/')
def getView():
    return render_template('dashboard.html')

if __name__ == '__main__':
      app.run(host='0.0.0.0', port=54876, debug=True)
