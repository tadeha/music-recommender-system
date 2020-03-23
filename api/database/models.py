#~api/database/models.py
from .db import db
import datetime

class TrackSim(db.Document):
    TRACK_ID = db.FloatField(required=True, unique=True)
    TRACK_RECS = db.ListField(db.StringField(), required=True)
    
class TrackSimTracks(db.Document):
    TRACK_ID = db.FloatField(required=True)
    SIMILARITIES = db.StringField(required=True)
    
    def extract_similarities(self):
        return self.SIMILARITIES[1:-1].split(',')
    
class UserRec(db.Document):
    USER_ID = db.FloatField(required=True)
    RECOMMENDATION_IDS = db.StringField(required=True)
    
    def extract_recommendations(self):
        return self.RECOMMENDATION_IDS[1:-1].split(',')
    
class MiniActionTime(db.Document):
    USER_ID = db.FloatField(required=True)
    TRACK_IDS = db.StringField(required=True)
    ACTION_COUNTS = db.FloatField(required=True)
    MIN_C_DATE = db.DateTimeField(required=True)
    MAX_C_DATE = db.DateTimeField(required=True)
    
    def extract_track_ids(self):
        return self.TRACK_IDS[1:-1].split(',')

class ActionTime(db.Document):
    USER_ID = db.FloatField(required=True)
    TRACK_ID = db.FloatField(required=True)
    C_DATE = db.DateTimeField(required=True, default=datetime.datetime.now)
    PUBLISH_DATE = db.DateTimeField(required=True)
    ACTION = db.StringField(required=True)
    
class Trends(db.Document):
    TRACK_ID = db.FloatField(required=True, unique=True)
    ACTIONS_COUNT = db.FloatField(required=True)
    
class Tracks(db.Document):
    TRACK_ID = db.FloatField(required=True, unique=True)
    TAG_IDS = db.ListField(db.StringField(), required=True)
    TYPE_KEYS = db.ListField(db.StringField(), required=True)
    