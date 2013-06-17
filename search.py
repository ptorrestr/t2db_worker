from datetime import datetime

#Search object. Contains the id of the current search, the started date and
#the query used.
class Search(object):
    def __init__(self, _id, query):
        self.id = _id
        self.created_at = str(datetime.now()) 
        self.query = query
