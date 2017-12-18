import pickle
import boto3
import random
import numpy as np
from sklearn.metrics.pairwise import paired_cosine_distances
import heapq
import json

class Restaurant:
    def __init__(self, restaurant):
        self.business_id = restaurant['business_id']
        self.name = restaurant['name']
        self.address = restaurant['address']
        self.lat = restaurant['latitude']
        self.lon = restaurant['longitude']
        self.stars = restaurant['stars']
        self.review_counts = restaurant['review_count']

# load picture map as a mock database
with open("indexedPicData") as f:
    pictureDict = pickle.load(f)

# load restaurant topic weights
with open("normedMap") as f:
    restaurantWeights = pickle.load(f)

# dynamodb access
mysession = boto3.session.Session(aws_access_key_id='',
                                  aws_secret_access_key='',
                                  region_name='us-east-1')
dynamodb = mysession.resource('dynamodb')
table = dynamodb.Table('restaurant')

def getElement(key):
    response = table.get_item(
        Key={ 'business_id': key }
    )
    return response['Item'] if 'Item' in response else None

def getRecommendation(data):
    dataDict = json.loads(data)
    length = len(dataDict['pic'])
    weights = np.array([sum(col)/length for col in zip(*[pictureDict[int(picdata)]['weights'] for picdata in dataDict['pic']])]).reshape(1,-1)
    minHeap = []
    for restaurant, thatWeights in restaurantWeights.items():
        heapq.heappush(minHeap, (-paired_cosine_distances(weights, np.array(thatWeights).reshape(1,-1)), restaurant))
        if len(minHeap) > 10:
            heapq.heappop(minHeap)
    res = []
    while minHeap:
        res.append(heapq.heappop(minHeap)[1])
    return {'restaurants': [getElement(i) for i in res[::-1]] }

def getPic():
    ids = random.sample(range(2924), 9)
    return {'pic':[ { 'id': pictureDict[id]['uid'],
                            'url': 'https://s3.amazonaws.com/cc6998test/photo/' + pictureDict[id]['pic_id'] + '.jpg',
                            'selected': False } for id in ids]}
