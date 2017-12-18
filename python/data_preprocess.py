import json
import os
import glob
import shutil

restaurantsIdSet = set()
restaurantsIdSetLv = set()

# restaurants_lv.json contains all restaurant in LV
with open("business.json") as businesses, open("restaurants_lv.json", "w") as output:
    for business in businesses:
        business = json.loads(business)
        if "Restaurants" not in business["categories"]: continue
        restaurantsIdSet.add(business["business_id"])
        if business["city"] != "Las Vegas": continue
        restaurantsIdSetLv.add(business["business_id"])
        output.write(json.dumps(business) + '\n')

# restaurant_reviews.json contains all reviews about restaurant, which would be used in LDA model training
with open("review.json") as reviews, open("restaurant_reviews.json", "w") as output:
    for review in reviews:
        review = json.loads(review)
        if review["business_id"] not in restaurantsIdSet: continue
        output.write(json.dumps(review) + '\n')

photoIdSet = set()
with open("./yelp_photos/photos.json") as photos, open("photos_lv.json", "w") as output:
    for photo in photos:
        photo = json.loads(photo)
        if photo["business_id"] in restaurantsIdSetLv:
            photoIdSet.add(photo["photo_id"])
            output.write(json.dumps(photo) + '\n')

with open("photos_lv.json") as photos:
    for photo in photos:
        photo = json.loads(photo)
        photoIdSet.add(photo["photo_id"])

# extract lv restaurants' photos into another folder
for path in glob.glob("./yelp_photos/photos/*"):
    _, fileName = os.path.split(path)
    if fileName.split('.')[0] in photoIdSet:
        shutil.copy(path, './photo_lv/{}'.format(fileName))
