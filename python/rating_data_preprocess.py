import json
import pickle

# extract lv restaurant bid set
restaurantSet = set()
with open("processed_data/restaurants_lv.json") as restaurants:
    for restaurant in restaurants:
        restaurant = json.loads(restaurant)
        restaurantSet.add(restaurant['business_id'])
print len(restaurantSet)

# map hashed bid into integer value, write data into plain text file
uid = 0
rid = 0
userNameMap = {}
restaurantNameMap = {}
reversed_userNameMap = {}
reversed_restaurantNameMap = {}
with open("processed_data/restaurant_reviews.json") as reviews, open('data.txt', 'w') as output:
    # output.write('user,item,rating\n')
    for review in reviews:
        review = json.loads(review)
        if review["business_id"] not in restaurantSet: continue
        if review['user_id'] not in userNameMap:
            userNameMap[review['user_id']] = uid
            reversed_userNameMap[uid] = review['user_id']
            uid += 1
        if review['business_id'] not in restaurantNameMap:
            restaurantNameMap[review['business_id']] = rid
            reversed_restaurantNameMap[rid] = review['business_id']
            rid += 1
        output.write(','.join([userNameMap[review['user_id']], restaurantNameMap[review["business_id"]], str(review['stars'])]) + '\n')

# save reversed id-uid map for result retrieving
with open("reversed_userNameMap","w") as f1, open("reversed_restaurantNameMap", "w") as f2:
    pickle.dump(reversed_userNameMap, f1)
    pickle.dump(reversed_restaurantNameMap, f2)
