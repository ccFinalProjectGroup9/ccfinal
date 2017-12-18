import pickle

with open("reversed_userNameMap") as f1, open("reversed_restaurantNameMap") as f2:
    reversed_userNameMap = pickle.load(f1)
    reversed_restaurantNameMap = pickle.load(f2)

userRecommendationMap = {}
with open("results") as results:
    for result in results:
        uid, restaurantRatings = result.strip().split('\t')
        userRecommendationMap[reversed_userNameMap[int(uid)]] = map(lambda restaurantRating: reversed_restaurantNameMap[int(restaurantRating.split(':')[0])],
                                                                    restaurantRatings.split(','))
# save recommendation result
with open("userRecommendationMap","w") as f:
    pickle.dump(userRecommendationMap, f)
