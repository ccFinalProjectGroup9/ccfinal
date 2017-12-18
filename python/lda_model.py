import json
from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.wordnet import WordNetLemmatizer
import gensim
from gensim import corpora
import pickle

# filter words
tokenizer = RegexpTokenizer(r'\w+')
# filter out stop words
en_stop = get_stop_words('en')
# lemmatizer words
lemmatizer = WordNetLemmatizer()

# docs contains processed reviews
docs = []
with open("restaurant_reviews.json") as reviews:
    for review in reviews:
        review = tokenizer.tokenize(json.loads(review)["text"].lower())
        stemmed_tokens = [lemmatizer.lemmatize(token) for token in review if not token in en_stop]
        docs.append(stemmed_tokens)

# create word-id dictionary, keep 10000 most frequently words
dictionary = corpora.Dictionary(docs)
dictionary.filter_extremes(keep_n=10000)
dictionary.compactify()
corpora.Dictionary.save(dictionary, "dictionary.dict")
# change reviews into vector
corpus = [dictionary.doc2bow(doc) for doc in docs]
# train lda model, retain top50 topics
ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=50, id2word = dictionary)
ldamodel.save("lda_model.lda")

print(ldamodel.print_topics(num_topics=50, num_words=5))

#####################################################################################################

# generate topic vector for each restaurant
restaurantMap = {}
# merge a review vector to a restaurant
def mergeTopic(topics, weightsAndCount):
    weightsAndCount[1] += 1
    for topicId, weight in topics:
        weightsAndCount[0][topicId] += weight

# init restaurantMap for lv restaurant
with open("restaurants_lv.json") as restaurants:
    for restaurant in restaurants:
        restaurant = json.loads(restaurant)
        if restaurant["business_id"] not in restaurantMap:
            # key = restaurantId, value = [double[50], count]
            restaurantMap[restaurant["business_id"]] = [[0.0 for _ in range(50)], 0]

# generate restaurantMap
with open("restaurant_reviews.json") as reviews:
    for review in reviews:
        review = json.loads(review)
        if review["business_id"] not in restaurantMap: continue
        text = tokenizer.tokenize(review["text"].lower())
        stemmed_tokens = [lemmatizer.lemmatize(token) for token in text if not token in en_stop]
        topics = model[dictionary.doc2bow(stemmed_tokens)]
        weightsAndCount = restaurantMap[review["business_id"]]
        mergeTopic(topics, weightsAndCount)

# generate normalized map and save for the server
normedMap = {}
for id, weights in restaurantMap.items():
    normedMap[id] = map(lambda x: x/weights[1], weights[0])

with open("normedMap","w") as f:
    pickle.dump(normedMap, f)
