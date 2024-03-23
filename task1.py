import sys
from pyspark import SparkContext, SparkConf
import json

conf = SparkConf().setAppName("Task1")
sc = SparkContext(conf=conf)

input_file = sys.argv[1]
output_file = sys.argv[2]

revs = sc.textFile(input_file)

# A : Total reviews
numb_reviews = revs.count()

# B : Reviews in 2018
revs_2018 = revs.filter(lambda line: '2018' in json.loads(line)["date"])
numb_reviews_2018 = revs_2018.count()

# C : Distinct users who wrote reviews
dist_users = revs.map(lambda line: json.loads(line)["user_id"]).distinct()
numb_users = dist_users.count()

# D : Top 10 users with largest num of reviews
user_counts = revs.map(lambda line: (json.loads(line)["user_id"], 1)).reduceByKey(lambda a, b: a + b)
top = user_counts.takeOrdered(10, key=lambda x: (-x[1], x[0]))

# E : Number of distinct businesses that have been reviewed
dist_businesses = revs.map(lambda line: json.loads(line)["business_id"]).distinct()
numb_businesses = dist_businesses.count()

# F : Top 10 businesses with the largest number of reviews
busi_counts = revs.map(lambda line: (json.loads(line)["business_id"], 1)).reduceByKey(lambda a, b: a + b)
top_businesses = busi_counts.takeOrdered(10, key=lambda x: (-x[1], x[0]))

output_data = {
    "n_review": numb_reviews,
    "n_review_2018": numb_reviews_2018,
    "n_user": numb_users,
    "top10_user": [[user, count] for user, count in top],
    "n_business": numb_businesses,
    "top10_business": [[business, count] for business, count in top_businesses]
}

with open(output_file, 'w') as outfile:
    json.dump(output_data, outfile)

sc.stop()