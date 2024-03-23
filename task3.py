import sys
import json
from pyspark import SparkContext
from datetime import datetime

sc_m1 = SparkContext(appName="Task3_M1")

revs_file = sys.argv[1]
busi_file = sys.argv[2]
out_file_txt = sys.argv[3]
out_file_json = sys.argv[4]

## M1

start_time_m1 = datetime.now()

reviews_rdd = sc_m1.textFile(revs_file)
businesses_rdd = sc_m1.textFile(busi_file)

def parse_revs_m1(l):
    review = json.loads(l)
    return (review["business_id"], review["stars"])

def parse_busi_m1(l):
    business = json.loads(l)
    return (business["business_id"], business["city"])

rev_stars_rdd = reviews_rdd.map(parse_revs_m1)
busi_cities_rdd = businesses_rdd.map(parse_busi_m1)
joined_data_rdd = rev_stars_rdd.join(busi_cities_rdd)

city_avg_stars_rdd = joined_data_rdd.map(lambda x: (x[1][1], x[1][0])) \
                                    .groupByKey() \
                                    .mapValues(lambda stars: sum(stars) / len(stars))

top_10_cities_m1 = city_avg_stars_rdd.collect()
top_10_cities_m1 = sorted(top_10_cities_m1, key = lambda x: (-x[1], x[0]))[:10]

print("city,stars")
for city, avg_stars in top_10_cities_m1:
    print(f"{city},{avg_stars}")

execution_time_m1 = datetime.now() - start_time_m1
time_m1=execution_time_m1.total_seconds()
time_m1=float(time_m1)

print(f"\nExecution Time for M1: {time_m1}")

sc_m1.stop()

## M2

# Initialize SparkContext for M2
sc_m2 = SparkContext(appName="Task3_M2")

start_time_m2 = datetime.now()

reviews_rdd = sc_m2.textFile(revs_file)
businesses_rdd = sc_m2.textFile(busi_file)

def parse_revs_m2(l):
    review = json.loads(l)
    return (review["business_id"], review["stars"])

def parse_busi_m2(l):
    business = json.loads(l)
    return (business["business_id"], business["city"])

rev_stars_rdd = reviews_rdd.map(parse_revs_m2)
busi_cities_rdd = businesses_rdd.map(parse_busi_m2)
joined_data_rdd = rev_stars_rdd.join(busi_cities_rdd)

city_avg_stars_rdd = joined_data_rdd.map(lambda x: (x[1][1], x[1][0])) \
                                    .groupByKey() \
                                    .mapValues(lambda stars: sum(stars) / len(stars))

sorted_results_rdd = city_avg_stars_rdd.sortBy(lambda x: (-x[1], x[0]))

top_10_cities_m2 = sorted_results_rdd.take(10)

print("city,stars")
for city, avg_stars in top_10_cities_m2:
    print(f"{city},{avg_stars}")

execution_time_m2 = datetime.now() - start_time_m2
time_m2=execution_time_m2.total_seconds()
time_m2=float(time_m2)

print(f"\nExecution Time for M2: {time_m2}")

out_data = sorted_results_rdd.map(lambda x: "{},{}".format(x[0], x[1]))

with open(out_file_txt, 'w') as outfile:
    outfile.write("city,stars\n")
    for l in out_data.collect():
        outfile.write(l + "\n")

output_data_json = {
    "m1": time_m1,
    "m2": time_m2,
    "reason": "In this code, M1 is faster than M2. This is because M1 does not involve sorting within the Spark framework and utilizes Python's native sorting capabilities,  which are more suitable for smaller datasets like in this case. M2 uses sorting operations using sortBy and take which have their own additional overheads due to the distributed Spark framework. But it is inconsistent and will vary for different cases with varying dataset sizes."
}

with open(out_file_json, 'w') as outfile:
    json.dump(output_data_json, outfile)

sc_m2.stop()