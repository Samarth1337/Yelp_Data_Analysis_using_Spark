import sys
import json
from time import time
from pyspark import SparkContext

rev_filepath = sys.argv[1]
put_filepath = sys.argv[2]
numb_part = int(sys.argv[3])

sc = SparkContext("local[*]", "Task2")
sc.setLogLevel("ERROR") 

revs = sc.textFile(rev_filepath).map(lambda x: json.loads(x))

def custom_part(key):
    return int(hash(key) % numb_part)

default_start_time = time()
busi_counts_default = revs.map(lambda line: (line["business_id"], 1)).reduceByKey(lambda a, b: a + b)
top_businesses_default = busi_counts_default.takeOrdered(10, key=lambda x: (-x[1], x[0]))
default_exe_time = time() - default_start_time
numb_part_default = busi_counts_default.getNumPartitions()
items_part_default = busi_counts_default.glom().map(len).collect()

custom_start_time = time()
revs_custom = revs.map(lambda line: (line["business_id"], 1)).partitionBy(numb_part, custom_part)
busi_counts_custom = revs_custom.reduceByKey(lambda a, b: a + b)
top_businesses_custom = busi_counts_custom.top(10, key=lambda x: x[1])
custom_exe_time = time() - custom_start_time
numb_part_custom = busi_counts_custom.getNumPartitions()
items_part_custom = busi_counts_custom.glom().map(len).collect()

output ={
    "default":{
        "n_partition": numb_part_default,
        "n_items":items_part_default,
        "exe_time": default_exe_time
    },
    "customized":{
        "n_partition":numb_part_custom,
        "n_items":items_part_custom,
        "exe_time":custom_exe_time
    }
}

with open(put_filepath, 'w') as out_file:
    json.dump(output, out_file)

sc.stop()
