# -*- coding: utf-8 -*-
"""
Created on Tue June 10:11:23 2023

@author: Umut, Chel, Nick
"""

from pyspark import SparkContext, SparkConf, StorageLevel 
from pyspark.streaming import StreamingContext
import sys
import threading
import numpy as np
import random
from typing import Callable, Tuple, List
 

# After how many items should we stop?
THRESHOLD = 10000
p = 8191

def generate_hash_functions(D: int, W: int) -> Tuple[List[Callable], List[Callable], List[List[int]]]:    
    p = 8191
    hash_params = [(random.randint(1, p - 1), random.randint(0, p - 1)) for _ in range(D)] 
    
    hash_h_functions: List[Callable] = [
        lambda x, a=a, b=b: ((a * x + b) % p) % W
        for (a, b) in hash_params
    ]
    hash_g_functions: List[Callable] = [
        lambda x, a=a, b=b: ((((a * x + b) % p) % 2) - 0.5) * 2
        for (a, b) in hash_params
    ]

    C: List[List[int]] = [[0 for _ in range(W)] for _ in range(D)]

    return hash_h_functions, hash_g_functions, C

def calculate_frequencies(left, right, D, hashes_h, hashes_g, C):
    frequencies = {}
    for u in range(left, right + 1):
        f_u = [hashes_g[j](u) * C[j][hashes_h[j](u)] for j in range(D)]
        frequencies[u] = sorted(f_u)[int(D/2)]
        if frequencies[u] < 0:
            frequencies[u] *= -1
    return frequencies


# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    # We are working on the batch at time `time`.
    global streamLength, histogram, streamRelevant

    batch_size = batch.count()
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0] >= THRESHOLD:
        return
    streamLength[0] += batch_size

    # Filtering range
    filteredBatch = batch \
        .map(lambda s: (int(s), 1)) \
        .filter(lambda element: (element[0] >= left) and (element[0] <= right))

    filteredBatchSize = filteredBatch.count()
    streamRelevant += filteredBatchSize

    # After filter collect the items
    batchItems = filteredBatch \
        .reduceByKey(lambda a, b: a + b) \
        .collectAsMap()

    # Update the streaming state for exact Counts of each distinct item seen so far
    for key, val in batchItems.items():
        if key not in histogram:
            histogram[key] = val
        else:
            histogram[key] += val
        for row in range(len(C)):
            C[row][hashes_h[row](key)] += val * hashes_g[row](key)

    # If we wanted, here we could run some additional code on the global histogram
    if batch_size > 0:
        print("Batch size at time [{0}] is: {1}".format(time, batch_size))

    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()


if __name__ == '__main__':
    assert len(sys.argv) == 7, "USAGE: D W left right K port"
    # IMPORTANT: when running locally, it is *fundamental* that the
    # `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
    # there will be no processor running the streaming computation and your
    # code will crash with an out of memory (because the input keeps accumulating).
    conf = SparkConf().setMaster("local[*]").setAppName("HW3")
    # If you get an OutOfMemory error in the heap consider to increase the
    # executor and drivers heap space with the following lines:
    # conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    
    
    # Here, with the duration you can control how large to make your batches.
    # Beware that the data generator we are using is very fast, so the suggestion
    # is to use batches of less than a second, otherwise you might exhaust the memory.
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)  # Batch duration of 1 second
    ssc.sparkContext.setLogLevel("ERROR")
    
    # TECHNICAL DETAIL:
    # The streaming spark context and our code and the tasks that are spawned all
    # work concurrently. To ensure a clean shut down we use this semaphore.
    # The main thread will first acquire the only permit available and then try
    # to acquire another one right after spinning up the streaming computation.
    # The second tentative at acquiring the semaphore will make the main thread
    # wait on the call. Then, in the `foreachRDD` call, when the stopping condition
    # is met we release the semaphore, basically giving "green light" to the main
    # thread to shut down the computation.
    # We cannot call `ssc.stop()` directly in `foreachRDD` because it might lead
    # to deadlocks.
    stopping_condition = threading.Event()
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # INPUT READING
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    D = int(sys.argv[1])
    W = int(sys.argv[2])
    left = int(sys.argv[3])
    right = int(sys.argv[4])
    K = int(sys.argv[5]) 
    
    portExp = int(sys.argv[6])
    print("Receiving data from port =", portExp)

    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    streamLength = [0] # Stream length (an array to be passed by reference)
    streamRelevant = 0 
    histogram = {} # Hash Table for the distinct elements
    

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following.
    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.
    
    # Generate hash functions
    hashes_h, hashes_g, C = generate_hash_functions(D, W)

    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    ssc.stop(False, True)
    print("Streaming engine stopped")

    # Exact frequencies of distinct items in Σ𝑅 are stored in histogram

    frequencies = calculate_frequencies(left, right, D, hashes_h, hashes_g, C)
    
    F2_estimationList = [sum([C[j][k]**2 for k in range(W)]) for j in range(D)]
    F2_estimated = F2_estimationList[int(D/2)] / streamRelevant**2
    F2 = sum([histogram[k]**2 for k in range(left, right + 1) if k in histogram]) / streamRelevant**2

    # Sort histogram 
    histogram = dict(sorted(histogram.items(), key=lambda item: item[1], reverse=True))
    
    print("D = {}, W = {}, [left,right] = [{},{}], K = {}, Port = {}".format(D, W, left, right, K, portExp))
    print("Total number of items = {}".format(streamLength[0]))
    print("Total number of items in [{},{}] = {}".format(left, right, streamRelevant))
    print("Number of distinct items in [{},{}] = {}".format(left, right, len(histogram))) 

    error_sum = 0
    keys = list(histogram.keys())[:K]
    for key in keys:
        absolute_error = abs(histogram[key] - frequencies[key])
        relative_error = absolute_error / histogram[key]
        error_sum += relative_error
    if K <= 20:
        print("Estimation Errors:")
        for key in keys:
            print("Item {}: Freq = {}, Est. Freq = {}".format(key, histogram[key], int(frequencies[key])))
    average_error = error_sum / K
    print("Avg err for top {} = {}:".format(K, average_error)) 
    print("F2 {} F2 Estimate {}".format(F2, F2_estimated)) 