# Yang Zheng  8812635860
from pyspark import SparkContext, SparkConf
from itertools import combinations
import time
import sys


def split_to_biz_usr(x):
    pair = x.split(',')
    return str(pair[1]), [str(pair[0])]
    

def jaccard_similarity(x):
    a = BIZ_USRSET.get(x[0])
    b = BIZ_USRSET.get(x[1])
    sim = float(len(a & b)) / float(len(a | b))
    return x, sim


def to_binary_vec(x):
    result = [0 for i in range(0, len(USER_TO_IDX))]
    for usr in x[1]:
        result[USER_TO_IDX.get(usr)] = 1
    return x[0], result


def minhash(x):
    biz, vector = x[0], x[1]
    signature = [U + 1 for i in range(K)]
    for i in range(0, U):
        if vector[i] == 1:
            for k in range(0, K):
                signature[k] = min(signature[k], PERMUTATIONS[k][i])
    return biz, signature


def LSH(x):
    signature_matrix = list(x)
    candidates = []
    for b in range(0, B):
        bucket = {}
        for i in range(0, M):
            band = tuple(signature_matrix[i][1][b * R: (b + 1) * R])
            if band not in bucket:
                bucket[band] = [signature_matrix[i][0]]
            else:
                bucket[band].append(signature_matrix[i][0])
        for key in bucket:
            if len(bucket[key]) >= 2:
                candidates += list(combinations(sorted(bucket[key]), 2))
    return candidates


def get_permutations():
    permutations = []
    original = [i for i in range(0, M)]
    para = [
        (29, 113), (37, 72), (41, 67), (47, 35), (53, 101),
        (73, 803), (97, 27), (83, 79), (89, 44), (101, 83),
        (163, 3), (167, 12), (193, 28), (197, 656), (133, 89),
        (211, 27), (179, 12), (233, 54), (311, 2), (353, 23),
        (389, 29), (443, 551), (497, 12), (577, 77), (599, 98),
        (757, 12), (769, 210), (913, 901), (919, 19), (929, 29),
        (1543, 211), (1487, 234), (1489, 62), (2309, 12),(2423, 1005),
        (3079, 212), (3677, 73), (2617, 123), (2663, 1423), (4001, 197),
        (4547, 18), (5099, 79), (6043, 254),(6151, 213),(7703, 233),
        (9613, 46), (8867, 21), (6833, 34)
    ] 
    for p in para:
        permutation = [(p[0] * x + p[1]) % M for x in original]
        permutations.append(permutation)
    return permutations


if __name__ == "__main__":
    start = time.time()

    appName = "spark-similar-items"
    master = "local[*]"
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    # arguments
    train_file, output_file = sys.argv[1:3]
    
    train_rdd = sc.textFile(train_file)
    header = train_rdd.first()
    train_rdd = train_rdd \
        .filter(lambda row: row != header) \
        .map(lambda x: split_to_biz_usr(x))

    all_users = train_rdd \
        .map(lambda x: x[1][0]).distinct().collect()

    biz_usr =  train_rdd \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[0], set(x[1])))

    BIZ_USRSET = biz_usr.collectAsMap()
    M = len(BIZ_USRSET) # number of businesses
    U = len(all_users)  # number of users
    K = 48 # number of hash functions
    B = 16 # number of bands
    R = 3  # number of rows in the band
    PERMUTATIONS = get_permutations()

    print(len(PERMUTATIONS[0]))
    USER_TO_IDX = {}
    for i in range(len(all_users)):
        USER_TO_IDX[all_users[i]] = i

    #_______________________________________________________________
    # Minhashing and Locality Sensitive Hashing
    #_______________________________________________________________

    # minhash: generate signature matrix
    signature_matrix = biz_usr \
        .map(lambda x: to_binary_vec(x)) \
        .map(lambda x: minhash(x)) \
        .repartition(1)
    
    # LSH: find candidate pairs
    candidates = signature_matrix \
        .mapPartitions(LSH) \
        .distinct()

    # eliminate all False Positives
    similar_pairs = candidates \
        .map(lambda x: jaccard_similarity(x)) \
        .filter(lambda x: x[1] >= 0.5) \
        .sortBy(lambda x: (x[0], x[1])) \
        .collect()

    with open(output_file, 'w') as f:
        f.write('business_id_1, business_id_2, similarity\n')
        for pair in similar_pairs:
            f.write(str(pair[0][0]) + ',' + str(pair[0][1]) + ',' + str(pair[1]))
            f.write('\n')
    
    print("Duration:" + str(time.time() - start))