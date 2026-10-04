"""
Tugas Individu - Implementasi WordCount dengan PySpark
Alur: Dataset -> Baca (textFile) -> Preprocessing -> Transformation -> Action -> Hasil
"""
import re, time, collections
from pyspark.sql import SparkSession

# 1. Inisialisasi SparkSession (mode lokal memakai semua core)
spark = (SparkSession.builder.appName("WordCount").master("local[*]")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")
PATH = "data/bigdata.txt"

def preprocess(line):
    """lowercase + hapus tanda baca + split"""
    return re.sub(r"[^a-z\s]", "", line.lower()).split()

def wordcount(num_partitions=None):
    text = sc.textFile(PATH) if num_partitions is None else sc.textFile(PATH, minPartitions=num_partitions)
    words  = text.flatMap(preprocess)                       # transformation (narrow)
    pairs  = words.map(lambda w: (w, 1))                    # transformation (narrow)
    counts = pairs.reduceByKey(lambda a, b: a + b)          # transformation (wide -> shuffle)
    return text, counts

# 2. Hasil WordCount + 10 kata terbanyak
text, counts = wordcount()
print("Jumlah baris      :", text.count())                  # action
print("Partition default :", text.getNumPartitions())
print("Kata unik         :", counts.count())                # action
top10 = counts.sortBy(lambda x: -x[1]).take(10)             # action
for w, c in top10:
    print(f"{w:<12}{c}")
print("--- Lineage (DAG) ---")
print(counts.toDebugString().decode())

# 3. Eksperimen jumlah partition (rata-rata 3 kali, setelah 1x warm-up)
wordcount(4)[1].count()
results = []
for p in [1, 2, 4, 8, 16, 32, 64]:
    ts = []
    for _ in range(3):
        t0 = time.time()
        wordcount(p)[1].sortBy(lambda x: -x[1]).take(10)
        ts.append(time.time() - t0)
    results.append((p, sum(ts) / len(ts)))
    print(f"partition={p:<4} waktu rata-rata={results[-1][1]:.3f} detik")

# 4. Pembanding single machine (Python murni, 1 proses)
t0 = time.time()
cnt = collections.Counter()
with open(PATH, encoding="utf-8") as f:
    for line in f:
        cnt.update(preprocess(line))
single_time = time.time() - t0
print(f"Single machine (Python murni): {single_time:.3f} detik")
print("Top-10 single machine:", cnt.most_common(10))

import json
json.dump({"partitions": results, "single_machine": single_time,
           "cores": sc.defaultParallelism}, open("results.json", "w"))
spark.stop()
