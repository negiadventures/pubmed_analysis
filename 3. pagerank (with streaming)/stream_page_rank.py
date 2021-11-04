import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


def computeContribs(neighbors, rank):
    for neighbor in neighbors:
        yield (neighbor, rank / len(neighbors))


def main(input_folder_location):
    sc = SparkContext.getOrCreate()
    ssc = StreamingContext(sc, 3)  # Streaming will execute in each 3 seconds
    lines = ssc.textFileStream(input_folder_location)  # 'log/ mean directory name
    counts = lines.map(lambda line: line.split(",")).map(lambda pages: (pages[0], pages[1])).transform(lambda rdd: rdd.distinct()).groupByKey().map(lambda x: (x[0], list(x[1])))
    ranks = counts.map(lambda element: (element[0], 1.0))
    contribs = counts.join(ranks).flatMap(lambda row: computeContribs(row[1][0], row[1][1]))
    print(" iter ---------")
    ranks = contribs.reduceByKey(lambda v1, v2: v1 + v2)
    ranks
    ranks.pprint()
    print(" finishing the task")
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    main('edges')
