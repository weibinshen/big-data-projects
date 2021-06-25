import re

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

# spark-submit --packages org.apache.spark:spark-streaming-flume_2.11:2.3.0 spark-streaming-from-flume.py

parts = [
    r'(?P<host>\S+)',                   # host %h
    r'\S+',                             # indent %l (unused)
    r'(?P<user>\S+)',                   # user %u
    r'\[(?P<time>.+)\]',                # time %t
    r'"(?P<request>.+)"',               # request "%r"
    r'(?P<status>[0-9]+)',              # status %>s
    r'(?P<size>\S+)',                   # size %b (careful, can be '-')
    r'"(?P<referer>.*)"',               # referer "%{Referer}i"
    r'"(?P<agent>.*)"',                 # user agent "%{User-agent}i"
]
pattern = re.compile(r'\s+'.join(parts)+r'\s*\Z')

def extractURLRequest(line):
    exp = pattern.match(line)
    if exp:
        request = exp.groupdict()["request"]
        if request:
           requestFields = request.split()
           if (len(requestFields) > 1):
                return requestFields[1] # extracts the second segment from "request"


if __name__ == "__main__":

    sc = SparkContext(appName="StreamingFlumeLogAggregator")
    sc.setLogLevel("ERROR")
    # Creating a streaming context from Spark Context, with 1-second batches.
    ssc = StreamingContext(sc, 1)

    # A push model: we push data from Flume into Spark Streaming via Avro.
    # An alternative will be to use a poll model using a custom sink in Flume, 
    # that will form a bidirectional connection between spark and flume, 
    # which is more complicated to setup, but is much more robust.

    # Here the flumeStream is actually a DStream object.
    flumeStream = FlumeUtils.createStream(ssc, "localhost", 9092)

    # These map/reduce operation is only coded once here, but since it is operating on the FlumeStream, it is going to repeat forever.
    lines = flumeStream.map(lambda x: x[1])
    urls = lines.map(extractURLRequest)

    # Reduce by URL over a 5-minute (300 seconds) window sliding every 1 second
    # because here we have a window sliding, we need to provide two reducers, 
    # one for introducing a new batch into the window, and one for removing an old batch.
    urlCounts = urls.map(lambda x: (x, 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y : x - y, 300, 1)

    # Sort and print the results
    sortedResults = urlCounts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))
    sortedResults.pprint()

    # checkpoint: the state store for the windowed operation. If the processor is down, we can restart from where we left off.
    ssc.checkpoint("/home/maria_dev/checkpoint")
    ssc.start()
    ssc.awaitTermination()
