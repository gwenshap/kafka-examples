# to run on a CDH cluster:
spark-submit  --master local[3] --class com.shapira.examples.streamingavg.StreamingAvg uber-StreamingAvg-1.0-SNAPSHOT.jar localhost:2181/kafka g1 t3 1
