# SimpleCounter

SimpleCounter is an example class to demonstrate how to implement a Kafka producer using Java.
There're two versions of Scala producer, see DemoProducerOld for old Scala producer
and DemoProducerNewJava for new Scala producer.

## Building Uber JAR

Uber JAR is built using [Apache Maven](http://maven.apache.org/).
To build Uber JAR, run:

    mvn clean package

## Run SimpleCounter

Running SimpleCounter first requires [Building Uber JAR](#building-uber-jar).
Once Uber JAR is built, SimpleCounter can be run using:

    ./run-params.sh broker-list topic old/new sync/async delay count

        broker-list : list of Kafka broker(s)
        topic : Kafka topic to write message(s)
        old/new : specify which producer to use (DemoProducerOld or DemoProducerNewJava)
        sync/async : specify synchronous or asynchronous mode
        delay : delay in milliseconds
        count : numbers to generate

## A Note for O'Reilly Kafka Training Video viewers

If you saw Chapter 3, "Writing Kafka Producer" of
["Introduction to Apache Kafka, A Quick Primer for Developers and Administrators"](http://shop.oreilly.com/product/0636920038603.do)
and came to this site, the example code is slightly changed to show how to use new API.
Code shown in the video is moved into DemoProducerOld.java file and new argument is introduced to select
which version of API to use.
