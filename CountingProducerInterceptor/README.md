This is a ProducerInterceptor that every N milliseconds prints how many records were produced and how many were 
acknowledged in last interval.

To try:

* Build it with `mvn clean package`
* Add jar to classpath: `export CLASSPATH=$CLASSPATH:~./target/CountingProducerInterceptor-1.0-SNAPSHOT.jar`
* Create a config file that includes:
```
interceptor.classes=com.shapira.examples.interceptors.CountingProducerInterceptor
counting.interceptor.window.size.ms=10000
```
* Try with the console producer:
` bin/kafka-console-producer.sh --broker-list localhost:9092 --topic interceptor-test --producer.config /tmp/producer.config`
