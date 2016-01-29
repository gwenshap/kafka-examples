Using a MessageHandler in MirrorMaker
======================================

* Add the jar containing the handler to the class path. For example:

    `export CLASSPATH=$CLASSPATH:/Users/gwen/workspaces/kafka-examples/MirrorMakerHandler/target/TopicSwitchingHandler-1.0-SNAPSHOT.jar`

* Start MirrorMaker. Specify the Handler class in "--message.handler" and any arguments in "--message.handler.args". For example:

    `bin/kafka-mirror-maker.sh --consumer.config config/consumer.properties --message.handler com.shapira.examples.TopicSwitchingHandler --message.handler.args dc1 --producer.config config/producer.properties --whitelist mm1`

* Test it with a producer on source topic and consumer on destination:

    `bin/kafka-console-producer.sh --topic mm1 --broker-list localhost:9092`
    
    `bin/kafka-console-consumer.sh --topic dc1.mm1 --zookeeper localhost:2181 --from-beginning`
