package com.shapira.examples.kstreamavg;

import org.apache.kafka.streams.kstream.Aggregator;

public class AvgAggregator<K, V, T> implements Aggregator<String, Integer, AvgValue> {

    public AvgValue initialValue() {
        return new AvgValue(0,0);
    }

    public AvgValue add(String aggKey, Integer value, AvgValue aggregate) {
        return new AvgValue(aggregate.count + 1, aggregate.sum + value );
    }

    public AvgValue remove(String aggKey, Integer value, AvgValue aggregate) {
        return new AvgValue(aggregate.count - 1, aggregate.sum - value);
    }

    public AvgValue merge(AvgValue aggr1, AvgValue aggr2) {
        return new AvgValue(aggr1.count + aggr2.count, aggr1.sum + aggr2.sum);
    }
}
