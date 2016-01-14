package com.shapira.examples.kstreamavg;


import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.AggregatorSupplier;

public class AvgAggregatorSupplier<K, V, T> implements AggregatorSupplier {
    @Override
    public Aggregator get() {
        return new AvgAggregator();
    }
}
