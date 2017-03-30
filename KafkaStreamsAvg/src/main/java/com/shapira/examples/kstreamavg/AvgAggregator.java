package com.shapira.examples.kstreamavg;

import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;

//AvgAggregator<K, V, T> implements Aggregator<String, Integer, AvgValue>
public class AvgAggregator {
	
	public static class InternalAvgInitializer implements Initializer<AvgValue> {

		@Override
		public AvgValue apply() {
	        return new AvgValue(0,0);
		}
		
	}
	
	public static Initializer<AvgValue> initializer() {
		return new InternalAvgInitializer();
	}
	
	public static class InternalAvgAggregator implements Aggregator<String, Integer, AvgValue> {

		@Override
		public AvgValue apply(String aggKey, Integer value, AvgValue aggregate) {
	        return new AvgValue(aggregate.count + 1, aggregate.sum + value );
		}
		
	}
	
	public static Aggregator<String, Integer, AvgValue> aggregator() {
		return new InternalAvgAggregator();
	}

//	
//
//    public AvgValue initialValue() {
//        return new AvgValue(0,0);
//    }
//
//    public AvgValue add(String aggKey, Integer value, AvgValue aggregate) {
//        return new AvgValue(aggregate.count + 1, aggregate.sum + value );
//    }
//
//    public AvgValue remove(String aggKey, Integer value, AvgValue aggregate) {
//        return new AvgValue(aggregate.count - 1, aggregate.sum - value);
//    }
//
//    public AvgValue merge(AvgValue aggr1, AvgValue aggr2) {
//        return new AvgValue(aggr1.count + aggr2.count, aggr1.sum + aggr2.sum);
//    }
}
