package com.netflix.hystrix.util;

public class HystrixLongAdderBucket extends HystrixMetricsBucket {
	final LongAdder[] adderForCounterType;
	final LongMaxUpdater[] updaterForCounterType;

	HystrixLongAdderBucket(long startTime) {
		super(startTime);

		/*
		 * We support both LongAdder and LongMaxUpdater in a bucket but don't want the memory allocation
		 * of all types for each so we only allocate the objects if the HystrixRollingNumberEvent matches
		 * the correct type - though we still have the allocation of empty arrays to the given length
		 * as we want to keep using the type.ordinal() value for fast random access.
		 */

		// initialize the array of LongAdders
		adderForCounterType = new LongAdder[HystrixRollingNumberEvent.values().length];
		for (HystrixRollingNumberEvent type : HystrixRollingNumberEvent.values()) {
			if (type.isCounter()) {
				adderForCounterType[type.ordinal()] = new LongAdder();
			}
		}

		updaterForCounterType = new LongMaxUpdater[HystrixRollingNumberEvent.values().length];
		for (HystrixRollingNumberEvent type : HystrixRollingNumberEvent.values()) {
			if (type.isMaxUpdater()) {
				updaterForCounterType[type.ordinal()] = new LongMaxUpdater();
				// initialize to 0 otherwise it is Long.MIN_VALUE
				updaterForCounterType[type.ordinal()].update(0);
			}
		}
	}

	long get(HystrixRollingNumberEvent type) {
		if (type.isCounter()) {
			return adderForCounterType[type.ordinal()].sum();
		}
		if (type.isMaxUpdater()) {
			return updaterForCounterType[type.ordinal()].max();
		}
		throw new IllegalStateException("Unknown type of event: " + type.name());
	}

	LongAdder getAdder(HystrixRollingNumberEvent type) {
		if (!type.isCounter()) {
			throw new IllegalStateException("Type is not a Counter: " + type.name());
		}
		return adderForCounterType[type.ordinal()];
	}

	LongMaxUpdater getMaxUpdater(HystrixRollingNumberEvent type) {
		if (!type.isMaxUpdater()) {
			throw new IllegalStateException("Type is not a MaxUpdater: " + type.name());
		}
		return updaterForCounterType[type.ordinal()];
	}
}
