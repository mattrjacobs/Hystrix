package com.netflix.hystrix.util;

import java.util.concurrent.atomic.AtomicLongArray;

public class HystrixAtomicLongBucket extends HystrixMetricsBucket {
	final AtomicLongArray values;

	HystrixAtomicLongBucket(long startTime) {
		super(startTime);

		// initialize the array of AtomicLongs
		values = new AtomicLongArray(HystrixRollingNumberEvent.values().length);
	}

	long get(HystrixRollingNumberEvent type) {
		return values.get(type.ordinal());
	}
}
