package com.netflix.hystrix.util.time;

import java.util.concurrent.atomic.AtomicInteger;

public class HystrixMockedTime implements HystrixTime {

	private AtomicInteger time = new AtomicInteger(0);

	@Override
	public long getCurrentTimeInMillis() {
		return time.get();
	}
	
	public void increment(int millis) {
		time.addAndGet(millis);
	}
}
