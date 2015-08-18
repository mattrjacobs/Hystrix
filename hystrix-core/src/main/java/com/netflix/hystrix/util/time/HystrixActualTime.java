package com.netflix.hystrix.util.time;

public class HystrixActualTime implements HystrixTime {
	private static final HystrixActualTime INSTANCE = new HystrixActualTime();

    private HystrixActualTime() {

    }

	@Override
	public long getCurrentTimeInMillis() {
		return System.currentTimeMillis();
	}

	public static HystrixTime getInstance() {
		return INSTANCE;
	}
}
