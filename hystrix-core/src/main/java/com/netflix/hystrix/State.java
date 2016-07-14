package com.netflix.hystrix;

import rx.Observable;

public class State<R> {

    private final long startTimestamp;

    private State(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public static State create() {
        final long startTimestamp = System.currentTimeMillis();

        return new State(startTimestamp);
    }

    public Observable<R> getValues() {
        //TODO
        return Observable.empty();
    }
}
