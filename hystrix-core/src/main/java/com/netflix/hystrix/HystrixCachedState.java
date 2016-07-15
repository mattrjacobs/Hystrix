package com.netflix.hystrix;

import com.netflix.hystrix.state.State;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action0;
import rx.subjects.ReplaySubject;

class HystrixCachedState<R> {
    private final Observable<State<R>> cachedObservable;
    private final Subscription sourceSubscription;
    private volatile int outstandingSubscriptions = 0;

    public HystrixCachedState(Observable<State<R>> stateToCache) {
        final ReplaySubject<State<R>> readOnlyCache = ReplaySubject.create();

        this.sourceSubscription = stateToCache.subscribe(readOnlyCache);
        this.cachedObservable = readOnlyCache
                .doOnSubscribe(new Action0() {
                    @Override
                    public void call() {
                        outstandingSubscriptions++;
                    }
                })
                .doOnUnsubscribe(new Action0() {
                    @Override
                    public void call() {
                        outstandingSubscriptions--;
                        if (outstandingSubscriptions == 0) {
                            sourceSubscription.unsubscribe();
                        }
                    }
                });

    }

    static <R> HystrixCachedState<R> from(Observable<State<R>> stateToCache) {
        return new HystrixCachedState<R>(stateToCache);
    }

    void unsubscribe() {
        sourceSubscription.unsubscribe();
    }

    Observable<State<R>> toObservable() {
        return cachedObservable;
    }
}
