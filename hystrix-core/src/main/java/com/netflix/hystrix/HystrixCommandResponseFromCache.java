package com.netflix.hystrix;

import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class should not be used externally
 * @deprecated This class shold not be used externally
 */
@Deprecated
public class HystrixCommandResponseFromCache<R> extends HystrixCachedObservable<R> {
    /* package-private */ HystrixCommandResponseFromCache(Observable<R> originalObservable, final AbstractCommand<R> originalCommand) {
        super(originalObservable);
    }

    public Observable<R> toObservableWithStateCopiedInto(final AbstractCommand<R> commandToCopyStateInto) {
        return Observable.empty();
    }

    private void commandCompleted(final AbstractCommand<R> commandToCopyStateInto) {
    }

    private void commandUnsubscribed(final AbstractCommand<R> commandToCopyStateInto) {
    }
}
