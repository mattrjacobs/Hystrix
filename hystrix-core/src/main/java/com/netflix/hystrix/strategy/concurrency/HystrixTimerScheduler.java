/**
 * Copyright 2013 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.hystrix.strategy.concurrency;

import java.util.concurrent.*;

import rx.*;
import rx.functions.Action0;
import rx.functions.Func0;
import rx.internal.schedulers.ScheduledAction;
import rx.subscriptions.*;

import com.netflix.hystrix.HystrixThreadPool;
import com.netflix.hystrix.strategy.HystrixPlugins;

/**
 * Wrap a {@link Scheduler} so that scheduled actions are wrapped with {@link HystrixContexSchedulerAction} so that
 * the {@link HystrixRequestContext} is properly copied across threads (if they are used by the {@link Scheduler}).
 */
public class HystrixTimerScheduler extends Scheduler {

    private final HystrixConcurrencyStrategy concurrencyStrategy;
    private final Scheduler actualScheduler;

    public HystrixTimerScheduler(HystrixConcurrencyStrategy concurrencyStrategy, ScheduledThreadPoolExecutor executorService, Func0<Boolean> shouldInterruptThread) {
        this.concurrencyStrategy = concurrencyStrategy;
        this.actualScheduler = new ThreadPoolScheduler(executorService, shouldInterruptThread);
    }

    @Override
    public Worker createWorker() {
        return new HystrixTimerSchedulerWorker(actualScheduler.createWorker());
    }

    private class HystrixTimerSchedulerWorker extends Worker {

        private final Worker worker;

        private HystrixTimerSchedulerWorker(Worker actualWorker) {
            this.worker = actualWorker;
        }

        @Override
        public void unsubscribe() {
            worker.unsubscribe();
        }

        @Override
        public boolean isUnsubscribed() {
            return worker.isUnsubscribed();
        }

        @Override
        public Subscription schedule(Action0 action, long delayTime, TimeUnit unit) {
            return worker.schedule(new HystrixContexSchedulerAction(concurrencyStrategy, action), delayTime, unit);
        }

        @Override
        public Subscription schedule(Action0 action) {
            return worker.schedule(new HystrixContexSchedulerAction(concurrencyStrategy, action));
        }

    }

    private static class ThreadPoolScheduler extends Scheduler {

        private final ScheduledThreadPoolExecutor executorService;
        private final Func0<Boolean> shouldInterruptThread;

        public ThreadPoolScheduler(ScheduledThreadPoolExecutor executorService, Func0<Boolean> shouldInterruptThread) {
            this.executorService = executorService;
            this.shouldInterruptThread = shouldInterruptThread;
        }

        @Override
        public Worker createWorker() {
            return new ThreadPoolWorker(executorService, shouldInterruptThread);
        }

    }

    /**
     * Purely for scheduling work on a thread-pool.
     * <p>
     * This is not natively supported by RxJava as of 0.18.0 because thread-pools
     * are contrary to sequential execution.
     * <p>
     * For the Hystrix case, each Command invocation has a single action so the concurrency
     * issue is not a problem.
     */
    private static class ThreadPoolWorker extends Worker {

        private final ScheduledThreadPoolExecutor executorService;
        private final CompositeSubscription subscription = new CompositeSubscription();
        private final Func0<Boolean> shouldInterruptThread;

        public ThreadPoolWorker(ScheduledThreadPoolExecutor executorService, Func0<Boolean> shouldInterruptThread) {
            this.executorService = executorService;
            this.shouldInterruptThread = shouldInterruptThread;
        }

        @Override
        public void unsubscribe() {
            subscription.unsubscribe();
        }

        @Override
        public boolean isUnsubscribed() {
            return subscription.isUnsubscribed();
        }

        @Override
        public Subscription schedule(final Action0 action) {
            if (subscription.isUnsubscribed()) {
                // don't schedule, we are unsubscribed
                return Subscriptions.unsubscribed();
            }

            // This is internal RxJava API but it is too useful.
            ScheduledAction sa = new ScheduledAction(action);
            
            subscription.add(sa);
            sa.addParent(subscription);

            Future<?> f = executorService.submit(sa);
            sa.add(new FutureCompleterWithConfigurableInterrupt(f, shouldInterruptThread));

            return sa;
        }

        @Override
        public Subscription schedule(Action0 action, long delayTime, TimeUnit unit) {
            if (subscription.isUnsubscribed()) {
                // don't schedule, we are unsubscribed
                return Subscriptions.unsubscribed();
            }

            // This is internal RxJava API but it is too useful.
            ScheduledAction sa = new ScheduledAction(action);

            subscription.add(sa);
            sa.addParent(subscription);

            Future<?> f = executorService.schedule(sa, delayTime, unit);
            sa.add(new FutureCompleterWithConfigurableInterrupt(f, shouldInterruptThread));

            return sa;
        }

    }

    /**
     * Very similar to rx.internal.schedulers.ScheduledAction.FutureCompleter, but with configurable interrupt behavior
     */
    private static class FutureCompleterWithConfigurableInterrupt implements Subscription {
        private final Future<?> f;
        private final Func0<Boolean> shouldInterruptThread;

        private FutureCompleterWithConfigurableInterrupt(Future<?> f, Func0<Boolean> shouldInterruptThread) {
            this.f = f;
            this.shouldInterruptThread = shouldInterruptThread;
        }

        @Override
        public void unsubscribe() {
            if (shouldInterruptThread.call()) {
                f.cancel(true);
            } else {
                f.cancel(false);
            }
        }

        @Override
        public boolean isUnsubscribed() {
            return f.isCancelled();
        }
    }

}
