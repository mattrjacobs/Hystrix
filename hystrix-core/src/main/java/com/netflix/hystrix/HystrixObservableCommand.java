/**
 * Copyright 2012 Netflix, Inc.
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
package com.netflix.hystrix;

import com.netflix.hystrix.state.State;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;
import rx.Notification;
import rx.Observable;

import com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy;
import com.netflix.hystrix.strategy.executionhook.HystrixCommandExecutionHook;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesStrategy;
import com.netflix.hystrix.strategy.eventnotifier.HystrixEventNotifier;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * Used to wrap code that will execute potentially risky functionality (typically meaning a service call over the network)
 * with fault and latency tolerance, statistics and performance metrics capture, circuit breaker and bulkhead functionality.
 * This command should be used for a purely non-blocking call pattern. The caller of this command will be subscribed to the Observable<R> returned by the run() method.
 * 
 * @param <R>
 *            the return type
 * 
 * @ThreadSafe
 */
public abstract class HystrixObservableCommand<R> extends AbstractCommand<R> implements HystrixObservable<R>, HystrixInvokableInfo<R> {

    /**
     * Construct a {@link HystrixObservableCommand} with defined {@link HystrixCommandGroupKey}.
     * <p>
     * The {@link HystrixCommandKey} will be derived from the implementing class name.
     * 
     * @param group
     *            {@link HystrixCommandGroupKey} used to group together multiple {@link HystrixObservableCommand} objects.
     *            <p>
     *            The {@link HystrixCommandGroupKey} is used to represent a common relationship between commands. For example, a library or team name, the system all related commands interace with,
     *            common business purpose etc.
     */
    protected HystrixObservableCommand(HystrixCommandGroupKey group) {
        // use 'null' to specify use the default
        this(new Setter(group));
    }

    /**
     *
     * Overridden to true so that all onNext emissions are captured
     *
     * @return if onNext events should be reported on
     * This affects {@link HystrixRequestLog}, and {@link HystrixEventNotifier} currently.  Metrics/Hooks later
     */
    @Override
    protected boolean shouldOutputOnNextEvents() {
        return true;
    }

    @Override
    protected String getFallbackMethodName() {
        return "resumeWithFallback";
    }

    /**
     * Construct a {@link HystrixObservableCommand} with defined {@link Setter} that allows injecting property and strategy overrides and other optional arguments.
     * <p>
     * NOTE: The {@link HystrixCommandKey} is used to associate a {@link HystrixObservableCommand} with {@link HystrixCircuitBreaker}, {@link HystrixCommandMetrics} and other objects.
     * <p>
     * Do not create multiple {@link HystrixObservableCommand} implementations with the same {@link HystrixCommandKey} but different injected default properties as the first instantiated will win.
     * <p>
     * Properties passed in via {@link Setter#andCommandPropertiesDefaults} are cached for the given {@link HystrixCommandKey} for the life of the JVM
     * or until {@link Hystrix#reset()} is called. Dynamic properties allow runtime changes. Read more on the <a href="https://github.com/Netflix/Hystrix/wiki/Configuration">Hystrix Wiki</a>.
     * 
     * @param setter
     *            Fluent interface for constructor arguments
     */
    protected HystrixObservableCommand(Setter setter) {
        // use 'null' to specify use the default
        this(setter.groupKey, setter.commandKey, setter.threadPoolKey, null, null, setter.commandPropertiesDefaults, setter.threadPoolPropertiesDefaults, null, null, null, null, null);
    }

    /**
     * Allow constructing a {@link HystrixObservableCommand} with injection of most aspects of its functionality.
     * <p>
     * Some of these never have a legitimate reason for injection except in unit testing.
     * <p>
     * Most of the args will revert to a valid default if 'null' is passed in.
     */
    HystrixObservableCommand(HystrixCommandGroupKey group, HystrixCommandKey key, HystrixThreadPoolKey threadPoolKey, HystrixCircuitBreaker circuitBreaker, HystrixThreadPool threadPool,
            HystrixCommandProperties.Setter commandPropertiesDefaults, HystrixThreadPoolProperties.Setter threadPoolPropertiesDefaults,
            HystrixCommandMetrics metrics, TryableSemaphore fallbackSemaphore, TryableSemaphore executionSemaphore,
            HystrixPropertiesStrategy propertiesStrategy, HystrixCommandExecutionHook executionHook) {
        super(group, key, threadPoolKey, circuitBreaker, threadPool, commandPropertiesDefaults, threadPoolPropertiesDefaults, metrics, fallbackSemaphore, executionSemaphore, propertiesStrategy, executionHook);
    }

    /**
     * Fluent interface for arguments to the {@link HystrixObservableCommand} constructor.
     * <p>
     * The required arguments are set via the 'with' factory method and optional arguments via the 'and' chained methods.
     * <p>
     * Example:
     * <pre> {@code
     *  Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("GroupName"))
                .andCommandKey(HystrixCommandKey.Factory.asKey("CommandName"))
                .andEventNotifier(notifier);
     * } </pre>
     * 
     * @NotThreadSafe
     */
    final public static class Setter {

        protected final HystrixCommandGroupKey groupKey;
        protected HystrixCommandKey commandKey;
        protected HystrixThreadPoolKey threadPoolKey;
        protected HystrixCommandProperties.Setter commandPropertiesDefaults;
        protected HystrixThreadPoolProperties.Setter threadPoolPropertiesDefaults;

        /**
         * Setter factory method containing required values.
         * <p>
         * All optional arguments can be set via the chained methods.
         * 
         * @param groupKey
         *            {@link HystrixCommandGroupKey} used to group together multiple {@link HystrixObservableCommand} objects.
         *            <p>
         *            The {@link HystrixCommandGroupKey} is used to represent a common relationship between commands. For example, a library or team name, the system all related commands interace
         *            with,
         *            common business purpose etc.
         */
        protected Setter(HystrixCommandGroupKey groupKey) {
            this.groupKey = groupKey;

            // default to using SEMAPHORE for ObservableCommand
            commandPropertiesDefaults = setDefaults(HystrixCommandProperties.Setter());
        }

        /**
         * Setter factory method with required values.
         * <p>
         * All optional arguments can be set via the chained methods.
         * 
         * @param groupKey
         *            {@link HystrixCommandGroupKey} used to group together multiple {@link HystrixObservableCommand} objects.
         *            <p>
         *            The {@link HystrixCommandGroupKey} is used to represent a common relationship between commands. For example, a library or team name, the system all related commands interace
         *            with,
         *            common business purpose etc.
         */
        public static Setter withGroupKey(HystrixCommandGroupKey groupKey) {
            return new Setter(groupKey);
        }

        /**
         * @param commandKey
         *            {@link HystrixCommandKey} used to identify a {@link HystrixObservableCommand} instance for statistics, circuit-breaker, properties, etc.
         *            <p>
         *            By default this will be derived from the instance class name.
         *            <p>
         *            NOTE: Every unique {@link HystrixCommandKey} will result in new instances of {@link HystrixCircuitBreaker}, {@link HystrixCommandMetrics} and {@link HystrixCommandProperties}.
         *            Thus,
         *            the number of variants should be kept to a finite and reasonable number to avoid high-memory usage or memory leacks.
         *            <p>
         *            Hundreds of keys is fine, tens of thousands is probably not.
         * @return Setter for fluent interface via method chaining
         */
        public Setter andCommandKey(HystrixCommandKey commandKey) {
            this.commandKey = commandKey;
            return this;
        }

        /**
         * Optional
         * 
         * @param commandPropertiesDefaults
         *            {@link HystrixCommandProperties.Setter} with property overrides for this specific instance of {@link HystrixObservableCommand}.
         *            <p>
         *            See the {@link HystrixPropertiesStrategy} JavaDocs for more information on properties and order of precedence.
         * @return Setter for fluent interface via method chaining
         */
        public Setter andCommandPropertiesDefaults(HystrixCommandProperties.Setter commandPropertiesDefaults) {
            this.commandPropertiesDefaults = setDefaults(commandPropertiesDefaults);
            return this;
        }

        private HystrixCommandProperties.Setter setDefaults(HystrixCommandProperties.Setter commandPropertiesDefaults) {
            if (commandPropertiesDefaults.getExecutionIsolationStrategy() == null) {
                // default to using SEMAPHORE for ObservableCommand if the user didn't set it
                commandPropertiesDefaults.withExecutionIsolationStrategy(ExecutionIsolationStrategy.SEMAPHORE);
            }
            return commandPropertiesDefaults;
        }

    }

    /**
     * Implement this method with code to be executed when {@link #observe()} or {@link #toObservable()} are invoked.
     * 
     * @return R response type
     */
    protected abstract Observable<R> construct();

    /**
     * If {@link #observe()} or {@link #toObservable()} fails in any way then this method will be invoked to provide an opportunity to return a fallback response.
     * <p>
     * This should do work that does not require network transport to produce.
     * <p>
     * In other words, this should be a static or cached result that can immediately be returned upon failure.
     * <p>
     * If network traffic is wanted for fallback (such as going to MemCache) then the fallback implementation should invoke another {@link HystrixObservableCommand} instance that protects against
     * that network
     * access and possibly has another level of fallback that does not involve network access.
     * <p>
     * DEFAULT BEHAVIOR: It throws UnsupportedOperationException.
     * 
     * @return R or UnsupportedOperationException if not implemented
     */
    protected Observable<R> resumeWithFallback() {
        return Observable.error(new UnsupportedOperationException("No fallback available."));
    }

    @Override
    final protected Observable<R> getExecutionObservable() {
        return construct();
    }
    
    @Override
    final protected Observable<R> getFallbackObservable() {
        return resumeWithFallback();
    }

    @Override
    protected CommandDataStyle getCommandDataStyle() {
        return CommandDataStyle.MULTIVALUED;
    }

    @Override
    protected Observable<State<R>> getExecutionStateObservable(final State<R> commandStart, final ExecutionIsolationStrategy isolationStrategy) {
        final AbstractCommand<R> _cmd = this;
        final HystrixRequestContext currentContext = HystrixRequestContext.getContextForCurrentThread();

        return Observable.defer(new Func0<Observable<State<R>>>() {
            @Override
            public Observable<State<R>> call() {
                final State<R> executionStart;
                switch (isolationStrategy) {
                    case SEMAPHORE : executionStart = commandStart.startSemaphoreExecution();
                        break;
                    case THREAD    : executionStart = commandStart.startExecutionOnThread(Thread.currentThread());
                        break;
                    default        : throw new IllegalStateException("Unexpected isolationStrategy : " + isolationStrategy);
                }

                Throwable hookStartError = executeExecutionStartHooks(isolationStrategy);
                if (hookStartError != null) {
                    Observable<State<R>> hookError = Observable.just(executionStart.withExecutionNotification(Notification.<R>createOnError(hookStartError)));
                    return hookError.startWith(commandStart, executionStart);
                }

                Observable<State<R>> actualExecution;
                try {
                    actualExecution = construct()
                            .doOnEach(new Action1<Notification<? super R>>() {
                                @Override
                                public void call(Notification<? super R> notification) {
                                    HystrixRequestContext.setContextOnCurrentThread(currentContext);
                                }
                            })
                            .map(new Func1<R, R>() {
                                @Override
                                public R call(R userValue) {
                                    return wrapValueWithExecutionHooks(userValue, _cmd);
                                }
                            })
                            .onErrorResumeNext(new Func1<Throwable, Observable<? extends R>>() {
                                @Override
                                public Observable<? extends R> call(Throwable throwable) {
                                    return Observable.error(wrapFailureWithExecutionFailureHooks(throwable, _cmd));
                                }
                            })
                            .materialize()
                            .scan(executionStart, new Func2<State<R>, Notification<R>, State<R>>() {
                                @Override
                                public State<R> call(State<R> lastState, Notification<R> constructNotification) {
                                    switch (constructNotification.getKind()) {
                                        case OnNext:  return lastState.withExecutionNotification(constructNotification);
                                        case OnError: return lastState.withExecutionNotification(constructNotification);
                                        case OnCompleted:
                                            Throwable hookSuccessError = executeExecutionSuccessHooks(_cmd);
                                            if (hookSuccessError != null) {
                                                return lastState.withExecutionNotification(Notification.<R>createOnError(hookSuccessError));
                                            } else {
                                                return lastState.withExecutionNotification(Notification.<R>createOnCompleted());
                                            }
                                        default:
                                            throw new IllegalArgumentException("Unknown Notification kind : " + constructNotification.getKind());

                                    }
                                }
                            });

                } catch (Throwable syncConstructEx) {
                    //in case construct() throws synchronous error
                    Throwable hookEx = wrapFailureWithExecutionFailureHooks(syncConstructEx, _cmd);
                    actualExecution = Observable.just(executionStart, executionStart.withExecutionNotification(Notification.<R>createOnError(hookEx)));
                }

                return actualExecution.startWith(commandStart);
            }
        });
    }

    @Override
    protected Observable<State<R>> getFallbackStateObservable(final State<R> executionState) {
        final AbstractCommand<R> _cmd = this;
        final boolean shouldApplyFallbackHooks = isFallbackUserSupplied(this);
        final State<R> initialFallbackState = executionState.startFallbackExecution(Thread.currentThread());
        final HystrixRequestContext currentContext = HystrixRequestContext.getContextForCurrentThread();

        return Observable.defer(new Func0<Observable<State<R>>>() {
            @Override
            public Observable<State<R>> call() {
                if (shouldApplyFallbackHooks) {
                    Throwable hookError = executeFallbackStartHooks(_cmd);
                    if (hookError != null) {
                        return Observable.just(initialFallbackState.withExecutionNotification(Notification.<R>createOnError(hookError)));
                    }
                }

                try {
                    Observable<R> fallbackWithContext = resumeWithFallback().doOnEach(new Action1<Notification<? super R>>() {
                        @Override
                        public void call(Notification<? super R> notification) {
                            HystrixRequestContext.setContextOnCurrentThread(currentContext);
                        }
                    });
                    Observable<R> fallbackValue;
                    if (shouldApplyFallbackHooks) {
                        Observable<R> userFallbackValue = fallbackWithContext;
                        Observable<R> hookFallbackValue = userFallbackValue.map(new Func1<R, R>() {
                            @Override
                            public R call(R r) {
                                return wrapValueWithFallbackHooks(r, _cmd);
                            }
                        });
                        fallbackValue = hookFallbackValue;
                    } else {
                        Observable<R> userFallbackValue = fallbackWithContext;
                        fallbackValue = userFallbackValue;
                    }

                    return fallbackValue
                            .onErrorResumeNext(new Func1<Throwable, Observable<? extends R>>() {
                                @Override
                                public Observable<? extends R> call(Throwable throwable) {
                                    Throwable fallbackException;
                                    if (shouldApplyFallbackHooks) {
                                        Throwable wrappedEx = wrapFallbackFailureWithFallbackFailureHooks(throwable, _cmd);
                                        fallbackException = wrappedEx;
                                    } else {
                                        fallbackException = throwable;
                                    }
                                    return Observable.error(fallbackException);
                                }
                            })
                            .materialize()
                            .scan(initialFallbackState, new Func2<State<R>, Notification<R>, State<R>>() {
                                @Override
                                public State<R> call(State<R> lastState, Notification<R> fallbackNotification) {
                                    switch (fallbackNotification.getKind()) {
                                        case OnNext:
                                            return lastState.withFallbackExecutionNotification(fallbackNotification);
                                        case OnError:
                                            return lastState.withFallbackExecutionNotification(fallbackNotification);
                                        case OnCompleted:
                                            Throwable hookSuccessError = executeFallbackSuccessHooks(_cmd);
                                            if (hookSuccessError != null) {
                                                return lastState.withFallbackExecutionNotification(Notification.<R>createOnError(hookSuccessError));
                                            } else {
                                                return lastState.withFallbackExecutionNotification(Notification.<R>createOnCompleted());
                                            }
                                        default:
                                            throw new IllegalArgumentException("Unknown Notification kind : " + fallbackNotification.getKind());

                                    }
                                }
                            });
                } catch (Throwable syncFallbackEx) {
                    System.out.println("!!! Caught syncfallback ex : " + syncFallbackEx);
                    Throwable hookEx;
                    if (shouldApplyFallbackHooks) {
                        hookEx = wrapFallbackFailureWithFallbackFailureHooks(syncFallbackEx, _cmd);
                    } else {
                        hookEx = syncFallbackEx;
                    }
                    return Observable.just(initialFallbackState.withFallbackExecutionNotification(Notification.<R>createOnError(hookEx)));
                }
            }
        });
    }
}
