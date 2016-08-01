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
package com.netflix.hystrix;

import com.netflix.hystrix.HystrixCircuitBreaker.NoOpCircuitBreaker;
import com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import com.netflix.hystrix.exception.HystrixRuntimeException.FailureType;
import com.netflix.hystrix.state.State;
import com.netflix.hystrix.strategy.HystrixPlugins;
import com.netflix.hystrix.strategy.concurrency.HystrixConcurrencyStrategy;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;
import com.netflix.hystrix.strategy.eventnotifier.HystrixEventNotifier;
import com.netflix.hystrix.strategy.executionhook.HystrixCommandExecutionHook;
import com.netflix.hystrix.strategy.metrics.HystrixMetricsPublisherFactory;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesFactory;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesStrategy;
import com.netflix.hystrix.strategy.properties.HystrixProperty;
import com.netflix.hystrix.util.HystrixTimer;
import com.netflix.hystrix.util.HystrixTimer.TimerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observable.Operator;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;
import rx.subjects.ReplaySubject;

import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/* package */abstract class AbstractCommand<R> implements HystrixInvokableInfo<R>, HystrixObservable<R> {
    //TODO This is an infinite observable - needs to get cleaned up?
    //TODO This is the unserialized version - since I'm writing onNexts from multiple threads, I think I need to synchronize this (but SerializedSubject doesn't have the get* methods)
    protected final BehaviorSubject<State<R>> stateCache = BehaviorSubject.create();

    private static final Logger logger = LoggerFactory.getLogger(AbstractCommand.class);
    protected final HystrixCircuitBreaker circuitBreaker;
    protected final HystrixThreadPool threadPool;
    protected final HystrixThreadPoolKey threadPoolKey;
    protected final HystrixCommandProperties properties;

    protected enum TimedOutStatus {
        NOT_EXECUTED, COMPLETED, TIMED_OUT
    }

    protected enum CommandState {
        NOT_STARTED, OBSERVABLE_CHAIN_CREATED, USER_CODE_EXECUTED, UNSUBSCRIBED, TERMINAL
    }

    protected enum ThreadState {
        NOT_USING_THREAD, STARTED, UNSUBSCRIBED, TERMINAL
    }

    protected final HystrixCommandMetrics metrics;

    protected final HystrixCommandKey commandKey;
    protected final HystrixCommandGroupKey commandGroup;

    /**
     * Plugin implementations
     */
    protected final HystrixEventNotifier eventNotifier;
    protected final HystrixConcurrencyStrategy concurrencyStrategy;
    protected final HystrixCommandExecutionHook executionHook;

    /* FALLBACK Semaphore */
    protected final TryableSemaphore fallbackSemaphoreOverride;
    /* each circuit has a semaphore to restrict concurrent fallback execution */
    protected static final ConcurrentHashMap<String, TryableSemaphore> fallbackSemaphorePerCircuit = new ConcurrentHashMap<String, TryableSemaphore>();
    /* END FALLBACK Semaphore */

    /* EXECUTION Semaphore */
    protected final TryableSemaphore executionSemaphoreOverride;
    /* each circuit has a semaphore to restrict concurrent fallback execution */
    protected static final ConcurrentHashMap<String, TryableSemaphore> executionSemaphorePerCircuit = new ConcurrentHashMap<String, TryableSemaphore>();
    /* END EXECUTION Semaphore */

    /**
     * This should not be used by subclasses
     * @deprecated This should not be used by subclasses
     */
    @Deprecated
    protected final AtomicReference<Reference<TimerListener>> timeoutTimer = new AtomicReference<Reference<TimerListener>>();

    /**
     * This should not be used by subclasses
     * @deprecated This should not be used by subclasses
     */
    @Deprecated
    protected AtomicReference<CommandState> commandState = new AtomicReference<CommandState>(CommandState.NOT_STARTED);
    /**
     * This should not be used by subclasses
     * @deprecated This should not be used by subclasses
     */
    @Deprecated
    protected AtomicReference<ThreadState> threadState = new AtomicReference<ThreadState>(ThreadState.NOT_USING_THREAD);

    /*
     * {@link ExecutionResult} refers to what happened as the user-provided code ran.  If request-caching is used,
     * then multiple command instances will have a reference to the same {@link ExecutionResult}.  So all values there
     * should be the same, even in the presence of request-caching.
     *
     * If some values are not properly shareable, then they belong on the command instance, so they are not visible to
     * other commands.
     *
     * Examples: RESPONSE_FROM_CACHE, CANCELLED HystrixEventTypes
     */
    /**
     * This should not be used by subclasses
     * @deprecated This should not be used by subclasses
     */
    @Deprecated
    protected volatile ExecutionResult executionResult = ExecutionResult.EMPTY; //state on shared execution

    /**
     * This should not be used by subclasses
     * @deprecated This should not be used by subclasses
     */
    @Deprecated
    protected volatile boolean isResponseFromCache = false;
    /**
     * This should not be used by subclasses
     * @deprecated This should not be used by subclasses
     */
    @Deprecated
    protected volatile ExecutionResult executionResultAtTimeOfCancellation;
    /**
     * This should not be used by subclasses
     * @deprecated This should not be used by subclasses
     */
    @Deprecated
    protected volatile long commandStartTimestamp = -1L;

    /* If this command executed and timed-out */
    /**
     * This should not be used by subclasses
     * @deprecated This should not be used by subclasses
     */
    @Deprecated
    protected final AtomicReference<TimedOutStatus> isCommandTimedOut = new AtomicReference<TimedOutStatus>(TimedOutStatus.NOT_EXECUTED);
    /**
     * This should not be used by subclasses
     * @deprecated This should not be used by subclasses
     */
    @Deprecated
    protected volatile Action0 endCurrentThreadExecutingCommand;

    /**
     * Instance of RequestCache logic
     */
    protected final HystrixRequestCache requestCache;
    protected final HystrixRequestLog currentRequestLog;

    // this is a micro-optimization but saves about 1-2microseconds (on 2011 MacBook Pro) 
    // on the repetitive string processing that will occur on the same classes over and over again
    private static ConcurrentHashMap<Class<?>, String> defaultNameCache = new ConcurrentHashMap<Class<?>, String>();

    private static ConcurrentHashMap<HystrixCommandKey, Boolean> commandContainsFallback = new ConcurrentHashMap<HystrixCommandKey, Boolean>();

    /* package */static String getDefaultNameFromClass(Class<?> cls) {
        String fromCache = defaultNameCache.get(cls);
        if (fromCache != null) {
            return fromCache;
        }
        // generate the default
        // default HystrixCommandKey to use if the method is not overridden
        String name = cls.getSimpleName();
        if (name.equals("")) {
            // we don't have a SimpleName (anonymous inner class) so use the full class name
            name = cls.getName();
            name = name.substring(name.lastIndexOf('.') + 1, name.length());
        }
        defaultNameCache.put(cls, name);
        return name;
    }

    protected AbstractCommand(HystrixCommandGroupKey group, HystrixCommandKey key, HystrixThreadPoolKey threadPoolKey, HystrixCircuitBreaker circuitBreaker, HystrixThreadPool threadPool,
            HystrixCommandProperties.Setter commandPropertiesDefaults, HystrixThreadPoolProperties.Setter threadPoolPropertiesDefaults,
            HystrixCommandMetrics metrics, TryableSemaphore fallbackSemaphore, TryableSemaphore executionSemaphore,
            HystrixPropertiesStrategy propertiesStrategy, HystrixCommandExecutionHook executionHook) {

        this.commandGroup = initGroupKey(group);
        this.commandKey = initCommandKey(key, getClass());
        this.properties = initCommandProperties(this.commandKey, propertiesStrategy, commandPropertiesDefaults);
        this.threadPoolKey = initThreadPoolKey(threadPoolKey, this.commandGroup, this.properties.executionIsolationThreadPoolKeyOverride().get());
        this.metrics = initMetrics(metrics, this.commandGroup, this.threadPoolKey, this.commandKey, this.properties);
        this.circuitBreaker = initCircuitBreaker(this.properties.circuitBreakerEnabled().get(), circuitBreaker, this.commandGroup, this.commandKey, this.properties, this.metrics);
        this.threadPool = initThreadPool(threadPool, this.threadPoolKey, threadPoolPropertiesDefaults);

        //Strategies from plugins
        this.eventNotifier = HystrixPlugins.getInstance().getEventNotifier();
        this.concurrencyStrategy = HystrixPlugins.getInstance().getConcurrencyStrategy();
        HystrixMetricsPublisherFactory.createOrRetrievePublisherForCommand(this.commandKey, this.commandGroup, this.metrics, this.circuitBreaker, this.properties);
        this.executionHook = initExecutionHook(executionHook);

        this.requestCache = HystrixRequestCache.getInstance(this.commandKey, this.concurrencyStrategy);
        this.currentRequestLog = initRequestLog(this.properties.requestLogEnabled().get(), this.concurrencyStrategy);

        /* fallback semaphore override if applicable */
        this.fallbackSemaphoreOverride = fallbackSemaphore;

        /* execution semaphore override if applicable */
        this.executionSemaphoreOverride = executionSemaphore;
    }

    private static HystrixCommandGroupKey initGroupKey(final HystrixCommandGroupKey fromConstructor) {
        if (fromConstructor == null) {
            throw new IllegalStateException("HystrixCommandGroup can not be NULL");
        } else {
            return fromConstructor;
        }
    }

    private static HystrixCommandKey initCommandKey(final HystrixCommandKey fromConstructor, Class<?> clazz) {
        if (fromConstructor == null || fromConstructor.name().trim().equals("")) {
            final String keyName = getDefaultNameFromClass(clazz);
            return HystrixCommandKey.Factory.asKey(keyName);
        } else {
            return fromConstructor;
        }
    }

    private static HystrixCommandProperties initCommandProperties(HystrixCommandKey commandKey, HystrixPropertiesStrategy propertiesStrategy, HystrixCommandProperties.Setter commandPropertiesDefaults) {
        if (propertiesStrategy == null) {
            return HystrixPropertiesFactory.getCommandProperties(commandKey, commandPropertiesDefaults);
        } else {
            // used for unit testing
            return propertiesStrategy.getCommandProperties(commandKey, commandPropertiesDefaults);
        }
    }

    /*
     * ThreadPoolKey
     *
     * This defines which thread-pool this command should run on.
     *
     * It uses the HystrixThreadPoolKey if provided, then defaults to use HystrixCommandGroup.
     *
     * It can then be overridden by a property if defined so it can be changed at runtime.
     */
    private static HystrixThreadPoolKey initThreadPoolKey(HystrixThreadPoolKey threadPoolKey, HystrixCommandGroupKey groupKey, String threadPoolKeyOverride) {
        if (threadPoolKeyOverride == null) {
            // we don't have a property overriding the value so use either HystrixThreadPoolKey or HystrixCommandGroup
            if (threadPoolKey == null) {
                /* use HystrixCommandGroup if HystrixThreadPoolKey is null */
                return HystrixThreadPoolKey.Factory.asKey(groupKey.name());
            } else {
                return threadPoolKey;
            }
        } else {
            // we have a property defining the thread-pool so use it instead
            return HystrixThreadPoolKey.Factory.asKey(threadPoolKeyOverride);
        }
    }

    private static HystrixCommandMetrics initMetrics(HystrixCommandMetrics fromConstructor, HystrixCommandGroupKey groupKey,
                                                     HystrixThreadPoolKey threadPoolKey, HystrixCommandKey commandKey,
                                                     HystrixCommandProperties properties) {
        if (fromConstructor == null) {
            return HystrixCommandMetrics.getInstance(commandKey, groupKey, threadPoolKey, properties);
        } else {
            return fromConstructor;
        }
    }

    private static HystrixCircuitBreaker initCircuitBreaker(boolean enabled, HystrixCircuitBreaker fromConstructor,
                                                            HystrixCommandGroupKey groupKey, HystrixCommandKey commandKey,
                                                            HystrixCommandProperties properties, HystrixCommandMetrics metrics) {
        if (enabled) {
            if (fromConstructor == null) {
                // get the default implementation of HystrixCircuitBreaker
                return HystrixCircuitBreaker.Factory.getInstance(commandKey, groupKey, properties, metrics);
            } else {
                return fromConstructor;
            }
        } else {
            return new NoOpCircuitBreaker();
        }
    }

    private static HystrixCommandExecutionHook initExecutionHook(HystrixCommandExecutionHook fromConstructor) {
        if (fromConstructor == null) {
            return new ExecutionHookDeprecationWrapper(HystrixPlugins.getInstance().getCommandExecutionHook());
        } else {
            // used for unit testing
            if (fromConstructor instanceof ExecutionHookDeprecationWrapper) {
                return fromConstructor;
            } else {
                return new ExecutionHookDeprecationWrapper(fromConstructor);
            }
        }
    }

    private static HystrixThreadPool initThreadPool(HystrixThreadPool fromConstructor, HystrixThreadPoolKey threadPoolKey, HystrixThreadPoolProperties.Setter threadPoolPropertiesDefaults) {
        if (fromConstructor == null) {
            // get the default implementation of HystrixThreadPool
            return HystrixThreadPool.Factory.getInstance(threadPoolKey, threadPoolPropertiesDefaults);
        } else {
            return fromConstructor;
        }
    }

    private static HystrixRequestLog initRequestLog(boolean enabled, HystrixConcurrencyStrategy concurrencyStrategy) {
        if (enabled) {
            /* store reference to request log regardless of which thread later hits it */
            return HystrixRequestLog.getCurrentRequest(concurrencyStrategy);
        } else {
            return null;
        }
    }

    /**
     * Allow the Collapser to mark this command instance as being used for a collapsed request and how many requests were collapsed.
     * 
     * @param sizeOfBatch number of commands in request batch
     */
    /* package */void markAsCollapsedCommand(HystrixCollapserKey collapserKey, int sizeOfBatch) {
        State<R> initialCollapsedState = State.create(getCommandDataStyle(), getClass(), getCommandKey(), collapserKey, circuitBreaker, sizeOfBatch);
        eventNotifier.markEvent(HystrixEventType.COLLAPSED, this.commandKey);
        stateCache.onNext(initialCollapsedState);
    }

    /**
     * Used for asynchronous execution of command with a callback by subscribing to the {@link Observable}.
     * <p>
     * This eagerly starts execution of the command the same as {@link HystrixCommand#queue()} and {@link HystrixCommand#execute()}.
     * <p>
     * A lazy {@link Observable} can be obtained from {@link #toObservable()}.
     * <p>
     * See https://github.com/Netflix/RxJava/wiki for more information.
     * 
     * @return {@code Observable<R>} that executes and calls back with the result of command execution or a fallback if the command fails for any reason.
     * @throws HystrixRuntimeException
     *             if a fallback does not exist
     *             <p>
     *             <ul>
     *             <li>via {@code Observer#onError} if a failure occurs</li>
     *             <li>or immediately if the command can not be queued (such as short-circuited, thread-pool/semaphore rejected)</li>
     *             </ul>
     * @throws HystrixBadRequestException
     *             via {@code Observer#onError} if invalid arguments or state were used representing a user failure, not a system failure
     * @throws IllegalStateException
     *             if invoked more than once
     */
    public Observable<R> observe() {
        // us a ReplaySubject to buffer the eagerly subscribed-to Observable
        ReplaySubject<R> subject = ReplaySubject.create();
        // eagerly kick off subscription
        final Subscription sourceSubscription = toObservable().subscribe(subject);
        // return the subject that can be subscribed to later while the execution has already started
        return subject.doOnUnsubscribe(new Action0() {
            @Override
            public void call() {
                sourceSubscription.unsubscribe();
            }
        });
    }

    protected abstract Observable<R> getExecutionObservable();

    protected abstract Observable<R> getFallbackObservable();

    protected abstract Observable<State<R>> getExecutionStateObservable(State<R> initalState, ExecutionIsolationStrategy isolationStrategy);

    protected abstract Observable<State<R>> getFallbackStateObservable(State<R> executionState);

    protected abstract CommandDataStyle getCommandDataStyle();

    public Observable<R> toObservable() {
        final AbstractCommand<R> _cmd = this;
        final HystrixInvokable<R>  _invokable = this;

        return Observable.defer(new Func0<Observable<R>>() {
            @Override
            public Observable<R> call() {
                final State<R> initialState;

                if (stateCache.hasValue()) {
                    //if initial state was set up by collapser, then it's ok for this state to exist (this is inelegant)
                    State<R> initialStateFromCache = stateCache.getValue();
                    if (initialStateFromCache.isCreatedByCollapser()) {
                        initialState = initialStateFromCache;
                    } else {
                        IllegalStateException ex = new IllegalStateException("This instance can only be executed once. Please instantiate a new instance.");
                        //TODO make a new error type for this
                        return Observable.error(new HystrixRuntimeException(FailureType.BAD_REQUEST_EXCEPTION, _cmd.getClass(), getLogMessagePrefix() + " command executed multiple times - this is not permitted.", ex, null));
                    }
                } else {
                    //set up a new state to start execution with
                    initialState = State.create(getCommandDataStyle(), _invokable.getClass(), _cmd.commandKey, circuitBreaker);
                    stateCache.onNext(initialState); //if this command instance gets observed again, cache will be non-empty
                }

                if (properties.requestLogEnabled().get()) {
                    // log this command execution regardless of what happened
                    if (currentRequestLog != null) {
                        currentRequestLog.addExecutedCommand(_cmd);
                    }
                }

                final String cacheKey = getCacheKey();
                final boolean requestCacheEnabled = properties.requestCacheEnabled().get() && (cacheKey != null);

                if (requestCacheEnabled) {
                    Observable<R> fromCache = checkCacheForPreviouslyExecutedCommand(requestCache, cacheKey);
                    if (fromCache != null) {
                        try {
                            executionHook.onCacheHit(_cmd);
                        } catch (Throwable hookEx) {
                            logger.warn("Error calling HystrixCommandExecutionHook", hookEx);
                        }
                        return fromCache;
                    }
                }

                try {
                    executionHook.onStart(_cmd);
                } catch (Throwable hookEx) {
                    logger.warn("Error calling HystrixCommandExecutionHook.onStart", hookEx);
                }
                final ExecutionIsolationStrategy isolationStrategy = properties.executionIsolationStrategy().get();
                final Observable<State<R>> executionAttempt = applyCircuitBreaker(circuitBreaker, initialState, new Func0<Observable<State<R>>>() {

                    @Override
                    public Observable<State<R>> call() {
                        final Observable<State<R>> lazyUserExecution = getExecutionStateObservable(initialState, isolationStrategy);
                        final Observable<State<R>> timeoutApplied = applyTimeout(lazyUserExecution, _cmd);
                        final Observable<State<R>> bulkheadApplied = applyBulkhead(timeoutApplied, isolationStrategy);
                        return bulkheadApplied;
                    }
                });

                final Observable<State<R>> commandExecution = executionAttempt
                        .flatMap(new Func1<State<R>, Observable<State<R>>>() {
                            @Override
                            public Observable<State<R>> call(State<R> state) {
                                if (!stateCache.getValue().isCancelled()) {
                                    stateCache.onNext(state);
                                }
                                if (state.getExecutionNotification() != null) {
                                    switch (state.getExecutionNotification().getKind()) {
                                        case OnError     : return applyFallback(state);
                                        case OnNext      : return Observable.just(state);
                                        case OnCompleted : return Observable.just(state);
                                        default          : return Observable.error(new IllegalArgumentException("Unknown notification type : " + state.getExecutionNotification().getKind()));
                                    }
                                } else {
                                    if (state.getExecutionThrowable() != null) {
                                        return applyFallback(state);
                                    } else {
                                        return Observable.just(state);
                                    }
                                }
                            }
                        })
                        //write each value to the command state cache
                        .doOnNext(new Action1<State<R>>() {
                            @Override
                            public void call(State<R> state) {
                                if (!stateCache.getValue().isCancelled()) {
                                    stateCache.onNext(state);
                                }
                            }
                        });

                final Observable<State<R>> afterCachePut;

                // put in cache
                if (requestCacheEnabled) {
                    afterCachePut = putIntoRequestCache(requestCache, cacheKey, commandExecution);
                } else {
                    afterCachePut = commandExecution;
                }

                final AtomicBoolean commandCleanedUp = new AtomicBoolean(false);

                //strip off all the state and just return the values
                Observable<R> commandValues = afterCachePut
                        .doOnNext(new Action1<State<R>>() {
                            @Override
                            public void call(State<R> state) {
                                System.out.println("$$" + state.getCommandLifecycle() + "$$ " + System.currentTimeMillis() + " #" + _cmd.hashCode() + "# : " + state + " : " + state.timing);
                                if (state.getCommandLifecycle() == State.CommandLifecycle.Start) {
                                    metrics.markExecutionStart(commandKey, threadPoolKey, isolationStrategy);
                                }
                            }
                        })
                        .flatMap(new Func1<State<R>, Observable<R>>() {
                            @Override
                            public Observable<R> call(State<R> state) {
                                return state.getValue();
                            }
                        })
                        .doOnTerminate(new Action0() {
                            @Override
                            public void call() {
                                if (commandCleanedUp.compareAndSet(false, true)) {
                                    metrics.markExecutionDone(stateCache.getValue(), commandKey, threadPoolKey);
                                }
                            }
                        })
                        .doOnUnsubscribe(new Action0() {
                            @Override
                            public void call() {
                                if (commandCleanedUp.compareAndSet(false, true)) {
                                    State<R> stateBeforeCancellation = stateCache.getValue();
                                    State<R> stateAfterCancellation = stateBeforeCancellation;
                                    if (!stateBeforeCancellation.isExecutionComplete()) {
                                        stateAfterCancellation = stateBeforeCancellation.withCancellation();
                                        stateCache.onNext(stateAfterCancellation);
                                    }
                                    metrics.markExecutionDone(stateAfterCancellation, commandKey, threadPoolKey);
                                }
                            }
                        });

                return applyCommandHooks(executionHook, commandValues, _cmd);
            }
        });
    }

    private Observable<R> checkCacheForPreviouslyExecutedCommand(HystrixRequestCache cache, final String cacheKey) {
        HystrixCachedState<R> fromCache = cache.get(cacheKey);
        if (fromCache != null) {
            return fromCache.toObservable()
                    .doOnUnsubscribe(new Action0() {
                        @Override
                        public void call() {
                            State<R> stateBeforeCancellation = stateCache.getValue();
                            if (!stateBeforeCancellation.isExecutionComplete()) {
                                stateCache.onNext(stateBeforeCancellation.withCancellation());
                            }
                            metrics.markExecutionDone(stateCache.getValue(), commandKey, threadPoolKey);
                        }
                    })
                    .doOnNext(new Action1<State<R>>() {
                        @Override
                        public void call(State<R> state) {
                            State<R> updatedState = state.withResponseFromCache();
                            System.out.println("$C$C$C$C " + System.currentTimeMillis() + " STATE #" + cacheKey + "# : " + updatedState + " : " + updatedState.timing);
                            stateCache.onNext(updatedState);
                        }
                    })
                    .flatMap(new Func1<State<R>, Observable<R>>() {
                        @Override
                        public Observable<R> call(State<R> state) {
                            return state.getValue();
                        }
                    });
        } else {
            return null;
        }
    }

    private Observable<State<R>> putIntoRequestCache(HystrixRequestCache cache, String cacheKey, Observable<State<R>> execution) {
        // wrap it for caching
        HystrixCachedState<R> toCache = HystrixCachedState.from(execution);
        HystrixCachedState<R> fromCache = cache.putIfAbsent(cacheKey, toCache);
        if (fromCache != null) {
            // another thread beat us so we'll use the cached value instead
            toCache.unsubscribe();
            return fromCache.toObservable()
                    .doOnNext(new Action1<State<R>>() {
                        @Override
                        public void call(State<R> state) {
                            stateCache.onNext(state.withResponseFromCache());
                        }
                    });
        } else {
            return toCache.toObservable();
        }
    }

    private Observable<State<R>> applyCircuitBreaker(HystrixCircuitBreaker circuitBreaker, final State<R> initialState, final Func0<Observable<State<R>>> execution) {
        if (circuitBreaker.allowRequest()) {
            return execution.call();
        } else {
            State<R> circuitOpenState = initialState.withShortCircuit();
            return Observable.just(circuitOpenState);
        }
    }

    private Observable<State<R>> applyBulkhead(final Observable<State<R>> userExecution, final ExecutionIsolationStrategy isolationStrategy) {
        switch (isolationStrategy) {
            case THREAD:
                final Scheduler hystrixThreadPool = threadPool.getScheduler(new Func0<Boolean>() {
                    @Override
                    public Boolean call() {
                        if (properties.executionIsolationThreadInterruptOnTimeout().get()) {
                            boolean commandIsTimedOut = false;

                            if (stateCache.hasValue()) {
                                commandIsTimedOut = stateCache.getValue().getEventCounts().contains(HystrixEventType.TIMEOUT);
                            }
                            return commandIsTimedOut;
                        } else {
                            return false;
                        }
                    }
                });

                return userExecution
                        .subscribeOn(hystrixThreadPool)
                        .onErrorResumeNext(new Func1<Throwable, Observable<? extends State<R>>>() {
                            @Override
                            public Observable<? extends State<R>> call(Throwable t) {
                                if (t instanceof RejectedExecutionException) {
                                    return Observable.just(stateCache.getValue().withThreadPoolRejection(t));
                                } else {
                                    return Observable.error(t);
                                }
                            }
                        });
            case SEMAPHORE:
                final TryableSemaphore semaphore = getExecutionSemaphore();
                /*
                 * This semaphore must be released as early as possible.
                 * If we chose terminal-only, then unsubscriptions would leave semaphore unreleased
                 * If we chose unsubscribe-only, then command execution finishes and the consumer gets the value
                 * and can carry on with its work. That work races the unsubscription back upstream, so it can view
                 * inconsistent state (unreleased semaphore).
                 *
                 * By doing it as early as possible, we avoid those problems (and introduce some complexity).
                 */
                final AtomicBoolean semaphoreHasBeenReleased = new AtomicBoolean(false);
                if (semaphore.tryAcquire()) {
                    return userExecution
                            .doOnTerminate(new Action0() {
                                @Override
                                public void call() {
                                    if (semaphoreHasBeenReleased.compareAndSet(false, true)) {
                                        semaphore.release();
                                    }
                                }
                            })
                            .doOnUnsubscribe(new Action0() {
                                @Override
                                public void call() {
                                    if (semaphoreHasBeenReleased.compareAndSet(false, true)) {
                                        semaphore.release();
                                    }
                                }
                            });
                } else {
                    return Observable.just(stateCache.getValue().withSemaphoreRejection());
                }
            default:
                return Observable.error(new RuntimeException("Unknown ExecutionIsolationStrategy : " + isolationStrategy));
        }
    }

    private Observable<State<R>> applyTimeout(final Observable<State<R>> execution, final AbstractCommand<R> _cmd) {
        final HystrixRequestContext requestContext = HystrixRequestContext.getContextForCurrentThread();
        if (properties.executionTimeoutEnabled().get()) {
            return execution.timeout(properties.executionTimeoutInMilliseconds().get(), TimeUnit.MILLISECONDS,
                    Observable.defer(new Func0<Observable<State<R>>>() {
                                         @Override
                                         public Observable<State<R>> call() {
                                             HystrixRequestContext.setContextOnCurrentThread(requestContext);
                                             if (stateCache.getValue() != null) {
                                                 if (stateCache.getValue().isExecutedInThread()) {
                                                     executionHook.onThreadComplete(_cmd);
                                                 }
                                             }
                                             State<R> stateWithTimeout = stateCache.getValue().withTimeout();
                                             stateCache.onNext(stateWithTimeout);
                                             return Observable.just(stateWithTimeout);
                                         }
                                     }
                    ),
                    HystrixTimer.getInstance().getScheduler());
        } else {
            return execution;
        }
    }

    private Observable<State<R>> applyFallback(final State<R> stateWithException) {
        logger.debug("Error executing HystrixCommand.run(). Proceeding to fallback logic ...", stateWithException.getExecutionThrowable());
        final Throwable executionThrowable = stateWithException.getExecutionThrowable();
        if (isUnrecoverable(executionThrowable)) {
            Exception e = getExceptionFromThrowable(executionThrowable);
            logger.error("Unrecoverable Error for HystrixCommand so will throw HystrixRuntimeException and not apply fallback. ", e);
            return Observable.just(stateWithException.withUnrecoverableError(e));
        } else if (stateWithException.getEventCounts().contains(HystrixEventType.BAD_REQUEST)) {
            //no fallback, just return the HystrixBadRequestException
            return Observable.just(stateWithException.withBadRequest(executionThrowable));
        } else {
            if (isRecoverableError(executionThrowable)) {
                logger.warn("Recovered from java.lang.Error by serving Hystrix fallback", executionThrowable);
            }

            if (properties.fallbackEnabled().get()) {
                return applyFallbackBulkhead(stateWithException);
            } else {
                //TODO Add FALLBACK_DISABLED state
                return Observable.just(stateWithException);
            }
        }
    }

    private Observable<State<R>> applyFallbackBulkhead(State<R> stateWithException) {
        final TryableSemaphore fallbackSemaphore = getFallbackSemaphore();
        final AtomicBoolean fallbackSemaphoreHasBeenReleased = new AtomicBoolean(false);
        if (fallbackSemaphore.tryAcquire()) {
            final Observable<State<R>> fallbackExecution = getFallbackStateObservable(stateWithException);

            return fallbackExecution
                        /*
                         * This semaphore must be released as early as possible.
                         * If we chose terminal-only, then unsubscriptions would leave semaphore unreleased
                         * If we chose unsubscribe-only, then command execution finishes and the consumer gets the value
                         * and can carry on with its work. That work races the unsubscription back upstream, so it can view
                         * inconsistent state (unreleased semaphore).
                         *
                         * By doing it as early as possible, we avoid those problems (and introduce some complexity).
                         */

                    .doOnTerminate(new Action0() {
                        @Override
                        public void call() {
                            if (fallbackSemaphoreHasBeenReleased.compareAndSet(false, true)) {
                                fallbackSemaphore.release();
                            }
                        }
                    })
                    .doOnUnsubscribe(new Action0() {
                        @Override
                        public void call() {
                            if (fallbackSemaphoreHasBeenReleased.compareAndSet(false, true)) {
                                fallbackSemaphore.release();
                            }
                        }
                    });
        } else {
            return Observable.just(stateWithException.withFallbackRejection());
        }
    }

    protected Throwable executeExecutionStartHooks(ExecutionIsolationStrategy isolationStrategy) {
        if (isolationStrategy == HystrixCommandProperties.ExecutionIsolationStrategy.THREAD) {
            try {
                executionHook.onThreadStart(this);
            } catch (Throwable hookEx) {
                logger.warn("Error calling HystrixCommandExecutionHook.onThreadStart", hookEx);
                return hookEx;
            }
        }

        try {
            executionHook.onRunStart(this);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onRunStart", hookEx);
            return hookEx;
        }

        try {
            executionHook.onExecutionStart(this);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onExecutionStart", hookEx);
            return hookEx;
        }

        return null;
    }

    protected R wrapValueWithExecutionHooks(R value, AbstractCommand<R> _cmd) {
        R afterFirstHook;
        try {
            afterFirstHook = executionHook.onExecutionEmit(_cmd, value);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onExecutionEmit", hookEx);
            afterFirstHook = value;
        }

        R afterSecondHook;
        try {
            afterSecondHook = executionHook.onRunSuccess(_cmd, afterFirstHook);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onRunSuccess", hookEx);
            afterSecondHook = afterFirstHook;
        }

        return afterSecondHook;
    }

    protected Throwable executeExecutionSuccessHooks(AbstractCommand<R> _cmd) {
        try {
            executionHook.onExecutionSuccess(_cmd);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onExecutionSuccess", hookEx);
            return hookEx;
        }

        return null;
    }

    protected Exception wrapFailureWithExecutionFailureHooks(Throwable t, AbstractCommand<R> _cmd) {
        Exception e = getExceptionFromThrowable(t);

        Exception exAfterFirstHook;
        try {
            exAfterFirstHook = executionHook.onExecutionError(_cmd, e);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onExecutionError", hookEx);
            exAfterFirstHook = e;
        }

        Exception exAfterSecondHook;
        try {
            exAfterSecondHook = executionHook.onRunError(_cmd, exAfterFirstHook);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onRunError", hookEx);
            exAfterSecondHook = exAfterFirstHook;
        }

        return exAfterSecondHook;
    }

    protected Throwable executeFallbackStartHooks(AbstractCommand<R> _cmd) {
        try {
            executionHook.onFallbackStart(_cmd);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onFallbackStart", hookEx);
            return hookEx;
        }

        return null;
    }

    protected R wrapValueWithFallbackHooks(R fallbackValue, AbstractCommand<R> _cmd) {
        R afterFirstHook;
        try {
            afterFirstHook = executionHook.onFallbackEmit(_cmd, fallbackValue);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onFallbackEmit", hookEx);
            afterFirstHook = fallbackValue;
        }

        R afterSecondHook;
        try {
            afterSecondHook = executionHook.onFallbackSuccess(_cmd, afterFirstHook);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onFallbackSuccess", hookEx);
            afterSecondHook = afterFirstHook;
        }

        return afterSecondHook;
    }

    protected Throwable executeFallbackSuccessHooks(AbstractCommand<R> _cmd) {
        try {
            executionHook.onFallbackSuccess(_cmd);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onFallbackSuccess", hookEx);
            return hookEx;
        }

        return null;
    }

    protected Exception wrapFallbackFailureWithFallbackFailureHooks(Throwable t, AbstractCommand<R> _cmd) {
        Exception e = getExceptionFromThrowable(t);
        try {
            return executionHook.onFallbackError(_cmd, e);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onFallbackError", hookEx);
            return e;
        }
    }

    private Observable<R> applyCommandHooks(final HystrixCommandExecutionHook hook, Observable<R> commandValues, final AbstractCommand<R> _cmd) {
        return commandValues.lift(new Operator<R, R>() {
            @Override
            public Subscriber<? super R> call(final Subscriber<? super R> subscriber) {
                return new Subscriber<R>(subscriber) {
                    @Override
                    public void onCompleted() {
                        try {
                            hook.onSuccess(_cmd);
                        } catch (Throwable hookEx) {
                            logger.warn("Error calling HystrixCommandExecutionHook.onSuccess", hookEx);
                        }

                        if (stateCache.getValue().isExecutedInThread() && stateCache.getValue().getExecutionThread().equals(Thread.currentThread())) {
                            try {
                                hook.onThreadComplete(_cmd);
                            } catch (Throwable hookEx) {
                                logger.warn("Error calling HystrixCommandExecutionHook.onThreadComplete", hookEx);
                            }
                        }
                        subscriber.onCompleted();
                    }

                    @Override
                    public void onError(Throwable t) {
                        Exception e = getExceptionFromThrowable(t);

                        Exception wrappedEx;
                        try {
                            wrappedEx = hook.onError(_cmd, stateCache.getValue().getFailureType(), e);
                        } catch (Throwable hookEx) {
                            logger.warn("Error calling HystrixCommandExecutionHook.onError", hookEx);
                            wrappedEx = e;
                        }

                        if (stateCache.getValue().isExecutedInThread() && stateCache.getValue().getExecutionThread().equals(Thread.currentThread())) {
                            try {
                                hook.onThreadComplete(_cmd);
                            } catch (Throwable hookEx) {
                                logger.warn("Error calling HystrixCommandExecutionHook.onThreadComplete", hookEx);
                            }
                        }

                        subscriber.onError(wrappedEx);
                    }

                    @Override
                    public void onNext(R r) {
                        R afterFirstApplication = r;

                        try {
                            afterFirstApplication = hook.onComplete(_cmd, r);
                        } catch (Throwable hookEx) {
                            logger.warn("Error calling HystrixCommandExecutionHook.onComplete", hookEx);
                        }

                        try {
                            subscriber.onNext(hook.onEmit(_cmd, afterFirstApplication));
                        } catch (Throwable hookEx) {
                            logger.warn("Error calling HystrixCommandExecutionHook.onEmit", hookEx);
                            subscriber.onNext(afterFirstApplication);
                        }
                    }
                };
            }
        });
    }

    /**
     * Returns true iff the t was caused by a java.lang.Error that is unrecoverable.  Note: not all java.lang.Errors are unrecoverable.
     * @see <a href="https://github.com/Netflix/Hystrix/issues/713"></a> for more context
     * Solution taken from <a href="https://github.com/ReactiveX/RxJava/issues/748"></a>
     *
     * The specific set of Error that are considered unrecoverable are:
     * <ul>
     * <li>{@code StackOverflowError}</li>
     * <li>{@code VirtualMachineError}</li>
     * <li>{@code ThreadDeath}</li>
     * <li>{@code LinkageError}</li>
     * </ul>
     *
     * @param t throwable to check
     * @return true iff the t was caused by a java.lang.Error that is unrecoverable
     */
    private boolean isUnrecoverable(Throwable t) {
        if (t != null && t.getCause() != null) {
            Throwable cause = t.getCause();
            if (cause instanceof StackOverflowError) {
                return true;
            } else if (cause instanceof VirtualMachineError) {
                return true;
            } else if (cause instanceof ThreadDeath) {
                return true;
            } else if (cause instanceof LinkageError) {
                return true;
            }
        }
        return false;
    }

    private boolean isRecoverableError(Throwable t) {
        if (t != null && t.getCause() != null) {
            Throwable cause = t.getCause();
            if (cause instanceof java.lang.Error) {
                return !isUnrecoverable(t);
            }
        }
        return false;
    }

    /**
     * This method should not be used externally
     * @deprecated Should not be used externally
     */
    @Deprecated
    protected void handleThreadEnd(AbstractCommand<R> _cmd) {
        HystrixCounters.decrementGlobalConcurrentThreads();
        threadPool.markThreadCompletion();
        try {
            executionHook.onThreadComplete(_cmd);
        } catch (Throwable hookEx) {
            logger.warn("Error calling HystrixCommandExecutionHook.onThreadComplete", hookEx);
        }
    }

    /**
     * This method should not be used externally
     * @deprecated Should not be used externally
     */
    protected boolean shouldOutputOnNextEvents() {
        return false;
    }

    /**
     * Get the TryableSemaphore this HystrixCommand should use if a fallback occurs.
     * 
     * @return TryableSemaphore
     */
    protected TryableSemaphore getFallbackSemaphore() {
        if (fallbackSemaphoreOverride == null) {
            TryableSemaphore _s = fallbackSemaphorePerCircuit.get(commandKey.name());
            if (_s == null) {
                // we didn't find one cache so setup
                fallbackSemaphorePerCircuit.putIfAbsent(commandKey.name(), new TryableSemaphoreActual(properties.fallbackIsolationSemaphoreMaxConcurrentRequests()));
                // assign whatever got set (this or another thread)
                return fallbackSemaphorePerCircuit.get(commandKey.name());
            } else {
                return _s;
            }
        } else {
            return fallbackSemaphoreOverride;
        }
    }

    /**
     * Get the TryableSemaphore this HystrixCommand should use for execution if not running in a separate thread.
     * 
     * @return TryableSemaphore
     */
    protected TryableSemaphore getExecutionSemaphore() {
        if (properties.executionIsolationStrategy().get() == ExecutionIsolationStrategy.SEMAPHORE) {
            if (executionSemaphoreOverride == null) {
                TryableSemaphore _s = executionSemaphorePerCircuit.get(commandKey.name());
                if (_s == null) {
                    // we didn't find one cache so setup
                    executionSemaphorePerCircuit.putIfAbsent(commandKey.name(), new TryableSemaphoreActual(properties.executionIsolationSemaphoreMaxConcurrentRequests()));
                    // assign whatever got set (this or another thread)
                    return executionSemaphorePerCircuit.get(commandKey.name());
                } else {
                    return _s;
                }
            } else {
                return executionSemaphoreOverride;
            }
        } else {
            // return NoOp implementation since we're not using SEMAPHORE isolation
            return TryableSemaphoreNoOp.DEFAULT;
        }
    }

    /**
     * Each concrete implementation of AbstractCommand should return the name of the fallback method as a String
     * This will be used to determine if the fallback "exists" for firing the onFallbackStart/onFallbackError hooks
     * @return method name of fallback
     */
    protected abstract String getFallbackMethodName();

    /**
     * For the given command instance, does it define an actual fallback method?
     * @param cmd command instance
     * @return true iff there is a user-supplied fallback method on the given command instance
     */
    /*package-private*/ static boolean isFallbackUserSupplied(final AbstractCommand<?> cmd) {
        HystrixCommandKey commandKey = cmd.commandKey;
        Boolean containsFromMap = commandContainsFallback.get(commandKey);
        if (containsFromMap != null) {
            return containsFromMap;
        } else {
            Boolean toInsertIntoMap;
            try {
                cmd.getClass().getDeclaredMethod(cmd.getFallbackMethodName());
                toInsertIntoMap = true;
            } catch (NoSuchMethodException nsme) {
                toInsertIntoMap = false;
            }
            commandContainsFallback.put(commandKey, toInsertIntoMap);
            return toInsertIntoMap;
        }
    }

    /**
     * @return {@link HystrixCommandGroupKey} used to group together multiple {@link AbstractCommand} objects.
     *         <p>
     *         The {@link HystrixCommandGroupKey} is used to represent a common relationship between commands. For example, a library or team name, the system all related commands interace with,
     *         common business purpose etc.
     */
    public HystrixCommandGroupKey getCommandGroup() {
        return commandGroup;
    }

    /**
     * @return {@link HystrixCommandKey} identifying this command instance for statistics, circuit-breaker, properties, etc.
     */
    public HystrixCommandKey getCommandKey() {
        return commandKey;
    }

    /**
     * @return {@link HystrixThreadPoolKey} identifying which thread-pool this command uses (when configured to run on separate threads via
     *         {@link HystrixCommandProperties#executionIsolationStrategy()}).
     */
    public HystrixThreadPoolKey getThreadPoolKey() {
        return threadPoolKey;
    }

    /* package */HystrixCircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    /**
     * The {@link HystrixCommandMetrics} associated with this {@link AbstractCommand} instance.
     *
     * @return HystrixCommandMetrics
     */
    public HystrixCommandMetrics getMetrics() {
        return metrics;
    }

    /**
     * The {@link HystrixCommandProperties} associated with this {@link AbstractCommand} instance.
     * 
     * @return HystrixCommandProperties
     */
    public HystrixCommandProperties getProperties() {
        return properties;
    }

    /**
     * Take an Exception and determine whether to throw it, its cause or a new HystrixRuntimeException.
     * <p>
     * This will only throw an HystrixRuntimeException, HystrixBadRequestException or IllegalStateException
     * 
     * @param e initial exception
     * @return HystrixRuntimeException, HystrixBadRequestException or IllegalStateException
     */
    protected RuntimeException decomposeException(Exception e) {
        if (e instanceof IllegalStateException) {
            return (IllegalStateException) e;
        }
        if (e instanceof HystrixBadRequestException) {
            return (HystrixBadRequestException) e;
        }
        if (e.getCause() instanceof HystrixBadRequestException) {
            return (HystrixBadRequestException) e.getCause();
        }
        if (e instanceof HystrixRuntimeException) {
            return (HystrixRuntimeException) e;
        }
        // if we have an exception we know about we'll throw it directly without the wrapper exception
        if (e.getCause() instanceof HystrixRuntimeException) {
            return (HystrixRuntimeException) e.getCause();
        }
        // we don't know what kind of exception this is so create a generic message and throw a new HystrixRuntimeException
        String message = getLogMessagePrefix() + " failed while executing.";
        logger.debug(message, e); // debug only since we're throwing the exception and someone higher will do something with it
        return new HystrixRuntimeException(FailureType.COMMAND_EXCEPTION, this.getClass(), message, e, null);

    }

    /* ******************************************************************************** */
    /* ******************************************************************************** */
    /* TryableSemaphore */
    /* ******************************************************************************** */
    /* ******************************************************************************** */

    /**
     * Semaphore that only supports tryAcquire and never blocks and that supports a dynamic permit count.
     * <p>
     * Using AtomicInteger increment/decrement instead of java.util.concurrent.Semaphore since we don't need blocking and need a custom implementation to get the dynamic permit count and since
     * AtomicInteger achieves the same behavior and performance without the more complex implementation of the actual Semaphore class using AbstractQueueSynchronizer.
     */
    /* package */static class TryableSemaphoreActual implements TryableSemaphore {
        protected final HystrixProperty<Integer> numberOfPermits;
        private final AtomicInteger count = new AtomicInteger(0);

        public TryableSemaphoreActual(HystrixProperty<Integer> numberOfPermits) {
            this.numberOfPermits = numberOfPermits;
        }

        @Override
        public boolean tryAcquire() {
            int currentCount = count.incrementAndGet();
            if (currentCount > numberOfPermits.get()) {
                count.decrementAndGet();
                return false;
            } else {
                return true;
            }
        }

        @Override
        public void release() {
            count.decrementAndGet();
        }

        @Override
        public int getNumberOfPermitsUsed() {
            return count.get();
        }

    }

    /* package */static class TryableSemaphoreNoOp implements TryableSemaphore {

        public static final TryableSemaphore DEFAULT = new TryableSemaphoreNoOp();

        @Override
        public boolean tryAcquire() {
            return true;
        }

        @Override
        public void release() {

        }

        @Override
        public int getNumberOfPermitsUsed() {
            return 0;
        }

    }

    /* package */static interface TryableSemaphore {

        /**
         * Use like this:
         * <p>
         * 
         * <pre>
         * if (s.tryAcquire()) {
         * try {
         * // do work that is protected by 's'
         * } finally {
         * s.release();
         * }
         * }
         * </pre>
         * 
         * @return boolean
         */
        public abstract boolean tryAcquire();

        /**
         * ONLY call release if tryAcquire returned true.
         * <p>
         * 
         * <pre>
         * if (s.tryAcquire()) {
         * try {
         * // do work that is protected by 's'
         * } finally {
         * s.release();
         * }
         * }
         * </pre>
         */
        public abstract void release();

        public abstract int getNumberOfPermitsUsed();

    }

    /* ******************************************************************************** */
    /* ******************************************************************************** */
    /* RequestCache */
    /* ******************************************************************************** */
    /* ******************************************************************************** */

    /**
     * Key to be used for request caching.
     * <p>
     * By default this returns null which means "do not cache".
     * <p>
     * To enable caching override this method and return a string key uniquely representing the state of a command instance.
     * <p>
     * If multiple command instances in the same request scope match keys then only the first will be executed and all others returned from cache.
     * 
     * @return cacheKey
     */
    protected String getCacheKey() {
        return null;
    }

    public String getPublicCacheKey() {
        return getCacheKey();
    }

    protected boolean isRequestCachingEnabled() {
        return properties.requestCacheEnabled().get() && getCacheKey() != null;
    }

    protected String getLogMessagePrefix() {
        return getCommandKey().name();
    }

    /**
     * Whether the 'circuit-breaker' is open meaning that <code>execute()</code> will immediately return
     * the <code>getFallback()</code> response and not attempt a HystrixCommand execution.
     *
     * 4 columns are ForcedOpen | ForcedClosed | CircuitBreaker open due to health ||| Expected Result
     *
     * T | T | T ||| OPEN (true)
     * T | T | F ||| OPEN (true)
     * T | F | T ||| OPEN (true)
     * T | F | F ||| OPEN (true)
     * F | T | T ||| CLOSED (false)
     * F | T | F ||| CLOSED (false)
     * F | F | T ||| OPEN (true)
     * F | F | F ||| CLOSED (false)
     *
     * @return boolean
     */
    public boolean isCircuitBreakerOpen() {
        return properties.circuitBreakerForceOpen().get() || (!properties.circuitBreakerForceClosed().get() && circuitBreaker.isOpen());
    }

    /**
     * If this command has completed execution either successfully, via fallback or failure.
     * 
     * @return boolean
     */
    public boolean isExecutionComplete() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().isExecutionComplete();
        } else {
            return false;
        }
    }

    /**
     * Whether the execution occurred in a separate thread.
     * <p>
     * This should be called only once execute()/queue()/fireOrForget() are called otherwise it will always return false.
     * <p>
     * This specifies if a thread execution actually occurred, not just if it is configured to be executed in a thread.
     * 
     * @return boolean
     */
    public boolean isExecutedInThread() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().isExecutedInThread();
        } else {
            return false;
        }
    }

    /**
     * Whether the response was returned successfully either by executing <code>run()</code> or from cache.
     * 
     * @return boolean
     */
    public boolean isSuccessfulExecution() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().contains(HystrixEventType.SUCCESS);
        } else {
            return false;
        }
    }

    /**
     * Whether the <code>run()</code> resulted in a failure (exception).
     * 
     * @return boolean
     */
    public boolean isFailedExecution() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().isFailedExecution();
        } else {
            return false;
        }
    }

    /**
     * Get the Throwable/Exception thrown that caused the failure.
     * <p>
     * If <code>isFailedExecution() == true</code> then this would represent the Exception thrown by the <code>run()</code> method.
     * <p>
     * If <code>isFailedExecution() == false</code> then this would return null.
     * 
     * @return Throwable or null
     */
    public Throwable getFailedExecutionException() {
        if (stateCache.hasValue()) {
            State<R> state = stateCache.getValue();
            if (state.getExecutionNotification() != null) {
                return state.getExecutionNotification().getThrowable();
            }
        }
        return null;
    }

    /**
     * Get the Throwable/Exception emitted by this command instance prior to checking the fallback.
     * This exception instance may have been generated via a number of mechanisms:
     * 1) failed execution (in this case, same result as {@link #getFailedExecutionException()}.
     * 2) timeout
     * 3) short-circuit
     * 4) rejection
     * 5) bad request
     *
     * If the command execution was successful, then this exception instance is null (there was no exception)
     *
     * Note that the caller of the command may not receive this exception, as fallbacks may be served as a response to
     * the exception.
     *
     * @return Throwable or null
     */
    public Throwable getExecutionException() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getExecutionThrowable();
        } else {
            return null;
        }
    }

    /**
     * Whether the response received from was the result of some type of failure
     * and <code>getFallback()</code> being called.
     * 
     * @return boolean
     */
    public boolean isResponseFromFallback() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().contains(HystrixEventType.FALLBACK_SUCCESS);
        } else {
            return false;
        }
    }

    /**
     * Whether the response received was the result of a timeout
     * and <code>getFallback()</code> being called.
     * 
     * @return boolean
     */
    public boolean isResponseTimedOut() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().contains(HystrixEventType.TIMEOUT);
        } else {
            return false;
        }
    }

    /**
     * Whether the response received was a fallback as result of being
     * short-circuited (meaning <code>isCircuitBreakerOpen() == true</code>) and <code>getFallback()</code> being called.
     * 
     * @return boolean
     */
    public boolean isResponseShortCircuited() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().contains(HystrixEventType.SHORT_CIRCUITED);
        } else {
            return false;
        }
    }

    /**
     * Whether the response is from cache and <code>run()</code> was not invoked.
     * 
     * @return boolean
     */
    public boolean isResponseFromCache() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().isResponseFromCache();
        } else {
            return false;
        }
    }

    /**
     * Whether the response received was a fallback as result of being rejected via sempahore
     *
     * @return boolean
     */
    public boolean isResponseSemaphoreRejected() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().contains(HystrixEventType.SEMAPHORE_REJECTED);
        } else {
            return false;
        }
    }

    /**
     * Whether the response received was a fallback as result of being rejected via threadpool
     *
     * @return boolean
     */
    public boolean isResponseThreadPoolRejected() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().contains(HystrixEventType.THREAD_POOL_REJECTED);
        } else {
            return false;
        }
    }

    /**
     * Whether the response received was a fallback as result of being rejected (either via threadpool or semaphore)
     *
     * @return boolean
     */
    public boolean isResponseRejected() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().isRejected();
        } else {
            return false;
        }
    }

    /**
     * List of HystrixCommandEventType enums representing events that occurred during execution.
     * <p>
     * Examples of events are SUCCESS, FAILURE, TIMEOUT, and SHORT_CIRCUITED
     * 
     * @return {@code List<HystrixEventType>}
     */
    public List<HystrixEventType> getExecutionEvents() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getOrderedEventList();
        } else {
            return new ArrayList<HystrixEventType>();
        }
    }

    /**
     * Number of emissions of the execution of a command.  Only interesting in the streaming case.
     * @return number of <code>OnNext</code> emissions by a streaming command
     */
    @Override
    public int getNumberEmissions() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().getCount(HystrixEventType.EMIT);
        } else {
            return 0;
        }
    }

    /**
     * Number of emissions of the execution of a fallback.  Only interesting in the streaming case.
     * @return number of <code>OnNext</code> emissions by a streaming fallback
     */
    @Override
    public int getNumberFallbackEmissions() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().getCount(HystrixEventType.FALLBACK_EMIT);
        } else {
            return 0;
        }
    }

    @Override
    public int getNumberCollapsed() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getEventCounts().getCount(HystrixEventType.COLLAPSED);
        } else {
            return 0;
        }
    }

    @Override
    public HystrixCollapserKey getOriginatingCollapserKey() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getOriginatingCollapserKey();
        } else {
            return null;
        }
    }

    /**
     * The execution time of this command instance in milliseconds, or -1 if not executed.
     * 
     * @return int
     */
    public int getExecutionTimeInMilliseconds() {
        if (stateCache.hasValue()) {
            return (int) stateCache.getValue().getExecutionLatency();
        } else {
            return -1;
        }
    }

    /**
     * Time in Nanos when this command instance's run method was called, or -1 if not executed 
     * for e.g., command threw an exception
      *
      * @return long
     */
    public long getCommandRunStartTimeInNanos() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getExecutionStartTimestamp() * 1000;
        } else {
            return -1;
        }
    }

    @Override
    public ExecutionResult.EventCounts getEventCounts() {
        if (stateCache.hasValue()) {
            return stateCache.getValue().getDeprecatedEventCounts();
        } else {
            return new ExecutionResult.EventCounts();
        }
    }

    protected static Exception getExceptionFromThrowable(Throwable t) {
        Exception e;
        if (t instanceof Exception) {
            e = (Exception) t;
        } else {
            // Hystrix 1.x uses Exception, not Throwable so to prevent a breaking change Throwable will be wrapped in Exception
            e = new Exception("Throwable caught while executing.", t);
        }
        return e;
    }

    private static class ExecutionHookDeprecationWrapper extends HystrixCommandExecutionHook {

        private final HystrixCommandExecutionHook actual;

        ExecutionHookDeprecationWrapper(HystrixCommandExecutionHook actual) {
            this.actual = actual;
        }

        @Override
        public <T> T onEmit(HystrixInvokable<T> commandInstance, T value) {
            return actual.onEmit(commandInstance, value);
        }

        @Override
        public <T> void onSuccess(HystrixInvokable<T> commandInstance) {
            actual.onSuccess(commandInstance);
        }

        @Override
        public <T> void onExecutionStart(HystrixInvokable<T> commandInstance) {
            actual.onExecutionStart(commandInstance);
        }

        @Override
        public <T> T onExecutionEmit(HystrixInvokable<T> commandInstance, T value) {
            return actual.onExecutionEmit(commandInstance, value);
        }

        @Override
        public <T> Exception onExecutionError(HystrixInvokable<T> commandInstance, Exception e) {
            return actual.onExecutionError(commandInstance, e);
        }

        @Override
        public <T> void onExecutionSuccess(HystrixInvokable<T> commandInstance) {
            actual.onExecutionSuccess(commandInstance);
        }

        @Override
        public <T> T onFallbackEmit(HystrixInvokable<T> commandInstance, T value) {
            return actual.onFallbackEmit(commandInstance, value);
        }

        @Override
        public <T> void onFallbackSuccess(HystrixInvokable<T> commandInstance) {
            actual.onFallbackSuccess(commandInstance);
        }

        @Override
        @Deprecated
        public <T> void onRunStart(HystrixCommand<T> commandInstance) {
            actual.onRunStart(commandInstance);
        }

        @Override
        public <T> void onRunStart(HystrixInvokable<T> commandInstance) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                onRunStart(c);
            }
            actual.onRunStart(commandInstance);
        }

        @Override
        @Deprecated
        public <T> T onRunSuccess(HystrixCommand<T> commandInstance, T response) {
            return actual.onRunSuccess(commandInstance, response);
        }

        @Override
        @Deprecated
        public <T> T onRunSuccess(HystrixInvokable<T> commandInstance, T response) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                response = onRunSuccess(c, response);
            }
            return actual.onRunSuccess(commandInstance, response);
        }

        @Override
        @Deprecated
        public <T> Exception onRunError(HystrixCommand<T> commandInstance, Exception e) {
            return actual.onRunError(commandInstance, e);
        }

        @Override
        @Deprecated
        public <T> Exception onRunError(HystrixInvokable<T> commandInstance, Exception e) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                e = onRunError(c, e);
            }
            return actual.onRunError(commandInstance, e);
        }

        @Override
        @Deprecated
        public <T> void onFallbackStart(HystrixCommand<T> commandInstance) {
            actual.onFallbackStart(commandInstance);
        }

        @Override
        public <T> void onFallbackStart(HystrixInvokable<T> commandInstance) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                onFallbackStart(c);
            }
            actual.onFallbackStart(commandInstance);
        }

        @Override
        @Deprecated
        public <T> T onFallbackSuccess(HystrixCommand<T> commandInstance, T fallbackResponse) {
            return actual.onFallbackSuccess(commandInstance, fallbackResponse);
        }

        @Override
        @Deprecated
        public <T> T onFallbackSuccess(HystrixInvokable<T> commandInstance, T fallbackResponse) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                fallbackResponse = onFallbackSuccess(c, fallbackResponse);
            }
            return actual.onFallbackSuccess(commandInstance, fallbackResponse);
        }

        @Override
        @Deprecated
        public <T> Exception onFallbackError(HystrixCommand<T> commandInstance, Exception e) {
            return actual.onFallbackError(commandInstance, e);
        }

        @Override
        public <T> Exception onFallbackError(HystrixInvokable<T> commandInstance, Exception e) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                e = onFallbackError(c, e);
            }
            return actual.onFallbackError(commandInstance, e);
        }

        @Override
        @Deprecated
        public <T> void onStart(HystrixCommand<T> commandInstance) {
            actual.onStart(commandInstance);
        }

        @Override
        public <T> void onStart(HystrixInvokable<T> commandInstance) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                onStart(c);
            }
            actual.onStart(commandInstance);
        }

        @Override
        @Deprecated
        public <T> T onComplete(HystrixCommand<T> commandInstance, T response) {
            return actual.onComplete(commandInstance, response);
        }

        @Override
        @Deprecated
        public <T> T onComplete(HystrixInvokable<T> commandInstance, T response) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                response = onComplete(c, response);
            }
            return actual.onComplete(commandInstance, response);
        }

        @Override
        @Deprecated
        public <T> Exception onError(HystrixCommand<T> commandInstance, FailureType failureType, Exception e) {
            return actual.onError(commandInstance, failureType, e);
        }

        @Override
        public <T> Exception onError(HystrixInvokable<T> commandInstance, FailureType failureType, Exception e) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                e = onError(c, failureType, e);
            }
            return actual.onError(commandInstance, failureType, e);
        }

        @Override
        @Deprecated
        public <T> void onThreadStart(HystrixCommand<T> commandInstance) {
            actual.onThreadStart(commandInstance);
        }

        @Override
        public <T> void onThreadStart(HystrixInvokable<T> commandInstance) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                onThreadStart(c);
            }
            actual.onThreadStart(commandInstance);
        }

        @Override
        @Deprecated
        public <T> void onThreadComplete(HystrixCommand<T> commandInstance) {
            actual.onThreadComplete(commandInstance);
        }

        @Override
        public <T> void onThreadComplete(HystrixInvokable<T> commandInstance) {
            HystrixCommand<T> c = getHystrixCommandFromAbstractIfApplicable(commandInstance);
            if (c != null) {
                onThreadComplete(c);
            }
            actual.onThreadComplete(commandInstance);
        }

        @Override
        public <T> void onCacheHit(HystrixInvokable<T> commandInstance) {
            actual.onCacheHit(commandInstance);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private <T> HystrixCommand<T> getHystrixCommandFromAbstractIfApplicable(HystrixInvokable<T> commandInstance) {
            if (commandInstance instanceof HystrixCommand) {
                return (HystrixCommand) commandInstance;
            } else {
                return null;
            }
        }
    }
}
