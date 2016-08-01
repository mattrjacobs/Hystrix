package com.netflix.hystrix.state;

import com.netflix.hystrix.CommandDataStyle;
import com.netflix.hystrix.HystrixCircuitBreaker;
import com.netflix.hystrix.HystrixCollapser;
import com.netflix.hystrix.HystrixCollapserKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixEventType;
import com.netflix.hystrix.HystrixInvokable;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Notification;
import rx.Observable;

import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Holds state of a HystrixCommand as it executes along with values observed
 * @param <R> type of value
 */
public class State<R> {

    private final static Logger logger = LoggerFactory.getLogger(State.class);

    //make private
    public final CommandDataStyle commandDataStyle;
    private final Class<? extends HystrixInvokable> commandClass;
    private final HystrixCommandKey commandKey;
    private final HystrixCollapserKey originatingCollapserKey;
    private final HystrixCircuitBreaker circuitBreaker;

    private final EventCounts eventCounts;
    //make private
    public final Timing timing;

    private final CommandLifecycle commandLifecycle;
    private final Run<R> run;
    private final Run<R> fallbackRun;
    private final Throwable commandThrowable;

    private final boolean fromCache;

    private State(CommandDataStyle commandDataStyle, Class<? extends HystrixInvokable> commandClass, HystrixCommandKey commandKey,
                  HystrixCollapserKey originatingCollapserKey, HystrixCircuitBreaker circuitBreaker,
                  EventCounts eventCounts, Timing timing, CommandLifecycle commandLifecycle,
                  Run<R> run, Run<R> fallbackRun, Throwable commandThrowable, boolean fromCache) {
        this.commandDataStyle = commandDataStyle;
        this.commandClass = commandClass;
        this.commandKey = commandKey;
        this.originatingCollapserKey = originatingCollapserKey;
        this.circuitBreaker = circuitBreaker;
        this.eventCounts = eventCounts;
        this.timing = timing;
        this.commandLifecycle = commandLifecycle;
        this.run = run;
        this.fallbackRun = fallbackRun;
        this.commandThrowable = commandThrowable;
        this.fromCache = fromCache;
    }

    public static <R> State<R> create(CommandDataStyle commandDataStyle, Class<? extends HystrixInvokable> commandClass, HystrixCommandKey commandKey, HystrixCircuitBreaker circuitBreaker) {
        return new State<R>(commandDataStyle, commandClass, commandKey, null, circuitBreaker,
                EventCounts.create(), Timing.startCommand(System.currentTimeMillis()),
                CommandLifecycle.Start, Run.<R>empty(), Run.<R>empty(), null, false);
    }

    public static <R> State<R> create(CommandDataStyle commandDataStyle, Class<? extends HystrixInvokable> commandClass, HystrixCommandKey commandKey,
                                      HystrixCollapserKey originatingCollapserKey, HystrixCircuitBreaker circuitBreaker, int sizeOfCollapsedBatch) {
        return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker, EventCounts.create().plus(HystrixEventType.COLLAPSED, sizeOfCollapsedBatch),
                Timing.startCommand(System.currentTimeMillis()), CommandLifecycle.Start, Run.<R>empty(), Run.<R>empty(), null, false);
    }

    public Observable<R> getValue() {
        //this is the only user-facing throwable that should make it out of the command
        //the run and fallbackRun may contains Throwables but those shouldn't go to users
        if (commandThrowable != null) {
            return Observable.error(commandThrowable);
        }
        if (fallbackRun.executionNotification != null) {
            Notification<R> n = fallbackRun.executionNotification;
            switch (n.getKind()) {
                case OnNext      : return Observable.just(n.getValue());
                case OnError     : return Observable.empty(); //only send out the commandException
                case OnCompleted : return Observable.empty();
                default          : return Observable.error(new IllegalStateException("Unexpected Execution Notification Type : " + n.getKind()));
            }
        } else if (run.executionNotification != null) {
            Notification<R> n = run.executionNotification;
            switch (n.getKind()) {
                case OnNext      : return Observable.just(n.getValue());
                case OnError     : return Observable.empty(); //only send out the commandException
                case OnCompleted : return Observable.empty();
                default          : return Observable.error(new IllegalStateException("Unexpected Fallback Execution Notification Type : " + n.getKind()));
            }
        } else {
            return Observable.empty();
        }
    }

    public CommandLifecycle getCommandLifecycle() {
        return commandLifecycle;
    }

    public Notification<R> getExecutionNotification() {
        return run.executionNotification;
    }

    public Throwable getExecutionThrowable() {
        return run.getThrowable();
    }

    public long getCommandLatency() {
        return timing.getCommandLatency();
    }

    public long getFallbackLatency() {
        return timing.getFallbackLatency();
    }

    public long getExecutionLatency() {
        return timing.getExecutionLatency();
    }

    public Notification<R> getFallbackNotification() {
        return fallbackRun.executionNotification;
    }

    public Throwable getFallbackThrowable() {
        return fallbackRun.getThrowable();
    }

    public EventCounts getEventCounts() {
        return eventCounts;
    }

    public List<HystrixEventType> getOrderedEventList() {
        return eventCounts.getOrderedEventList();
    }

    public boolean isExecutedInThread() {
        return run.thread != null;
    }

    public boolean isResponseFromCache() {
        return fromCache;
    }

    public boolean isTimedOut() {
        return eventCounts.contains(HystrixEventType.TIMEOUT);
    }

    public boolean isCancelled() {
        return eventCounts.contains(HystrixEventType.CANCELLED);
    }

    public boolean isExecutionComplete() {
        System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " : isExecutionComplete : " + this);
        return eventCounts.isExecutionComplete();
    }

    public boolean isCreatedByCollapser() {
        return originatingCollapserKey != null;
    }

    public HystrixEventType getTerminalExecutionEventType() {
        if (eventCounts.contains(HystrixEventType.SUCCESS)) {
            return HystrixEventType.SUCCESS;
        } else if (eventCounts.contains(HystrixEventType.FAILURE)) {
            return HystrixEventType.FAILURE;
        } else if (eventCounts.contains(HystrixEventType.THREAD_POOL_REJECTED)) {
            return HystrixEventType.THREAD_POOL_REJECTED;
        } else if (eventCounts.contains(HystrixEventType.SEMAPHORE_REJECTED)) {
            return HystrixEventType.SEMAPHORE_REJECTED;
        } else if (eventCounts.contains(HystrixEventType.TIMEOUT)) {
            return HystrixEventType.TIMEOUT;
        } else if (eventCounts.contains(HystrixEventType.SHORT_CIRCUITED)) {
            return HystrixEventType.SHORT_CIRCUITED;
        } else if (eventCounts.contains(HystrixEventType.BAD_REQUEST)) {
            return HystrixEventType.BAD_REQUEST;
        } else return null;
    }

    public Thread getExecutionThread() {
        return run.thread;
    }

    public HystrixRuntimeException.FailureType getFailureType() {
        return getFailureTypeForFallbackException(getTerminalExecutionEventType());
    }

    public State<R> startSemaphoreExecution() {
        return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                eventCounts, timing.withExecutionStart(), CommandLifecycle.SemaphoreExecutionStart,
                run, fallbackRun, commandThrowable, fromCache);
    }

    public State<R> startExecutionOnThread(Thread executionThread) {
        return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                eventCounts, timing.withExecutionStart(), CommandLifecycle.ThreadExecutionStart,
                run.onThread(executionThread), fallbackRun, commandThrowable, fromCache);
    }

    public State<R> startFallbackExecution(Thread executionThread) {
        return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                eventCounts, timing.withFallbackStart(),
                CommandLifecycle.FallbackStart, run, fallbackRun.onThread(executionThread), commandThrowable, fromCache);
    }

    public State<R> withExecutionNotification(Notification<R> notification) {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            Timing updatedTiming = timing.withExecution();
            switch (notification.getKind()) {
                case OnNext:
                    switch (commandDataStyle) {
                        case SCALAR:
                            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                                    eventCounts.plus(HystrixEventType.SUCCESS), updatedTiming, CommandLifecycle.Execution,
                                    run.withExecutionNotification(notification), fallbackRun, commandThrowable, fromCache);
                        case MULTIVALUED:
                            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                                    eventCounts.plus(HystrixEventType.EMIT), updatedTiming, CommandLifecycle.Execution,
                                    run.withExecutionNotification(notification), fallbackRun, commandThrowable, fromCache);
                        default:
                            throw new IllegalStateException("Unexpected command data style : " + commandDataStyle);
                    }
                case OnError:
                    HystrixEventType eventType = getEventType(notification.getThrowable());
                    if (eventType.equals(HystrixEventType.SEMAPHORE_REJECTED)) {
                        logger.debug("HystrixCommand Execution Rejection by Semaphore."); // debug only since we're throwing the exception and someone higher will do something with it
                    }
                    return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                            eventCounts.plus(eventType), updatedTiming, CommandLifecycle.Execution,
                            run.withExecutionNotification(notification), fallbackRun, commandThrowable, fromCache);
                case OnCompleted:
                    circuitBreaker.markSuccess();
                    switch (commandDataStyle) {
                        case SCALAR:
                            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                                    eventCounts, updatedTiming, CommandLifecycle.Execution,
                                    run.withExecutionNotification(notification), fallbackRun, commandThrowable, fromCache); //already sent the SUCCESS
                        case MULTIVALUED:
                            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                                    eventCounts.plus(HystrixEventType.SUCCESS), updatedTiming, CommandLifecycle.Execution,
                                    run.withExecutionNotification(notification), fallbackRun, commandThrowable, fromCache);
                        default:
                            throw new IllegalStateException("Unexpected command data style : " + commandDataStyle);
                    }
                default:
                    throw new IllegalStateException("Unexpected Notification type : " + notification.getKind());
            }
        } else {
            return this;
        }
    }

    public State<R> withFallbackExecutionNotification(Notification<R> notification) {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            Timing updatedTiming = timing.withFallbackExecution();
            switch (notification.getKind()) {
                case OnNext:
                    switch (commandDataStyle) {
                        case SCALAR:
                            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                                    eventCounts.plus(HystrixEventType.FALLBACK_SUCCESS), updatedTiming, CommandLifecycle.Fallback,
                                    run, fallbackRun.withExecutionNotification(notification), commandThrowable, fromCache);
                        case MULTIVALUED:
                            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                                    eventCounts.plus(HystrixEventType.FALLBACK_EMIT), updatedTiming, CommandLifecycle.Fallback,
                                    run, fallbackRun.withExecutionNotification(notification), commandThrowable, fromCache);
                        default:
                            throw new IllegalStateException("Unexpected command data style : " + commandDataStyle);
                    }
                case OnError:
                    HystrixEventType fallbackEventType = getFallbackEventType(notification.getThrowable());
                    if (fallbackEventType.equals(HystrixEventType.FALLBACK_MISSING)) {
                        logger.debug("No fallback for HystrixCommand. ", notification.getThrowable()); // debug only since we're throwing the exception and someone higher will do something with it
                    } else if (fallbackEventType.equals(HystrixEventType.FALLBACK_FAILURE)) {
                        logger.debug("HystrixCommand execution " +
                                getFailureTypeForFallbackException(getTerminalExecutionEventType()).name() + " and fallback failed.", notification.getThrowable());
                    } else if (fallbackEventType.equals(HystrixEventType.FALLBACK_REJECTION)) {
                        logger.debug("HystrixCommand Fallback Rejection."); // debug only since we're throwing the exception and someone higher will do something with it
                        //TODO add back once FALLBACK_DISABLED type has been defined
                        //} else if (fallbackEventType.equals(HystrixEventType.FALLBACK_DISABLED)) {
                    //    logger.debug("Fallback disabled for HystrixCommand so will throw HystrixRuntimeException. ", notification.getThrowable()); // debug only since we're throwing the exception and someone higher will do something with it
                    }
                    Throwable userFacingThrowable = getUserFacingThrowable(getExecutionThrowable(), notification.getThrowable(),
                            getTerminalExecutionEventType(), fallbackEventType,
                            commandClass, commandKey);
                    return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                            eventCounts.plus(fallbackEventType), updatedTiming, CommandLifecycle.Fallback,
                            run, fallbackRun.withExecutionNotification(notification), userFacingThrowable, fromCache);
                case OnCompleted:
                    switch (commandDataStyle) {
                        case SCALAR:
                            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                                    eventCounts, updatedTiming, CommandLifecycle.Fallback,
                                    run, fallbackRun.withExecutionNotification(notification), commandThrowable, fromCache); //already sent the FALLBACK_SUCCESS
                        case MULTIVALUED:
                            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                                    eventCounts.plus(HystrixEventType.FALLBACK_SUCCESS), updatedTiming, CommandLifecycle.Fallback,
                                    run, fallbackRun.withExecutionNotification(notification), commandThrowable, fromCache);
                        default:
                            throw new IllegalStateException("Unexpected command data style : " + commandDataStyle);
                    }
                default:
                    throw new IllegalStateException("Unexpected Notification type : " + notification.getKind());
            }
        } else {
            return this;
        }
    }

    public State<R> withShortCircuit() {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            Throwable shortCircuitException = new RuntimeException("Hystrix circuit short-circuited and is OPEN");
            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                    eventCounts.plus(HystrixEventType.SHORT_CIRCUITED), timing.withCommandExecutionOnly(), CommandLifecycle.Execution,
                    run.withUnexecutedThrowable(shortCircuitException), fallbackRun, commandThrowable, fromCache);
        } else {
            return this;
        }
    }

    public State<R> withThreadPoolRejection(Throwable threadPoolRejectionException) {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                    eventCounts.plus(HystrixEventType.THREAD_POOL_REJECTED), timing.withCommandExecutionOnly(), CommandLifecycle.Execution,
                    run.withUnexecutedThrowable(threadPoolRejectionException), fallbackRun, commandThrowable, fromCache);
        } else {
            return this;
        }
    }

    public State<R> withSemaphoreRejection() {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            Throwable semaphoreRejectedException = new RuntimeException("could not acquire a semaphore for execution");
            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                    eventCounts.plus(HystrixEventType.SEMAPHORE_REJECTED), timing.withCommandExecutionOnly(), CommandLifecycle.Execution,
                    run.withUnexecutedThrowable(semaphoreRejectedException), fallbackRun, commandThrowable, fromCache);
        } else {
            return this;
        }
    }

    public State<R> withTimeout() {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            Throwable timeoutException = new TimeoutException();
            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                    eventCounts.plus(HystrixEventType.TIMEOUT), timing.withExecution(), CommandLifecycle.Execution,
                    run.withUnexecutedThrowable(timeoutException), fallbackRun, commandThrowable, fromCache);
        } else {
            return this;
        }
    }

    public State<R> withBadRequest(Throwable throwable) {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                    eventCounts, timing.withExecution(),
                    CommandLifecycle.End, run, fallbackRun, throwable, fromCache);
        } else {
            return this;
        }
    }

    public State<R> withUnrecoverableError(Throwable throwable) {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            Throwable unrecoverableErrorException = new HystrixRuntimeException(HystrixRuntimeException.FailureType.COMMAND_EXCEPTION,
                    commandClass, commandKey.name() + " and encountered unrecoverable error.", throwable, null);
            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                    eventCounts.plus(HystrixEventType.FAILURE), timing.withExecution(), CommandLifecycle.End,
                    run, fallbackRun, unrecoverableErrorException, fromCache);
        } else {
            return this;
        }
    }

    public State<R> withFallbackRejection() {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            Throwable fallbackSemaphoreRejectedException = new HystrixRuntimeException(HystrixRuntimeException.FailureType.REJECTED_SEMAPHORE_FALLBACK,
                    commandClass, commandKey.name() + " " + getExecutionMessageForFallbackException(getTerminalExecutionEventType()) + " and fallback execution rejected.",
                    getExecutionThrowable(), null);
            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                    eventCounts.plus(HystrixEventType.FALLBACK_REJECTION), timing.withCommandExecutionOnly(), CommandLifecycle.Fallback,
                    run, fallbackRun.withUnexecutedThrowable(fallbackSemaphoreRejectedException), fallbackSemaphoreRejectedException, fromCache);
        } else {
            return this;
        }
    }

    public State<R> withResponseFromCache() {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            System.out.println("FIRST : " + this.eventCounts);
            State<R> next =  new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                    eventCounts.plus(HystrixEventType.RESPONSE_FROM_CACHE), timing.withResponseFromCache(), CommandLifecycle.ResponseFromCache,
                    run.onThread(null), fallbackRun, commandThrowable, true);
            System.out.println("SECOND : " + next.eventCounts);
            return next;
        } else {
            return this;
        }
    }

    public State<R> withCancellation() {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                    eventCounts.plus(HystrixEventType.CANCELLED), timing.withCancellation(), CommandLifecycle.Cancelled,
                    run, fallbackRun, commandThrowable, fromCache);
        } else {
            return this;
        }
    }

    public State<R> withCollapsed(HystrixCollapserKey collapserKey, int sizeOfBatch) {
        if (!eventCounts.contains(HystrixEventType.CANCELLED)) {
            return new State<R>(commandDataStyle, commandClass, commandKey, originatingCollapserKey, circuitBreaker,
                    eventCounts.plus(HystrixEventType.COLLAPSED, sizeOfBatch), timing.withCommandExecutionOnly(), CommandLifecycle.Execution,
                    run, fallbackRun, commandThrowable, fromCache);
        } else {
            return this;
        }
    }

    private static Throwable getUserFacingThrowable(Throwable executionException, Throwable fallbackException,
                                             HystrixEventType eventType, HystrixEventType fallbackEventType,
                                             Class<? extends HystrixInvokable> commandClass, HystrixCommandKey commandKey) {
        return new HystrixRuntimeException(getFailureTypeForFallbackException(eventType), commandClass,
                commandKey.name() + " " + getExecutionMessageForFallbackException(eventType) + " and " + getFallbackExecutionMessage(fallbackEventType) + ".",
                executionException, fallbackException);
    }

    private static HystrixRuntimeException.FailureType getFailureTypeForFallbackException(HystrixEventType eventType) {
        switch (eventType) {
            case FAILURE              : return HystrixRuntimeException.FailureType.COMMAND_EXCEPTION;
            case SHORT_CIRCUITED      : return HystrixRuntimeException.FailureType.SHORTCIRCUIT;
            case THREAD_POOL_REJECTED : return HystrixRuntimeException.FailureType.REJECTED_THREAD_EXECUTION;
            case SEMAPHORE_REJECTED   : return HystrixRuntimeException.FailureType.REJECTED_SEMAPHORE_EXECUTION;
            case TIMEOUT              : return HystrixRuntimeException.FailureType.TIMEOUT;
            case BAD_REQUEST          : return HystrixRuntimeException.FailureType.BAD_REQUEST_EXCEPTION;
            default                   : throw new IllegalStateException("Unexpected eventType : " + eventType + " conversion to FailureType");
        }
    }

    private static String getExecutionMessageForFallbackException(HystrixEventType eventType) {
        switch (eventType) {
            case FAILURE              : return "failed";
            case SHORT_CIRCUITED      : return "short-circuited";
            case THREAD_POOL_REJECTED : return "could not be queued for execution";
            case SEMAPHORE_REJECTED   : return "could not acquire a semaphore for execution";
            case TIMEOUT              : return "timed-out";
            default                   : throw new IllegalStateException("Unexpected eventType : " + eventType + " conversion to message");
        }
    }

    private static String getFallbackExecutionMessage(HystrixEventType eventType) {
        switch (eventType) {
            case FALLBACK_FAILURE   : return "fallback failed";
            case FALLBACK_MISSING   : return "no fallback available";
            case FALLBACK_REJECTION : return "fallback execution rejected";
            default                 : throw new IllegalStateException("Unexpected eventType : " + eventType + " conversion to fallback message");
        }
    }

    private static HystrixEventType getEventType(Throwable t) {
        if (t instanceof TimeoutException) {
            return HystrixEventType.TIMEOUT;
        } else if (t instanceof HystrixBadRequestException) {
            return HystrixEventType.BAD_REQUEST;
        } else {
            return HystrixEventType.FAILURE;
        }
    }

    private static HystrixEventType getFallbackEventType(Throwable t) {
        if (t instanceof UnsupportedOperationException) {

            return HystrixEventType.FALLBACK_MISSING;
        } else {
            return HystrixEventType.FALLBACK_FAILURE;
        }
    }

    @Override
    public String toString() {
        return ((run.executionNotification == null) ? "<NO EN>" : run.executionNotification.getKind().name()) +
                ((fallbackRun.executionNotification == null) ? "<NO FN>" : fallbackRun.executionNotification.getKind().name()) +
                " @ " + timing.commandStart + " : { C:" + timing.getCommandLatency() + ", E:" + timing.getExecutionLatency() + ", F:" + timing.getFallbackLatency() + "}";
    }

    public boolean didExecutionOccur() {
        return getExecutionLatency() > -1;
    }



    public enum CommandLifecycle {
        Start, ThreadExecutionStart, SemaphoreExecutionStart, Cancelled, FallbackStart, None, Execution, End, Fallback, ResponseFromCache;
    }

    private static class Timing {
        private final long commandStart;
        private final long commandEnd;
        private final long executionStart;
        private final long executionEnd;
        private final long fallbackStart;
        private final long fallbackEnd;

        Timing(long commandStart, long commandEnd, long executionStart, long executionEnd, long fallbackStart, long fallbackEnd) {
            this.commandStart = commandStart;
            this.commandEnd = commandEnd;
            this.executionStart = executionStart;
            this.executionEnd = executionEnd;
            this.fallbackStart = fallbackStart;
            this.fallbackEnd = fallbackEnd;
        }

        private static Timing startCommand(long startTimestamp) {
            return new Timing(startTimestamp, startTimestamp, -1, -1, -1, -1);
        }

        int getCommandLatency() {
            if (commandEnd < 0 || commandStart < 0) {
                return -1;
            } else {
                return (int) (commandEnd - commandStart);
            }
        }

        int getExecutionLatency() {
            if (executionEnd < 0 || executionStart < 0) {
                return -1;
            } else {
                return (int) (executionEnd - executionStart);
            }
        }

        int getFallbackLatency() {
            if (fallbackEnd < 0 || fallbackStart < 0) {
                return -1;
            } else {
                return (int) (fallbackEnd - fallbackStart);
            }
        }

        public Timing withCommandExecutionOnly() {
            long timestamp = System.currentTimeMillis();
            return new Timing(commandStart, timestamp, executionStart, executionEnd, fallbackStart, fallbackEnd);
        }

        public Timing withExecutionStart() {
            long timestamp = System.currentTimeMillis();
            return new Timing(commandStart, timestamp, timestamp, timestamp, fallbackStart, fallbackEnd);
        }

        public Timing withExecution() {
            long timestamp = System.currentTimeMillis();
            return new Timing(commandStart, timestamp, executionStart, timestamp, fallbackStart, fallbackEnd);
        }

        public Timing withFallbackStart() {
            long timestamp = System.currentTimeMillis();
            return new Timing(commandStart, timestamp, executionStart, executionEnd, timestamp, timestamp);
        }

        public Timing withFallbackExecution() {
            long timestamp = System.currentTimeMillis();
            return new Timing(commandStart, timestamp, executionStart, executionEnd, fallbackStart, timestamp);
        }

        public Timing withCancellation() {
            long timestamp = System.currentTimeMillis();
            if (executionStart < 0) {
                return new Timing(commandStart, timestamp, executionStart, executionEnd, fallbackStart, fallbackEnd);
            } else if (fallbackStart < 0) {
                return new Timing(commandStart, timestamp, executionStart, timestamp, fallbackStart, fallbackEnd);
            } else {
                return new Timing(commandStart, timestamp, executionStart, executionEnd, fallbackStart, timestamp);
            }
        }

        public Timing withResponseFromCache() {
            return new Timing(commandStart, System.currentTimeMillis(), -1, -1, -1, -1);
        }

        @Override
        public String toString() {
            return "Command[" + commandStart + " -> " + commandEnd + "], Execution[" + executionStart + " -> " + executionEnd + "], Fallback[" + fallbackStart + " -> " + fallbackEnd + "]";
        }
    }

    /**
     * Represents the execution of some user code.
     *
     * The notification is the actual data coming from the execution
     * The thread is the thread on which the execution took place
     * The throwable is set whenever
     * @param <R>
     */
    private static class Run<R> {
        private final Thread thread;
        private final Notification<R> executionNotification;
        private final Throwable unexecutedThrowable;

        private Run(final Thread thread, final Notification<R> executionNotification, final Throwable unexecutedThrowable) {
            this.thread = thread;
            this.executionNotification = executionNotification;
            this.unexecutedThrowable = unexecutedThrowable;
        }

        static <R> Run<R> empty() {
            return new Run<R>(null, null, null);
        }

        Run<R> onThread(Thread thread) {
            return new Run<R>(thread, executionNotification, unexecutedThrowable);
        }

        Run<R> withExecutionNotification(Notification<R> executionNotification) {
            return new Run<R>(thread, executionNotification, unexecutedThrowable);
        }

        Run<R> withUnexecutedThrowable(Throwable unexecutedThrowable) {
            return new Run<R>(thread, executionNotification, unexecutedThrowable);
        }

        Throwable getThrowable() {
            if (unexecutedThrowable != null) {
                return unexecutedThrowable;
            } else {
                if (executionNotification != null) {
                    return executionNotification.getThrowable();
                } else {
                    return null;
                }
            }
        }
    }
}
