package com.netflix.hystrix.state;

import com.netflix.hystrix.CommandDataStyle;
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

    private final CommandDataStyle commandDataStyle;
    private final Class<HystrixInvokable> commandClass;
    private final HystrixCommandKey commandKey;
    private final long startTimestamp;
    private final long latestTimestamp;

    private final EventCounts eventCounts;

    private final Run<R> run;
    private final Run<R> fallbackRun;
    private final Throwable commandThrowable;

    private final boolean fromCache;

    private State(CommandDataStyle commandDataStyle, Class<HystrixInvokable> commandClass, HystrixCommandKey commandKey,
                  long startTimestamp, long latestTimestamp, EventCounts eventCounts,
                  Run<R> run, Run<R> fallbackRun, Throwable commandThrowable, boolean fromCache) {
        this.commandDataStyle = commandDataStyle;
        this.commandClass = commandClass;
        this.commandKey = commandKey;
        this.startTimestamp = startTimestamp;
        this.latestTimestamp = latestTimestamp;
        this.eventCounts = eventCounts;
        this.run = run;
        this.fallbackRun = fallbackRun;
        this.commandThrowable = commandThrowable;
        this.fromCache = fromCache;
    }

    public static <R> State<R> create(CommandDataStyle commandDataStyle, Class<HystrixInvokable> commandClass, HystrixCommandKey commandKey) {
        final long startTimestamp = System.currentTimeMillis();

        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, startTimestamp, EventCounts.create(), Run.<R>empty(), Run.<R>empty(), null, false);
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

    public Notification<R> getExecutionNotification() {
        return run.executionNotification;
    }

    public Throwable getExecutionThrowable() {
        return run.getThrowable();
    }

    //TODO Model fallback latency separately?
    public long getExecutionLatency() {
        return latestTimestamp - startTimestamp;
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

    public boolean isExecutionComplete() {
        if (fallbackRun.executionNotification != null) {
            switch (fallbackRun.executionNotification.getKind()) {
                case OnCompleted : return true;
                case OnError     : return true;
                default          : return false;
            }
        } else if (run.executionNotification != null) {
            switch (run.executionNotification.getKind()) {
                case OnCompleted : return true;
                case OnError     : return true;
                default          : return false;
            }
        } else {
            return false;
        }
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public Thread getExecutionThread() {
        return run.thread;
    }

    public HystrixRuntimeException.FailureType getFailureType() {
        return getFailureTypeForFallbackException(getTerminalExecutionEventType());
    }

    public State<R> withExecutionOnThread(Thread executionThread) {
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(), eventCounts,
                run.onThread(executionThread), fallbackRun, commandThrowable, fromCache);
    }

    public State<R> withFallbackExecutionOnThread(Thread executionThread) {
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(), eventCounts,
                run, fallbackRun.onThread(executionThread), commandThrowable, fromCache);
    }

    public State<R> withCommandThrowable(Throwable throwable) {
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(), eventCounts,
                run, fallbackRun, throwable, fromCache);
    }

    public State<R> withExecutionNotification(Notification<R> notification) {
        long newTimestamp = System.currentTimeMillis();
        if (notification != null) {
            switch (notification.getKind()) {
                case OnNext:
                    System.out.println("OnNext : " + notification.getValue());
                    switch (commandDataStyle) {
                        case SCALAR:
                            return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                                    eventCounts.plus(HystrixEventType.SUCCESS),
                                    run.withExecutionNotification(notification), fallbackRun, commandThrowable, fromCache);
                        case MULTIVALUED:
                            return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                                    eventCounts.plus(HystrixEventType.EMIT),
                                    run.withExecutionNotification(notification), fallbackRun, commandThrowable, fromCache);
                        default:
                            throw new IllegalStateException("Unexpected command data style : " + commandDataStyle);
                    }
                case OnError:
                    System.out.println("OnError : " + notification.getThrowable());
                    HystrixEventType eventType = getEventType(notification.getThrowable());
                    return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                            eventCounts.plus(eventType),
                            run.withExecutionNotification(notification), fallbackRun, commandThrowable, fromCache);
                case OnCompleted:
                    System.out.println("OnCompleted");
                    switch (commandDataStyle) {
                        case SCALAR:
                            return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                                    eventCounts, run.withExecutionNotification(notification), fallbackRun, commandThrowable, fromCache); //already sent the SUCCESS
                        case MULTIVALUED:
                            return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                                    eventCounts.plus(HystrixEventType.SUCCESS),
                                    run.withExecutionNotification(notification), fallbackRun, commandThrowable, fromCache);
                        default:
                            throw new IllegalStateException("Unexpected command data style : " + commandDataStyle);
                    }
                default:
                    throw new IllegalStateException("Unexpected Notification type : " + notification.getKind());
            }
        } else {
            return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp, eventCounts,
                    run.withExecutionNotification(null), fallbackRun, commandThrowable, fromCache);
        }
    }

    public State<R> withFallbackExecutionNotification(Notification<R> notification) {
        long newTimestamp = System.currentTimeMillis();
        switch (notification.getKind()) {
            case OnNext:
                System.out.println("OnNext(Fallback) : " + notification.getValue());
                switch (commandDataStyle) {
                    case SCALAR      : return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                            eventCounts.plus(HystrixEventType.FALLBACK_SUCCESS),
                            run, fallbackRun.withExecutionNotification(notification), commandThrowable, fromCache);
                    case MULTIVALUED : return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                            eventCounts.plus(HystrixEventType.FALLBACK_EMIT),
                            run, fallbackRun.withExecutionNotification(notification), commandThrowable, fromCache);
                    default          : throw new IllegalStateException("Unexpected command data style : " + commandDataStyle);
                }
            case OnError:
                System.out.println("OnError(Fallback) : " + notification.getThrowable());
                HystrixEventType fallbackEventType = getFallbackEventType(notification.getThrowable());
                Throwable userFacingThrowable = getUserFacingThrowable(getExecutionThrowable(), notification.getThrowable(),
                        getTerminalExecutionEventType(), fallbackEventType,
                        commandClass, commandKey);
                return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                        eventCounts.plus(fallbackEventType),
                        run, fallbackRun.withExecutionNotification(notification), userFacingThrowable, fromCache);
            case OnCompleted:
                System.out.println("OnCompleted(Fallback)");
                switch (commandDataStyle) {
                    case SCALAR      : return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                            eventCounts, run, fallbackRun.withExecutionNotification(notification), commandThrowable, fromCache); //already sent the FALLBACK_SUCCESS
                    case MULTIVALUED : return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                            eventCounts.plus(HystrixEventType.FALLBACK_SUCCESS),
                            run, fallbackRun.withExecutionNotification(notification), commandThrowable, fromCache);
                    default          : throw new IllegalStateException("Unexpected command data style : " + commandDataStyle);
                }
            default:
                throw new IllegalStateException("Unexpected Notification type : " + notification.getKind());
        }
    }

    public State<R> withShortCircuit() {
        Throwable shortCircuitException = new RuntimeException("Hystrix circuit short-circuited and is OPEN");
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.SHORT_CIRCUITED),
                run.withUnexecutedThrowable(shortCircuitException), fallbackRun, commandThrowable, fromCache);
    }

    public State<R> withThreadPoolRejection(Throwable threadPoolRejectionException) {
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.THREAD_POOL_REJECTED),
                run.withUnexecutedThrowable(threadPoolRejectionException), fallbackRun, commandThrowable, fromCache);
    }

    public State<R> withSemaphoreRejection() {
        Throwable semaphoreRejectedException = new RuntimeException("could not acquire a semaphore for execution");
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.SEMAPHORE_REJECTED),
                run.withUnexecutedThrowable(semaphoreRejectedException), fallbackRun, commandThrowable, fromCache);
    }

    public State<R> withTimeout() {
        Throwable timeoutException = new TimeoutException();
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.TIMEOUT),
                run.withUnexecutedThrowable(timeoutException), fallbackRun, commandThrowable, fromCache);
    }

    public State<R> withFallbackRejection() {
        Throwable fallbackSemaphoreRejectedException = new HystrixRuntimeException(HystrixRuntimeException.FailureType.REJECTED_SEMAPHORE_FALLBACK,
                commandClass, commandKey.name() + " "  + getExecutionMessageForFallbackException(getTerminalExecutionEventType()) + " and fallback execution rejected.",
                getExecutionThrowable(), null);
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.FALLBACK_REJECTION),
                run, fallbackRun.withUnexecutedThrowable(fallbackSemaphoreRejectedException), commandThrowable, fromCache);
    }

    public State<R> withResponseFromCache() {
        return new State<R>(commandDataStyle, commandClass, commandKey, System.currentTimeMillis(), System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.RESPONSE_FROM_CACHE),
                run, fallbackRun, commandThrowable, true);
    }

    public State<R> withCancellation() {
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.CANCELLED),
                run, fallbackRun, commandThrowable, fromCache);
    }

    private static Throwable getUserFacingThrowable(Throwable executionException, Throwable fallbackException,
                                             HystrixEventType eventType, HystrixEventType fallbackEventType,
                                             Class<HystrixInvokable> commandClass, HystrixCommandKey commandKey) {
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
        return ((run.executionNotification == null) ? "<NO EN>" : run.executionNotification.toString()) +
                ((fallbackRun.executionNotification == null) ? "<NO FN>" : fallbackRun.executionNotification.toString()) +
                " @ " + startTimestamp + " - " + latestTimestamp +
                " : " + ((run.thread == null) ? "<NO THREAD>" : run.thread.getName());
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
