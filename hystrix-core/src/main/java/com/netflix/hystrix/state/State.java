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

    private final Execution<R> execution;
    private final Execution<R> fallbackExecution;

    private final boolean fromCache;

    private State(CommandDataStyle commandDataStyle, Class<HystrixInvokable> commandClass, HystrixCommandKey commandKey,
                  long startTimestamp, long latestTimestamp, EventCounts eventCounts,
                  Execution<R> execution, Execution<R> fallbackExecution, boolean fromCache) {
        this.commandDataStyle = commandDataStyle;
        this.commandClass = commandClass;
        this.commandKey = commandKey;
        this.startTimestamp = startTimestamp;
        this.latestTimestamp = latestTimestamp;
        this.eventCounts = eventCounts;
        this.execution = execution;
        this.fallbackExecution = fallbackExecution;
        this.fromCache = fromCache;
    }

    public static <R> State<R> create(CommandDataStyle commandDataStyle, Class<HystrixInvokable> commandClass, HystrixCommandKey commandKey) {
        final long startTimestamp = System.currentTimeMillis();

        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, startTimestamp, EventCounts.create(), Execution.<R>empty(), Execution.<R>empty(), false);
    }

    public Observable<R> getValue() {
        if (fallbackExecution.notification != null) {
            Notification<R> n = fallbackExecution.notification;
            switch (n.getKind()) {
                case OnNext      : return Observable.just(n.getValue());
                case OnError     : return Observable.error(n.getThrowable());
                case OnCompleted : return Observable.empty();
                default          : return Observable.error(new IllegalStateException("Unexpected Execution Notification Type : " + n.getKind()));
            }
        } else if (fallbackExecution.throwable != null) {
            return Observable.error(fallbackExecution.throwable);
        } else if (execution.notification != null) {
            Notification<R> n = execution.notification;
            switch (n.getKind()) {
                case OnNext      : return Observable.just(n.getValue());
                case OnError     : return Observable.error(n.getThrowable());
                case OnCompleted : return Observable.empty();
                default          : return Observable.error(new IllegalStateException("Unexpected Fallback Execution Notification Type : " + n.getKind()));
            }
        } else {
            return Observable.empty();
        }
    }

    public Notification<R> getExecutionNotification() {
        return execution.notification;
    }

    public Throwable getExecutionThrowable() {
        return execution.throwable;
    }

    //TODO Model fallback latency separately?
    public long getExecutionLatency() {
        return latestTimestamp - startTimestamp;
    }

    public EventCounts getEventCounts() {
        return eventCounts;
    }

    public List<HystrixEventType> getOrderedEventList() {
        return eventCounts.getOrderedEventList();
    }

    public boolean isExecutedInThread() {
        return execution.thread != null;
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
        if (fallbackExecution.notification != null) {
            switch (fallbackExecution.notification.getKind()) {
                case OnCompleted : return true;
                case OnError     : return true;
                default          : return false;
            }
        } else if (execution.notification != null) {
            switch (execution.notification.getKind()) {
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
        return execution.thread;
    }

    public State<R> withStartTimestamp(long startTimestamp) {
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, latestTimestamp, eventCounts,
                execution, fallbackExecution, fromCache);
    }

    public State<R> withExecutionOnThread(Thread executionThread) {
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(), eventCounts,
                execution.onThread(executionThread), fallbackExecution, fromCache);
    }

    public State<R> withFallbackExecutionOnThread(Thread executionThread) {
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(), eventCounts,
                execution, fallbackExecution.onThread(executionThread), fromCache);
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
                                    execution.withNotification(notification), fallbackExecution, fromCache);
                        case MULTIVALUED:
                            return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                                    eventCounts.plus(HystrixEventType.EMIT),
                                    execution.withNotification(notification), fallbackExecution, fromCache);
                        default:
                            throw new IllegalStateException("Unexpected command data style : " + commandDataStyle);
                    }
                case OnError:
                    System.out.println("OnError : " + notification.getThrowable());
                    HystrixEventType eventType = getEventType(notification.getThrowable());
                    return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                            eventCounts.plus(eventType),
                            execution.withNotification(notification).withThrowable(notification.getThrowable()), fallbackExecution, fromCache);
                case OnCompleted:
                    System.out.println("OnCompleted");
                    switch (commandDataStyle) {
                        case SCALAR:
                            return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                                    eventCounts, execution.withNotification(notification), fallbackExecution, fromCache); //already sent the SUCCESS
                        case MULTIVALUED:
                            return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                                    eventCounts.plus(HystrixEventType.SUCCESS),
                                    execution.withNotification(notification), fallbackExecution, fromCache);
                        default:
                            throw new IllegalStateException("Unexpected command data style : " + commandDataStyle);
                    }
                default:
                    throw new IllegalStateException("Unexpected Notification type : " + notification.getKind());
            }
        } else {
            return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp, eventCounts,
                    execution.withNotification(null), fallbackExecution, fromCache);
        }
    }

    //TODO Right now the map to put execution state on fallback observable is happening too late.
    public State<R> withFallbackExecutionNotification(Notification<R> notification) {
        long newTimestamp = System.currentTimeMillis();
        switch (notification.getKind()) {
            case OnNext:
                System.out.println("OnNext(Fallback) : " + notification.getValue());
                switch (commandDataStyle) {
                    case SCALAR      : return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                            eventCounts.plus(HystrixEventType.FALLBACK_SUCCESS),
                            execution, fallbackExecution.withNotification(notification), fromCache);
                    case MULTIVALUED : return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                            eventCounts.plus(HystrixEventType.FALLBACK_EMIT),
                            execution, fallbackExecution.withNotification(notification), fromCache);
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
                        execution, fallbackExecution.withNotification(Notification.<R>createOnError(userFacingThrowable)).withThrowable(notification.getThrowable()), fromCache);
            case OnCompleted:
                System.out.println("OnCompleted(Fallback)");
                switch (commandDataStyle) {
                    case SCALAR      : return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                            eventCounts, execution, fallbackExecution.withNotification(notification), fromCache); //already sent the FALLBACK_SUCCESS
                    case MULTIVALUED : return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, newTimestamp,
                            eventCounts.plus(HystrixEventType.FALLBACK_SUCCESS),
                            execution, fallbackExecution.withNotification(notification), fromCache);
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
                execution.withThrowable(shortCircuitException), fallbackExecution, fromCache);
    }

    public State<R> withThreadPoolRejection(Throwable threadPoolRejectionException) {
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.THREAD_POOL_REJECTED),
                execution.withThrowable(threadPoolRejectionException), fallbackExecution, fromCache);
    }

    public State<R> withSemaphoreRejection() {
        Throwable semaphoreRejectedException = new RuntimeException("could not acquire a semaphore for execution");
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.SEMAPHORE_REJECTED),
                execution.withThrowable(semaphoreRejectedException), fallbackExecution, fromCache);
    }

    public State<R> withTimeout() {
        Throwable timeoutException = new TimeoutException();
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.TIMEOUT),
                execution.withThrowable(timeoutException), fallbackExecution, fromCache);
    }

    public State<R> withFallbackRejection() {
        Throwable fallbackSemaphoreRejectedException = new HystrixRuntimeException(HystrixRuntimeException.FailureType.REJECTED_SEMAPHORE_FALLBACK,
                commandClass, commandKey.name() + " "  + getExecutionMessageForFallbackException(getTerminalExecutionEventType()) + " and fallback execution rejected.",
                getExecutionThrowable(), null);
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.FALLBACK_REJECTION),
                execution, fallbackExecution.withThrowable(fallbackSemaphoreRejectedException), fromCache);
    }

    public State<R> withResponseFromCache() {
        return new State<R>(commandDataStyle, commandClass, commandKey, System.currentTimeMillis(), System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.RESPONSE_FROM_CACHE),
                execution, fallbackExecution, true);
    }

    public State<R> withCancellation() {
        return new State<R>(commandDataStyle, commandClass, commandKey, startTimestamp, System.currentTimeMillis(),
                eventCounts.plus(HystrixEventType.CANCELLED),
                execution, fallbackExecution, fromCache);
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
        return ((execution.notification == null) ? "<NO EN>" : execution.notification.toString()) +
                ((fallbackExecution.notification == null) ? "<NO FN>" : fallbackExecution.notification.toString()) +
                " @ " + startTimestamp + " - " + latestTimestamp +
                " : " + ((execution.thread == null) ? "<NO THREAD>" : execution.thread.getName());
    }

    private static class Execution<R> {
        final Thread thread;
        final Notification<R> notification;
        final Throwable throwable;

        private Execution(final Thread thread, final Notification<R> notification, final Throwable throwable) {
            this.thread = thread;
            this.notification = notification;
            this.throwable = throwable;
        }

        static <R> Execution<R> empty() {
            return new Execution<R>(null, null, null);
        }

        Execution<R> onThread(Thread thread) {
            return new Execution<R>(thread, notification, throwable);
        }

        Execution<R> withNotification(Notification<R> notification) {
            return new Execution<R>(thread, notification, throwable);
        }

        Execution<R> withThrowable(Throwable throwable) {
            return new Execution<R>(thread, notification, throwable);
        }
    }
}
