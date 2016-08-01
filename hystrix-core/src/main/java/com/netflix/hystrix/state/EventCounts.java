package com.netflix.hystrix.state;

import com.netflix.hystrix.HystrixEventType;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class EventCounts {

    private static final HystrixEventType[] ALL_EVENT_TYPES = HystrixEventType.values();
    private static final int NUM_EVENT_TYPES = ALL_EVENT_TYPES.length;
    private static final BitSet EXCEPTION_PRODUCING_EVENTS = new BitSet(NUM_EVENT_TYPES);
    private static final BitSet COMMAND_REJECTED = new BitSet(NUM_EVENT_TYPES);
    private static final BitSet FAILED_EXECUTION = new BitSet(NUM_EVENT_TYPES);
    private static final BitSet COMPLETED_EXECUTION = new BitSet(NUM_EVENT_TYPES);

    static {
        for (HystrixEventType eventType: HystrixEventType.EXCEPTION_PRODUCING_EVENT_TYPES) {
            EXCEPTION_PRODUCING_EVENTS.set(eventType.ordinal());
        }

        COMMAND_REJECTED.set(HystrixEventType.THREAD_POOL_REJECTED.ordinal());
        COMMAND_REJECTED.set(HystrixEventType.SEMAPHORE_REJECTED.ordinal());

        FAILED_EXECUTION.set(HystrixEventType.BAD_REQUEST.ordinal());
        FAILED_EXECUTION.set(HystrixEventType.FAILURE.ordinal());

        COMPLETED_EXECUTION.set(HystrixEventType.SUCCESS.ordinal());
        COMPLETED_EXECUTION.set(HystrixEventType.BAD_REQUEST.ordinal());
        COMPLETED_EXECUTION.set(HystrixEventType.FALLBACK_FAILURE.ordinal());
        COMPLETED_EXECUTION.set(HystrixEventType.FALLBACK_MISSING.ordinal());
        COMPLETED_EXECUTION.set(HystrixEventType.FALLBACK_REJECTION.ordinal());
        COMPLETED_EXECUTION.set(HystrixEventType.FALLBACK_SUCCESS.ordinal());
    }

    private final BitSet events;
    private final int numEmissions;
    private final int numFallbackEmissions;
    private final int numCollapsed;

    private EventCounts() {
        this.events = new BitSet(NUM_EVENT_TYPES);
        this.numEmissions = 0;
        this.numFallbackEmissions = 0;
        this.numCollapsed = 0;
    }

    private EventCounts(BitSet events, int numEmissions, int numFallbackEmissions, int numCollapsed) {
        this.events = events;
        this.numEmissions = numEmissions;
        this.numFallbackEmissions = numFallbackEmissions;
        this.numCollapsed = numCollapsed;
    }

    public static EventCounts from(HystrixEventType... eventTypes) {
        BitSet newBitSet = new BitSet(NUM_EVENT_TYPES);
        int localNumEmits = 0;
        int localNumFallbackEmits = 0;
        int localNumCollapsed = 0;
        for (HystrixEventType eventType: eventTypes) {
            switch (eventType) {
                case EMIT:
                    newBitSet.set(HystrixEventType.EMIT.ordinal());
                    localNumEmits++;
                    break;
                case FALLBACK_EMIT:
                    newBitSet.set(HystrixEventType.FALLBACK_EMIT.ordinal());
                    localNumFallbackEmits++;
                    break;
                case COLLAPSED:
                    newBitSet.set(HystrixEventType.COLLAPSED.ordinal());
                    localNumCollapsed++;
                    break;
                default:
                    newBitSet.set(eventType.ordinal());
                    break;
            }
        }
        return new EventCounts(newBitSet, localNumEmits, localNumFallbackEmits, localNumCollapsed);
    }

    EventCounts plus(HystrixEventType eventType) {
        return plus(eventType, 1);
    }

    EventCounts plus(HystrixEventType eventType, int count) {
        BitSet newBitSet = (BitSet) events.clone();
        int localNumEmits = numEmissions;
        int localNumFallbackEmits =  numFallbackEmissions;
        int localNumCollapsed = numCollapsed;
        switch (eventType) {
            case EMIT:
                newBitSet.set(HystrixEventType.EMIT.ordinal());
                localNumEmits += count;
                break;
            case FALLBACK_EMIT:
                newBitSet.set(HystrixEventType.FALLBACK_EMIT.ordinal());
                localNumFallbackEmits += count;
                break;
            case COLLAPSED:
                newBitSet.set(HystrixEventType.COLLAPSED.ordinal());
                localNumCollapsed += count;
                break;
            default:
                newBitSet.set(eventType.ordinal());
                break;
        }
        return new EventCounts(newBitSet, localNumEmits, localNumFallbackEmits, localNumCollapsed);
    }

    public boolean contains(HystrixEventType eventType) {
        return events.get(eventType.ordinal());
    }

    public boolean containsAnyOf(BitSet other) {
        return events.intersects(other);
    }

    public int getCount(HystrixEventType eventType) {
        switch (eventType) {
            case EMIT: return numEmissions;
            case FALLBACK_EMIT: return numFallbackEmissions;
            case EXCEPTION_THROWN: return containsAnyOf(EXCEPTION_PRODUCING_EVENTS) ? 1 : 0;
            case COLLAPSED: return numCollapsed;
            default: return contains(eventType) ? 1 : 0;
        }
    }

    public List<HystrixEventType> getOrderedEventList() {
        List<HystrixEventType> eventList = new ArrayList<HystrixEventType>();
        for (HystrixEventType eventType: ALL_EVENT_TYPES) {
            if (contains(eventType)) {
                eventList.add(eventType);
            }
        }
        return eventList;
    }

    public boolean isRejected() {
        return containsAnyOf(COMMAND_REJECTED);
    }

    public boolean isFailedExecution() {
        return containsAnyOf(FAILED_EXECUTION);
    }

    public boolean isExecutionComplete() {
        return containsAnyOf(COMPLETED_EXECUTION);
    }

    public BitSet getBitSet() {
        return events;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventCounts that = (EventCounts) o;

        if (numEmissions != that.numEmissions) return false;
        if (numFallbackEmissions != that.numFallbackEmissions) return false;
        if (numCollapsed != that.numCollapsed) return false;
        return events.equals(that.events);

    }

    @Override
    public int hashCode() {
        int result = events.hashCode();
        result = 31 * result + numEmissions;
        result = 31 * result + numFallbackEmissions;
        result = 31 * result + numCollapsed;
        return result;
    }

    @Override
    public String toString() {
        return "EventCounts{" +
                "events=" + events +
                ", numEmissions=" + numEmissions +
                ", numFallbackEmissions=" + numFallbackEmissions +
                ", numCollapsed=" + numCollapsed +
                '}';
    }

    public static EventCounts create() {
        return new EventCounts();
    }
}
