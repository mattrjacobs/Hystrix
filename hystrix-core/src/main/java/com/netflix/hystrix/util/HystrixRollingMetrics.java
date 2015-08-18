/**
 * Copyright 2015 Netflix, Inc.
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
package com.netflix.hystrix.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

import com.netflix.hystrix.util.time.HystrixActualTime;
import com.netflix.hystrix.util.time.HystrixTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.hystrix.strategy.properties.HystrixProperty;
import rx.functions.Func0;

/**
 * Abstraction which allows for multi-threaded writes, and multi-threaded reads which collect metrics over a rolling window.
 * <p>
 * The underlying data structure contains a circular array of buckets that "roll" over time.
 * <p>
 * For example, if the time window is configured to 60 seconds with 12 buckets of 5 seconds each, values will be captured in each 5 second bucket and rotate each 5 seconds.
 * <p>
 * This means that value reads are for the "rolling window" of 55-60 seconds up to 5 seconds ago.
 */
public abstract class HystrixRollingMetrics<B extends HystrixMetricsBucket> {

    protected static final Logger logger = LoggerFactory.getLogger(HystrixRollingMetrics.class);

    private static final HystrixTime ACTUAL_TIME = HystrixActualTime.getInstance();
    protected final HystrixTime time;
    /* package for testing */ final BucketCircularArray buckets;
    protected final int timeInMilliseconds;
    protected final int numberOfBuckets;
    protected final int bucketSizeInMilliseconds;
    protected final HystrixProperty<Boolean> enabled;

    /**
     *
     * @param timeInMilliseconds
     *            number of milliseconds of data that should be tracked
     *            <p>
     *            Example: 60000 for 1 minute
     * @param numberOfBuckets
     *            number of buckets that the time window should be divided into
     *            <p>
     *            Example: 12 for 5 second buckets in a 1 minute window
     * @param enabled
     *            {@code HystrixProperty<Boolean>} whether data should be tracked and percentiles calculated.
     *            <p>
     *            If 'false' methods will do nothing.
     */
    public HystrixRollingMetrics(int timeInMilliseconds, int numberOfBuckets, HystrixProperty<Boolean> enabled) {
        this(ACTUAL_TIME, timeInMilliseconds, numberOfBuckets, enabled);

    }

    /* package for testing */ HystrixRollingMetrics(HystrixTime time, int timeInMilliseconds, int numberOfBuckets, HystrixProperty<Boolean> enabled) {
        this.time = time;
        this.timeInMilliseconds = timeInMilliseconds;
        this.numberOfBuckets = numberOfBuckets;
        this.enabled = enabled;

        if (this.timeInMilliseconds % this.numberOfBuckets != 0) {
            throw new IllegalArgumentException("The timeInMilliseconds must divide equally into numberOfBuckets. For example 1000/10 is ok, 1000/11 is not.");
        }
        this.bucketSizeInMilliseconds = this.timeInMilliseconds / this.numberOfBuckets;

        buckets = new BucketCircularArray(this.numberOfBuckets);
    }

    protected <T> T readValueFromWindow(final Func0<T> readFunc, T disabledValue) {
        /* no-op if disabled */
        if (!enabled.get())
            return disabledValue;

        // force logic to move buckets forward in case other requests aren't making it happen
        getCurrentBucket();
        return readFunc.call();
    }

    private ReentrantLock newBucketLock = new ReentrantLock();

    protected abstract B getNewBucket(final long startTime, B bucketToRecycle);
    protected abstract void rollInternalDataStructures(final BucketCircularArray buckets, B lastFullBucket);

    protected B getCurrentBucket() {
        long currentTime = time.getCurrentTimeInMillis();

        /* a shortcut to try and get the most common result of immediately finding the current bucket */

        /**
         * Retrieve the latest bucket if the given time is BEFORE the end of the bucket window, otherwise it returns NULL.
         * 
         * NOTE: This is thread-safe because it's accessing 'buckets' which is a LinkedBlockingDeque
         */
        B currentBucket = buckets.peekLast();
        if (currentBucket != null && currentTime < currentBucket.windowStart + this.bucketSizeInMilliseconds) {
            // if we're within the bucket 'window of time' return the current one
            // NOTE: We do not worry if we are BEFORE the window in a weird case of where thread scheduling causes that to occur,
            // we'll just use the latest as long as we're not AFTER the window
            return currentBucket;
        }

        /* if we didn't find the current bucket above, then we have to create one */

        /**
         * The following needs to be synchronized/locked even with a synchronized/thread-safe data structure such as LinkedBlockingDeque because
         * the logic involves multiple steps to check existence, create an object then insert the object. The 'check' or 'insertion' themselves
         * are thread-safe by themselves but not the aggregate algorithm, thus we put this entire block of logic inside synchronized.
         * 
         * I am using a tryLock if/then (http://download.oracle.com/javase/6/docs/api/java/util/concurrent/locks/Lock.html#tryLock())
         * so that a single thread will get the lock and as soon as one thread gets the lock all others will go the 'else' block
         * and just return the currentBucket until the newBucket is created. This should allow the throughput to be far higher
         * and only slow down 1 thread instead of blocking all of them in each cycle of creating a new bucket based on some testing
         * (and it makes sense that it should as well).
         * 
         * This means the timing won't be exact to the millisecond as to what data ends up in a bucket, but that's acceptable.
         * It's not critical to have exact precision to the millisecond, as long as it's rolling, if we can instead reduce the impact synchronization.
         * 
         * More importantly though it means that the 'if' block within the lock needs to be careful about what it changes that can still
         * be accessed concurrently in the 'else' block since we're not completely synchronizing access.
         * 
         * For example, we can't have a multi-step process to add a bucket, remove a bucket, then update the sum since the 'else' block of code
         * can retrieve the sum while this is all happening. The trade-off is that we don't maintain the rolling sum and let readers just iterate
         * bucket to calculate the sum themselves. This is an example of favoring write-performance instead of read-performance and how the tryLock
         * versus a synchronized block needs to be accommodated.
         */
        if (newBucketLock.tryLock()) {
            try {
                if (buckets.peekLast() == null) {
                    // the list is empty so create the first bucket
                    B newBucket = getNewBucket(currentTime, null);
                    buckets.addLast(newBucket);
                    return newBucket;
                } else {
                    // We go into a loop so that it will create as many buckets as needed to catch up to the current time
                    // as we want the buckets complete even if we don't have transactions during a period of time.
                    for (int i = 0; i < numberOfBuckets; i++) {
                        // we have at least 1 bucket so retrieve it
                        B lastBucket = buckets.peekLast();
                        if (currentTime < lastBucket.windowStart + this.bucketSizeInMilliseconds) {
                            // if we're within the bucket 'window of time' return the current one
                            // NOTE: We do not worry if we are BEFORE the window in a weird case of where thread scheduling causes that to occur,
                            // we'll just use the latest as long as we're not AFTER the window
                            return lastBucket;
                        } else if (currentTime - (lastBucket.windowStart + this.bucketSizeInMilliseconds) > timeInMilliseconds) {
                            // the time passed is greater than the entire rolling counter so we want to clear it all and start from scratch
                            reset();
                            // recursively call getCurrentBucket which will create a new bucket and return it
                            return getCurrentBucket();
                        } else { // we're past the window so we need to create a new bucket
                            B newBucket;
                            if (isFull(buckets)) {
                                B bucketToRecycle = buckets.peekFirst();
                                newBucket = getNewBucket(lastBucket.windowStart + this.bucketSizeInMilliseconds, bucketToRecycle);
                            } else {
                                newBucket = getNewBucket(lastBucket.windowStart + this.bucketSizeInMilliseconds, null);
                            }
                            // create a new bucket and add it as the new 'last' (once this is done other threads will start using it on subsequent retrievals)
                            buckets.addLast(newBucket);
                            rollInternalDataStructures(buckets, lastBucket);
                        }
                    }
                    // we have finished the for-loop and created all of the buckets, so return the lastBucket now
                    return buckets.peekLast();
                }
            } finally {
                newBucketLock.unlock();
            }
        } else {
            currentBucket = buckets.peekLast();
            if (currentBucket != null) {
                // we didn't get the lock so just return the latest bucket while another thread creates the next one
                return currentBucket;
            } else {
                // the rare scenario where multiple threads raced to create the very first bucket
                // wait slightly and then use recursion while the other thread finishes creating a bucket
                try {
                    Thread.sleep(5);
                } catch (Exception e) {
                    // ignore
                }
                return getCurrentBucket();
            }
        }
    }

    protected boolean isFull(BucketCircularArray buckets) {
        return buckets.state.get().isFull();
    }

    /**
     * Force a reset so that metrics start being gathered from scratch.
     */
    public void reset() {
        /* no-op if disabled */
        if (!enabled.get())
            return;

        // clear buckets so we start over again
        buckets.clear();
    }

    /**
     * This is a circular array acting as a FIFO queue.
     * <p>
     * It purposefully does NOT implement Deque or some other Collection interface as it only implements functionality necessary for this RollingNumber use case.
     * <p>
     * Important Thread-Safety Note: This is ONLY thread-safe within the context of RollingNumber and the protection it gives in the <code>getCurrentBucket</code> method. It uses AtomicReference
     * objects to ensure anything done outside of <code>getCurrentBucket</code> is thread-safe, and to ensure visibility of changes across threads (ie. volatility) but the addLast and removeFirst
     * methods are NOT thread-safe for external access they depend upon the lock.tryLock() protection in <code>getCurrentBucket</code> which ensures only a single thread will access them at at time.
     * <p>
     * benjchristensen => This implementation was chosen based on performance testing I did and documented at: http://benjchristensen.com/2011/10/08/atomiccirculararray/
     */
    /* package for testing */ class BucketCircularArray implements Iterable<B> {
        private final AtomicReference<ListState> state;
        protected final int dataLength; // we don't resize, we always stay the same, so remember this
        protected final int numBuckets;

        BucketCircularArray(int size) {
            AtomicReferenceArray<B> _buckets = new AtomicReferenceArray<B>(size + 1); // + 1 as extra room for the add/remove;
            state = new AtomicReference<ListState>(new ListState(_buckets, 0, 0));
            dataLength = _buckets.length();
            numBuckets = size;
        }

        /**
         * Immutable object that is atomically set every time the state of the BucketCircularArray changes
         * <p>
         * This handles the compound operations
         */
        private class ListState {
            /*
             * this is an AtomicReferenceArray and not a normal Array because we're copying the reference
             * between ListState objects and multiple threads could maintain references across these
             * compound operations so I want the visibility/concurrency guarantees
             */
            private final AtomicReferenceArray<B> data;
            private final int size;
            private final int tail;
            private final int head;

            private ListState(AtomicReferenceArray<B> data, int head, int tail) {
                this.head = head;
                this.tail = tail;
                if (head == 0 && tail == 0) {
                    size = 0;
                } else {
                    this.size = (tail + dataLength - head) % dataLength;
                }
                this.data = data;
            }

            public B head() {
                if (size == 0) {
                    return null;
                } else {
                    //we want the first item, so 0
                    return data.get(convert(0));
                }
            }

            public B tail() {
                if (size == 0) {
                    return null;
                } else {
                    // we want to get the last item, so size()-1
                    return data.get(convert(size - 1));
                }
            }

            private List<B> toList() {
                /*
                 * this isn't technically thread-safe since it requires multiple reads on something that can change
                 * but since we never clear the data directly, only increment/decrement head/tail we would never get a NULL
                 * just potentially return stale data which we are okay with doing
                 */
                ArrayList<B> array = new ArrayList<B>();
                for (int i = 0; i < size; i++) {
                    array.add(data.get(convert(i)));
                }
                return array;
            }

            private ListState incrementTail() {
                /* if incrementing results in growing larger than 'length' which is the max we should be at, then also increment head (equivalent of removeFirst but done atomically) */
                if (size == numBuckets) {
                    // increment tail and head
                    return new ListState(data, (head + 1) % dataLength, (tail + 1) % dataLength);
                } else {
                    // increment only tail
                    return new ListState(data, head, (tail + 1) % dataLength);
                }
            }

            public ListState clear() {
                return new ListState(new AtomicReferenceArray<B>(dataLength), 0, 0);
            }

            public ListState addBucket(B b) {
                /*
                 * We could in theory have 2 threads addBucket concurrently and this compound operation would interleave.
                 * <p>
                 * This should NOT happen since getCurrentBucket is supposed to be executed by a single thread.
                 * <p>
                 * If it does happen, it's not a huge deal as incrementTail() will be protected by compareAndSet and one of the two addBucket calls will succeed with one of the Buckets.
                 * <p>
                 * In either case, a single Bucket will be returned as "last" and data loss should not occur and everything keeps in sync for head/tail.
                 * <p>
                 * Also, it's fine to set it before incrementTail because nothing else should be referencing that index position until incrementTail occurs.
                 */
                data.set(tail, b);
                return incrementTail();
            }

            // The convert() method takes a logical index (as if head was
            // always 0) and calculates the index within elementData
            private int convert(int index) {
                return (index + head) % dataLength;
            }

            private boolean isFull() {
                return (head + dataLength - tail) % dataLength == 1;
            }
        }

        public void clear() {
            while (true) {
                /*
                 * it should be very hard to not succeed the first pass thru since this is typically is only called from
                 * a single thread protected by a tryLock, but there is at least 1 other place (at time of writing this comment)
                 * where reset can be called from (CircuitBreaker.markSuccess after circuit was tripped) so it can
                 * in an edge-case conflict.
                 * 
                 * Instead of trying to determine if someone already successfully called clear() and we should skip
                 * we will have both calls reset the circuit, even if that means losing data added in between the two
                 * depending on thread scheduling.
                 * 
                 * The rare scenario in which that would occur, we'll accept the possible data loss while clearing it
                 * since the code has stated its desire to clear() anyways.
                 */
                ListState current = state.get();
                ListState newState = current.clear();
                if (state.compareAndSet(current, newState)) {
                    return;
                }
            }
        }

        /**
         * Returns an iterator on a copy of the internal array so that the iterator won't fail by buckets being added/removed concurrently.
         */
        public Iterator<B> iterator() {
            return Collections.unmodifiableList(toList()).iterator();
        }

        public void addLast(B b) {
            ListState currentState = state.get();
            // create new version of state (what we want it to become)
            ListState newState = currentState.addBucket(b);

            /*
             * use compareAndSet to set in case multiple threads are attempting (which shouldn't be the case because since addLast will ONLY be called by a single thread at a time due to protection
             * provided in <code>getCurrentBucket</code>)
             */
            if (state.compareAndSet(currentState, newState)) {
                // we succeeded
                return;
            } else {
                // we failed, someone else was adding or removing
                // instead of trying again and risking multiple addLast concurrently (which shouldn't be the case)
                // we'll just return and let the other thread 'win' and if the timing is off the next call to getCurrentBucket will fix things
                return;
            }
        }

        public int size() {
            // the size can also be worked out each time as:
            // return (tail + data.length() - head) % data.length();
            return state.get().size;
        }

        public B peekLast() {
            return state.get().tail();
        }

        public B peekFirst() {
            return state.get().head();
        }

        protected List<B> toList() {
            return state.get().toList();
        }
    }
}
