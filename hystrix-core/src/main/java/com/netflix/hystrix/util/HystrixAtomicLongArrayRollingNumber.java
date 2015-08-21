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

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.ReentrantLock;

import com.netflix.hystrix.util.time.HystrixActualTime;
import com.netflix.hystrix.util.time.HystrixTime;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.hystrix.strategy.properties.HystrixProperty;
import rx.functions.Func0;
//TODO mark which methods must be accessed in a single-threaded manner
//TODO What about different types for placeholderBuckets and buckets with data? This could avoid the weird null checks everywhere
//TODO update below Javadoc
/**
 * A number which can be used to track counters (increment) or set values over time.
 * <p>
 * It is "rolling" in the sense that a 'timeInMilliseconds' is given that you want to track (such as 10 seconds) and then that is broken into buckets (defaults to 10) so that the 10 second window
 * doesn't empty out and restart every 10 seconds, but instead every 1 second you have a new bucket added and one dropped so that 9 of the buckets remain and only the newest starts from scratch.
 * <p>
 * This is done so that the statistics are gathered over a rolling 10 second window with data being added/dropped in 1 second intervals (or whatever granularity is defined by the arguments) rather
 * than each 10 second window starting at 0 again.
 * <p>
 * Performance-wise this class is optimized for writes, not reads. This is done because it expects far higher write volume (thousands/second) than reads (a few per second).
 * <p>
 * For example, on each read to getSum/getCount it will iterate buckets to sum the data so that on writes we don't need to maintain the overall sum and pay the synchronization cost at each write to
 * ensure the sum is up-to-date when the read can easily iterate each bucket to get the sum when it needs it.
 * <p>
 * See UnitTest for usage and expected behavior examples.
 * 
 * @ThreadSafe
 *
 *
 * //TODO tighten up docs, but current thinking is:
 * 1 Histogram can keep track of incremental counts by using each value in [1..n] as a key
 * 1 Histogram per rolling max can keep track of entire distribution
 * split out classes into incremental and distribution
 */

//TODO should be HystrixRollingCounter and separate out rollingMax into HystrixRollingMax
public class HystrixAtomicLongArrayRollingNumber extends HystrixRollingNumber<HystrixAtomicLongBucket> {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HystrixRollingNumber.class);

    private final CumulativeSum cumulativeSum = new CumulativeSum();

    /**
     * Construct a counter, with configurable properties for how many buckets, and how long of an interval to track
     * @param timeInMilliseconds length of time to report metrics over
     * @param numberOfBuckets number of buckets to use
     *
     * @deprecated Please use {@link HystrixRollingNumber(int, int) instead}.  These values are no longer allowed to
     * be updated at runtime.
     */
    @Deprecated
    public HystrixAtomicLongArrayRollingNumber(HystrixProperty<Integer> timeInMilliseconds, HystrixProperty<Integer> numberOfBuckets) {
        this(timeInMilliseconds.get(), numberOfBuckets.get());
    }

    public HystrixAtomicLongArrayRollingNumber(int timeInMilliseconds, int numberOfBuckets) {
        this(HystrixActualTime.getInstance(), timeInMilliseconds, numberOfBuckets);
    }

    //TODO was not public in 1.4, should not be public in 1.5
    public HystrixAtomicLongArrayRollingNumber(HystrixTime time, int timeInMilliseconds, int numberOfBuckets) {
        super(time, timeInMilliseconds, numberOfBuckets);
    }

    /**
     * Increment the counter in the current bucket by one for the given {@link HystrixRollingNumberEvent} type.
     * <p>
     * The {@link HystrixRollingNumberEvent} must be a "counter" type <code>HystrixRollingNumberEvent.isCounter() == true</code>.
     *
     * @param type
     *            HystrixRollingNumberEvent defining which counter to increment
     */
    public void increment(HystrixRollingNumberEvent type) {
        getCurrentBucket().values.incrementAndGet(type.ordinal());
    }

    /**
     * Add to the counter in the current bucket for the given {@link HystrixRollingNumberEvent} type.
     * <p>
     * The {@link HystrixRollingNumberEvent} must be a "counter" type <code>HystrixRollingNumberEvent.isCounter() == true</code>.
     *
     * @param type
     *            HystrixRollingNumberEvent defining which counter to add to
     * @param value
     *            long value to be added to the current bucket
     */
    public void add(HystrixRollingNumberEvent type, long value) {
        getCurrentBucket().values.addAndGet(type.ordinal(), value);
    }

    /**
     * Update a value and retain the max value.
     * <p>
     * The {@link HystrixRollingNumberEvent} must be a "max updater" type <code>HystrixRollingNumberEvent.isMaxUpdater() == true</code>.
     *
     * @param type  HystrixRollingNumberEvent defining which counter to retrieve values from
     * @param value long value to be given to the max updater
     */
    public void updateRollingMax(HystrixRollingNumberEvent type, long value) {
        long currentValue = getCurrentBucket().values.get(type.ordinal());
        if (value > currentValue) {
            if (!getCurrentBucket().values.compareAndSet(type.ordinal(), currentValue, value)) {
                updateRollingMax(type, value);
            }
        }
    }
    @Override
    protected long getCumulativeSumUpToLatestBucket(HystrixRollingNumberEvent type) {
        return cumulativeSum.get(type);
    }

    @Override
    protected long getWindowRollingSum(HystrixRollingNumberEvent type) {
        long sum = 0L;
        for (HystrixAtomicLongBucket bucket: buckets) {
            sum += bucket.values.get(type.ordinal());
        }
        return sum;
    }

    /**
     * Get the value of the latest (current) bucket in the rolling counter for the given {@link HystrixRollingNumberEvent} type.
     * <p>
     * The {@link HystrixRollingNumberEvent} must be a "counter" type <code>HystrixRollingNumberEvent.isCounter() == true</code>.
     *
     * @param type
     *            HystrixRollingNumberEvent defining which counter to retrieve value from
     * @return
     *         value from latest bucket for given {@link HystrixRollingNumberEvent} counter type
     */
    @Override
    public long getValueOfLatestBucket(HystrixRollingNumberEvent type) {
        HystrixAtomicLongBucket lastBucket = getCurrentBucket();
        if (lastBucket == null)
            return 0;
        // we have bucket data so we'll return the lastBucket
        return lastBucket.get(type);
    }

    /**
     * Get an array of values for all buckets in the rolling counter for the given {@link HystrixRollingNumberEvent} type.
     * <p>
     * Index 0 is the oldest bucket.
     * <p>
     * @param type
     *            HystrixRollingNumberEvent defining which counter to retrieve values from
     * @return array of values from each of the rolling buckets for given {@link HystrixRollingNumberEvent} counter type
     */
    //TODO not deprecated, but note that it requires synchronization
    //TODO Add unit test for concurrent reads
    public long[] getValues(HystrixRollingNumberEvent type) {
        HystrixAtomicLongBucket lastBucket = getCurrentBucket();
        if (lastBucket == null)
            return new long[0];

        // we have bucket data so we'll return an array of values for all buckets
        long values[] = new long[buckets.numBuckets];
        int i = 0;
        for (HystrixAtomicLongBucket bucket : buckets) {
            values[i++] = bucket.values.get(type.ordinal());
        }
        return values;
    }

    /**
     * Get the max value of values in all buckets for the given {@link HystrixRollingNumberEvent} type.
     * <p>
     * The {@link HystrixRollingNumberEvent} must be a "max updater" type <code>HystrixRollingNumberEvent.isMaxUpdater() == true</code>.
     *
     * @param type
     *            HystrixRollingNumberEvent defining which "max updater" to retrieve values from
     * @return max value for given {@link HystrixRollingNumberEvent} type during rolling window
     */
    public long getRollingMaxValue(final HystrixRollingNumberEvent type) {
        long values[] = getValues(type);
        if (values.length == 0) {
            return 0;
        } else {
            Arrays.sort(values);
            return values[values.length - 1];
        }
    }

    @Override
    protected HystrixAtomicLongBucket getNewBucket(long startTime, HystrixAtomicLongBucket bucketToRecycle) {
        return new HystrixAtomicLongBucket(startTime);
    }

    @Override
    protected void rollInternalDataStructures(BucketCircularArray buckets, HystrixAtomicLongBucket lastFullBucket) {
        cumulativeSum.addBucket(lastFullBucket);
    }

    /**
     * Force a reset of all rolling counters (clear all buckets) so that statistics start being gathered from scratch.
     * <p>
     * This does NOT reset the CumulativeSum values.
     */
    @Override
    public void reset() {
        // if we are resetting, that means the lastBucket won't have a chance to be captured in CumulativeSum, so let's do it here
        HystrixAtomicLongBucket lastBucket = buckets.peekLast();
        if (lastBucket != null) {
            cumulativeSum.addBucket(lastBucket);
        }

        // clear buckets so we start over again
        buckets.clear();
    }

    /**
     * Cumulative counters (from start of JVM) from each Type
     */
    /* package */static class CumulativeSum {
        final AtomicLongArray values;

        CumulativeSum() {

            values = new AtomicLongArray(HystrixRollingNumberEvent.values().length);
        }

        public void addBucket(HystrixAtomicLongBucket lastBucket) {
            for (HystrixRollingNumberEvent type : HystrixRollingNumberEvent.values()) {
                if (type.isCounter()) {
                    values.addAndGet(type.ordinal(), lastBucket.values.get(type.ordinal()));
                }
                if (type.isMaxUpdater()) {
                    long currentMaxValue = values.get(type.ordinal());
                    if (lastBucket.get(type) > currentMaxValue) {
                        values.set(type.ordinal(), lastBucket.get(type));
                    }
                }
            }
        }

        long get(HystrixRollingNumberEvent type) {
            return values.get(type.ordinal());
        }
    }
}