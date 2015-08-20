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
package com.netflix.hystrix.util;

import com.netflix.hystrix.util.time.HystrixActualTime;
import com.netflix.hystrix.util.time.HystrixTime;
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
public abstract class HystrixRollingNumber extends HystrixRollingMetrics<HystrixCountersBucket> {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HystrixRollingNumber.class);

    /**
     * Construct a counter, with configurable properties for how many buckets, and how long of an interval to track
     * @param timeInMilliseconds length of time to report metrics over
     * @param numberOfBuckets number of buckets to use
     *
     * @deprecated Please use {@link HystrixRollingNumber(int, int) instead}.  These values are no longer allowed to
     * be updated at runtime.
     */
    @Deprecated
    public HystrixRollingNumber(HystrixProperty<Integer> timeInMilliseconds, HystrixProperty<Integer> numberOfBuckets) {
        this(timeInMilliseconds.get(), numberOfBuckets.get());
    }

    public HystrixRollingNumber(int timeInMilliseconds, int numberOfBuckets) {
        this(HystrixActualTime.getInstance(), timeInMilliseconds, numberOfBuckets);
    }

    //TODO was not public in 1.4, should not be public in 1.5
    public HystrixRollingNumber(HystrixTime time, int timeInMilliseconds, int numberOfBuckets) {
        super(time, timeInMilliseconds, numberOfBuckets, HystrixProperty.Factory.asProperty(true));
    }

    /**
     * Increment the counter in the current bucket by one for the given {@link HystrixRollingNumberEvent} type.
     * <p>
     * The {@link HystrixRollingNumberEvent} must be a "counter" type <code>HystrixRollingNumberEvent.isCounter() == true</code>.
     * 
     * @param type
     *            HystrixRollingNumberEvent defining which counter to increment
     */
    public abstract void increment(HystrixRollingNumberEvent type);

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
    public abstract void add(HystrixRollingNumberEvent type, long value);

    /**
     * Update a value and retain the max value.
     * <p>
     * The {@link HystrixRollingNumberEvent} must be a "max updater" type <code>HystrixRollingNumberEvent.isMaxUpdater() == true</code>.
     * 
     * @param type  HystrixRollingNumberEvent defining which counter to retrieve values from
     * @param value long value to be given to the max updater
     */
    public abstract void updateRollingMax(HystrixRollingNumberEvent type, long value);

    /**
     * Get the cumulative sum of all buckets ever since the JVM started without rolling for the given {@link HystrixRollingNumberEvent} type.
     * <p>
     * See {@link #getRollingSum(HystrixRollingNumberEvent)} for the rolling sum.
     * <p>
     * The {@link HystrixRollingNumberEvent} must be a "counter" type <code>HystrixRollingNumberEvent.isCounter() == true</code>.
     * 
     * @param type HystrixRollingNumberEvent defining which counter to retrieve values from
     * @return cumulative sum of all increments and adds for the given {@link HystrixRollingNumberEvent} counter type
     */
    public long getCumulativeSum(final HystrixRollingNumberEvent type) {
        return readValueFromWindow(new Func0<Long>() {
            @Override
            public Long call() {
                return getCumulativeSumUpToLatestBucket(type) + getValueOfLatestBucket(type);
            }
        }, -1L);
    }

    protected abstract long getCumulativeSumUpToLatestBucket(final HystrixRollingNumberEvent type);

    /**
     * Get the sum of all buckets in the rolling counter for the given {@link HystrixRollingNumberEvent} type.
     * <p>
     * The {@link HystrixRollingNumberEvent} must be a "counter" type <code>HystrixRollingNumberEvent.isCounter() == true</code>.
     * 
     * @param type
     *            HystrixRollingNumberEvent defining which counter to retrieve values from
     * @return
     *         value from the given {@link HystrixRollingNumberEvent} counter type
     */
    public long getRollingSum(final HystrixRollingNumberEvent type) {
        return readValueFromWindow(new Func0<Long>() {
            @Override
            public Long call() {
                return getWindowRollingSum(type) + getValueOfLatestBucket(type);
            }
        }, -1L);
    }

    protected abstract long getWindowRollingSum(final HystrixRollingNumberEvent type);

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
    public abstract long getValueOfLatestBucket(HystrixRollingNumberEvent type);

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
    public abstract long[] getValues(HystrixRollingNumberEvent type);

    /**
     * Get the max value of values in all buckets for the given {@link HystrixRollingNumberEvent} type.
     * <p>
     * The {@link HystrixRollingNumberEvent} must be a "max updater" type <code>HystrixRollingNumberEvent.isMaxUpdater() == true</code>.
     * 
     * @param type
     *            HystrixRollingNumberEvent defining which "max updater" to retrieve values from
     * @return max value for given {@link HystrixRollingNumberEvent} type during rolling window
     */
    //TODO do I care about recency of rolling max and want to possibly include latest bucket?
    public abstract long getRollingMaxValue(final HystrixRollingNumberEvent type);
}
