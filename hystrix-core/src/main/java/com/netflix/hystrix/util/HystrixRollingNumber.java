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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
//TODO What about different types for placeholderBuckets and buckets with data? This could avoid the weired null checks everywhere
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
public class HystrixRollingNumber extends HystrixRollingMetrics<HystrixCountersBucket> {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HystrixRollingNumber.class);

    /*
    * This will get flipped each time a new bucket is created.
    */
    //for rolling sum of increments
    volatile CounterSnapshot currentCounterSnapshot = new CounterSnapshot();
    //for cumulative sum of increments
    volatile CounterSnapshot cumulativeCounterSnapshot = new CounterSnapshot();
    //for writing increments - represents counts by type as a histogram
    private final Recorder counter = new Recorder(2);
    private final Histogram partialLatestSnapshot = new Histogram(2);
    private volatile Histogram partialHistogramToRecycle;
    //for writing a multi-valued counter by event
    private final ConcurrentMap<HystrixRollingNumberEvent, Recorder> writeOnlyDistributions = new ConcurrentHashMap<HystrixRollingNumberEvent, Recorder>();
    //for reading a multi-valued counter by event
    //TODO instead of DistributionSnapshot, what about just a long?
    private final ConcurrentMap<HystrixRollingNumberEvent, DistributionSnapshot> distributionSnapshots = new ConcurrentHashMap<HystrixRollingNumberEvent, DistributionSnapshot>();

    private ReentrantLock readLatestBucketLock = new ReentrantLock();

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
    public void increment(HystrixRollingNumberEvent type) {
        getCurrentBucket();
        counter.recordValue(type.ordinal());
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
        getCurrentBucket();
        counter.recordValueWithCount(type.ordinal(), value);
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
        getCurrentBucket();
        Recorder writeOnlyDistribution = writeOnlyDistributions.get(type);
        if (writeOnlyDistribution == null) {
            Recorder newRecorder = new Recorder(2);
            newRecorder.recordValue(value);
            writeOnlyDistributions.putIfAbsent(type, newRecorder);
        } else {
            writeOnlyDistribution.recordValue(value);
        }
    }

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
                return cumulativeCounterSnapshot.getCount(type);
            }
        }, -1L);
    }

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
                return currentCounterSnapshot.getCount(type);
            }
        }, -1L);
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
    //TODO Add documentation - principle is that this method is available, but expensive.  Common calculations (getRollingSum) should
    //not include it, but it may be done ad-hoc
    //TODO In some concrete class, this should get added in to make sure we always call it for getRollingSum - it just happens to return 0 in this impl
    public long getValueOfLatestBucket(HystrixRollingNumberEvent type) {
        return 0;
//        readLatestBucketLock.lock();
//        try {
//            //acquired the lock
//            Histogram partialIntervalHistogram;
//            if (partialHistogramToRecycle != null) {
//                partialIntervalHistogram = counter.getIntervalHistogram(partialHistogramToRecycle);
//            } else {
//                partialIntervalHistogram = counter.getIntervalHistogram();
//            }
//            partialLatestSnapshot.add(partialIntervalHistogram);
//            partialHistogramToRecycle = partialIntervalHistogram;
//            return partialLatestSnapshot.getCountAtValue(type.ordinal());
//        } finally {
//            readLatestBucketLock.unlock();
//        }
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
        long[] values = new long[buckets.numBuckets];
        int i = 0;
        for (HystrixCountersBucket bucket : buckets) {
            synchronized(bucket) {
                if (type.isMaxUpdater()) {
                    if (bucket.getReadOnlyDistribution(type) != null) {
                        values[i] = bucket.getReadOnlyDistribution(type).getMaxValue();
                    } else {
                        values[i] = 0;
                    }
                } else {
                    if (bucket.histogram != null) {
                        values[i] = bucket.histogram.getCountAtValue(type.ordinal());
                    } else {
                        values[i] = 0;
                    }
                }
            }
            i++;
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
        return readValueFromWindow(new Func0<Long>() {
            @Override
            public Long call() {
                if (type.isMaxUpdater()) {
                    DistributionSnapshot distribution = distributionSnapshots.get(type);
                    if (distribution == null) {
                        return 0L;
                    } else {
                        return distribution.getMaxValue();
                    }
                } else {
                    return 0L;
                }
            }
        }, -1L);

    }

    @Override
    protected HystrixCountersBucket getNewBucket(long startTime, HystrixCountersBucket bucketToRecycle) {
        System.out.println("getNewBucket : " + startTime + ", recycling : " + bucketToRecycle);
        HystrixCountersBucket mostRecentBucket = buckets.peekLast();
        if (mostRecentBucket != null) {
            //we should create a new histogram
            if (bucketToRecycle != null) {
                //we can recycle the old histograms
                mostRecentBucket.histogram = counter.getIntervalHistogram(bucketToRecycle.histogram);
                for (HystrixRollingNumberEvent eventType: HystrixRollingNumberEvent.values()) {
                    Histogram toRecycle = bucketToRecycle.getReadOnlyDistribution(eventType);
                    Recorder writeOnlyDistribution = writeOnlyDistributions.get(eventType);
                    if (writeOnlyDistribution == null) {
                        mostRecentBucket.setReadOnlyDistribution(eventType, new Histogram(1));
                    } else {
                        Histogram refreshedHistogram = writeOnlyDistribution.getIntervalHistogram(toRecycle);
                        mostRecentBucket.setReadOnlyDistribution(eventType, refreshedHistogram);
                    }
                }
            } else {
                //we will allocate a new one
                mostRecentBucket.histogram = counter.getIntervalHistogram();
                for (HystrixRollingNumberEvent eventType: HystrixRollingNumberEvent.values()) {
                    Recorder writeOnlyDistribution = writeOnlyDistributions.get(eventType);
                    if (writeOnlyDistribution == null) {
                        mostRecentBucket.setReadOnlyDistribution(eventType, new Histogram(1));
                    } else {
                        Histogram newReadOnlyDistribution = writeOnlyDistribution.getIntervalHistogram();
                        mostRecentBucket.setReadOnlyDistribution(eventType, newReadOnlyDistribution);
                    }
                }
            }
            System.out.println("Assigning current recorder value to : " + mostRecentBucket);
        }
        HystrixCountersBucket newBucket = new HystrixCountersBucket(startTime);
        System.out.println("created bucket : " + newBucket + " @ " + startTime);
        return newBucket;
    }

    @Override
    protected void rollInternalDataStructures(BucketCircularArray buckets, HystrixCountersBucket lastFullBucket) {
        // we created a new bucket so let's re-generate the CounterSnapshot (not including the new bucket)
        currentCounterSnapshot = new CounterSnapshot(buckets);
        if (lastFullBucket != null) {
            cumulativeCounterSnapshot.add(lastFullBucket);
        }
        for (HystrixRollingNumberEvent eventType: HystrixRollingNumberEvent.values()) {
            if (eventType.isMaxUpdater()) {
                DistributionSnapshot newDistributionSnapshot = new DistributionSnapshot(buckets, eventType);
                System.out.println("New distributionSnapshot : " + newDistributionSnapshot + " : " + newDistributionSnapshot.getMaxValue());
                distributionSnapshots.put(eventType, newDistributionSnapshot);
            }
        }
        partialLatestSnapshot.reset();
    }

    /* package for testing */ static class CounterSnapshot {
        //TODO can i save on allocation by doing a subtract and add
        private final Long[] counter = new Long[HystrixRollingNumberEvent.values().length]; //represents counters per event, indexed by eventType.ordinal

        /* package for testing */ CounterSnapshot(final BucketCircularArray buckets) {
            this();
            for (HystrixCountersBucket bucket: buckets) {
                if (bucket.histogram != null) {
                    System.out.println("Bucket @ " + bucket.windowStart + " has non-null histogram : " + bucket.histogram);
                    for (HystrixRollingNumberEvent eventType: HystrixRollingNumberEvent.values()) {
                        int eventOrdinal = eventType.ordinal();
                        counter[eventOrdinal] += (int) bucket.histogram.getCountAtValue(eventOrdinal);
                    }
                } else {
                    System.out.println("Bucket @ " + bucket.windowStart + " has null histogram : " + bucket.histogram);
                }
            }
        }

        CounterSnapshot() {
            for (int i = 0; i < counter.length; i++) {
                counter[i] = 0L;
            }
        }

        public long getCount(HystrixRollingNumberEvent eventType) {
            return counter[eventType.ordinal()];
        }

        public void add(HystrixCountersBucket bucket) {
            Histogram newHistogram = bucket.histogram;
            System.out.println("Add bucket @ " + bucket.windowStart + " : " + bucket.histogram);
            for (int i = 0; i < counter.length; i++) {
                counter[i] = counter[i] + newHistogram.getCountAtValue(i);
            }
        }
    }

    static class DistributionSnapshot {
        private Long maxValue = -1L;

        DistributionSnapshot(final BucketCircularArray buckets, final HystrixRollingNumberEvent eventType) {
            for (HystrixCountersBucket bucket: buckets) {
                long maxPerBucket = bucket.getReadOnlyDistribution(eventType).getMaxValue();
                if (maxPerBucket > maxValue) {
                    maxValue = maxPerBucket;
                }
            }
        }

        public long getMaxValue() {
            return maxValue;
        }
    }
}
