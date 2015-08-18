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

import org.HdrHistogram.Histogram;
import org.HdrHistogram.IntCountsHistogram;
import org.HdrHistogram.Recorder;

import com.netflix.hystrix.strategy.properties.HystrixProperty;
import rx.functions.Func0;

/**
 * Add values to a rolling window and retrieve percentile calculations such as median, 90th, 99th, etc.
 * <p>
 * The underlying data structure contains a circular array of buckets that "roll" over time.
 * <p>
 * For example, if the time window is configured to 60 seconds with 12 buckets of 5 seconds each, values will be captured in each 5 second bucket and rotate each 5 seconds.
 * <p>
 * This means that percentile calculations are for the "rolling window" of 55-60 seconds up to 5 seconds ago.
 * <p>
 * Each bucket will contain a circular array of long values and if more than the configured amount (1000 values for example) it will wrap around and overwrite values until time passes and a new bucket
 * is allocated. This sampling approach for high volume metrics is done to conserve memory and reduce sorting time when calculating percentiles.
 */
//TODO should be HystrixRollingDistribution
public class HystrixRollingPercentile extends HystrixRollingMetrics<HystrixHistogramBucket> {

    /*
     * This will get flipped each time a new bucket is created.
     */
    /* package for testing */ volatile PercentileSnapshot currentPercentileSnapshot = new PercentileSnapshot(0);
    /* package for testing */ final Recorder recorder = new Recorder(2); //2 significant digits

    /**
     * 
     * @param timeInMilliseconds
     *            {@code HystrixProperty<Integer>} for number of milliseconds of data that should be tracked
     *            Note that this value is represented as a {@link HystrixProperty}, but can not actually be modified
     *            at runtime, to avoid data loss
     *            <p>
     *            Example: 60000 for 1 minute
     * @param numberOfBuckets
     *            {@code HystrixProperty<Integer>} for number of buckets that the time window should be divided into
     *            Note that this value is represented as a {@link HystrixProperty}, but can not actually be modified
     *            at runtime, to avoid data loss
     *            <p>
     *            Example: 12 for 5 second buckets in a 1 minute window
     * @param bucketDataLength
     *            {@code HystrixProperty<Integer>} for number of values stored in each bucket
     *            Note that this value is represented as a {@link HystrixProperty}, but can not actually be modified
     *            at runtime, to avoid data loss
     *            <p>
     *            Example: 1000 to store a max of 1000 values in each 5 second bucket
     * @param enabled
     *            {@code HystrixProperty<Boolean>} whether data should be tracked and percentiles calculated.
     *            <p>
     *            If 'false' methods will do nothing.
     * @deprecated Please use the constructor with non-configurable properties {@link HystrixRollingPercentile(Time, int, int, int, HystrixProperty<Boolean>}
     */
    @Deprecated
    public HystrixRollingPercentile(HystrixProperty<Integer> timeInMilliseconds, HystrixProperty<Integer> numberOfBuckets, HystrixProperty<Integer> bucketDataLength, HystrixProperty<Boolean> enabled) {
        this(timeInMilliseconds.get(), numberOfBuckets.get(), bucketDataLength.get(), enabled);
    }

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
     * @param bucketDataLength
     *            number of values stored in each bucket
     *            <p>
     *            Example: 1000 to store a max of 1000 values in each 5 second bucket
     * @param enabled
     *            {@code HystrixProperty<Boolean>} whether data should be tracked and percentiles calculated.
     *            <p>
     *            If 'false' methods will do nothing.
     */
    public HystrixRollingPercentile(int timeInMilliseconds, int numberOfBuckets, int bucketDataLength, HystrixProperty<Boolean> enabled) {
        super(timeInMilliseconds, numberOfBuckets, enabled);

    }

    /* package for testing */ HystrixRollingPercentile(HystrixRollingMetrics.Time time, int timeInMilliseconds, int numberOfBuckets, HystrixProperty<Boolean> enabled) {
        super(time, timeInMilliseconds, numberOfBuckets, enabled);
    }

    /**
     * Add value (or values) to current bucket.
     *
     * @param value
     *            Value to be stored in current bucket such as execution latency in milliseconds
     */
    public void addValue(int... value) {
        /* no-op if disabled */
        if (!enabled.get())
            return;

        getCurrentBucket();
        for (int v : value) {
            try {
                recorder.recordValue(v);
            } catch (Exception e) {
                logger.error("Failed to add value: " + v, e);
            }
        }
    }

    /**
     * Compute a percentile from the underlying rolling buckets of values.
     * <p>
     * For performance reasons it maintains a single snapshot of the sorted values from all buckets that is re-generated each time the bucket rotates.
     * <p>
     * This means that if a bucket is 5000ms, then this method will re-compute a percentile at most once every 5000ms.
     * 
     * @param percentile
     *            value such as 99 (99th percentile), 99.5 (99.5th percentile), 50 (median, 50th percentile) to compute and retrieve percentile from rolling buckets.
     * @return int percentile value
     */
    public int getPercentile(final double percentile) {
        return readValueFromWindow(new Func0<Integer>() {
            @Override
            public Integer call() {
                return getCurrentPercentileSnapshot().getPercentile(percentile);
            }
        }, -1);
    }

    /**
     * This returns the mean (average) of all values in the current snapshot. This is not a percentile but often desired so captured and exposed here.
     * 
     * @return mean of all values
     */
    public int getMean() {
        return readValueFromWindow(new Func0<Integer>() {
            @Override
            public Integer call() {
                return getCurrentPercentileSnapshot().getMean();
            }
        }, -1);
    }

    /**
     * This will retrieve the current snapshot or create a new one if one does not exist.
     * <p>
     * It will NOT include data from the current bucket, but all previous buckets.
     * <p>
     * It remains cached until the next bucket rotates at which point a new one will be created.
     */
    private PercentileSnapshot getCurrentPercentileSnapshot() {
        return currentPercentileSnapshot;
    }

    @Override
    protected HystrixHistogramBucket getNewBucket(long startTime, HystrixHistogramBucket bucketToRecycle) {
        System.out.println("getNewBucket : " + startTime + ", recycling : " + bucketToRecycle);
        HystrixHistogramBucket mostRecentBucket = buckets.peekLast();
        if (mostRecentBucket != null) {
            //we should create a new histogram
            if (bucketToRecycle != null) {
                //we can recycle the old histogram
                mostRecentBucket.histogram = recorder.getIntervalHistogram(bucketToRecycle.histogram);
            } else {
                //we will allocate a new one
                mostRecentBucket.histogram = recorder.getIntervalHistogram();
            }
        }
        return new HystrixHistogramBucket(startTime);
    }

    @Override
    protected void rollInternalDataStructures(final BucketCircularArray buckets, HystrixHistogramBucket lastFullBucket) {
        // we created a new bucket so let's re-generate the PercentileSnapshot (not including the new bucket)
        currentPercentileSnapshot = new PercentileSnapshot(buckets);
    }

    /**
     * @NotThreadSafe
     */
    /* package for testing */ static class PercentileSnapshot {
        private final IntCountsHistogram aggregateHistogram;
        private final int mean;
        private final int count;
        private final int p0;
        private final int p5;
        private final int p10;
        private final int p20;
        private final int p25;
        private final int p30;
        private final int p40;
        private final int p50;
        private final int p60;
        private final int p70;
        private final int p75;
        private final int p80;
        private final int p90;
        private final int p95;
        private final int p99;
        private final int p995;
        private final int p999;
        private final int p9995;
        private final int p9999;
        private final int p100;

        /* package for testing */ PercentileSnapshot(final BucketCircularArray buckets) {
            //TODO can i save an allocation by subtracting falling-out bucket and adding new bucket?
            aggregateHistogram = new IntCountsHistogram(2);

            for (HystrixHistogramBucket bucket: buckets) {
                if (bucket.histogram != null) {
                    aggregateHistogram.add(bucket.histogram);
                }
            }

            mean = (int) aggregateHistogram.getMean();
            count = (int) aggregateHistogram.getTotalCount();
            p0 = (int) aggregateHistogram.getValueAtPercentile(0);
            p5 = (int) aggregateHistogram.getValueAtPercentile(5);
            p10 = (int) aggregateHistogram.getValueAtPercentile(10);
            p20 = (int) aggregateHistogram.getValueAtPercentile(20);
            p25 = (int) aggregateHistogram.getValueAtPercentile(25);
            p30 = (int) aggregateHistogram.getValueAtPercentile(30);
            p40 = (int) aggregateHistogram.getValueAtPercentile(40);
            p50 = (int) aggregateHistogram.getValueAtPercentile(50);
            p60 = (int) aggregateHistogram.getValueAtPercentile(60);
            p70 = (int) aggregateHistogram.getValueAtPercentile(70);
            p75 = (int) aggregateHistogram.getValueAtPercentile(75);
            p80 = (int) aggregateHistogram.getValueAtPercentile(80);
            p90 = (int) aggregateHistogram.getValueAtPercentile(90);
            p95 = (int) aggregateHistogram.getValueAtPercentile(95);
            p99 = (int) aggregateHistogram.getValueAtPercentile(99);
            p995 = (int) aggregateHistogram.getValueAtPercentile(99.5);
            p999 = (int) aggregateHistogram.getValueAtPercentile(99.9);
            p9995 = (int) aggregateHistogram.getValueAtPercentile(99.95);
            p9999 = (int) aggregateHistogram.getValueAtPercentile(99.99);
            p100 = (int) aggregateHistogram.getValueAtPercentile(100);
        }

        /* package for testing */ PercentileSnapshot(int... data) {

            aggregateHistogram = new IntCountsHistogram(2);
            for (int d: data) {
                aggregateHistogram.recordValue(d);
            }
            mean = (int) aggregateHistogram.getMean();
            count = (int) aggregateHistogram.getTotalCount();
            p0 = (int) aggregateHistogram.getValueAtPercentile(0);
            p5 = (int) aggregateHistogram.getValueAtPercentile(5);
            p10 = (int) aggregateHistogram.getValueAtPercentile(10);
            p20 = (int) aggregateHistogram.getValueAtPercentile(20);
            p25 = (int) aggregateHistogram.getValueAtPercentile(25);
            p30 = (int) aggregateHistogram.getValueAtPercentile(30);
            p40 = (int) aggregateHistogram.getValueAtPercentile(40);
            p50 = (int) aggregateHistogram.getValueAtPercentile(50);
            p60 = (int) aggregateHistogram.getValueAtPercentile(60);
            p70 = (int) aggregateHistogram.getValueAtPercentile(70);
            p75 = (int) aggregateHistogram.getValueAtPercentile(75);
            p80 = (int) aggregateHistogram.getValueAtPercentile(80);
            p90 = (int) aggregateHistogram.getValueAtPercentile(90);
            p95 = (int) aggregateHistogram.getValueAtPercentile(95);
            p99 = (int) aggregateHistogram.getValueAtPercentile(99);
            p995 = (int) aggregateHistogram.getValueAtPercentile(99.5);
            p999 = (int) aggregateHistogram.getValueAtPercentile(99.9);
            p9995 = (int) aggregateHistogram.getValueAtPercentile(99.95);
            p9999 = (int) aggregateHistogram.getValueAtPercentile(99.99);
            p100 = (int) aggregateHistogram.getValueAtPercentile(100);
        }

        /* package for testing */ int getMean() {
            return mean;
        }

        /**
         * Provides percentile computation.
         */
        public int getPercentile(double percentile) {
            int permyriad = (int) (percentile * 100);
            switch(permyriad) {
                case 0   : return p0;
                case 500 : return p5;
                case 1000: return p10;
                case 2000: return p20;
                case 2500: return p25;
                case 3000: return p30;
                case 4000: return p40;
                case 5000: return p50;
                case 6000: return p60;
                case 7000: return p70;
                case 7500: return p75;
                case 8000: return p80;
                case 9000: return p90;
                case 9500: return p95;
                case 9900: return p99;
                case 9950: return p995;
                case 9990: return p999;
                case 9995: return p9995;
                case 9999: return p9999;
                case 10000: return p100;
                default: return getArbitraryPercentile(percentile);
            }
        }

        private synchronized int getArbitraryPercentile(double percentile) {
            return (int) aggregateHistogram.getValueAtPercentile(percentile);
        }

        public int getCount() {
            return count;
        }
    }
}
