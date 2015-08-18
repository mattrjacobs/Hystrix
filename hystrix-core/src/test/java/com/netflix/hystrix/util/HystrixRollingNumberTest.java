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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.netflix.hystrix.util.time.HystrixMockedTime;
import org.junit.Test;

public class HystrixRollingNumberTest {

    @Test
    public void testCreatesBuckets() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);
            // confirm the initial settings
            assertEquals(200, counter.timeInMilliseconds);
            assertEquals(10, counter.numberOfBuckets);
            assertEquals(20, counter.bucketSizeInMilliseconds);

            // we start out with 0 buckets in the queue
            assertEquals(0, counter.buckets.size());

            // add a success in each interval which should result in all 10 buckets being created with 1 success in each
            for (int i = 0; i < counter.numberOfBuckets; i++) {
                counter.increment(HystrixRollingNumberEvent.SUCCESS);
                time.increment(counter.bucketSizeInMilliseconds);
            }

            // confirm we have all 10 buckets
            assertEquals(10, counter.buckets.size());

            // add 1 more and we should still only have 10 buckets since that's the max
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            assertEquals(10, counter.buckets.size());

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testResetBuckets() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            // we start out with 0 buckets in the queue
            assertEquals(0, counter.buckets.size());

            // add 1
            counter.increment(HystrixRollingNumberEvent.SUCCESS);

            // confirm we have 1 bucket
            assertEquals(1, counter.buckets.size());

            // confirm we still have 1 bucket
            assertEquals(1, counter.buckets.size());

            // add 1
            counter.increment(HystrixRollingNumberEvent.SUCCESS);

            // we should now have a single bucket with no values in it instead of 2 or more buckets
            assertEquals(1, counter.buckets.size());

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testEmptyBucketsFillIn() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            // add 1
            counter.increment(HystrixRollingNumberEvent.SUCCESS);

            // we should have 1 bucket
            assertEquals(1, counter.buckets.size());

            // wait past 3 bucket time periods (the 1st bucket then 2 empty ones)
            time.increment(counter.bucketSizeInMilliseconds * 3);

            // add another
            counter.increment(HystrixRollingNumberEvent.SUCCESS);

            // we should have 4 (1 + 2 empty + 1 new one) buckets
            assertEquals(4, counter.buckets.size());

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testIncrementInSingleBucket() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            // increment
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.FAILURE);
            counter.increment(HystrixRollingNumberEvent.FAILURE);
            counter.increment(HystrixRollingNumberEvent.TIMEOUT);

            // we should have 1 bucket
            assertEquals(1, counter.buckets.size());

            // the count should be 4 - but these are in the write-only bucket.  so assert they are 0 now and show up after the next bucket rolls in
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.FAILURE));
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.TIMEOUT));

            time.increment(counter.bucketSizeInMilliseconds);

            assertEquals(4, counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(2, counter.getRollingSum(HystrixRollingNumberEvent.FAILURE));
            assertEquals(1, counter.getRollingSum(HystrixRollingNumberEvent.TIMEOUT));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testTimeout() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            // increment
            counter.increment(HystrixRollingNumberEvent.TIMEOUT);

            // we should have 1 bucket
            assertEquals(1, counter.buckets.size());

            // the count should be 0 until we roll the write-only bucket nto the read-only snapshot, and 1 after
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.TIMEOUT));
            assertEquals(0, counter.getCumulativeSum(HystrixRollingNumberEvent.TIMEOUT));
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(1, counter.getRollingSum(HystrixRollingNumberEvent.TIMEOUT));
            assertEquals(1, counter.getCumulativeSum(HystrixRollingNumberEvent.TIMEOUT));

            // sleep to get to a new bucket
            time.increment(counter.bucketSizeInMilliseconds * 2);

            // increment again in latest bucket
            counter.increment(HystrixRollingNumberEvent.TIMEOUT);

            // we should have 4 buckets
            assertEquals(4, counter.buckets.size());

            // the total counts should show up after the write-only bucket becomes readable
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(2, counter.getRollingSum(HystrixRollingNumberEvent.TIMEOUT));
            assertEquals(2, counter.getCumulativeSum(HystrixRollingNumberEvent.TIMEOUT));

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testShortCircuited() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            // increment
            counter.increment(HystrixRollingNumberEvent.SHORT_CIRCUITED);

            // we should have 1 bucket
            assertEquals(1, counter.buckets.size());

            // the count should be 0 until we roll the write-only bucket into the read-only snapshot, and 1 after
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.SHORT_CIRCUITED));
            assertEquals(0, counter.getCumulativeSum(HystrixRollingNumberEvent.SHORT_CIRCUITED));
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(1, counter.getRollingSum(HystrixRollingNumberEvent.SHORT_CIRCUITED));
            assertEquals(1, counter.getCumulativeSum(HystrixRollingNumberEvent.SHORT_CIRCUITED));

            // sleep to get to a new bucket
            time.increment(counter.bucketSizeInMilliseconds * 2);

            // increment again in latest bucket
            counter.increment(HystrixRollingNumberEvent.SHORT_CIRCUITED);

            // we should have 4 buckets
            assertEquals(4, counter.buckets.size());

            // the total counts should show up after the write-only bucket becomes readable
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(2, counter.getRollingSum(HystrixRollingNumberEvent.SHORT_CIRCUITED));
            assertEquals(2, counter.getCumulativeSum(HystrixRollingNumberEvent.SHORT_CIRCUITED));

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testThreadPoolRejection() {
        testCounterType(HystrixRollingNumberEvent.THREAD_POOL_REJECTED);
    }

    @Test
    public void testFallbackSuccess() {
        testCounterType(HystrixRollingNumberEvent.FALLBACK_SUCCESS);
    }

    @Test
    public void testFallbackFailure() {
        testCounterType(HystrixRollingNumberEvent.FALLBACK_FAILURE);
    }

    @Test
    public void testExceptionThrow() {
        testCounterType(HystrixRollingNumberEvent.EXCEPTION_THROWN);
    }

    @Test
    public void testBadRequest() {
        testCounterType(HystrixRollingNumberEvent.BAD_REQUEST);
    }

    private void testCounterType(HystrixRollingNumberEvent type) {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            // increment
            counter.increment(type);

            // we should have 1 bucket
            assertEquals(1, counter.buckets.size());

            // the count should be 0 until we roll the write-only bucket into the read-only snapshot, and 1 after
            assertEquals(0, counter.getRollingSum(type));
            assertEquals(0, counter.getCumulativeSum(type));
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(1, counter.getRollingSum(type));
            assertEquals(1, counter.getCumulativeSum(type));

            // sleep to get to a new bucket
            time.increment(counter.bucketSizeInMilliseconds * 2);

            // increment again in latest bucket
            counter.increment(type);

            // we should have 4 buckets
            assertEquals(4, counter.buckets.size());

            // the total counts should show up after the write-only bucket becomes readable
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(2, counter.getRollingSum(type));
            assertEquals(2, counter.getCumulativeSum(type));
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testIncrementInMultipleBuckets() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            // increment
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.FAILURE);
            counter.increment(HystrixRollingNumberEvent.FAILURE);
            counter.increment(HystrixRollingNumberEvent.TIMEOUT);
            counter.increment(HystrixRollingNumberEvent.TIMEOUT);
            counter.increment(HystrixRollingNumberEvent.SHORT_CIRCUITED);

            // sleep to get to a new bucket
            time.increment(counter.bucketSizeInMilliseconds * 3);

            // the total counts
            assertEquals(4, counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(2, counter.getRollingSum(HystrixRollingNumberEvent.FAILURE));
            assertEquals(2, counter.getRollingSum(HystrixRollingNumberEvent.TIMEOUT));
            assertEquals(1, counter.getRollingSum(HystrixRollingNumberEvent.SHORT_CIRCUITED));

            // increment
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.FAILURE);
            counter.increment(HystrixRollingNumberEvent.FAILURE);
            counter.increment(HystrixRollingNumberEvent.FAILURE);
            counter.increment(HystrixRollingNumberEvent.TIMEOUT);
            counter.increment(HystrixRollingNumberEvent.SHORT_CIRCUITED);

            // we should have 4 buckets
            assertEquals(4, counter.buckets.size());

            //increment the time to make the write-only bucket show up in the read-only data
            time.increment(counter.bucketSizeInMilliseconds);

            // the total counts
            assertEquals(6, counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(5, counter.getRollingSum(HystrixRollingNumberEvent.FAILURE));
            assertEquals(3, counter.getRollingSum(HystrixRollingNumberEvent.TIMEOUT));
            assertEquals(2, counter.getRollingSum(HystrixRollingNumberEvent.SHORT_CIRCUITED));

            // wait until window passes
            time.increment(counter.timeInMilliseconds);

            // increment
            counter.increment(HystrixRollingNumberEvent.SUCCESS);

            // the total counts should initially all be 0, as all read-only data has aged-out, and write-only bucket can not be read yet
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.FAILURE));
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.TIMEOUT));

            // after time passes, write-only bucket can be read, and the single SUCCESS event is visible
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(1, counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.FAILURE));
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.TIMEOUT));

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testCounterRetrievalRefreshesBuckets() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            // increment
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.SUCCESS);
            counter.increment(HystrixRollingNumberEvent.FAILURE);
            counter.increment(HystrixRollingNumberEvent.FAILURE);

            // sleep to get to a new bucket
            time.increment(counter.bucketSizeInMilliseconds * 3);

            // we should have 1 bucket since nothing has triggered the update of buckets in the elapsed time
            assertEquals(1, counter.buckets.size());

            // the total counts
            assertEquals(4, counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(2, counter.getRollingSum(HystrixRollingNumberEvent.FAILURE));

            // we should have 4 buckets as the counter 'gets' should have triggered the buckets being created to fill in time
            assertEquals(4, counter.buckets.size());

            // wait until window passes
            time.increment(counter.timeInMilliseconds);

            // the total counts should all be 0 (and the buckets cleared by the get, not only increment)
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.FAILURE));

            // increment
            counter.increment(HystrixRollingNumberEvent.SUCCESS);

            // the total counts should now be empty as the read-only data is empty and the latest write is in the write-only data
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.FAILURE));
            assertEquals(4, counter.getCumulativeSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(2, counter.getCumulativeSum(HystrixRollingNumberEvent.FAILURE));

            //but it shows up in read-only as soon as bucket rolls
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(1, counter.getRollingSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(0, counter.getRollingSum(HystrixRollingNumberEvent.FAILURE));
            assertEquals(5, counter.getCumulativeSum(HystrixRollingNumberEvent.SUCCESS));
            assertEquals(2, counter.getCumulativeSum(HystrixRollingNumberEvent.FAILURE));

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testUpdateMax1() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            // increment
            counter.updateRollingMax(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE, 10);

            // we should have 1 bucket
            assertEquals(1, counter.buckets.size());

            // the max should be updated only in the write-only bucket
            assertEquals(0, counter.getRollingMaxValue(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE));

            // once we roll that into the read-only data, it should be the new max
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(10, counter.getRollingMaxValue(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE));

            // sleep to get to a new bucket
            time.increment(counter.bucketSizeInMilliseconds * 2);

            // increment again in latest bucket
            counter.updateRollingMax(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE, 20);

            // we should have 4 buckets
            assertEquals(4, counter.buckets.size());


            // the new max should be updated only in the write-only bucket
            assertEquals(10, counter.getRollingMaxValue(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE));

            // once we roll that into the read-only data, it should be the new max
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(20, counter.getRollingMaxValue(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE));

            // counts per bucket
            long values[] = counter.getValues(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE);
            assertEquals(10, values[0]); // oldest bucket
            assertEquals(0, values[1]);
            assertEquals(0, values[2]);
            assertEquals(20, values[3]); // latest bucket

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testUpdateMax2() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            // increment
            counter.updateRollingMax(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE, 10);
            counter.updateRollingMax(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE, 30);
            counter.updateRollingMax(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE, 20);

            // we should have 1 bucket
            assertEquals(1, counter.buckets.size());

            // the count should be 0 since all writes are not visible yet
            assertEquals(0, counter.getRollingMaxValue(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE));

            // and they should become visible once the bucket show up in read-only data
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(30, counter.getRollingMaxValue(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE));

            // sleep to get to a new bucket
            time.increment(counter.bucketSizeInMilliseconds * 2);

            counter.updateRollingMax(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE, 30);
            counter.updateRollingMax(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE, 30);
            counter.updateRollingMax(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE, 50);

            // we should have 4 buckets
            assertEquals(4, counter.buckets.size());

            // the count should still be 30, as the write of 50 is in write-only space for now
            assertEquals(30, counter.getRollingMaxValue(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE));

            //and when it rolls into read-only space, it becomes the new rolling max
            time.increment(counter.bucketSizeInMilliseconds);
            assertEquals(50, counter.getRollingMaxValue(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE));

            // values per bucket
            long values[] = counter.getValues(HystrixRollingNumberEvent.THREAD_MAX_ACTIVE);
            assertEquals(30, values[0]); // oldest bucket
            assertEquals(0, values[1]);
            assertEquals(0, values[2]);
            assertEquals(50, values[3]); // latest bucket

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testMaxValue() {
        HystrixMockedTime time = new HystrixMockedTime();
        try {
            HystrixRollingNumberEvent type = HystrixRollingNumberEvent.THREAD_MAX_ACTIVE;

            HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);

            counter.updateRollingMax(type, 10);

            // sleep to get to a new bucket
            time.increment(counter.bucketSizeInMilliseconds);

            counter.updateRollingMax(type, 30);

            // sleep to get to a new bucket
            time.increment(counter.bucketSizeInMilliseconds);

            counter.updateRollingMax(type, 40);

            // sleep to get to a new bucket
            time.increment(counter.bucketSizeInMilliseconds);

            counter.updateRollingMax(type, 15);

            assertEquals(40, counter.getRollingMaxValue(type));

        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception: " + e.getMessage());
        }
    }

    @Test
    public void testEmptySum() {
        HystrixMockedTime time = new HystrixMockedTime();
        HystrixRollingNumberEvent type = HystrixRollingNumberEvent.COLLAPSED;
        HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);
        assertEquals(0, counter.getRollingSum(type));
    }

    @Test
    public void testEmptyMax() {
        HystrixMockedTime time = new HystrixMockedTime();
        HystrixRollingNumberEvent type = HystrixRollingNumberEvent.THREAD_MAX_ACTIVE;
        HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);
        assertEquals(0, counter.getRollingMaxValue(type));
    }

    @Test
    public void testEmptyLatestValue() {
        HystrixMockedTime time = new HystrixMockedTime();
        HystrixRollingNumberEvent type = HystrixRollingNumberEvent.THREAD_MAX_ACTIVE;
        HystrixRollingNumber counter = new HystrixRollingNumber(time, 200, 10);
        assertEquals(0, counter.getValueOfLatestBucket(type));
    }

    @Test
    public void testRolling() {
        HystrixMockedTime time = new HystrixMockedTime();
        HystrixRollingNumberEvent type = HystrixRollingNumberEvent.THREAD_MAX_ACTIVE;
        HystrixRollingNumber counter = new HystrixRollingNumber(time, 20, 2);
        // iterate over 20 buckets on a queue sized for 2
        for (int i = 0; i < 20; i++) {
            // first bucket
            counter.getCurrentBucket();
            try {
                time.increment(counter.bucketSizeInMilliseconds);
            } catch (Exception e) {
                // ignore
            }

            assertEquals(2, counter.getValues(type).length);

            counter.getValueOfLatestBucket(type);
        }
    }

    @Test
    public void testCumulativeCounterAfterRolling() {
        HystrixMockedTime time = new HystrixMockedTime();
        HystrixRollingNumberEvent type = HystrixRollingNumberEvent.SUCCESS;
        HystrixRollingNumber counter = new HystrixRollingNumber(time, 20, 2);

        assertEquals(0, counter.getCumulativeSum(type));

        // iterate over 20 buckets on a queue sized for 2
        for (int i = 0; i < 20; i++) {
            // first bucket
            counter.increment(type);
            try {
                time.increment(counter.bucketSizeInMilliseconds);
            } catch (Exception e) {
                // ignore
            }

            assertEquals(2, counter.getValues(type).length);

            counter.getValueOfLatestBucket(type);

        }

        // cumulative count should be 20 (for the number of loops above) regardless of buckets rolling
        assertEquals(20, counter.getCumulativeSum(type));
    }

    @Test
    public void testCumulativeCounterAfterRollingAndReset() {
        HystrixMockedTime time = new HystrixMockedTime();
        HystrixRollingNumberEvent type = HystrixRollingNumberEvent.SUCCESS;
        HystrixRollingNumber counter = new HystrixRollingNumber(time, 20, 2);

        assertEquals(0, counter.getCumulativeSum(type));

        // iterate over 20 buckets on a queue sized for 2
        for (int i = 0; i < 20; i++) {
            // first bucket
            counter.increment(type);
            try {
                time.increment(counter.bucketSizeInMilliseconds);
            } catch (Exception e) {
                // ignore
            }

            assertEquals(2, counter.getValues(type).length);

            counter.getValueOfLatestBucket(type);

            if (i == 5 || i == 15) {
                // simulate a reset occurring every once in a while
                // so we ensure the absolute sum is handling it okay
                counter.reset();
            }
        }

        // cumulative count should be 20 (for the number of loops above) regardless of buckets rolling
        assertEquals(20, counter.getCumulativeSum(type));
    }

    @Test
    public void testCumulativeCounterAfterRollingAndReset2() {
        HystrixMockedTime time = new HystrixMockedTime();
        HystrixRollingNumberEvent type = HystrixRollingNumberEvent.SUCCESS;
        HystrixRollingNumber counter = new HystrixRollingNumber(time, 20, 2);

        assertEquals(0, counter.getCumulativeSum(type));

        counter.increment(type);
        counter.increment(type);
        counter.increment(type);

        // iterate over 20 buckets on a queue sized for 2
        for (int i = 0; i < 20; i++) {
            try {
                time.increment(counter.bucketSizeInMilliseconds);
            } catch (Exception e) {
                // ignore
            }

            if (i == 5 || i == 15) {
                // simulate a reset occurring every once in a while
                // so we ensure the absolute sum is handling it okay
                counter.reset();
            }
        }

        // no increments during the loop, just some before and after
        counter.increment(type);
        counter.increment(type);

        // cumulative count should be 5 regardless of buckets rolling, after we let the writes show up in read-only data
        time.increment(counter.bucketSizeInMilliseconds);
        assertEquals(5, counter.getCumulativeSum(type));
    }

    @Test
    public void testCumulativeCounterAfterRollingAndReset3() {
        HystrixMockedTime time = new HystrixMockedTime();
        HystrixRollingNumberEvent type = HystrixRollingNumberEvent.SUCCESS;
        HystrixRollingNumber counter = new HystrixRollingNumber(time, 20, 2);

        assertEquals(0, counter.getCumulativeSum(type));

        counter.increment(type);
        counter.increment(type);
        counter.increment(type);

        // iterate over 20 buckets on a queue sized for 2
        for (int i = 0; i < 20; i++) {
            try {
                time.increment(counter.bucketSizeInMilliseconds);
            } catch (Exception e) {
                // ignore
            }
        }

        // since we are rolling over the buckets it should reset naturally

        // no increments during the loop, just some before and after
        counter.increment(type);
        counter.increment(type);

        // cumulative count should be 5 regardless of buckets rolling, after we let the writes show up in read-only data
        time.increment(counter.bucketSizeInMilliseconds);
        assertEquals(5, counter.getCumulativeSum(type));
    }
}
