/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.hystrix.contrib.reactivesocket.metrics;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rroeser on 5/19/16.
 */
public class HystrixCommandMetricsStreamTest {
    @Test
    public void test() throws Exception {
        CountDownLatch latch = new CountDownLatch(23);
        HystrixCommandMetricsStream
            .getInstance()
            .get()
            .subscribe(payload -> {
                ByteBuffer data = payload.getData();
                String s = new String(data.array());

                System.out.println(s);
                latch.countDown();
            });

        for (int i = 0; i < 20; i++) {
            TestCommand test = new TestCommand(latch);

            test.execute();
        }

        latch.await();
    }

    class TestCommand extends HystrixCommand<Boolean> {
        final CountDownLatch latch;
        protected TestCommand(CountDownLatch latch) {
            super(HystrixCommandGroupKey.Factory.asKey("HystrixMetricsPollerTest"));
            this.latch = latch;
        }

        @Override
        protected Boolean run() throws Exception {
            latch.countDown();
            return true;
        }
    }

}