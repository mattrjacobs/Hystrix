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
package com.netflix.hystrix.contrib.reactivesocket.sample;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.contrib.reactivesocket.metrics.HystrixCommandMetricsStream;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rroeser on 5/19/16.
 */
public class HystrixConfigStreamTest {
    @Test
    public void test() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
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
            TestCommand test = new TestCommand();

            test.execute();
        }

        latch.await();
    }

    class TestCommand extends HystrixCommand<Boolean> {
        protected TestCommand() {
            super(HystrixCommandGroupKey.Factory.asKey("HystrixMetricsPollerTest"));
        }

        @Override
        protected Boolean run() throws Exception {
            System.out.println("IM A HYSTRIX COMMAND!!!!!");
            return true;
        }
    }
}