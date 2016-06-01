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
package com.netflix.hystrix.examples.reactivesocket;

import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.contrib.reactivesocket.EventStreamEnum;
import com.netflix.hystrix.contrib.reactivesocket.client.HystrixMetricsReactiveSocketClient;
import com.netflix.hystrix.contrib.reactivesocket.sample.HystrixUtilizationStream;
import com.netflix.hystrix.metric.sample.HystrixCommandUtilization;
import com.netflix.hystrix.metric.sample.HystrixUtilization;
import io.netty.channel.nio.NioEventLoopGroup;
import io.reactivesocket.Payload;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;
import rx.Subscription;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class HystrixMetricsReactiveSocketClientRunner {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting HystrixMetricsReactiveSocketClient...");

        HystrixMetricsReactiveSocketClient client = new HystrixMetricsReactiveSocketClient("127.0.0.1", 8025, new NioEventLoopGroup());
        client.startAndWait();

        //Publisher<Payload> publisher = client.requestResponse(EventStreamEnum.UTILIZATION_EVENT_STREAM);
        Publisher<Payload> publisher = client.requestStream(EventStreamEnum.UTILIZATION_EVENT_STREAM, 10);
        //Publisher<Payload> publisher = client.requestSubscription(EventStreamEnum.UTILIZATION_EVENT_STREAM);
        Observable<Payload> o = RxReactiveStreams.toObservable(publisher);

        final CountDownLatch latch = new CountDownLatch(1);

        Subscription s = o.subscribe(new Subscriber<Payload>() {
            @Override
            public void onCompleted() {
                System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnCompleted");
                latch.countDown();
            }

            @Override
            public void onError(Throwable e) {
                System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnError : " + e);
                e.printStackTrace();
                latch.countDown();
            }

            @Override
            public void onNext(Payload payload) {
                HystrixUtilization utilization = HystrixUtilizationStream.getInstance().fromByteBuffer(payload.getData());
                Map<HystrixCommandKey, HystrixCommandUtilization> commandMap = utilization.getCommandUtilizationMap();
                StringBuilder bldr = new StringBuilder();
                bldr.append("Command[");
                for (Map.Entry<HystrixCommandKey, HystrixCommandUtilization> entry: commandMap.entrySet()) {
                    bldr.append(entry.getKey().name()).append(" -> ").append(entry.getValue().getConcurrentCommandCount()).append(", ");
                }
                bldr.append("]");
                System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnNext : " + bldr.toString());
            }
        });

        latch.await(10000, TimeUnit.MILLISECONDS);
        s.unsubscribe();
        System.exit(0);
    }
}
