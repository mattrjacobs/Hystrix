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
package com.netflix.hystrix.contrib.reactivesocket;

import com.netflix.hystrix.config.HystrixConfigurationStream;
import com.netflix.hystrix.contrib.reactivesocket.metrics.HystrixCollapserMetricsStream;
import com.netflix.hystrix.contrib.reactivesocket.metrics.HystrixCommandMetricsStream;
import com.netflix.hystrix.contrib.reactivesocket.metrics.HystrixThreadPoolMetricsStream;
import com.netflix.hystrix.contrib.reactivesocket.requests.HystrixRequestEventsStream;
import com.netflix.hystrix.contrib.reactivesocket.serialize.SerialHystrixConfiguration;
import com.netflix.hystrix.contrib.reactivesocket.serialize.SerialHystrixMetric;
import com.netflix.hystrix.contrib.reactivesocket.serialize.SerialHystrixUtilization;
import com.netflix.hystrix.metric.sample.HystrixUtilizationStream;
import io.reactivesocket.Payload;
import rx.Observable;

import java.util.Arrays;

public enum EventStreamEnum implements TimedObservableSupplier<Payload> {

    CONFIG_STREAM(1) {
        @Override
        public Observable<Payload> getOnIntervalInMilliseconds(int delay) {
            return new HystrixConfigurationStream(delay)
                    .observe()
                    .doOnNext(n -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " Pushing OnNext : " + n))
                    .doOnError(ex -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " Pushing OnError : " + ex))
                    .doOnCompleted(() -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " Pushing OnCompleted"))
                    .doOnSubscribe(() -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnSubscribe"))
                    .doOnUnsubscribe(() -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnUnsubscribe"))
                    .map(SerialHystrixConfiguration::toBytes)
                    .map(SerialHystrixMetric::toPayload);
        }
    },
    REQUEST_EVENT_STREAM(2) {
        @Override
        public Observable<Payload> getOnIntervalInMilliseconds(int delay) {
            return HystrixRequestEventsStream.getInstance().get();
        }
    },
    UTILIZATION_STREAM(3) {
        @Override
        public Observable<Payload> getOnIntervalInMilliseconds(int delay) {
            return new HystrixUtilizationStream(delay)
                    .observe()
                    .doOnNext(n -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " Pushing OnNext : " + n))
                    .doOnError(ex -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " Pushing OnError : " + ex))
                    .doOnCompleted(() -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " Pushing OnCompleted"))
                    .doOnSubscribe(() -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnSubscribe"))
                    .doOnUnsubscribe(() -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnUnsubscribe"))
                    .map(SerialHystrixUtilization::toBytes)
                    .map(SerialHystrixMetric::toPayload);
        }
    },
    GENERAL_DASHBOARD_STREAM(4) {
        @Override
        public Observable<Payload> getOnIntervalInMilliseconds(int delay) {
            return Observable.merge(
                    HystrixCommandMetricsStream.getInstance().get(),
                    HystrixThreadPoolMetricsStream.getInstance().get(),
                    HystrixCollapserMetricsStream.getInstance().get());
        }
    };

    private final int typeId;

    EventStreamEnum(int typeId) {
        this.typeId = typeId;
    }

    public static EventStreamEnum findByTypeId(int typeId) {
        return Arrays
            .asList(EventStreamEnum.values())
            .stream()
            .filter(t -> t.typeId == typeId)
            .findAny()
            .orElseThrow(() -> new IllegalStateException("no type id found for id => " + typeId));
    }

    public int getTypeId() {
        return typeId;
    }
}
