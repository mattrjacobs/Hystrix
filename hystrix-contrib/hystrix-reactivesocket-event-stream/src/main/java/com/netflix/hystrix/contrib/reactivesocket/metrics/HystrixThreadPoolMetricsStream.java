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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.hystrix.HystrixEventType;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.HystrixThreadPoolMetrics;
import com.netflix.hystrix.contrib.reactivesocket.StreamingSupplier;
import io.reactivesocket.Payload;
import org.agrona.LangUtil;
import rx.Observable;
import rx.functions.Func0;

import java.io.ByteArrayOutputStream;
import java.util.stream.Stream;

public class HystrixThreadPoolMetricsStream extends StreamingSupplier<HystrixThreadPoolMetrics> {
    private static final HystrixThreadPoolMetricsStream INSTANCE = new HystrixThreadPoolMetricsStream();

    private HystrixThreadPoolMetricsStream() {
        super();
    }

    public static HystrixThreadPoolMetricsStream getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean filter(HystrixThreadPoolMetrics threadPoolMetrics) {
        return threadPoolMetrics.getCurrentCompletedTaskCount().intValue() > 0;
    }

    @Override
    public Observable<Payload> get() {
        return super.get();
    }

    @Override
    public byte[] toBytes(JsonNode object) {
        return new byte[0];
    }

    @Override
    protected byte[] getPayloadData(HystrixThreadPoolMetrics threadPoolMetrics) {
        byte[] retVal = null;

        try {
            HystrixThreadPoolKey key = threadPoolMetrics.getThreadPoolKey();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            JsonGenerator json = cborFactory.createGenerator(bos);
            json.writeStartObject();

            json.writeStringField("type", "HystrixThreadPool");
            json.writeStringField("name", key.name());
            json.writeNumberField("currentTime", System.currentTimeMillis());

            json.writeNumberField("currentActiveCount", threadPoolMetrics.getCurrentActiveCount().intValue());
            json.writeNumberField("currentCompletedTaskCount", threadPoolMetrics.getCurrentCompletedTaskCount().longValue());
            json.writeNumberField("currentCorePoolSize", threadPoolMetrics.getCurrentCorePoolSize().intValue());
            json.writeNumberField("currentLargestPoolSize", threadPoolMetrics.getCurrentLargestPoolSize().intValue());
            json.writeNumberField("currentMaximumPoolSize", threadPoolMetrics.getCurrentMaximumPoolSize().intValue());
            json.writeNumberField("currentPoolSize", threadPoolMetrics.getCurrentPoolSize().intValue());
            json.writeNumberField("currentQueueSize", threadPoolMetrics.getCurrentQueueSize().intValue());
            json.writeNumberField("currentTaskCount", threadPoolMetrics.getCurrentTaskCount().longValue());
            safelyWriteNumberField(json, "rollingCountThreadsExecuted", () -> threadPoolMetrics.getRollingCount(HystrixEventType.ThreadPool.EXECUTED));
            json.writeNumberField("rollingMaxActiveThreads", threadPoolMetrics.getRollingMaxActiveThreads());
            safelyWriteNumberField(json, "rollingCountCommandRejections", () -> threadPoolMetrics.getRollingCount(HystrixEventType.ThreadPool.REJECTED));

            json.writeNumberField("propertyValue_queueSizeRejectionThreshold", threadPoolMetrics.getProperties().queueSizeRejectionThreshold().get());
            json.writeNumberField("propertyValue_metricsRollingStatisticalWindowInMilliseconds", threadPoolMetrics.getProperties().metricsRollingStatisticalWindowInMilliseconds().get());

            json.writeNumberField("reportingHosts", 1); // this will get summed across all instances in a cluster

            json.writeEndObject();
            json.close();

            retVal = bos.toByteArray();

        } catch (Exception e) {
            LangUtil.rethrowUnchecked(e);
        }

        return retVal;
    }

    @Override
    protected Stream<HystrixThreadPoolMetrics> getStream() {
        return HystrixThreadPoolMetrics.getInstances().stream();
    }
}