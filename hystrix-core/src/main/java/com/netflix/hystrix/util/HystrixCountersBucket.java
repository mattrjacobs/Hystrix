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

import org.HdrHistogram.Histogram;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class HystrixCountersBucket extends HystrixMetricsBucket {
    //read-only incremental counter for this bucket
    Histogram histogram;
    private ConcurrentMap<HystrixRollingNumberEvent, Histogram> distributions = new ConcurrentHashMap<HystrixRollingNumberEvent, Histogram>();

    HystrixCountersBucket(long startTime) {
        super(startTime);
        for (HystrixRollingNumberEvent eventType: HystrixRollingNumberEvent.values()) {
            distributions.put(eventType, new Histogram(1));
        }
    }

    void setReadOnlyDistribution(HystrixRollingNumberEvent eventType, Histogram h) {
        distributions.put(eventType, h);
    }

    Histogram getReadOnlyDistribution(HystrixRollingNumberEvent eventType) {
        return distributions.get(eventType);
    }
}
