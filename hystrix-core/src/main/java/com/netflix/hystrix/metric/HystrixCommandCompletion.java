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
package com.netflix.hystrix.metric;

import com.netflix.hystrix.ExecutionResult;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixEventType;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.state.EventCounts;
import com.netflix.hystrix.state.State;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Data class which gets fed into event stream when a command completes (with any of the outcomes in {@link HystrixEventType}).
 */
public class HystrixCommandCompletion extends HystrixCommandEvent {
    private final EventCounts eventCounts;
    private final ExecutionResult.EventCounts deprecatedEventCounts;
    private final boolean isExecutedInThread;
    private final boolean didExecutionOccur;
    private final long executionLatency;
    private final long totalLatency;

    private final static HystrixEventType[] ALL_EVENT_TYPES = HystrixEventType.values();

    HystrixCommandCompletion(ExecutionResult executionResult, HystrixCommandKey commandKey,
                             HystrixThreadPoolKey threadPoolKey, HystrixRequestContext requestContext) {
        super(commandKey, threadPoolKey);
        this.deprecatedEventCounts = executionResult.getEventCounts();
        this.eventCounts = null;
        this.isExecutedInThread = executionResult.isExecutedInThread();
        this.didExecutionOccur = executionResult.executionOccurred();
        this.executionLatency = executionResult.getExecutionLatency();
        this.totalLatency = executionResult.getUserThreadLatency();
    }

    HystrixCommandCompletion(State<?> state, HystrixCommandKey commandKey, HystrixThreadPoolKey threadPoolKey) {
        super(commandKey, threadPoolKey);
        this.eventCounts = state.getEventCounts();
        this.deprecatedEventCounts = null;
        this.isExecutedInThread = state.isExecutedInThread();
        this.didExecutionOccur = state.didExecutionOccur();
        this.executionLatency = state.getExecutionLatency();
        this.totalLatency = state.getCommandLatency();
    }

    public static HystrixCommandCompletion from(ExecutionResult executionResult, HystrixCommandKey commandKey, HystrixThreadPoolKey threadPoolKey) {
        return from(executionResult, commandKey, threadPoolKey, HystrixRequestContext.getContextForCurrentThread());
    }

    public static HystrixCommandCompletion from(ExecutionResult executionResult, HystrixCommandKey commandKey, HystrixThreadPoolKey threadPoolKey, HystrixRequestContext requestContext) {
        return new HystrixCommandCompletion(executionResult, commandKey, threadPoolKey, requestContext);
    }

    public static HystrixCommandCompletion from(State<?> state, HystrixCommandKey commandKey, HystrixThreadPoolKey threadPoolKey) {
        return new HystrixCommandCompletion(state, commandKey, threadPoolKey);
    }

    @Override
    public boolean isResponseThreadPoolRejected() {
        return getEventCounts().contains(HystrixEventType.THREAD_POOL_REJECTED);
    }

    @Override
    public boolean isExecutionStart() {
        return false;
    }

    @Override
    public boolean isExecutedInThread() {
        return isExecutedInThread;
    }

    @Override
    public boolean isCommandCompletion() {
        return true;
    }

    //TODO Deprecate this
    public HystrixRequestContext getRequestContext() {
        return null;
    }

    public ExecutionResult.EventCounts getEventCounts() {
        if (deprecatedEventCounts != null) {
            return deprecatedEventCounts;
        } else {
            return new ExecutionResult.EventCounts(eventCounts.getBitSet(), eventCounts.getCount(HystrixEventType.EMIT), eventCounts.getCount(HystrixEventType.FALLBACK_EMIT), eventCounts.getCount(HystrixEventType.COLLAPSED));
        }
    }

    public long getExecutionLatency() {
        return executionLatency;
    }

    public long getTotalLatency() {
        return totalLatency;
    }

    @Override
    public boolean didCommandExecute() {
        return didExecutionOccur;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        List<HystrixEventType> foundEventTypes = new ArrayList<HystrixEventType>();

        sb.append(getCommandKey().name()).append("[");
        for (HystrixEventType eventType: ALL_EVENT_TYPES) {
            if (eventCounts.contains(eventType)) {
                foundEventTypes.add(eventType);
            }
        }
        int i = 0;
        for (HystrixEventType eventType: foundEventTypes) {
            sb.append(eventType.name());
            int eventCount = eventCounts.getCount(eventType);
            if (eventCount > 1) {
                sb.append("x").append(eventCount);

            }
            if (i < foundEventTypes.size() - 1) {
                sb.append(", ");
            }
            i++;
        }
        sb.append("][").append(getExecutionLatency()).append(" ms]");
        return sb.toString();
    }
}
