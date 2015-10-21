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
package com.netflix.hystrix;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Random;

import com.netflix.hystrix.exception.HystrixBadRequestException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.netflix.hystrix.HystrixCircuitBreaker.HystrixCircuitBreakerImpl;
import com.netflix.hystrix.strategy.HystrixPlugins;
import com.netflix.hystrix.strategy.eventnotifier.HystrixEventNotifierDefault;
import com.netflix.hystrix.strategy.executionhook.HystrixCommandExecutionHook;
import com.netflix.hystrix.util.HystrixRollingNumberEvent;
import rx.Observable;

public class HystrixCircuitBreakerTest {

    @Before
    public void init() {
        for (HystrixCommandMetrics metricsInstance: HystrixCommandMetrics.getInstances()) {
            metricsInstance.resetStream();
        }

        HystrixCommandMetrics.reset();
        HystrixCircuitBreaker.Factory.reset();
        Hystrix.reset();
    }

    /**
     * A simple circuit breaker intended for unit testing of the {@link HystrixCommand} object, NOT production use.
     * <p>
     * This uses simple logic to 'trip' the circuit after 3 subsequent failures and doesn't recover.
     */
    public static class TestCircuitBreaker implements HystrixCircuitBreaker {

        final HystrixCommandMetrics metrics;
        private boolean forceShortCircuit = false;

        public TestCircuitBreaker() {
            this.metrics = getMetrics(HystrixCommandPropertiesTest.getUnitTestPropertiesSetter());
            forceShortCircuit = false;
        }

        public TestCircuitBreaker setForceShortCircuit(boolean value) {
            this.forceShortCircuit = value;
            return this;
        }

        @Override
        public boolean isOpen() {
            if (forceShortCircuit) {
                return true;
            } else {
                return metrics.getCumulativeCount(HystrixRollingNumberEvent.FAILURE) >= 3;
            }
        }

        @Override
        public void markSuccess() {
            // we don't need to do anything since we're going to permanently trip the circuit
        }

        @Override
        public boolean allowRequest() {
            return !isOpen();
        }

    }

    /**
     * Test that if all 'marks' are successes during the test window that it does NOT trip the circuit.
     * Test that if all 'marks' are failures during the test window that it trips the circuit.
     */
    @Test
    public void testTripCircuit() {
        try {
            HystrixCommand<Boolean> cmd1 = new SuccessCommand(1);
            HystrixCommand<Boolean> cmd2 = new SuccessCommand(1);
            HystrixCommand<Boolean> cmd3 = new SuccessCommand(1);
            HystrixCommand<Boolean> cmd4 = new SuccessCommand(1);

            HystrixCircuitBreaker cb = cmd1.circuitBreaker;

            cmd1.execute();
            cmd2.execute();
            cmd3.execute();
            cmd4.execute();

            // this should still allow requests as everything has been successful
            Thread.sleep(100);
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());

            // fail
            HystrixCommand<Boolean> cmd5 = new FailureCommand(1);
            HystrixCommand<Boolean> cmd6 = new FailureCommand(1);
            HystrixCommand<Boolean> cmd7 = new FailureCommand(1);
            HystrixCommand<Boolean> cmd8 = new FailureCommand(1);
            cmd5.execute();
            cmd6.execute();
            cmd7.execute();
            cmd8.execute();

            // everything has failed in the test window so we should return false now
            Thread.sleep(100);
            assertFalse(cb.allowRequest());
            assertTrue(cb.isOpen());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred: " + e.getMessage());
        }
    }

    /**
     * Test that if the % of failures is higher than the threshold that the circuit trips.
     */
    @Test
    public void testTripCircuitOnFailuresAboveThreshold() {
        try {
            HystrixCommand<Boolean> cmd1 = new SuccessCommand(60);
            HystrixCircuitBreaker cb = cmd1.circuitBreaker;

            // this should start as allowing requests
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());

            // success with high latency
            cmd1.execute();
            HystrixCommand<Boolean> cmd2 = new SuccessCommand(1);
            cmd2.execute();
            HystrixCommand<Boolean> cmd3 = new FailureCommand(1);
            cmd3.execute();
            HystrixCommand<Boolean> cmd4 = new SuccessCommand(1);
            cmd4.execute();
            HystrixCommand<Boolean> cmd5 = new FailureCommand(1);
            cmd5.execute();
            HystrixCommand<Boolean> cmd6 = new SuccessCommand(1);
            cmd6.execute();
            HystrixCommand<Boolean> cmd7 = new FailureCommand(1);
            cmd7.execute();
            HystrixCommand<Boolean> cmd8 = new FailureCommand(1);
            cmd8.execute();

            // this should trip the circuit as the error percentage is above the threshold
            Thread.sleep(100);
            assertFalse(cb.allowRequest());
            assertTrue(cb.isOpen());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred: " + e.getMessage());
        }
    }

    /**
     * Test that if the % of failures is higher than the threshold that the circuit trips.
     */
    @Test
    public void testCircuitDoesNotTripOnFailuresBelowThreshold() {
        try {
            HystrixCommand<Boolean> cmd1 = new SuccessCommand(60);
            HystrixCircuitBreaker cb = cmd1.circuitBreaker;

            // this should start as allowing requests
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());

            // success with high latency
            cmd1.execute();
            HystrixCommand<Boolean> cmd2 = new SuccessCommand(1);
            cmd2.execute();
            HystrixCommand<Boolean> cmd3 = new FailureCommand(1);
            cmd3.execute();
            HystrixCommand<Boolean> cmd4 = new SuccessCommand(1);
            cmd4.execute();
            HystrixCommand<Boolean> cmd5 = new FailureCommand(1);
            cmd5.execute();
            HystrixCommand<Boolean> cmd6 = new SuccessCommand(1);
            cmd6.execute();
            HystrixCommand<Boolean> cmd7 = new SuccessCommand(1);
            cmd7.execute();
            HystrixCommand<Boolean> cmd8 = new FailureCommand(1);
            cmd8.execute();

            // this should remain open as the failure threshold is below the percentage limit
            Thread.sleep(100);
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred: " + e.getMessage());
        }
    }

    /**
     * Test that if all 'marks' are timeouts that it will trip the circuit.
     */
    @Test
    public void testTripCircuitOnTimeouts() {
        try {
            HystrixCommand<Boolean> cmd1 = new TimeoutCommand();
            HystrixCircuitBreaker cb = cmd1.circuitBreaker;

            // this should start as allowing requests
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());

            // success with high latency
            cmd1.execute();
            HystrixCommand<Boolean> cmd2 = new TimeoutCommand();
            cmd2.execute();
            HystrixCommand<Boolean> cmd3 = new TimeoutCommand();
            cmd3.execute();
            HystrixCommand<Boolean> cmd4 = new TimeoutCommand();
            cmd4.execute();

            // everything has been a timeout so we should not allow any requests
            Thread.sleep(100);
            assertFalse(cb.allowRequest());
            assertTrue(cb.isOpen());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred: " + e.getMessage());
        }
    }

    /**
     * Test that if the % of timeouts is higher than the threshold that the circuit trips.
     */
    @Test
    public void testTripCircuitOnTimeoutsAboveThreshold() {
        try {
            HystrixCommand<Boolean> cmd1 = new SuccessCommand(60);
            HystrixCircuitBreaker cb = cmd1.circuitBreaker;

            // this should start as allowing requests
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());

            // success with high latency
            cmd1.execute();
            HystrixCommand<Boolean> cmd2 = new SuccessCommand(1);
            cmd2.execute();
            HystrixCommand<Boolean> cmd3 = new TimeoutCommand();
            cmd3.execute();
            HystrixCommand<Boolean> cmd4 = new SuccessCommand(1);
            cmd4.execute();
            HystrixCommand<Boolean> cmd5 = new TimeoutCommand();
            cmd5.execute();
            HystrixCommand<Boolean> cmd6 = new TimeoutCommand();
            cmd6.execute();
            HystrixCommand<Boolean> cmd7 = new SuccessCommand(1);
            cmd7.execute();
            HystrixCommand<Boolean> cmd8 = new TimeoutCommand();
            cmd8.execute();
            HystrixCommand<Boolean> cmd9 = new TimeoutCommand();
            cmd9.execute();

            // this should trip the circuit as the error percentage is above the threshold
            Thread.sleep(100);
            assertFalse(cb.allowRequest());
            assertTrue(cb.isOpen());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred: " + e.getMessage());
        }
    }

    /**
     * Test that on an open circuit that a single attempt will be allowed after a window of time to see if issues are resolved.
     */
    @Test
    public void testSingleTestOnOpenCircuitAfterTimeWindow() {
        try {
            int sleepWindow = 200;
            HystrixCommand<Boolean> cmd1 = new FailureCommand(60);
            HystrixCircuitBreaker cb = cmd1.circuitBreaker;

            // this should start as allowing requests
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());

            cmd1.execute();
            HystrixCommand<Boolean> cmd2 = new FailureCommand(1);
            cmd2.execute();
            HystrixCommand<Boolean> cmd3 = new FailureCommand(1);
            cmd3.execute();
            HystrixCommand<Boolean> cmd4 = new FailureCommand(1);
            cmd4.execute();

            // everything has failed in the test window so we should return false now
            Thread.sleep(100);
            assertFalse(cb.allowRequest());
            assertTrue(cb.isOpen());

            // wait for sleepWindow to pass
            Thread.sleep(sleepWindow + 50);

            // we should now allow 1 request
            assertTrue(cb.allowRequest());
            // but the circuit should still be open
            assertTrue(cb.isOpen());
            // and further requests are still blocked
            assertFalse(cb.allowRequest());

        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred: " + e.getMessage());
        }
    }

    /**
     * Test that an open circuit is closed after 1 success.  This also ensures that the rolling window (containing failures) is cleared after the sleep window
     * Otherwise, the next bucket roll would produce another signal to fail unless it is explicitly cleared (via {@link HystrixCommandMetrics#resetStream()}.
     */
    @Test
    public void testCircuitClosedAfterSuccess() {
        try {
            int sleepWindow = 20;
            HystrixCommand<Boolean> cmd1 = new FailureCommand(60, sleepWindow);
            HystrixCircuitBreaker cb = cmd1.circuitBreaker;

            // this should start as allowing requests
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());

            cmd1.execute();
            HystrixCommand<Boolean> cmd2 = new FailureCommand(1, sleepWindow);
            cmd2.execute();
            HystrixCommand<Boolean> cmd3 = new FailureCommand(1, sleepWindow);
            cmd3.execute();
            HystrixCommand<Boolean> cmd4 = new TimeoutCommand(sleepWindow);
            cmd4.execute();

            // everything has failed in the test window so we should return false now
            Thread.sleep(100);
            assertFalse(cb.allowRequest());
            assertTrue(cb.isOpen());

            // wait for sleepWindow to pass
            Thread.sleep(sleepWindow + 50);

            // but the circuit should still be open
            assertTrue(cb.isOpen());

            // we should now allow 1 request, and upon success, should cause the circuit to be closed
            HystrixCommand<Boolean> cmd5 = new SuccessCommand(60, sleepWindow);
            Observable<Boolean> asyncResult = cmd5.observe();

            // and further requests are still blocked while the singleTest command is in flight
            assertFalse(cb.allowRequest());

            asyncResult.toBlocking().single();

            // all requests should be open again
            assertTrue(cb.allowRequest());
            assertTrue(cb.allowRequest());
            assertTrue(cb.allowRequest());
            // and the circuit should be closed again
            assertFalse(cb.isOpen());

        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred: " + e.getMessage());
        }
    }

    /**
     * Over a period of several 'windows' a single attempt will be made and fail and then finally succeed and close the circuit.
     * <p>
     * Ensure the circuit is kept open through the entire testing period and that only the single attempt in each window is made.
     */
    @Test
    public void testMultipleTimeWindowRetriesBeforeClosingCircuit() {
        try {
            int sleepWindow = 200;
            HystrixCommand<Boolean> cmd1 = new FailureCommand(60);
            HystrixCircuitBreaker cb = cmd1.circuitBreaker;

            // this should start as allowing requests
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());

            cmd1.execute();
            HystrixCommand<Boolean> cmd2 = new FailureCommand(1);
            cmd2.execute();
            HystrixCommand<Boolean> cmd3 = new FailureCommand(1);
            cmd3.execute();
            HystrixCommand<Boolean> cmd4 = new TimeoutCommand();
            cmd4.execute();

            // everything has failed in the test window so we should return false now
            System.out.println("!!!! 1 4 failures, circuit will open on recalc");
            Thread.sleep(100);

            assertFalse(cb.allowRequest());
            assertTrue(cb.isOpen());

            // wait for sleepWindow to pass
            System.out.println("!!!! 2 Sleep window starting where all commands fail-fast");
            Thread.sleep(sleepWindow + 50);
            System.out.println("!!!! 3 Sleep window over, should allow singleTest()");

            // but the circuit should still be open
            assertTrue(cb.isOpen());

            // we should now allow 1 request, and upon failure, should not affect the circuit breaker, which should remain open
            HystrixCommand<Boolean> cmd5 = new FailureCommand(60);
            Observable<Boolean> asyncResult5 = cmd5.observe();
            System.out.println("!!!! Kicked off the single-test");

            // and further requests are still blocked while the singleTest command is in flight
            assertFalse(cb.allowRequest());
            System.out.println("!!!! Confirmed that no other requests go out during single-test");

            asyncResult5.toBlocking().single();
            System.out.println("!!!! SingleTest just completed");

            // all requests should still be blocked, because the singleTest failed
            assertFalse(cb.allowRequest());
            assertFalse(cb.allowRequest());
            assertFalse(cb.allowRequest());

            // wait for sleepWindow to pass
            System.out.println("!!!! 2nd sleep window START");
            Thread.sleep(sleepWindow + 50);
            System.out.println("!!!! 2nd sleep window over");

            // we should now allow 1 request, and upon failure, should not affect the circuit breaker, which should remain open
            HystrixCommand<Boolean> cmd6 = new FailureCommand(60);
            Observable<Boolean> asyncResult6 = cmd6.observe();
            System.out.println("2nd singleTest just kicked off");

            //and further requests are still blocked while the singleTest command is in flight
            assertFalse(cb.allowRequest());
            System.out.println("confirmed that 2nd singletest only happened once");

            asyncResult6.toBlocking().single();
            System.out.println("2nd singleTest now over");

            // all requests should still be blocked, because the singleTest failed
            assertFalse(cb.allowRequest());
            assertFalse(cb.allowRequest());
            assertFalse(cb.allowRequest());

            // wait for sleepWindow to pass
            Thread.sleep(sleepWindow + 50);

            // but the circuit should still be open
            assertTrue(cb.isOpen());

            // we should now allow 1 request, and upon success, should cause the circuit to be closed
            HystrixCommand<Boolean> cmd7 = new SuccessCommand(60);
            Observable<Boolean> asyncResult7 = cmd7.observe();

            // and further requests are still blocked while the singleTest command is in flight
            assertFalse(cb.allowRequest());

            asyncResult7.toBlocking().single();

            // all requests should be open again
            assertTrue(cb.allowRequest());
            assertTrue(cb.allowRequest());
            assertTrue(cb.allowRequest());
            // and the circuit should be closed again
            assertFalse(cb.isOpen());

            // and the circuit should be closed again
            assertFalse(cb.isOpen());
        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred: " + e.getMessage());
        }
    }

    /**
     * When volume of reporting during a statistical window is lower than a defined threshold the circuit
     * will not trip regardless of whatever statistics are calculated.
     */
    @Test
    public void testLowVolumeDoesNotTripCircuit() {
        try {
            int sleepWindow = 200;
            int lowVolume = 5;

            HystrixCommand<Boolean> cmd1 = new FailureCommand(60, sleepWindow, lowVolume);
            HystrixCircuitBreaker cb = cmd1.circuitBreaker;

            // this should start as allowing requests
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());

            cmd1.execute();
            HystrixCommand<Boolean> cmd2 = new FailureCommand(1, sleepWindow, lowVolume);
            cmd2.execute();
            HystrixCommand<Boolean> cmd3 = new FailureCommand(1, sleepWindow, lowVolume);
            cmd3.execute();
            HystrixCommand<Boolean> cmd4 = new FailureCommand(1, sleepWindow, lowVolume);
            cmd4.execute();

            // even though it has all failed we won't trip the circuit because the volume is low
            Thread.sleep(100);
            assertTrue(cb.allowRequest());
            assertFalse(cb.isOpen());

        } catch (Exception e) {
            e.printStackTrace();
            fail("Error occurred: " + e.getMessage());
        }
    }

    /**
     * Utility method for creating {@link HystrixCommandMetrics} for unit tests.
     */
    private static HystrixCommandMetrics getMetrics(HystrixCommandProperties.Setter properties) {
        return new HystrixCommandMetrics(CommandKeyForUnitTest.KEY_ONE, CommandOwnerForUnitTest.OWNER_ONE, ThreadPoolKeyForUnitTest.THREAD_POOL_ONE, HystrixCommandPropertiesTest.asMock(properties), HystrixPlugins.getInstance().getEventNotifier());
    }

    /**
     * Utility method for creating {@link HystrixCircuitBreaker} for unit tests.
     */
    private static HystrixCircuitBreaker getCircuitBreaker(HystrixCommandKey key, HystrixCommandGroupKey commandGroup, HystrixCommandMetrics metrics, HystrixCommandProperties.Setter properties) {
        return new HystrixCircuitBreakerImpl(key, commandGroup, HystrixCommandPropertiesTest.asMock(properties), metrics);
    }

    private static enum CommandOwnerForUnitTest implements HystrixCommandGroupKey {
        OWNER_ONE, OWNER_TWO
    }

    private static enum ThreadPoolKeyForUnitTest implements HystrixThreadPoolKey {
        THREAD_POOL_ONE, THREAD_POOL_TWO
    }

    private static enum CommandKeyForUnitTest implements HystrixCommandKey {
        KEY_ONE, KEY_TWO
    }

    // ignoring since this never ends ... useful for testing https://github.com/Netflix/Hystrix/issues/236
    @Ignore
    @Test
    public void testSuccessClosesCircuitWhenBusy() throws InterruptedException {
        HystrixPlugins.getInstance().registerCommandExecutionHook(new MyHystrixCommandExecutionHook());
        try {
            performLoad(200, 0, 40);
            performLoad(250, 100, 40);
            performLoad(600, 0, 40);
        } finally {
            Hystrix.reset();
        }

    }

    void performLoad(int totalNumCalls, int errPerc, int waitMillis) {

        Random rnd = new Random();

        for (int i = 0; i < totalNumCalls; i++) {
            //System.out.println(i);

            try {
                boolean err = rnd.nextFloat() * 100 < errPerc;

                TestCommand cmd = new TestCommand(err);
                cmd.execute();

            } catch (Exception e) {
                //System.err.println(e.getMessage());
            }

            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException e) {
            }
        }
    }

    public class TestCommand extends HystrixCommand<String> {

        boolean error;

        public TestCommand(final boolean error) {
            super(HystrixCommandGroupKey.Factory.asKey("group"));

            this.error = error;
        }

        @Override
        protected String run() throws Exception {

            if (error) {
                throw new Exception("forced failure");
            } else {
                return "success";
            }
        }

        @Override
        protected String getFallback() {
            if (isFailedExecution()) {
                return getFailedExecutionException().getMessage();
            } else {
                return "other fail reason";
            }
        }

    }

    private class Command extends HystrixCommand<Boolean> {

        private final boolean shouldFail;
        private final boolean shouldFailWithBadRequest;
        private final long latencyToAdd;

        public Command(boolean shouldFail, boolean shouldFailWithBadRequest, long latencyToAdd, int sleepWindow, int requestVolumeThreshold) {
            super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("Command")).andCommandKey(HystrixCommandKey.Factory.asKey("Command")).
                    andCommandPropertiesDefaults(HystrixCommandPropertiesTest.getUnitTestPropertiesSetter().
                            withExecutionTimeoutInMilliseconds(100).
                            withCircuitBreakerRequestVolumeThreshold(requestVolumeThreshold).
                            withCircuitBreakerSleepWindowInMilliseconds(sleepWindow)));
            this.shouldFail = shouldFail;
            this.shouldFailWithBadRequest = shouldFailWithBadRequest;
            this.latencyToAdd = latencyToAdd;
        }

        public Command(boolean shouldFail, long latencyToAdd) {
            this(shouldFail, false, latencyToAdd, 200, 1);
        }

        @Override
        protected Boolean run() throws Exception {
            Thread.sleep(latencyToAdd);
            if (shouldFail) {
                throw new RuntimeException("induced failure");
            }
            if (shouldFailWithBadRequest) {
                throw new HystrixBadRequestException("bad request");
            }
            return true;
        }

        @Override
        protected Boolean getFallback() {
            return false;
        }
    }

    private class SuccessCommand extends Command {

        SuccessCommand(long latencyToAdd) {
            super(false, latencyToAdd);
        }

        SuccessCommand(long latencyToAdd, int sleepWindow) {
            super(false, false, latencyToAdd, sleepWindow, 1);
        }
    }

    private class FailureCommand extends Command {

        FailureCommand(long latencyToAdd) {
            super(true, latencyToAdd);
        }

        FailureCommand(long latencyToAdd, int sleepWindow) {
            super(true, false, latencyToAdd, sleepWindow, 1);
        }

        FailureCommand(long latencyToAdd, int sleepWindow, int requestVolumeThreshold) {
            super(true, false, latencyToAdd, sleepWindow, requestVolumeThreshold);
        }
    }

    private class TimeoutCommand extends Command {

        TimeoutCommand() {
            super(false, 2000);
        }

        TimeoutCommand(int sleepWindow) {
            super(false, false, 2000, sleepWindow, 1);
        }
    }

    private class BadRequestCommand extends Command {
        BadRequestCommand(long latencyToAdd) {
            super(false, true, latencyToAdd, 200, 1);
        }

        BadRequestCommand(long latencyToAdd, int sleepWindow) {
            super(false, true, latencyToAdd, sleepWindow, 1);
        }
    }

    public class MyHystrixCommandExecutionHook extends HystrixCommandExecutionHook {

        @Override
        public <T> T onComplete(final HystrixInvokable<T> command, final T response) {

            logHC(command, response);

            return super.onComplete(command, response);
        }

        private int counter = 0;

        private <T> void logHC(HystrixInvokable<T> command, T response) {

            if(command instanceof HystrixInvokableInfo) {
                HystrixInvokableInfo<T> commandInfo = (HystrixInvokableInfo<T>)command;
            HystrixCommandMetrics metrics = commandInfo.getMetrics();
            System.out.println("cb/error-count/%/total: "
                    + commandInfo.isCircuitBreakerOpen() + " "
                    + metrics.getHealthCounts().getErrorCount() + " "
                    + metrics.getHealthCounts().getErrorPercentage() + " "
                    + metrics.getHealthCounts().getTotalRequests() + "  => " + response + "  " + commandInfo.getExecutionEvents());
            }
        }
    }
}
