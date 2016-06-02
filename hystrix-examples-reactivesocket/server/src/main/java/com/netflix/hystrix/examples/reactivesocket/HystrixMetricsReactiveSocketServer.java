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

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixObservableCommand;
import com.netflix.hystrix.contrib.reactivesocket.EventStreamRequestHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.reactivesocket.netty.tcp.server.ReactiveSocketServerHandler;
import rx.Observable;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.observers.Subscribers;
import rx.schedulers.Schedulers;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class HystrixMetricsReactiveSocketServer {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting HystrixMetricsReactiveSocketServer...");

        final ReactiveSocketServerHandler handler = ReactiveSocketServerHandler.create((setupPayload, rs) ->
                new EventStreamRequestHandler());

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(handler);
                        }
                    });
            Channel localhost = b.bind("127.0.0.1", 8025).sync().channel();

            executeCommands();
            localhost.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static void executeCommands() {
        final Random r = new Random();

        Observable.interval(100, TimeUnit.MILLISECONDS).flatMap(ts ->
                        new SyntheticObservableCommand(r).observe()
        ).subscribe(Subscribers.empty());

        Observable.interval(225, TimeUnit.MILLISECONDS).flatMap(ts ->
            new SyntheticBlockingCommand(r).observe()
        ).subscribe(Subscribers.empty());
    }

    static class SyntheticObservableCommand extends HystrixObservableCommand<Boolean> {
        private Random r;

        protected SyntheticObservableCommand(Random r) {
            super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("Example")));
            this.r = r;
        }

        @Override
        protected Observable<Boolean> construct() {
            return Observable.defer(() -> {
                try {
                    Thread.sleep(r.nextInt(50));
                    return Observable.just(true);
                } catch (InterruptedException ex) {
                    return Observable.error(ex);
                }
            }).subscribeOn(Schedulers.io());
        }

        @Override
        protected Observable<Boolean> resumeWithFallback() {
            return Observable.just(false);
        }
    }

    static class SyntheticBlockingCommand extends HystrixCommand<Integer> {
        private Random r;

        protected SyntheticBlockingCommand(Random r) {
            super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("BLOCKING")));
            this.r = r;
        }

        @Override
        protected Integer run() throws Exception {
            int randomInt = r.nextInt(400);
            Thread.sleep(randomInt);
            return randomInt;
        }

        @Override
        protected Integer getFallback() {
            return -1;
        }
    }
}
