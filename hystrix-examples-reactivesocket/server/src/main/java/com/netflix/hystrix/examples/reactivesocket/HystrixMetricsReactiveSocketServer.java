package com.netflix.hystrix.examples.reactivesocket;

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
        Observable.interval(100, TimeUnit.MILLISECONDS).flatMap(ts ->
                new SyntheticCommand().observe()
        ).subscribe(Subscribers.empty());
    }

    static class SyntheticCommand extends HystrixObservableCommand<Boolean> {

        private Random r;

        protected SyntheticCommand() {
            super(Setter.withGroupKey(HystrixCommandGroupKey.Factory.asKey("Example")));
            r = new Random();
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
}
