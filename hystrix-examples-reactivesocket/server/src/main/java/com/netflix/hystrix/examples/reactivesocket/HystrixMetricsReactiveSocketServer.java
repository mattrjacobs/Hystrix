package com.netflix.hystrix.examples.reactivesocket;

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

            Channel localhost = b.bind("0.0.0.0", 8025).sync().channel();
            localhost.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            System.out.println("Shutting down NIO threads!");
        }
    }
}
