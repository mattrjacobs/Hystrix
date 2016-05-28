package com.netflix.hystrix.examples.reactivesocket;

import com.netflix.hystrix.contrib.reactivesocket.EventStreamEnum;
import io.netty.channel.nio.NioEventLoopGroup;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DefaultReactiveSocket;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.netty.tcp.client.ClientTcpDuplexConnection;
import org.agrona.BitUtil;
import rx.RxReactiveStreams;
import rx.Subscriber;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class HystrixMetricsReactiveSocketClient {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting HystrixMetricsReactiveSocketClient...");

        ClientTcpDuplexConnection duplexConnection = RxReactiveStreams.toObservable(
                ClientTcpDuplexConnection.create(InetSocketAddress.createUnresolved("localhost", 8025), new NioEventLoopGroup())
        ).toBlocking().single();

        ReactiveSocket client = DefaultReactiveSocket
                .fromClientConnection(duplexConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8"), Throwable::printStackTrace);

        client.startAndWait();
        System.out.println("Created client : " + client);

        final CountDownLatch latch = new CountDownLatch(1);

        RxReactiveStreams
                .toObservable(client.requestSubscription(
                        createPayload(EventStreamEnum.METRICS_STREAM)))
                .take(10)
                .subscribe(new Subscriber<Payload>() {
                    @Override
                    public void onCompleted() {
                        System.out.println(Thread.currentThread().getName() + " OnCompleted");
                        latch.countDown();
                    }

                    @Override
                    public void onError(Throwable e) {
                        System.out.println(Thread.currentThread().getName() + " OnError : " + e);
                        e.printStackTrace();
                        latch.countDown();
                    }

                    @Override
                    public void onNext(Payload payload) {
                        System.out.println(Thread.currentThread().getName() + " OnNext : " + payload);
                    }
                });

        latch.await(2000, TimeUnit.MILLISECONDS);
    }

    private static Payload createPayload(EventStreamEnum eventStreamEnum) {
        System.out.println("CreatePayload : " + eventStreamEnum);
        Payload p = new Payload() {
            @Override
            public ByteBuffer getData() {
                try {
                    return ByteBuffer.allocate(BitUtil.SIZE_OF_INT).putInt(eventStreamEnum.getTypeId());
                } catch (Throwable ex) {
                    ex.printStackTrace();
                    return Frame.NULL_BYTEBUFFER;
                }
            }

            @Override
            public ByteBuffer getMetadata() {
                return Frame.NULL_BYTEBUFFER;
            }
        };

        return p;
    }
}
