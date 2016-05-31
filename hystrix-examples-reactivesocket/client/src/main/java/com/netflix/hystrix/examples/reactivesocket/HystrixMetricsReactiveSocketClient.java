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
import org.agrona.concurrent.UnsafeBuffer;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.observers.TestSubscriber;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class HystrixMetricsReactiveSocketClient {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting HystrixMetricsReactiveSocketClient...");

        ClientTcpDuplexConnection duplexConnection = RxReactiveStreams.toObservable(
                ClientTcpDuplexConnection.create(InetSocketAddress.createUnresolved("127.0.0.1", 8025), new NioEventLoopGroup())
        ).toBlocking().single();

        System.out.println("Created TCP Connection : " + duplexConnection);

        ReactiveSocket client = DefaultReactiveSocket
                .fromClientConnection(duplexConnection, ConnectionSetupPayload.create("UTF-8", "UTF-8"), Throwable::printStackTrace);

        client.startAndWait();
        System.out.println("Created client : " + client);

        Payload p = createPayload(EventStreamEnum.METRICS_STREAM);

        //Publisher<Payload> publisher = client.requestResponse(p);
        Publisher<Payload> publisher = client.requestSubscription(p);
        Observable<Payload> o = RxReactiveStreams.toObservable(publisher);

        TestSubscriber<Payload> subscriber = new TestSubscriber<>();

        o.subscribe(subscriber);

        subscriber.awaitTerminalEvent(1000, TimeUnit.MILLISECONDS);

        System.out.println("OnNexts : " + subscriber.getOnNextEvents());
        System.out.println("OnErrors : " + subscriber.getOnErrorEvents());
        System.out.println("OnCompleted : " + subscriber.getOnCompletedEvents());

        for (Throwable t : subscriber.getOnErrorEvents()) {
            t.printStackTrace();
        }
    }

    private static Payload createPayload(EventStreamEnum eventStreamEnum) {
        System.out.println("CreatePayload : " + eventStreamEnum);
        Payload p = new Payload() {
            @Override
            public ByteBuffer getData() {
//                UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocate(4));
//                unsafeBuffer.putInt(0, 4);
//                return unsafeBuffer.byteBuffer();
                return ByteBuffer.allocate(4).putInt(0, 4);
            }

            @Override
            public ByteBuffer getMetadata() {
                return Frame.NULL_BYTEBUFFER;
            }
        };

//        System.out.println("Payload : " + p);
//        for (byte b: p.getData().array()) {
//            System.out.println("Payload data byte : " + b);
//        }
//        for (byte b: p.getMetadata().array()) {
//            System.out.println("Payload metadata byte : " + b);
//        }
        return p;
    }
}
