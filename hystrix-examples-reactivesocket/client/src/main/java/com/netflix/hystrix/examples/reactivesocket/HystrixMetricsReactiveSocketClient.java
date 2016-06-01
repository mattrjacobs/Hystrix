package com.netflix.hystrix.examples.reactivesocket;

import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.contrib.reactivesocket.EventStreamEnum;
import com.netflix.hystrix.contrib.reactivesocket.sample.HystrixUtilizationStream;
import com.netflix.hystrix.metric.sample.HystrixCommandUtilization;
import com.netflix.hystrix.metric.sample.HystrixThreadPoolUtilization;
import com.netflix.hystrix.metric.sample.HystrixUtilization;
import io.netty.channel.nio.NioEventLoopGroup;
import io.reactivesocket.ConnectionSetupPayload;
import io.reactivesocket.DefaultReactiveSocket;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import io.reactivesocket.ReactiveSocket;
import io.reactivesocket.netty.tcp.client.ClientTcpDuplexConnection;
import org.agrona.BitUtil;
import org.reactivestreams.Publisher;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.Subscriber;
import rx.Subscription;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
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

        Payload p = createPayload(EventStreamEnum.UTILIZATION_EVENT_STREAM);

        Publisher<Payload> publisher = client.requestResponse(p);
        //Publisher<Payload> publisher = client.requestSubscription(p);
        Observable<Payload> o = RxReactiveStreams.toObservable(publisher);


        final CountDownLatch latch = new CountDownLatch(1);

        Subscription s = o.subscribe(new Subscriber<Payload>() {
            @Override
            public void onCompleted() {
                System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnCompleted");
                latch.countDown();
            }

            @Override
            public void onError(Throwable e) {
                System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnError : " + e);
                e.printStackTrace();
                latch.countDown();
            }

            @Override
            public void onNext(Payload payload) {
                HystrixUtilization utilization = HystrixUtilizationStream.getInstance().fromByteBuffer(payload.getData());
                Map<HystrixCommandKey, HystrixCommandUtilization> commandMap = utilization.getCommandUtilizationMap();
                StringBuilder bldr = new StringBuilder();
                bldr.append("Command[");
                for (Map.Entry<HystrixCommandKey, HystrixCommandUtilization> entry: commandMap.entrySet()) {
                    bldr.append(entry.getKey().name()).append(" -> ").append(entry.getValue().getConcurrentCommandCount()).append(", ");
                }
                bldr.append("]");
                System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnNext : " + bldr.toString());
            }
        });

        latch.await(10000, TimeUnit.MILLISECONDS);
        s.unsubscribe();
    }

    private static Payload createPayload(EventStreamEnum eventStreamEnum) {
        return new Payload() {
            @Override
            public ByteBuffer getData() {
                return ByteBuffer.allocate(BitUtil.SIZE_OF_INT).putInt(0, eventStreamEnum.getTypeId());
            }

            @Override
            public ByteBuffer getMetadata() {
                return Frame.NULL_BYTEBUFFER;
            }
        };
    }
}
