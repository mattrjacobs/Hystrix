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
package com.netflix.hystrix.contrib.reactivesocket;

import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.subjects.BehaviorSubject;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

public abstract class BasePayloadSupplier<T> implements Supplier<Observable<Payload>> {
    protected final static Logger logger = LoggerFactory.getLogger(BasePayloadSupplier.class);

    protected final CBORFactory cborFactory;

    protected final BehaviorSubject<Payload> subject;

    protected BasePayloadSupplier(Observable<T> stream) {
        this.cborFactory = new CBORFactory();
        this.subject = BehaviorSubject.create();

        Subscription s = stream
                .doOnNext(n -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " Pushing OnNext : " + n))
                .doOnError(ex -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " Pushing OnError : " + ex))
                .doOnCompleted(() -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " Pushing OnCompleted"))
                .doOnSubscribe(() -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnSubscribe"))
                .doOnUnsubscribe(() -> System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " OnUnsubscribe"))
                .map(this::toBytes)
                .map(b -> new Payload() {
                    @Override
                    public ByteBuffer getData() {
                        return ByteBuffer.wrap(b);
                    }

                    @Override
                    public ByteBuffer getMetadata() {
                        return Frame.NULL_BYTEBUFFER;
                    }
                })
                .subscribe(subject);

        subject.doOnUnsubscribe(s::unsubscribe);
    }

    @Override
    public Observable<Payload> get() {
        return subject;
    }

    public abstract byte[] toBytes(T object);
}
