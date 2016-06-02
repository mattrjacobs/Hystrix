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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import io.reactivesocket.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func0;

import java.io.IOException;
import java.util.stream.Stream;

public abstract class StreamingSupplier<T> extends BasePayloadSupplier<JsonNode> {

    protected final Logger logger = LoggerFactory.getLogger(StreamingSupplier.class);

    protected StreamingSupplier() {
        super(Observable.just(null));

//        Observable
//            .interval(500, TimeUnit.MILLISECONDS, Schedulers.computation())
//            .doOnNext(i ->
//                getStream()
//                    .filter(this::filter)
//                    .map(this::getPayloadData)
//                    .forEach(b -> {
//                        Payload p = new Payload() {
//                            @Override
//                            public ByteBuffer getData() {
//                                return ByteBuffer.wrap(b);
//                            }
//
//                            @Override
//                            public ByteBuffer getMetadata() {
//                                return Frame.NULL_BYTEBUFFER;
//                            }
//                        };
//
//                        subject.onNext(p);
//                    })
//            )
//            .retry()
//            .subscribe();
    }

    public boolean filter(T t) {
        return true;
    }

    @Override
    public Observable<Payload> get() {
        return subject;
    }

    protected abstract Stream<T> getStream();

    protected abstract byte[] getPayloadData(T t);

    protected void safelyWriteNumberField(JsonGenerator json, String name, Func0<Long> metricGenerator) throws IOException {
        try {
            json.writeNumberField(name, metricGenerator.call());
        } catch (NoSuchFieldError error) {
            logger.error("While publishing Hystrix metrics stream, error looking up eventType for : " + name + ".  Please check that all Hystrix versions are the same!");
            json.writeNumberField(name, 0L);
        }
    }
}
