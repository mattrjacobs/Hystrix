/**
 * Copyright 2016 Netflix, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.hystrix.contrib.reactivesocket.serialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;

import java.nio.ByteBuffer;

public class SerialHystrixMetric {
    protected final static CBORFactory cborFactory = new CBORFactory();
    protected final static ObjectMapper mapper = new ObjectMapper();

    public static Payload toPayload(byte[] byteArray) {
        return new Payload() {
            @Override
            public ByteBuffer getData() {
                return ByteBuffer.wrap(byteArray);
            }

            @Override
            public ByteBuffer getMetadata() {
                return Frame.NULL_BYTEBUFFER;
            }
        };
    }
}
