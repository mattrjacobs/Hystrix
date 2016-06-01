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
import io.reactivesocket.Payload;
import rx.Observable;
import rx.subjects.BehaviorSubject;

import java.util.function.Supplier;

public abstract class BasePayloadSupplier implements Supplier<Observable<Payload>> {
    protected final CBORFactory cborFactory;

    protected final BehaviorSubject<Payload> subject;

    protected BasePayloadSupplier() {
        this.cborFactory = new CBORFactory();
        this.subject = BehaviorSubject.create();
    }
}
