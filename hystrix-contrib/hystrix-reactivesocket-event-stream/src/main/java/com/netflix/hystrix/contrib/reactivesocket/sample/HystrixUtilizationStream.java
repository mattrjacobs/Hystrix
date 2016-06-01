package com.netflix.hystrix.contrib.reactivesocket.sample;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.contrib.reactivesocket.BasePayloadSupplier;
import com.netflix.hystrix.metric.sample.HystrixCommandUtilization;
import com.netflix.hystrix.metric.sample.HystrixThreadPoolUtilization;
import com.netflix.hystrix.metric.sample.HystrixUtilization;
import io.reactivesocket.Frame;
import io.reactivesocket.Payload;
import org.agrona.LangUtil;
import rx.Observable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HystrixUtilizationStream extends BasePayloadSupplier {
    private static final HystrixUtilizationStream INSTANCE = new HystrixUtilizationStream();

    private final ObjectMapper mapper = new ObjectMapper();

    private HystrixUtilizationStream() {
        super();

        com.netflix.hystrix.metric.sample.HystrixUtilizationStream stream
            = new com.netflix.hystrix.metric.sample.HystrixUtilizationStream(100);
        stream
            .observe()
            .map(this::getPayloadData)
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
    }

    public static HystrixUtilizationStream getInstance() {
        return INSTANCE;
    }

    @Override
    public Observable<Payload> get() {
        return subject;
    }

    public byte[] getPayloadData(HystrixUtilization utilization) {
        byte[] retVal = null;

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            JsonGenerator json = cborFactory.createGenerator(bos);

            json.writeStartObject();
            json.writeStringField("type", "HystrixUtilization");
            json.writeObjectFieldStart("commands");
            for (Map.Entry<HystrixCommandKey, HystrixCommandUtilization> entry: utilization.getCommandUtilizationMap().entrySet()) {
                final HystrixCommandKey key = entry.getKey();
                final HystrixCommandUtilization commandUtilization = entry.getValue();
                writeCommandUtilizationJson(json, key, commandUtilization);

            }
            json.writeEndObject();

            json.writeObjectFieldStart("threadpools");
            for (Map.Entry<HystrixThreadPoolKey, HystrixThreadPoolUtilization> entry: utilization.getThreadPoolUtilizationMap().entrySet()) {
                final HystrixThreadPoolKey threadPoolKey = entry.getKey();
                final HystrixThreadPoolUtilization threadPoolUtilization = entry.getValue();
                writeThreadPoolUtilizationJson(json, threadPoolKey, threadPoolUtilization);
            }
            json.writeEndObject();
            json.writeEndObject();
            json.close();

            retVal = bos.toByteArray();
        } catch (Exception e) {
            LangUtil.rethrowUnchecked(e);
        }

        return retVal;
    }

    private static void writeCommandUtilizationJson(JsonGenerator json, HystrixCommandKey key, HystrixCommandUtilization utilization) throws IOException {
        json.writeObjectFieldStart(key.name());
        json.writeNumberField("activeCount", utilization.getConcurrentCommandCount());
        json.writeEndObject();
    }

    private static void writeThreadPoolUtilizationJson(JsonGenerator json, HystrixThreadPoolKey threadPoolKey, HystrixThreadPoolUtilization utilization) throws IOException {
        json.writeObjectFieldStart(threadPoolKey.name());
        json.writeNumberField("activeCount", utilization.getCurrentActiveCount());
        json.writeNumberField("queueSize", utilization.getCurrentQueueSize());
        json.writeNumberField("corePoolSize", utilization.getCurrentCorePoolSize());
        json.writeNumberField("poolSize", utilization.getCurrentPoolSize());
        json.writeEndObject();
    }

    public HystrixUtilization fromByteBuffer(ByteBuffer bb) {
        byte[] byteArray = new byte[bb.remaining()];
        bb.get(byteArray);

        Map<HystrixCommandKey, HystrixCommandUtilization> commandUtilizationMap = new HashMap<>();
        Map<HystrixThreadPoolKey, HystrixThreadPoolUtilization> threadPoolUtilizationMap = new HashMap<>();

        try {
            CBORParser parser = cborFactory.createParser(byteArray);
            JsonNode rootNode = mapper.readTree(parser);
            Iterator<Map.Entry<String, JsonNode>> commands = rootNode.path("commands").fields();
            Iterator<Map.Entry<String, JsonNode>> threadPools = rootNode.path("threadpools").fields();

            while (commands.hasNext()) {
                Map.Entry<String, JsonNode> command = commands.next();
                HystrixCommandKey commandKey = HystrixCommandKey.Factory.asKey(command.getKey());
                HystrixCommandUtilization commandUtilization = new HystrixCommandUtilization(command.getValue().path("activeCount").asInt());
                commandUtilizationMap.put(commandKey, commandUtilization);
            }

            while (threadPools.hasNext()) {
                Map.Entry<String, JsonNode> threadPool = threadPools.next();
                HystrixThreadPoolKey threadPoolKey = HystrixThreadPoolKey.Factory.asKey(threadPool.getKey());
                HystrixThreadPoolUtilization threadPoolUtilization = new HystrixThreadPoolUtilization(
                        threadPool.getValue().path("activeCount").asInt(),
                        threadPool.getValue().path("corePoolSize").asInt(),
                        threadPool.getValue().path("poolSize").asInt(),
                        threadPool.getValue().path("queueSize").asInt()
                );
                threadPoolUtilizationMap.put(threadPoolKey, threadPoolUtilization);
            }
        } catch (IOException ioe) {
            System.out.println("IO Exception : " + ioe);
        }
        return new HystrixUtilization(commandUtilizationMap, threadPoolUtilizationMap);
    }
}
