/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.smithy.java.example.dynamodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.smithy.java.aws.client.awsjson.AwsJson1Protocol;
import software.amazon.smithy.java.context.Context;
import software.amazon.smithy.java.core.schema.ApiOperation;
import software.amazon.smithy.java.example.dynamodb.model.AttributeValue;
import software.amazon.smithy.java.example.dynamodb.model.GetItem;
import software.amazon.smithy.java.example.dynamodb.model.GetItemInput;
import software.amazon.smithy.java.example.dynamodb.model.GetItemOutput;
import software.amazon.smithy.java.example.dynamodb.model.PutItem;
import software.amazon.smithy.java.example.dynamodb.model.PutItemInput;
import software.amazon.smithy.java.example.dynamodb.model.PutItemOutput;
import software.amazon.smithy.java.http.api.HttpRequest;
import software.amazon.smithy.java.http.api.HttpResponse;
import software.amazon.smithy.java.io.datastream.DataStream;
import software.amazon.smithy.java.json.JsonCodec;
import software.amazon.smithy.model.shapes.ShapeId;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class DynamoDBSerde {

    private static final JsonCodec CODEC = JsonCodec.builder().build();

    @Benchmark
    public void putItem(PutItemState s, Blackhole bh) {
        var request = s.protocol.createRequest(s.operation, s.req, s.context, s.endpoint);
        bh.consume(request);
    }

    @Benchmark
    public void getItem(GetItemState s, Blackhole bh) {
        var resp = fullResponse(s.testItem.utf8);
        var result = s.protocol.deserializeResponse(s.operation, s.context, s.operation.errorRegistry(), s.req, resp);
        bh.consume(result);
    }

    @Benchmark
    public void putItemV2(PutItemV2State s, Blackhole bh) throws Exception {
        // Serialize PutItemRequest to JSON using the SDK's approach
        // We'll manually build the JSON structure that matches DynamoDB's format
        Map<String, Object> requestMap = Map.of(
                "TableName", s.req.tableName(),
                "Item", convertToJsonMap(s.req.item())
        );
        byte[] json = s.objectMapper.writeValueAsBytes(requestMap);
        bh.consume(json);
    }

    @Benchmark
    public void getItemV2(GetItemV2State s, Blackhole bh) throws Exception {
        // Deserialize JSON to GetItemResponse
        // Parse the JSON and reconstruct the response
        @SuppressWarnings("unchecked")
        Map<String, Object> responseMap = s.objectMapper.readValue(s.jsonBytes, Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> itemMap = (Map<String, Map<String, Object>>) responseMap.get("Item");
        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item = convertFromJsonMap(itemMap);
        GetItemResponse response = GetItemResponse.builder().item(item).build();
        bh.consume(response);
    }

    @State(Scope.Benchmark)
    public static class PutItemState {
        @Param({"TINY", "SMALL", "HUGE"})
        private TestItem testItem;

        ApiOperation<PutItemInput, PutItemOutput> operation;
        URI endpoint;
        AwsJson1Protocol protocol;
        PutItemInput req;
        Context context = Context.create();

        @Setup
        public void setup() throws URISyntaxException {
            endpoint = new URI("https://dynamodb.us-east-1.amazonaws.com");
            operation = PutItem.instance();
            protocol = new AwsJson1Protocol(ShapeId.from("com.amazonaws.dynamodb#DynamoDB_20120810"));
            req = PutItemInput.builder().tableName("a").item(testItem.getValue()).build();
        }
    }

    @State(Scope.Benchmark)
    public static class GetItemState {
        @Param({"TINY", "SMALL", "HUGE"})
        private TestItemUnmarshalling testItem;

        ApiOperation<GetItemInput, GetItemOutput> operation;
        Context context = Context.create();
        URI endpoint;
        AwsJson1Protocol protocol;
        HttpRequest req;

        @Setup
        public void setup() throws URISyntaxException {
            // This isn't actually used, but needed for the protocol implementation.
            endpoint = new URI("https://dynamodb.us-east-1.amazonaws.com");
            req = HttpRequest.builder().method("POST").uri(endpoint).build();
            operation = GetItem.instance();
            protocol = new AwsJson1Protocol(ShapeId.from("com.amazonaws.dynamodb#DynamoDB_20120810"));
        }
    }

    @State(Scope.Benchmark)
    public static class PutItemV2State {
        @Param({"TINY", "SMALL", "HUGE"})
        private TestItemV2 testItem;

        ObjectMapper objectMapper;
        PutItemRequest req;

        @Setup
        public void setup() {
            objectMapper = new ObjectMapper();
            req = PutItemRequest.builder()
                    .tableName("a")
                    .item(testItem.getValue())
                    .build();
        }
    }

    @State(Scope.Benchmark)
    public static class GetItemV2State {
        @Param({"TINY", "SMALL", "HUGE"})
        private TestItemUnmarshallingV2 testItem;

        ObjectMapper objectMapper;
        byte[] jsonBytes;

        @Setup
        public void setup() throws Exception {
            objectMapper = new ObjectMapper();
            jsonBytes = testItem.utf8();
        }
    }

    public enum TestItem {
        TINY,
        SMALL,
        HUGE;

        private static final ItemFactory FACTORY = new ItemFactory();

        private Map<String, AttributeValue> av;

        static {
            TINY.av = FACTORY.tiny();
            SMALL.av = FACTORY.small();
            HUGE.av = FACTORY.huge();
        }

        public Map<String, AttributeValue> getValue() {
            return av;
        }
    }

    public enum TestItemUnmarshalling {
        TINY,
        SMALL,
        HUGE;

        private byte[] utf8;

        static {
            TINY.utf8 = toUtf8ByteArray(TestItem.TINY.av);
            SMALL.utf8 = toUtf8ByteArray(TestItem.SMALL.av);
            HUGE.utf8 = toUtf8ByteArray(TestItem.HUGE.av);
        }

        public byte[] utf8() {
            return utf8;
        }
    }

    public enum TestItemV2 {
        TINY,
        SMALL,
        HUGE;

        private static final ItemFactoryV2 FACTORY = new ItemFactoryV2();

        private Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> av;

        static {
            TINY.av = FACTORY.tiny();
            SMALL.av = FACTORY.small();
            HUGE.av = FACTORY.huge();
        }

        public Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> getValue() {
            return av;
        }
    }

    public enum TestItemUnmarshallingV2 {
        TINY,
        SMALL,
        HUGE;

        private byte[] utf8;

        static {
            TINY.utf8 = toUtf8ByteArrayV2(TestItemV2.TINY.av);
            SMALL.utf8 = toUtf8ByteArrayV2(TestItemV2.SMALL.av);
            HUGE.utf8 = toUtf8ByteArrayV2(TestItemV2.HUGE.av);
        }

        public byte[] utf8() {
            return utf8;
        }
    }

    private static byte[] toUtf8ByteArray(Map<String, AttributeValue> item) {
        return CODEC.serializeToString(GetItemOutput.builder().item(item).build()).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] toUtf8ByteArrayV2(Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item) {
        // Create a simple JSON representation for GetItemResponse
        GetItemResponse response = GetItemResponse.builder().item(item).build();
        
        // Use Jackson or simple string building to serialize
        // For benchmark purposes, we'll create a minimal JSON structure
        StringBuilder json = new StringBuilder();
        json.append("{\"Item\":");
        json.append(serializeItemToJsonV2(item));
        json.append("}");
        
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static String serializeItemToJsonV2(Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        boolean first = true;
        for (Map.Entry<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> entry : item.entrySet()) {
            if (!first) json.append(",");
            first = false;
            json.append("\"").append(entry.getKey()).append("\":");
            json.append(serializeAttributeValueToJsonV2(entry.getValue()));
        }
        json.append("}");
        return json.toString();
    }

    private static String serializeAttributeValueToJsonV2(software.amazon.awssdk.services.dynamodb.model.AttributeValue av) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        if (av.s() != null) {
            json.append("\"S\":\"").append(av.s()).append("\"");
        } else if (av.b() != null) {
            json.append("\"B\":\"").append(java.util.Base64.getEncoder().encodeToString(av.b().asByteArray())).append("\"");
        } else if (av.l() != null && !av.l().isEmpty()) {
            json.append("\"L\":[");
            boolean first = true;
            for (software.amazon.awssdk.services.dynamodb.model.AttributeValue item : av.l()) {
                if (!first) json.append(",");
                first = false;
                json.append(serializeAttributeValueToJsonV2(item));
            }
            json.append("]");
        } else if (av.m() != null && !av.m().isEmpty()) {
            json.append("\"M\":");
            json.append(serializeItemToJsonV2(av.m()));
        }
        json.append("}");
        return json.toString();
    }

    private static Map<String, Object> convertToJsonMap(
            Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item) {
        Map<String, Object> result = new java.util.HashMap<>();
        for (Map.Entry<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> entry : item.entrySet()) {
            result.put(entry.getKey(), convertAttributeValueToJsonMap(entry.getValue()));
        }
        return result;
    }

    private static Map<String, Object> convertAttributeValueToJsonMap(
            software.amazon.awssdk.services.dynamodb.model.AttributeValue av) {
        Map<String, Object> result = new java.util.HashMap<>();
        if (av.s() != null) {
            result.put("S", av.s());
        } else if (av.b() != null) {
            result.put("B", java.util.Base64.getEncoder().encodeToString(av.b().asByteArray()));
        } else if (av.l() != null) {
            List<Map<String, Object>> list = new java.util.ArrayList<>();
            for (software.amazon.awssdk.services.dynamodb.model.AttributeValue item : av.l()) {
                list.add(convertAttributeValueToJsonMap(item));
            }
            result.put("L", list);
        } else if (av.m() != null) {
            result.put("M", convertToJsonMap(av.m()));
        }
        return result;
    }

    private static Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> convertFromJsonMap(
            Map<String, Map<String, Object>> itemMap) {
        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> result = new java.util.HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : itemMap.entrySet()) {
            result.put(entry.getKey(), convertAttributeValueFromJsonMap(entry.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static software.amazon.awssdk.services.dynamodb.model.AttributeValue convertAttributeValueFromJsonMap(
            Map<String, Object> avMap) {
        software.amazon.awssdk.services.dynamodb.model.AttributeValue.Builder builder =
                software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder();
        
        if (avMap.containsKey("S")) {
            builder.s((String) avMap.get("S"));
        } else if (avMap.containsKey("B")) {
            String base64 = (String) avMap.get("B");
            builder.b(SdkBytes.fromByteArray(java.util.Base64.getDecoder().decode(base64)));
        } else if (avMap.containsKey("L")) {
            List<Map<String, Object>> list = (List<Map<String, Object>>) avMap.get("L");
            List<software.amazon.awssdk.services.dynamodb.model.AttributeValue> avList = new java.util.ArrayList<>();
            for (Map<String, Object> item : list) {
                avList.add(convertAttributeValueFromJsonMap(item));
            }
            builder.l(avList);
        } else if (avMap.containsKey("M")) {
            Map<String, Map<String, Object>> map = (Map<String, Map<String, Object>>) avMap.get("M");
            builder.m(convertFromJsonMap(map));
        }
        
        return builder.build();
    }

    private HttpResponse fullResponse(byte[] itemBytes) {
        return HttpResponse.builder()
                .statusCode(200)
                .body(DataStream.ofBytes(itemBytes))
                .build();
    }

    static final class ItemFactory {

        private static final String ALPHA = "abcdefghijklmnopqrstuvwxyz";
        private static final Random RNG = new Random();

        Map<String, AttributeValue> tiny() {
            return Map.of("stringAttr", av(randomS()));
        }

        Map<String, AttributeValue> small() {
            return Map.of(
                    "stringAttr",
                    av(randomS()),
                    "binaryAttr",
                    av(randomB()),
                    "listAttr",
                    av(List.of(av(randomS()), av(randomB()), av(randomS()))));
        }

        Map<String, AttributeValue> huge() {
            return Map.of(
                    "hashKey",
                    av(randomS()),
                    "stringAttr",
                    av(randomS()),
                    "binaryAttr",
                    av(randomB()),
                    "listAttr",
                    av(
                            List.of(
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomB()),
                                    av(Collections.singletonList(av(randomS()))),
                                    av(Map.of("attrOne", av(randomS()))),
                                    av(
                                            Arrays.asList(
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomB()),
                                                    (av(randomS())),
                                                    av(Map.of("attrOne", av(randomS()))))))),
                    "mapAttr",
                    av(
                            Map.of(
                                    "attrOne",
                                    av(randomS()),
                                    "attrTwo",
                                    av(randomB()),
                                    "attrThree",
                                    av(
                                            List.of(
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(
                                                            Map.of(
                                                                    "attrOne",
                                                                    av(randomS()),
                                                                    "attrTwo",
                                                                    av(randomB()),
                                                                    "attrThree",
                                                                    av(
                                                                            List.of(
                                                                                    av(randomS()),
                                                                                    av(randomS()),
                                                                                    av(randomS()),
                                                                                    av(randomS()))))))))));
        }

        private AttributeValue av(String val) {
            return AttributeValue.builder().s(val).build();
        }

        private AttributeValue av(ByteBuffer val) {
            return AttributeValue.builder().b(val).build();
        }

        private AttributeValue av(List<AttributeValue> val) {
            return AttributeValue.builder().l(val).build();
        }

        private AttributeValue av(Map<String, AttributeValue> val) {
            return AttributeValue.builder().m(val).build();
        }

        private String randomS(int len) {
            StringBuilder sb = new StringBuilder(len);
            for (int i = 0; i < len; ++i) {
                sb.append(ALPHA.charAt(RNG.nextInt(ALPHA.length())));
            }
            return sb.toString();
        }

        private String randomS() {
            return randomS(16);
        }

        private ByteBuffer randomB(int len) {
            byte[] b = new byte[len];
            RNG.nextBytes(b);
            return ByteBuffer.wrap(b);
        }

        private ByteBuffer randomB() {
            return randomB(16);
        }
    }

    static final class ItemFactoryV2 {

        private static final String ALPHA = "abcdefghijklmnopqrstuvwxyz";
        private static final Random RNG = new Random();

        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> tiny() {
            return Map.of("stringAttr", av(randomS()));
        }

        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> small() {
            return Map.of(
                    "stringAttr",
                    av(randomS()),
                    "binaryAttr",
                    av(randomB()),
                    "listAttr",
                    av(List.of(av(randomS()), av(randomB()), av(randomS()))));
        }

        Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> huge() {
            return Map.of(
                    "hashKey",
                    av(randomS()),
                    "stringAttr",
                    av(randomS()),
                    "binaryAttr",
                    av(randomB()),
                    "listAttr",
                    av(
                            List.of(
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomS()),
                                    av(randomB()),
                                    av(Collections.singletonList(av(randomS()))),
                                    av(Map.of("attrOne", av(randomS()))),
                                    av(
                                            Arrays.asList(
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomB()),
                                                    (av(randomS())),
                                                    av(Map.of("attrOne", av(randomS()))))))),
                    "mapAttr",
                    av(
                            Map.of(
                                    "attrOne",
                                    av(randomS()),
                                    "attrTwo",
                                    av(randomB()),
                                    "attrThree",
                                    av(
                                            List.of(
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(randomS()),
                                                    av(
                                                            Map.of(
                                                                    "attrOne",
                                                                    av(randomS()),
                                                                    "attrTwo",
                                                                    av(randomB()),
                                                                    "attrThree",
                                                                    av(
                                                                            List.of(
                                                                                    av(randomS()),
                                                                                    av(randomS()),
                                                                                    av(randomS()),
                                                                                    av(randomS()))))))))));
        }

        private software.amazon.awssdk.services.dynamodb.model.AttributeValue av(String val) {
            return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(val).build();
        }

        private software.amazon.awssdk.services.dynamodb.model.AttributeValue av(SdkBytes val) {
            return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().b(val).build();
        }

        private software.amazon.awssdk.services.dynamodb.model.AttributeValue av(
                List<software.amazon.awssdk.services.dynamodb.model.AttributeValue> val) {
            return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().l(val).build();
        }

        private software.amazon.awssdk.services.dynamodb.model.AttributeValue av(
                Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> val) {
            return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().m(val).build();
        }

        private String randomS(int len) {
            StringBuilder sb = new StringBuilder(len);
            for (int i = 0; i < len; ++i) {
                sb.append(ALPHA.charAt(RNG.nextInt(ALPHA.length())));
            }
            return sb.toString();
        }

        private String randomS() {
            return randomS(16);
        }

        private SdkBytes randomB(int len) {
            byte[] b = new byte[len];
            RNG.nextBytes(b);
            return SdkBytes.fromByteArray(b);
        }

        private SdkBytes randomB() {
            return randomB(16);
        }
    }
}
