/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.smithy.java.example.dynamodb;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpFullResponse;
import software.amazon.awssdk.protocols.json.AwsJsonProtocol;
import software.amazon.awssdk.protocols.json.AwsJsonProtocolFactory;
import software.amazon.awssdk.protocols.json.JsonOperationMetadata;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.transform.PutItemRequestMarshaller;
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

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class DynamoDBSerde {

    private static final JsonCodec CODEC = JsonCodec.builder().build();

    // @Benchmark
    public void putItem(PutItemState s, Blackhole bh) {
        var request = s.protocol.createRequest(s.operation, s.req, s.context, s.endpoint);
        bh.consume(request);
    }

    // @Benchmark
    public void getItem(GetItemState s, Blackhole bh) {
        var resp = fullResponse(s.testItem.utf8);
        var result = s.protocol.deserializeResponse(s.operation, s.context, s.operation.errorRegistry(), s.req, resp);
        bh.consume(result);
    }

    // @Benchmark
    public void putItemV2(PutItemV2State s, Blackhole bh) throws Exception {
        // Use the actual AWS SDK V2 marshaller
        PutItemRequestMarshaller marshaller = new PutItemRequestMarshaller(s.protocolFactory);
        SdkHttpFullRequest marshalledRequest = marshaller.marshall(s.req);
        bh.consume(marshalledRequest);
    }

    @Benchmark
    public void getItemV2(GetItemV2State s, Blackhole bh) throws Exception {
        // Use the actual AWS SDK V2 response handler
        s.httpResponse.content().ifPresent(c -> {
            try {
                c.reset();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        GetItemResponse response = s.responseHandler.handle(s.httpResponse, s.executionAttributes);
        bh.consume(response);
    }

    public static void main(String[] args) throws  Exception {
        System.out.println("\n\n===================");
        GetItemV2State s = new GetItemV2State();
        s.testItem = TestItemUnmarshallingV2.HUGE;
        s.setup();
        GetItemResponse response = s.responseHandler.handle(s.httpResponse, s.executionAttributes);
        System.out.println("Response: \n" + response + "\n---------------");

        PutItemV2State s2 = new PutItemV2State();
        s2.testItem = TestItemV2.HUGE;
        s2.setup();

        PutItemRequestMarshaller marshaller = new PutItemRequestMarshaller(s2.protocolFactory);
        SdkHttpFullRequest marshalledRequest = marshaller.marshall(s2.req);
        System.out.println(marshalledRequest);
        marshalledRequest.contentStreamProvider().ifPresent(c -> {
            try {
                byte[] body = c.newStream().readAllBytes();
                System.out.println("Marshalled body: " + new String(body, StandardCharsets.UTF_8) + "\n--------");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @State(Scope.Thread)
    public static class PutItemState {
        @Param({"TINY", "SMALL", "HUGE"})
        private TestItem testItem;

        ApiOperation<PutItemInput, PutItemOutput> operation;
        URI endpoint;
        AwsJson1Protocol protocol;
        PutItemInput req;
        Context context = Context.create();

        @Setup(Level.Iteration)
        public void setup() throws URISyntaxException {
            endpoint = new URI("https://dynamodb.us-east-1.amazonaws.com");
            operation = PutItem.instance();
            protocol = new AwsJson1Protocol(ShapeId.from("com.amazonaws.dynamodb#DynamoDB_20120810"));
            req = PutItemInput.builder().tableName("a").item(testItem.getValue()).build();
        }
    }

    @State(Scope.Thread)
    public static class GetItemState {
        @Param({"TINY", "SMALL", "HUGE"})
        private TestItemUnmarshalling testItem;

        ApiOperation<GetItemInput, GetItemOutput> operation;
        Context context = Context.create();
        URI endpoint;
        AwsJson1Protocol protocol;
        HttpRequest req;

        @Setup(Level.Iteration)
        public void setup() throws URISyntaxException {
            // This isn't actually used, but needed for the protocol implementation.
            endpoint = new URI("https://dynamodb.us-east-1.amazonaws.com");
            req = HttpRequest.builder().method("POST").uri(endpoint).build();
            operation = GetItem.instance();
            protocol = new AwsJson1Protocol(ShapeId.from("com.amazonaws.dynamodb#DynamoDB_20120810"));
        }
    }

    @State(Scope.Thread)
    public static class PutItemV2State {
        @Param({"TINY", "SMALL", "HUGE"})
        private TestItemV2 testItem;

        AwsJsonProtocolFactory protocolFactory;
        PutItemRequest req;

        @Setup(Level.Iteration)
        public void setup() throws URISyntaxException {
            URI endpoint = new URI("https://dynamodb.us-east-1.amazonaws.com");
            protocolFactory = AwsJsonProtocolFactory.builder()
                    .protocol(AwsJsonProtocol.AWS_JSON)
                    .clientConfiguration(SdkClientConfiguration
                            .builder()
                            .lazyOption(SdkClientOption.ENDPOINT, (c) -> endpoint)
                            .build())
                    .protocolVersion("1.0")
                    .build();
            req = PutItemRequest.builder()
                    .tableName("a")
                    .item(testItem.getValue())
                    .build();
        }
    }

    @State(Scope.Thread)
    public static class GetItemV2State {
        @Param({"TINY", "SMALL", "HUGE"})
        private TestItemUnmarshallingV2 testItem;

        AwsJsonProtocolFactory protocolFactory;
        software.amazon.awssdk.core.http.HttpResponseHandler<GetItemResponse> responseHandler;
        SdkHttpFullResponse httpResponse;
        ExecutionAttributes executionAttributes;

        @Setup(Level.Iteration)
        public void setup() throws Exception {
            protocolFactory = AwsJsonProtocolFactory.builder()
                    .protocol(AwsJsonProtocol.AWS_JSON)
                    .protocolVersion("1.0")
                    .build();
            
            JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder()
                    .hasStreamingSuccessResponse(false)
                    .isPayloadJson(true)
                    .build();
            
            responseHandler = protocolFactory.createResponseHandler(
                    operationMetadata,
                    GetItemResponse::builder);
            
            // Create HTTP response with the JSON bytes
            AbortableInputStream content = AbortableInputStream.create(
                    new ByteArrayInputStream(testItem.utf8()));

            httpResponse = SdkHttpFullResponse.builder()
                    .statusCode(200)
                    .content(content)
                    .build();
            
            executionAttributes = new ExecutionAttributes();
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
            TINY.utf8 = toUtf8ByteArray(TestItem.TINY.av);
            SMALL.utf8 = toUtf8ByteArray(TestItem.SMALL.av);
            HUGE.utf8 = toUtf8ByteArray(TestItem.HUGE.av);
        }

        public byte[] utf8() {
            return utf8;
        }
    }

    private static byte[] toUtf8ByteArray(Map<String, AttributeValue> item) {
        return CODEC.serializeToString(GetItemOutput.builder().item(item).build()).getBytes(StandardCharsets.UTF_8);
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
