## Example: Dynamodb Client
Example AWS DynamoDB client using the AWS JSON 1.0 protocol and 
the SigV4 auth scheme.

**Note**: This example project is used to provide baseline performance 
benchmarks 

### Usage
To use this example as a template, run the following command with the [Smithy CLI](https://smithy.io/2.0/guides/smithy-cli/index.html):

```console
smithy init -t dynamodb-client --url https://github.com/smithy-lang/smithy-java
```

or

```console
smithy init -t dynamodb-client --url git@github.com:smithy-lang/smithy-java.git
```

### Benchmarks

This example includes JMH benchmarks comparing serialization/deserialization performance between:
- **Smithy Java** (generated client using `smithy-java` codegen)
- **AWS SDK for Java V2** (official AWS SDK)

The benchmarks measure marshalling (serialization) and unmarshalling (deserialization) of DynamoDB requests and responses with three data sizes:
- **TINY**: Single string attribute
- **SMALL**: String, binary, and list attributes
- **HUGE**: Complex nested structures with maps and lists

#### Running Benchmarks

Run all benchmarks:
```console
./gradlew :examples:dynamodb-client:jmh
```

Run specific benchmarks:
```console
# Smithy Java benchmarks only
./gradlew :examples:dynamodb-client:jmh -Pjmh.includes=".*putItem$|.*getItem$"

# AWS SDK V2 benchmarks only
./gradlew :examples:dynamodb-client:jmh -Pjmh.includes=".*putItemV2|.*getItemV2"
```

The V2 benchmarks use the AWS SDK's actual `PutItemRequestMarshaller` and protocol factory response handlers to provide an accurate comparison of the SDK's internal marshalling/unmarshalling performance.


To actually run:

./gradlew :examples:dynamodb-client:jmhJar

```
java -jar /Users/alexwoo/smithy/smithy-java/examples/dynamodb-client/build/libs/dynamodb-client-0.0.3-jmh.jar
```

```
scp -i "benchmark-ec2.pem" /Users/alexwoo/smithy/smithy-java/examples/dynamodb-client/build/libs/dynamodb-client-0.0.3-jmh.jar ec2-user@ec2-54-215-237-240.us-west-1.compute.amazonaws.com:~/

```


```
java -Xms2g -Xmx2g -XX:+UseG1GC -jar dynamodb-client-0.0.3-jmh.jar DynamoDBSerde -w 1s -r 3s -f 4
```


