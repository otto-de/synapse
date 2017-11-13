# Decisions & Open Questions

#### 1. Why does consuming all events take so long?

Retrieving records from Kinesis works by fetching a set of
records for a specific range of sequence numbers. Those 
numbers depend on time. So while consuming records, empty 
responses can occur. This happens if the records on the stream
have been written with more than a few minutes in-between.

Also, there is a limit on the rate of GetRecords requests
on a given Kinesis shard. The AWS documentation recommends
to wait at least 1 second before sending another request
to not exceed this rate limit.

See [AWS Documentation](http://docs.aws.amazon.com/de_de/streams/latest/dev/developing-consumers-with-sdk.html#kinesis-using-sdk-java-get-data-shard-iterators) 

#### 2. Why do we need @EnableEventSource and @EventSourceConsumer?

Good question. We removed @EnableEventSource. The EventSource is now created 
in EventSourceConsumerBeanPostProcessor.

#### 3. Why do I get the error: "Cannot register consumers for same streamName ... but with different payloadType"?

This error appears when the a developer registers multiple 
`@EventSourceConsumer` beans for the same stream name but with
different payload types. When an `@EventSourceConsumer` is registered, 
a `CompactingKinesisEventSource` instance is created with `payloadType` as a
parameter. If there are multiple consumers, the `CompactingKinesisEventSource`
is reused, but this does not work with different payload types. 