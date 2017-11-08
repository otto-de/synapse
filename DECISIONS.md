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
