# Release Notes

## 0.6.3
* Using key-value pairs in (some) log messages

## 0.6.2
* Speedup snapshot creation and log progress

## 0.6.1
* Introduce special SnapshotEventSourceNotification that additionally holds the timestamp of snapshot creation.

## 0.5.0 Major Refactoring
* Renamed project to OTTO Synapse:
  * eventsourcing-core -> synapse-core
  * eventsourcing-aws -> synapse-aws
  * eventsourcing-edison-integration -> synapse-edison
* Renamed packages to de.otto.synapse.*
* Renamed properties to synapse.*
* Introduced eventsourcing-aws and removed aws-specific parts from eventsourcing-/synapse-core
* Renamed Event to Message and removed EventBody

## 0.4.8
* **[eventsourcing-edison-integration]** Add health indicator for stream state.
 This means that a service goes unhealthy if a stream is in a not-recoverable state.
* **[eventsourcing-core]** Fix retry policy in `KinesisShardIterator` to retry
 also on connection errors, not only on throughput exceed errors. 

## 0.4.7
Remove `Clock` bean as this is required only for tests.
Don't set StatusDetailIndicator to warn when kinesis consumer has finished.

## 0.4.6
Add `Clock` bean that is required by `EventSourcingStatusDetailIndicator` 

## 0.4.5
* New sub project `eventsourcing-edison-integration`. This project contains a `StatusDetailIndicator` that provides 
StatusDetail information for each EventSource.

## 0.4.4
* Also publish EventSourceNotification Application Events for Kinesis and InMemory EventSources

## 0.4.3
* Remove further ChronicleMap closed errors on shutdown. 

## 0.4.2
* Prevent ChronicleMap closed errors on shutdown.

## 0.4.1
* Add `sendEvents` method to `EventSender` interface.

## 0.4.0
* Breaking changes:
  * Change signature of `KinesisEventSender.sendEvents`. Send events takes a list of `EventBody` now
  * Split Event class into `Event` and `EventBody`
  * `CompactionService` now requires a `StateRepository` with name `compactionStateRepository` 
* InMemory EventSender and EventSource for testing


## 0.3.0
* Events with "null"-payload will delete the entry.
* Remove client side encryption because kinesis now supports server side encryption.

## 0.2.2
* `SnapshotReadService` now allows to set a local snapshot file to read from instead retrieving it from AWS S3.   
This functionality was moved from `SnapshotEventSource` and also works for a `CompactingKinesisEventSource` now.

## 0.2.1
* Provide `EncryptedOrPlainJsonTextEncryptor` that checks whether data is plain json or is encrypted.

## 0.2.0
* Add option to send unencrypted events to `KinesisEventSender.sendEvent(String, Object, boolean)` 
and `KinesisEventSender.sendEvents(Map<String,Object>, boolean)`

## 0.1.11
* Fix statistics
* Clear state repository after compaction job

## 0.1.1
* Released to keep things stable

## 0.1.0
**Initial Release**
