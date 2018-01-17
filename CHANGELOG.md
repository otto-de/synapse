# Release Notes

## 0.2.2
* `SnapshotReadService` now allows to set a local snapshot file to read from instead retrieving it from AWS S3.   
This functionality was moved from `SnapshotEventSource` and also works for a `CompactingKinesisEventSource` now.

## 0.2.1
* Provide ```EncryptedOrPlainJsonTextEncryptor``` that checks whether data is plain json or is encrypted.

## 0.2.0
* Add option to send unencrypted events to ```KinesisEventSender.sendEvent(String, Object, boolean)``` 
and ```KinesisEventSender.sendEvents(Map<String,Object>, boolean)```

## 0.1.11
* Fix statistics
* Clear state repository after compaction job

## 0.1.1
* Released to keep things stable

## 0.1.0
**Initial Release**
