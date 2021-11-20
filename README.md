![CircleCI](https://img.shields.io/circleci/build/github/Figedi/node-wal2json)
![npm](https://img.shields.io/npm/v/@figedi/node-wal2json)

## node-wal2json

Small nodejs implementation for wal2json. Supports polling-mode ([via pg_logical_slot_get_changes](https://www.postgresql.org/docs/current/logicaldecoding-example.html)) or streaming-mode ([streaming-replication-protocol](https://www.postgresql.org/docs/current/protocol-replication.html)) to read changes from a given replication_slot

### Installation

```
npm i -s @figedi/node-wal2json
```

### Usage

In order to use this library, make sure that you have install all prerequisites:
1. Install [wal2json](https://github.com/eulerto/wal2json), follow the [configuration](https://github.com/eulerto/wal2json#configuration) setup
2. On your postgres-installation, create a user with `REPLICATION`-attribute
3. You are all set, run the library as follows:

#### Polling mode

```typescript
const clientOpts = { replication: "database", connectionString: "postgres://..." };
const wal2json = new PGPollingReplication(clientOpts, { 
  pollTimeoutMs: 100, // poll every 100ms
  slotName: "test_slot", // create slot-name 'test'-slot'
});


// the instance is a readable-stream, so it can be iterated over
for await (const change of wal2json) {
  console.log(change);
}
```

##### Options
The 2nd parameter of PGPollingReplication allows the following options:

| Opt  | Meaning |
| ------------- | ------------- |
| slotName  | The slotname for pg_logical, will be created if not exists  |
| pollTimeoutMs  | Interval for polling for changes in ms  |
| temporary  | (Optional) Creates a temporary slot bound to the connection  |
| destroySlotOnClose  | (Optional) If a permanent slot is chosen and client is stopped, destroy slot  |

-------

**Streaming mode**

```typescript
const clientOpts = { connectionString: "postgres://..." };
const wal2json = new PGStreamingReplication(clientOpts, {
  startLsn: "0/5711970", // start at given LSN
  updateIntervalMs: 1000, // send updates to pg roughly every 1000ms
  slotName: "test_slot", // use slot-name 'test'-slot'
});

// the instance is a readable-stream, so it can be iterated over
for await (const change of wal2json) {
  console.log(change);
}
```

The 2nd parameter of PGStreamingReplication allows the following options:

| Opt  | Meaning |
| ------------- | ------------- |
| slotName  | The slotname for pg_logical, will be created if not exists |
| updateIntervalMs  | (Optional) Interval for sending-back updates to pg, set 0 for reducing updates to the max |
| startLsn  | (Optional) The start-LSN for the replication, note when the slot is already advanced this is ignored |
| autoAckLsn  | (Optional) When set to true, set all received LSN directly as flushed upon next update-interval |
| temporary  | (Optional) Creates a temporary slot bound to the connection  |
| destroySlotOnClose  | (Optional) If a permanent slot is chosen and client is stopped, destroy slot |