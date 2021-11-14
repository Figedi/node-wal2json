![CircleCI](https://img.shields.io/circleci/build/github/Figedi/node-wal2json)
![npm](https://img.shields.io/npm/v/@figedi/node-wal2json)

## node-wal2json

Small nodejs implementation for wal2json. Uses polling and `pg_logical_slot_get_changes` to read changes from a given replication_slot

### Installation

```
npm i -s @figedi/node-wal2json
```

### Usage

In order to use this library, make sure that you have install all prerequisites:
1. Install [wal2json](https://github.com/eulerto/wal2json), follow the [configuration](https://github.com/eulerto/wal2json#configuration) setup
2. On your postgres-installation, create a user with `REPLICATION`-attribute
3. You are all set, run the library as follows:
```typescript
// initialize
const wal2json = new Wal2JSON("postgres://<repl_user>:<password>@<host>/<db>", {
  pollTimeoutMs: 100, // poll every 100ms
  slotName: "test_slot", // create slot-name 'test'-slot'
});

// read as observable
wal2json.asObservable().pipe(tap(console.log)).subscribe();

// OR: read as async iterable
for await (const change of wal2json.asAsyncIterator()) {
  console.log(change);
}
```

### Options
The reader can be initialized as follows:
```typescript
// plain postgres://-connection-string
const wal2json = new Wal2JSON('<connection-string>', opts);
// pg-client options
const wal2json = new Wal2JSON({ url: '...' }, opts);

// a pg-client instance
const wal2json = new Wal2JSON(client, opts);
```

Opts has the following properties:

| Opt  | Meaning |
| ------------- | ------------- |
| slotName  | The slotname for pg_logical, will be created if not exists  |
| pollTimeoutMs  | Interval for polling for changes in ms  |
| temporary  | Creates a temporary slot bound to the connection  |
| destroySlotOnClose  | (Optional) If a permanent slot is chosen and client is stopped, destroy slot  |

