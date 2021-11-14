import { Client, ClientConfig } from "pg";
import { EMPTY, from, Observable } from "rxjs";

import {
  IRawPGLogicalData,
  IRawPGLogicalRow,
  IChange,
  IDeleteOperation,
  IUpsertOperation,
  IWal2JSONOpts,
} from "./types";

export class Wal2JSON<T extends Record<string, any> = Record<string, any>> {
  private running = false;
  private client!: Client;

  constructor(
    clientOpts: string | ClientConfig | Client,
    private opts: IWal2JSONOpts
  ) {
    if (clientOpts instanceof Client) {
      this.client = clientOpts;
    } else {
      this.client = new Client(clientOpts);
    }
    if (this.opts.timeout < 100) {
      console.warn(
        `Very short timeout (${this.opts.timeout}ms) chosen, this can lead to overwhelming your postgres instance`
      );
    }
  }

  public asAsyncIterator(): AsyncIterable<IChange<T>> {
    if (this.running) {
      void this.onError(
        new Error(
          "Service is already running, please call stop() first if you wish to consume via Async-iterators"
        )
      );
      return {
        async *[Symbol.asyncIterator]() {}, // empty async-iteratable, returns right away
      };
    }
    this.running = true;
    const self = this;
    return {
      async *[Symbol.asyncIterator]() {
        await self.start();
        let changes: IChange<T>[];
        while (self.running) {
          await new Promise((resolve) =>
            setTimeout(resolve, self.opts.timeout)
          );
          changes = await self.readChanges();
          for (const change of changes) {
            yield change;
          }
        }
      },
    };
  }

  public asObservable(): Observable<IChange<T>> {
    if (this.running) {
      void this.onError(
        new Error(
          "Service is already running, please call stop() first if you wish to consume via Observables"
        )
      );
      return EMPTY;
    }

    return from(this.asAsyncIterator());
  }

  private async initReplicationSlot() {
    const results = await this.client.query(
      "SELECT * FROM pg_replication_slots WHERE slot_name = $1",
      [this.opts.slotName]
    );
    if (!results.rows.length) {
      await this.client.query(
        "SELECT pg_create_logical_replication_slot($1, 'wal2json')",
        [this.opts.slotName]
      );
    }
  }
  private async destroyReplicationSlot() {
    await this.client.query("SELECT pg_drop_replication_slot($1)", [
      this.opts.slotName,
    ]);
  }

  private normalizeResults<T>(rows: IRawPGLogicalRow[]): IChange<T>[] {
    return rows.flatMap((row) => {
      const data = JSON.parse(row.data) as IRawPGLogicalData;

      return data.change.map((change) => {
        let operation: IDeleteOperation<T> | IUpsertOperation<T>;
        if (change.kind === "delete") {
          operation = {
            kind: change.kind,
            schema: change.schema,
            table: change.table,
            raw: change,
            parsed: change.oldkeys!.keynames.reduce(
              (acc: any, name: string, i: number) => ({
                ...acc,
                [name]: change.oldkeys!.keyvalues[i],
              }),
              {} as T
            ),
          };
        } else {
          operation = {
            kind: change.kind,
            schema: change.schema,
            table: change.table,
            raw: change,
            parsed: change.columnnames.reduce(
              (acc: any, name: string, i: number) => ({
                ...acc,
                [name]: change.columnvalues[i],
              }),
              {} as T
            ),
          };
        }
        return {
          lsn: row.lsn,
          xid: row.xid,
          timestamp: new Date(data.timestamp),
          operation,
        };
      });
    });
  }

  private readChanges = (): Promise<IChange<T>[]> => {
    if (!(this.client as any).readyForQuery || (this.client as any)._ending) {
      return Promise.resolve([]);
    }
    return new Promise<IChange<T>[]>((resolve, reject) =>
      this.client.query(
        `SELECT * FROM pg_logical_slot_get_changes('${this.opts.slotName}', NULL, NULL, 'include-timestamp', '1')`,
        (err, results) => {
          if (err) {
            return this.onError(err).then(() => reject(err));
          }
          return resolve(this.normalizeResults<T>(results.rows));
        }
      )
    );
  };

  private async onError(e: Error) {
    console.error(e);
    await this.close();
    throw e;
  }

  private async close() {
    this.running = false;
    if (this.opts.destroySlotOnClose) {
      await this.destroyReplicationSlot();
    }
    await this.client.end();
  }

  private async start(): Promise<void> {
    try {
      await this.client.connect();
      await this.initReplicationSlot();
    } catch (e: any) {
      return this.onError(e);
    }
  }

  public async stop() {
    if (!this.running) {
      return this.onError(
        new Error("Service is not running yet, pleaase call start() firsts")
      );
    }
    return this.close();
  }
}
