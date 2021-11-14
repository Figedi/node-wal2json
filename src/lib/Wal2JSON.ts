import { Client, ClientConfig } from "pg";
import { Subject, from, defer, interval, Observable } from "rxjs";
import { takeUntil, mergeMap, switchMap, tap } from "rxjs/operators";
import {
  IChange,
  IDeleteOperation,
  IRawPGLogicalResult,
  IUpsertOperation,
  IWal2JSONOpts,
} from "./types";

export class Wal2JSON {
  private running = false;
  private client!: Client;
  public changes$?: Observable<IChange>;

  private stop$ = new Subject();

  constructor(
    clientOpts: string | ClientConfig | Client,
    private opts: IWal2JSONOpts
  ) {
    if (clientOpts instanceof Client) {
      this.client = clientOpts;
    } else {
      this.client = new Client(clientOpts);
    }
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

  private readChanges = (): Promise<IChange[]> => {
    if (!(this.client as any).readyForQuery) {
      return Promise.resolve([]);
    }
    return new Promise<IChange[]>((resolve, reject) =>
      this.client.query(
        `SELECT * FROM pg_logical_slot_get_changes('${this.opts.slotName}', NULL, NULL, 'include-timestamp', '1')`,
        (err, results) => {
          if (err) {
            this.stop(err);
            return reject(err);
          }
          return resolve(
            results.rows.flatMap((row) => {
              const data = JSON.parse(row.data) as IRawPGLogicalResult;

              return data.change.map((change) => {
                let operation: IDeleteOperation | IUpsertOperation;
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
                      {} as Record<string, any>
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
                      {} as Record<string, any>
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
            })
          );
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
    if (this.opts.destroySlotOnClose) {
      await this.destroyReplicationSlot();
      this.stop$.next(1);
    }
    await this.client.end();
    this.running = false;
  }

  public async start(): Promise<Observable<IChange>> {
    if (this.running) {
      await this.onError(
        new Error(
          "This listener is already running. If you would like to restart it use the restart method."
        )
      );
      // should throw
    }
    await this.client.connect();
    this.running = true;
    try {
      await this.initReplicationSlot();
      this.changes$ = interval(this.opts.timeout).pipe(
        takeUntil(this.stop$),
        switchMap(() =>
          defer(() => this.readChanges()).pipe(
            mergeMap((changes) => from(changes))
          )
        )
      );
      return this.changes$;
    } catch (e: any) {
      await this.onError(e);
      return null!; // never called, onError throws
    }
  }

  public async stop(error?: Error) {
    if (this.running) {
      if (error) {
        await this.onError(error); // calls close() as well
      } else {
        await this.close();
      }
    }
  }
}
