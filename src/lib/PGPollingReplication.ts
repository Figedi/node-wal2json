import { Client, ClientConfig } from "pg";
import { Readable } from "stream";
import _debug from "debug";

const debug = _debug("wal2json");

import { IChange, IPGPollingReplicationOpts } from "./types";
import { destroyReplicationSlot, initReplicationSlot, normalizePollingResults } from "./helpers";

export class PGPollingReplication<T extends Record<string, any> = Record<string, any>> extends Readable {
  private running = false;

  private client!: Client;

  constructor(clientOpts: string | ClientConfig, private opts: IPGPollingReplicationOpts) {
    super({ objectMode: true });

    this.client = new Client(clientOpts);
    if (this.opts.pollTimeoutMs < 100) {
      debug(
        `Very short timeout (${this.opts.pollTimeoutMs}ms) chosen, this can lead to ` +
          "overwhelming your postgres instance",
      );
    }
  }

  public async start(): Promise<void> {
    debug(this.opts, `Trying to initialize service for replication_slot ${this.opts.slotName}`);
    try {
      await this.client.connect();
      await initReplicationSlot(this.client, this.opts.slotName, this.opts.temporary);
      this.running = true;
      debug(`Service successfully initialized with replication_slot ${this.opts.slotName}`);
      this.initPolling().catch(e => this.onError(e));
    } catch (e: any) {
      return this.onError(e);
    }
  }

  public _read() {}

  public async stop() {
    if (!this.running) {
      return this.onError(new Error("Service is not running yet, pleaase call start() firsts"));
    }
    return this.close();
  }

  private async initPolling() {
    while (this.running) {
      const changes = await this.readChanges();
      changes.forEach(change => this.push(change));

      await new Promise(resolve => setTimeout(resolve, this.opts.pollTimeoutMs));
    }
  }

  private readChanges = async (): Promise<IChange<T>[]> => {
    // @todo do not use internal apis from pg anymore
    // eslint-disable-next-line no-underscore-dangle
    if (!(this.client as any).readyForQuery || (this.client as any)._ending) {
      return Promise.resolve([]);
    }
    try {
      const results = await this.client.query(
        "SELECT * FROM pg_logical_slot_get_changes($1, NULL, NULL, 'include-timestamp', '1')",
        [this.opts.slotName],
      );
      const normalizedRows = normalizePollingResults<T>(results.rows);
      if (normalizedRows.length) {
        debug({ normalizedRows }, `Received ${normalizedRows.length} changed rows from postgres`);
      }
      return normalizedRows;
    } catch (e) {
      await this.onError(e);
      throw e;
    }
  };

  private async onError(e: Error) {
    debug(e);
    await this.close();
    throw e;
  }

  private async close() {
    debug("Gracefully closing service");
    this.running = false;
    if (this.opts.destroySlotOnClose) {
      await destroyReplicationSlot(this.client, this.opts.slotName);
    }
    await this.client.end();
    debug("Service successfully closed");
  }
}
