import { Client } from "pg";
import { Transform, pipeline, finished } from "stream";
import { both, CopyBothQueryStream } from "pg-copy-streams";
import _debug from "debug";
import assert from "assert";

import { formatCenturyMicroToDate, getNanoseconds, normalizeStreamResults, nowAsMicroFromEpoch } from "./helpers";
import { IPGStreamingReplicationOpts } from "./types";
import { LogSequenceNumber } from "./LogSequenceNumber";

const debug = _debug("pglogical");
const BufferList = require("obuf");

const PG_CODE = 1;
const PG_MESSAGE = 2;

export class PGStreamingReplication<T extends Record<string, any> = Record<string, any>> extends Transform {
  private started = false;

  private buf!: any;

  private state = PG_CODE;

  private code: number | null = null;

  private copyBothStream!: CopyBothQueryStream;

  private lastStatusUpdate!: bigint;

  private updateIntervalMs!: number;

  private lastServerLSN = LogSequenceNumber.INVALID_LSN;

  private lastReceivedLSN = LogSequenceNumber.INVALID_LSN;

  private lastFlushedLSN = LogSequenceNumber.INVALID_LSN;

  private lastAppliedLSN = LogSequenceNumber.INVALID_LSN;

  constructor(private client: Client, private opts: IPGStreamingReplicationOpts) {
    super({ objectMode: true });
    assert(
      (client as any).connectionParameters.replication === "database",
      "Need to pass replication: 'database' to pg-client",
    );
    // eslint-disable-next-line no-underscore-dangle
    assert((client as any)._connected, "Need to pass a connected client, please call client.connect() first");
    this.buf = new BufferList();
    this.updateIntervalMs = this.opts.updateIntervalMs ?? 0;
    this.lastReceivedLSN = this.opts.startLsn
      ? LogSequenceNumber.fromString(this.opts.startLsn)
      : LogSequenceNumber.INVALID_LSN;
    this.lastStatusUpdate = getNanoseconds() - BigInt(this.updateIntervalMs) * 1000000n;
  }

  public _flush() {}

  /**
   * Main-hook for receiving chunks via the upstream copy-stream. See sub-package handlers
   * for the behaviour. Mostly copied from pg-copy-streams example
   */
  public _transform(chunk: Buffer, _encoding: string, next: (error?: Error) => void) {
    this.buf.push(chunk);
    while (this.buf.size > 0) {
      if (this.state === PG_CODE) {
        if (!this.buf.has(1)) break;
        this.code = this.buf.readUInt8();
        this.state = PG_MESSAGE;
      }
      if (this.state === PG_MESSAGE) {
        if (this.code === 0x6b /*k*/) {
          this.onKeepAlivePacketReceived();
          // x
        } else if (this.code === 0x77 /*w*/) {
          this.onXLogDataPacketReceived();
        } else {
          return next(new Error("wrong message code inside"));
        }
      }

      break;
    }
    next();
  }

  /**
   * Starts consumption of replication-logs
   */
  public async start() {
    if (this.started) {
      throw new Error("Cannot call PGStreamingReplication#start() twice, please call stop() first");
    }
    const startLsn = this.opts.startLsn ?? (await this.getStartLsn());
    this.copyBothStream = both(`START_REPLICATION SLOT ${this.opts.slotName} LOGICAL ${startLsn}`, {
      alignOnCopyDataFrame: true,
    } as any);
    this.client.query(this.copyBothStream);

    pipeline(this.copyBothStream, this, error => {
      if (error) {
        debug({ error }, "Unexpected error in parser-stream");
      } else {
        debug("Parser-stream ended");
      }
    });
    finished(this.copyBothStream, error => {
      if (error) {
        debug({ error }, "Unexpected error in copy-stream");
      } else {
        debug("Copy-stream ended");
      }
      this.client.end();
    });

    this.started = true;
  }

  /**
   * Stops replication-log consumption by stopping the upstream copy-stream
   */
  public stop() {
    this.copyBothStream.end();
  }

  /**
   * Set flushed LSN.
   * This parameter will be sent to backend on next update status iteration.
   * Flushed LSN position help backend define which WAL can be recycled.
   */
  public setLastFlushedLSN(lsn: LogSequenceNumber) {
    this.lastFlushedLSN = lsn;
  }

  /**
   * Set last-applied LSN
   * Inform backend which LSN has been applied on standby.
   * Feedback will send to backend on next update status iteration.
   */
  public setLastAppliedLSN(lsn: LogSequenceNumber) {
    this.lastAppliedLSN = lsn;
  }

  private async getStartLsn(): Promise<string> {
    const client = new Client({
      user: this.client.user,
      password: this.client.password,
      host: this.client.host,
      port: this.client.port,
      database: this.client.database,
    });
    try {
      await client.connect();
      const {
        rows: [row],
      } = await client.query("SELECT pg_current_wal_lsn()");
      if (!row.pg_current_wal_lsn) {
        throw new Error(`Did not find last pg_current_wal_lsn for slot-name ${this.opts.slotName}`);
      }
      this.lastReceivedLSN = LogSequenceNumber.fromString(row.pg_current_wal_lsn);
      return row.pg_current_wal_lsn;
    } finally {
      await client.end();
    }
  }

  /**
   * Processes an xlog-data packet from pg by normalizing the logical replication
   * data as json
   */
  private onXLogDataPacketReceived() {
    this.lastReceivedLSN = LogSequenceNumber.fromBuf(this.buf.take(8));
    this.lastServerLSN = LogSequenceNumber.fromBuf(this.buf.take(8));
    const systemClock: Buffer = this.buf.take(8);
    const data = this.buf.take(this.buf.size);

    debug({ systemClock, lastReceiveLSN: this.lastReceivedLSN }, "Received XLogData message");

    if (this.updateDue()) {
      this.updateStatus();
    }

    const normalizedRow = normalizeStreamResults<T>({
      timestamp: formatCenturyMicroToDate(systemClock),
      lsn: this.lastReceivedLSN.toString(),
      data: JSON.parse(data.toString("utf-8")),
    });
    this.push(normalizedRow);

    this.state = PG_CODE;
  }

  /**
   * Processes a keep-alive packet from pg. If pg indicates a status update is
   * required or the set up interval is due, this method sends back the currently
   * consumed LSN's
   */
  private onKeepAlivePacketReceived() {
    this.lastServerLSN = LogSequenceNumber.fromBuf(this.buf.take(8));
    const systemClock: Buffer = this.buf.take(8);
    const replyRequired = this.buf.take(1).readUInt8() !== 0;

    if (this.lastServerLSN.asInt64() > this.lastReceivedLSN.asInt64()) {
      this.lastReceivedLSN = this.lastServerLSN;
    }

    debug({ systemClock, replyRequired, lastReceiveLSN: this.lastReceivedLSN }, "Received Primary keepalive message");

    if (replyRequired || this.updateDue()) {
      this.updateStatus();
    }

    this.state = PG_CODE;
  }

  /**
   * Determines whether an update is due based on the last
   * update and the configured status-updates. If updateIntervalMs
   * is set to 0, no update will be performed (unless required from pg)
   */
  private updateDue(): boolean {
    if (this.updateIntervalMs == 0) {
      return false;
    }
    const diffMs = (getNanoseconds() - this.lastStatusUpdate) / BigInt(1e6);
    return diffMs >= this.updateIntervalMs;
  }

  /**
   * Sends back to pg a 'Standby status update'. This mainly
   * acknowledges a received-lsn by the stream
   */
  private updateStatus() {
    if (!this.lastReceivedLSN.empty) {
      return;
    }
    if (this.opts.autoAckLsn) {
      this.lastFlushedLSN = this.lastReceivedLSN;
      this.lastAppliedLSN = this.lastReceivedLSN;
    }
    const buf = Buffer.alloc(1 + 8 + 8 + 8 + 8 + 1);
    buf.write("r"); // 0x72, indicates Standby status update
    buf.writeBigInt64BE(this.lastReceivedLSN.asInt64(), 1);
    buf.writeBigInt64BE(this.lastFlushedLSN.asInt64(), 9);
    buf.writeBigInt64BE(this.lastAppliedLSN.asInt64(), 17);
    buf.writeBigInt64BE(nowAsMicroFromEpoch(), 25);
    buf.writeInt8(1, 33); // no reply required

    this.copyBothStream.write(buf);

    this.lastStatusUpdate = getNanoseconds();
  }
}
