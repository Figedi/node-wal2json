import { Client } from "pg";
import { Transform, pipeline, finished } from "stream";
import { both, CopyBothQueryStream } from "pg-copy-streams";
import _debug from "debug";

const debug = _debug("pglogical");
// eslint-disable-next-line no-underscore-dangle
const BufferList = require("obuf");

const POSTGRES_EPOCH_2000_01_01_BIGINT = 946684800000n; // milli-seconds <> 1970 -> 2000

const getNanoseconds = () => {
  return process.hrtime.bigint();
};

export class LogSequenceNumber {
  private constructor(public buf: Buffer) {}

  public static INVALID_LSN = LogSequenceNumber.fromBuf(Buffer.alloc(0));

  public get empty(): boolean {
    return !this.buf.length || this.buf.readInt8() === 0;
  }

  public static fromBuf(buf: Buffer): LogSequenceNumber {
    return new LogSequenceNumber(buf);
  }

  public static fromString(lsn: string): LogSequenceNumber {
    const [logicalXLogStr, segmentStr] = lsn.split("/");

    if (!logicalXLogStr || !segmentStr) {
      return LogSequenceNumber.INVALID_LSN;
    }

    const logicalXlog = parseInt(logicalXLogStr, 16);
    const segment = parseInt(segmentStr, 16);
    const buf = Buffer.alloc(8); // 8 bytes -> 1 int64
    buf.writeInt32BE(logicalXlog); // first 4 bytes
    buf.writeInt32BE(segment, 4); // next 4 bytes

    return new LogSequenceNumber(buf);
  }

  public toString() {
    if (!this.buf.length) {
      return "0/0";
    }

    const logicalXlog = this.buf.readInt32BE(0);
    const segment = this.buf.readInt32BE(4);
    return `${logicalXlog.toString(16)}/${segment.toString(16)}`;
  }

  public asInt64() {
    if (!this.buf?.length) {
      return 0n;
    }
    return this.buf.readBigInt64BE();
  }
}

const formatCenturyMicroToDate = (systemClockMicroSeconds: Buffer): Date => {
  const systemClockMillis = systemClockMicroSeconds.readBigInt64BE() / 1000n;

  return new Date(+(systemClockMillis + POSTGRES_EPOCH_2000_01_01_BIGINT).toString());
};

const nowAsMicroFromEpoch = (): bigint => {
  const systemClockMillis = (BigInt(Date.now()) - POSTGRES_EPOCH_2000_01_01_BIGINT) * 1000n;

  return systemClockMillis;
};

const PG_CODE = 1;
const PG_MESSAGE = 2;
class PgLogicalParser extends Transform {
  private buf = new BufferList();

  private state = PG_CODE;

  private code: number | null = null;

  private lastServerLSN: LogSequenceNumber = LogSequenceNumber.INVALID_LSN;

  private lastReceiveLSN: LogSequenceNumber = LogSequenceNumber.INVALID_LSN;

  public _flush() {}

  constructor(private cb: (data: any) => void) {
    super();
  }

  public _transform(chunk: Buffer, _encoding: string, callback: (error?: Error) => void) {
    this.buf.push(chunk);
    while (this.buf.size > 0) {
      if (this.state === PG_CODE) {
        if (!this.buf.has(1)) break;
        this.code = this.buf.readUInt8();
        this.state = PG_MESSAGE;
      }
      if (this.state === PG_MESSAGE) {
        if (this.code === 0x6b /*k*/) {
          this.lastServerLSN = LogSequenceNumber.fromBuf(this.buf.take(8));
          if (this.lastServerLSN.asInt64() > this.lastReceiveLSN.asInt64()) {
            this.lastReceiveLSN = this.lastServerLSN;
          }
          const systemClock: Buffer = this.buf.take(8);

          const replyRequired = this.buf.take(1).readUInt8() !== 0;

          this.cb({
            lastReceiveLSN: this.lastReceiveLSN,
            systemClock: formatCenturyMicroToDate(systemClock),
            replyRequired, // @todo either when this is true or when a configured timeout (based on the lastStatusUpdate diff -> update)
          });
          this.state = PG_CODE;
          // x
        } else if (this.code === 0x77 /*w*/) {
          this.lastReceiveLSN = LogSequenceNumber.fromBuf(this.buf.take(8));
          this.lastServerLSN = LogSequenceNumber.fromBuf(this.buf.take(8));
          const systemClock: Buffer = this.buf.take(8);

          const data = this.buf.take(this.buf.size); /* plugin data */

          this.cb({
            lastReceiveLSN: this.lastReceiveLSN,
            systemClock: formatCenturyMicroToDate(systemClock),
            data: JSON.parse(data.toString("utf-8")),
          });
          this.state = PG_CODE;
        } else {
          return callback(new Error("wrong message code inside"));
        }
      }
      break;
    }
    callback();
  }
}

export class PgLogical {
  private parser = new PgLogicalParser(data => this.onDataReceived(data));

  private copyBothStream!: CopyBothQueryStream;

  private lastReceiveLSN = LogSequenceNumber.INVALID_LSN;

  private lastFlushedLSN = LogSequenceNumber.INVALID_LSN;

  private lastAppliedLSN = LogSequenceNumber.INVALID_LSN;

  public lastStatusUpdate?: bigint;

  private interval?: NodeJS.Timer;

  constructor(private client: Client, private slotName: string, private startLsn: string) {
    this.lastReceiveLSN = LogSequenceNumber.fromString(startLsn);

    this.interval = setInterval(() => this.updateStatus(), 1000);
  }

  public start() {
    this.copyBothStream = both(`START_REPLICATION SLOT ${this.slotName} LOGICAL ${this.startLsn}`, {
      alignOnCopyDataFrame: true,
    } as any);
    this.client.query(this.copyBothStream);

    pipeline(this.copyBothStream, this.parser, error => {
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
  }

  public stop() {
    if (this.interval) {
      clearInterval(this.interval);
    }
    this.copyBothStream.end();
  }

  public updateStatus() {
    if (!this.lastReceiveLSN.empty) {
      return;
    }
    const buf = Buffer.alloc(1 + 8 + 8 + 8 + 8 + 1);
    buf.write("r"); // 0x72, indicates Standby status update
    buf.writeBigInt64BE(this.lastReceiveLSN.asInt64(), 1);
    buf.writeBigInt64BE(this.lastFlushedLSN.asInt64(), 9);
    buf.writeBigInt64BE(this.lastAppliedLSN.asInt64(), 17);
    buf.writeBigInt64BE(nowAsMicroFromEpoch(), 25);
    buf.writeInt8(1, 33); // no reply required

    this.copyBothStream.write(buf);

    this.lastStatusUpdate = getNanoseconds();
  }

  private onDataReceived({ lastReceiveLSN, systemClock, data }: any) {
    this.lastReceiveLSN = lastReceiveLSN;
    this.lastFlushedLSN = lastReceiveLSN;
    this.lastAppliedLSN = lastReceiveLSN;

    if (data) {
      console.log("onDataReceived", {
        timestamp: systemClock,
        lsn: lastReceiveLSN.toString(),
        data,
      });
    } else {
      // console.log("onHeartbeatReceived", { timestamp: systemClock, lsn: lastReceiveLSN.toString() });
    }
  }
}
