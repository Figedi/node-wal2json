export class LogSequenceNumber {
  private constructor(public buffer: Buffer) {}

  public static INVALID_LSN = LogSequenceNumber.fromBuf(Buffer.alloc(0));

  public get empty(): boolean {
    return !this.buffer.length || this.buffer.readInt8() === 0;
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
    if (!this.buffer.length) {
      return "0/0";
    }

    const logicalXlog = this.buffer.readInt32BE(0);
    const segment = this.buffer.readInt32BE(4);
    return `${logicalXlog.toString(16)}/${segment.toString(16)}`;
  }

  public asInt64() {
    if (!this.buffer?.length) {
      return 0n;
    }
    return this.buffer.readBigInt64BE();
  }
}
