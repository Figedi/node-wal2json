import { Client } from "pg";
import {
  IRawStreamingRow,
  IChange,
  IDeleteOperation,
  IRawPGLogicalData,
  IRawPGLogicalRow,
  IUpsertOperation,
} from "./types";

const POSTGRES_EPOCH_2000_01_01_BIGINT = 946684800000n; // milli-seconds <> 1970 -> 2000

export const getNanoseconds = () => {
  return process.hrtime.bigint();
};

export const formatCenturyMicroToDate = (systemClockMicroSeconds: Buffer): Date => {
  const systemClockMillis = systemClockMicroSeconds.readBigInt64BE() / 1000n;

  return new Date(+(systemClockMillis + POSTGRES_EPOCH_2000_01_01_BIGINT).toString());
};

export const nowAsMicroFromEpoch = (): bigint => {
  const systemClockMillis = (BigInt(Date.now()) - POSTGRES_EPOCH_2000_01_01_BIGINT) * 1000n;

  return systemClockMillis;
};

export const initReplicationSlot = async (client: Client, slotName: string, temporary?: boolean) => {
  const results = await client.query("SELECT * FROM pg_replication_slots WHERE slot_name = $1", [slotName]);
  if (!results.rows.length) {
    await client.query("SELECT pg_create_logical_replication_slot($1, 'wal2json', $2)", [slotName, temporary ?? false]);
  }
};

export const destroyReplicationSlot = async (client: Client, slotName: string) => {
  await client.query("SELECT pg_drop_replication_slot($1)", [slotName]);
};

const convertRawPgLogicalData = <T>(lsn: string, data: IRawPGLogicalData) => {
  return data.change.map(change => {
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
          {} as T,
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
          {} as T,
        ),
      };
    }
    return {
      lsn,
      timestamp: new Date(data.timestamp),
      operation,
    };
  });
};

export const normalizeStreamResults = <T>(row: IRawStreamingRow): IChange<T>[] => {
  return convertRawPgLogicalData(row.lsn, { ...row.data, timestamp: row.timestamp });
};

export const normalizePollingResults = <T>(rows: IRawPGLogicalRow[]): IChange<T>[] => {
  return rows.flatMap(row => {
    const data = JSON.parse(row.data) as IRawPGLogicalData;

    return convertRawPgLogicalData(row.lsn, data);
  });
};
