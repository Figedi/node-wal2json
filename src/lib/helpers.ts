import {
  IRawStreamingRow,
  IChange,
  IDeleteOperation,
  IRawPGLogicalData,
  IRawPGLogicalRow,
  IUpsertOperation,
} from "./types";

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
