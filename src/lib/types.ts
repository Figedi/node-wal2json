export interface IWal2JSONOpts {
  slotName: string;
  pollTimeoutMs: number;
  temporary?: boolean;
  destroySlotOnClose?: boolean;
}

export interface IChange<T = Record<string, any>> {
  lsn: string;
  xid: string;
  timestamp: Date;
  operation: IUpsertOperation<T> | IDeleteOperation<T>;
}

export interface IUpsertOperation<T = Record<string, any>> {
  kind: "update" | "insert";
  schema: string;
  table: string;
  raw: IRawPGLogicalChange;
  parsed: T;
}
export interface IDeleteOperation<T = Record<string, any>> {
  kind: "delete";
  schema: string;
  table: string;
  raw: IRawPGLogicalChange;
  parsed: T;
}

export interface IRawPGLogicalChange {
  kind: "update" | "insert" | "delete";
  schema: string;
  table: string;
  columnnames: string[];
  columntypes: string[];
  columnvalues: string[];
  oldkeys?: {
    keynames: string[];
    keytypes: string[];
    keyvalues: any[];
  };
}

export interface IRawPGLogicalData {
  xid: string;
  timestamp: string;
  change: IRawPGLogicalChange[];
}

export interface IRawPGLogicalRow {
  lsn: string;
  xid: string;
  data: string;
}
