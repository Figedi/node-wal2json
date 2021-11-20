import { Duplex, DuplexOptions } from "stream";
import { Submittable, Connection } from "pg";

export interface IPGBaseReplicationOpts {
  slotName: string;
  temporary?: boolean;
  destroySlotOnClose?: boolean;
}

export interface IPGPollingReplicationOpts extends IPGBaseReplicationOpts {
  pollTimeoutMs: number;
}

export interface IPGStreamingReplicationOpts extends IPGBaseReplicationOpts {
  startLsn?: string;
  updateIntervalMs?: number;
  autoAckLsn?: boolean;
}

export interface IChange<T = Record<string, any>> {
  lsn: string;
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
  timestamp: Date;
  change: IRawPGLogicalChange[];
}

export interface IRawPGLogicalRow {
  lsn: string;
  data: string;
}
export interface IRawStreamingRow {
  timestamp: Date;
  lsn: string;
  data: Omit<IRawPGLogicalData, "timestamp">;
}

declare module "pg-copy-streams" {
  export function both(txt: string, options?: DuplexOptions): CopyBothQueryStream;

  export class CopyBothQueryStream extends Duplex implements Submittable {
    submit(connection: Connection): void;
  }
}
