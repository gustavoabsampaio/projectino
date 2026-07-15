// SpacetimeDB connection service.
//
// One place that builds the connection (the injectable-service equivalent for
// React): `buildConnection()` returns a configured `DbConnectionBuilder` that
// `<SpacetimeDBProvider>` builds and manages — it is StrictMode-safe and opens
// a single websocket. Components read live table data via the `useTable` hooks;
// no manual subscription is needed.
//
// API verified against the installed `spacetimedb` 2.6.1 SDK.

import { DbConnection } from '../module_bindings';

const WS_URI: string = import.meta.env.VITE_STDB_WS_URI ?? 'ws://localhost:3000';
const DB_NAME: string = import.meta.env.VITE_STDB_DB_NAME ?? 'projectino';

export function buildConnection() {
  return DbConnection.builder()
    .withUri(WS_URI)
    .withDatabaseName(DB_NAME)
    .onConnect((_conn, identity) =>
      console.log('[spacetime] connected as', identity.toHexString()),
    )
    .onDisconnect(() => console.log('[spacetime] disconnected'))
    .onConnectError((_ctx, error) =>
      console.error('[spacetime] connection error:', error),
    );
}
