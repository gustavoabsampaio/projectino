// SpacetimeDB connection service (skeleton).
//
// The typed `DbConnection` client is produced by code generation off the Rust
// module — those bindings don't exist yet in this scaffold. Once the module is
// published, generate them with:
//
//   spacetime generate --lang typescript \
//     --out-dir frontend/src/module_bindings \
//     --project-path crates/spacetime-module
//
// then replace `checkSpacetimeStatus()` with a real connection (API verified
// against the `spacetimedb` 2.6 SDK readme):
//
//   import { DbConnection } from '../module_bindings';
//
//   export const connection = DbConnection.builder()
//     .withUri(WS_URI)
//     .withDatabaseName(DB_NAME)
//     .onConnect((_conn, identity) =>
//       console.log('[spacetime] connected as', identity.toHexString()))
//     .onConnectError(() => console.error('[spacetime] connect error'))
//     .onDisconnect(() => console.log('[spacetime] disconnected'))
//     .build();
//
// The SDK also ships React hooks under `spacetimedb/react`
// (SpacetimeDBProvider / useSpacetimeDB / useTable) — consider those once
// live subscriptions are wired up.

const HTTP_URI: string = import.meta.env.VITE_STDB_HTTP_URI ?? 'http://localhost:3000';
const DB_NAME: string = import.meta.env.VITE_STDB_DB_NAME ?? 'projectino';
export const WS_URI: string = import.meta.env.VITE_STDB_WS_URI ?? 'ws://localhost:3000';

/**
 * Placeholder connectivity check until module bindings exist.
 * `GET /v1/database/:name_or_identity` is a documented SpacetimeDB HTTP
 * endpoint: 200 means the server is up and the database is published.
 */
export async function checkSpacetimeStatus(): Promise<string> {
  try {
    const res = await fetch(`${HTTP_URI}/v1/database/${DB_NAME}`);
    if (res.ok) {
      return `SpacetimeDB reachable — database "${DB_NAME}" is published`;
    }
    if (res.status === 404) {
      return `SpacetimeDB reachable, but database "${DB_NAME}" is not published yet (run \`make module-publish\`)`;
    }
    return `SpacetimeDB responded with HTTP ${res.status}`;
  } catch {
    return `SpacetimeDB unreachable at ${HTTP_URI} — is \`docker compose up\` running?`;
  }
}
