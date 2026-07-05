import { useEffect, useState } from 'react';
import { checkSpacetimeStatus } from './lib/spacetime';

export default function App() {
  const [status, setStatus] = useState('checking SpacetimeDB…');

  useEffect(() => {
    let cancelled = false;
    checkSpacetimeStatus().then((s) => {
      console.log('[spacetime]', s);
      if (!cancelled) {
        setStatus(s);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <main style={{ fontFamily: 'monospace', padding: '2rem' }}>
      <h1>projectino</h1>
      <p>real-time crypto market data pipeline — skeleton</p>
      <p>
        <strong>SpacetimeDB:</strong> {status}
      </p>
      {/* TODO: live data via generated module bindings; history via the Axum API */}
    </main>
  );
}
