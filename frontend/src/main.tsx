import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { SpacetimeDBProvider } from 'spacetimedb/react';
import App from './App';
import { buildConnection } from './lib/spacetime';
import './index.css';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <SpacetimeDBProvider connectionBuilder={buildConnection()}>
      <App />
    </SpacetimeDBProvider>
  </StrictMode>,
);
