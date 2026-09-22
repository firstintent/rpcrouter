import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { ChainDetailPage, PublicChainPage } from './components';

const stats = { outboundRequests: 4, failures: 0, rateLimited: 0, coolingEvents: 0, probeSuccesses: 2 };
const chain = {
  chainId: 137, name: 'Polygon', shortName: 'pol', isTestnet: false, state: 'pinned', pinned: true, disabled: false,
  catalogEndpoints: 2, endpoints: 2, active: 2, cooling: 0, probation: 0, head: 80_000_000, lastIngressUnix: 0,
  ingressTotal: 10, cacheHitsTotal: 4, cacheLookupsTotal: 8, upstreamTotal: 6, userVisibleErrorsTotal: 0,
  settings: { source: 'config', blockTimeMs: 2000, confirmationDepth: 128, tipTtlMs: 2000, maxBlockLag: 5 },
  endpointRows: [
    { url: 'https://polygon-rpc.example', state: 'active', strikes: 0, latencyEwmaMs: 12.5, archive: 'yes', archiveLatencyEwmaMs: 80.2, lag: 0, rps: 15, concurrency: 8, disabled: false, source: 'chainlist', stats },
    { url: 'https://full-node.example', state: 'active', strikes: 0, latencyEwmaMs: 9, archive: 'no', archiveLatencyEwmaMs: 41, lag: 1, rps: 15, concurrency: 8, disabled: false, source: 'chainlist', stats },
  ],
  fallback: { url: 'https://example.quiknode.pro/<redacted>', state: 'probation', latencyEwmaMs: 21.4, archive: 'yes', archiveLatencyEwmaMs: 90, strikes: 0, stats },
};
const stateInfo = { backend: 'memory', namespace: 'rpcrouter', instanceId: 'test', up: true, writable: true, schemaVersion: 1, dirtyEndpoints: 0, lastFlushUnix: 0, lastFlushDurationMs: 0, lastPingUnix: 0 };

function renderAt(path: string, element: React.ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route path="/dashboard/chains/:id" element={element} />
          <Route path="/chain/:id" element={element} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('chain detail archive and fallback', () => {
  beforeEach(() => vi.restoreAllMocks());

  it('shows archive columns and a redacted paid fallback', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
      const url = String(input);
      const body = url.includes('/admin/api/state') ? stateInfo : chain;
      return new Response(JSON.stringify(body), { status: 200 });
    });
    renderAt('/dashboard/chains/137', <ChainDetailPage />);
    await waitFor(() => expect(screen.getByRole('heading', { name: 'Paid fallback' })).toBeInTheDocument());
    expect(screen.getByRole('columnheader', { name: 'Archive' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Archive latency' })).toBeInTheDocument();
    expect(screen.getByText('https://polygon-rpc.example')).toBeInTheDocument();
    expect(screen.getByText('80.2 ms')).toBeInTheDocument();
    expect(screen.getByText('41.0 ms')).toBeInTheDocument();
    expect(screen.getByText('https://example.quiknode.pro/<redacted>')).toBeInTheDocument();
    expect(screen.getByText(/archive yes/)).toBeInTheDocument();
    expect(screen.queryByText(/secret-token/)).not.toBeInTheDocument();
    expect(screen.getAllByText('no').length).toBeGreaterThan(0);
  });

  it('does not render a fallback card when the chain has none', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
      const url = String(input);
      const body = url.includes('/admin/api/state') ? stateInfo : { ...chain, fallback: undefined };
      return new Response(JSON.stringify(body), { status: 200 });
    });
    renderAt('/dashboard/chains/137', <ChainDetailPage />);
    await waitFor(() => expect(screen.getByRole('columnheader', { name: 'Archive' })).toBeInTheDocument());
    expect(screen.queryByRole('heading', { name: 'Paid fallback' })).not.toBeInTheDocument();
  });

  it('keeps the paid endpoint off the public chain page', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({
      chainId: 137, name: 'Polygon', state: 'available', endpoints: 2, active: 2, head: 80_000_000,
    }), { status: 200 }));
    renderAt('/chain/137', <PublicChainPage />);
    await waitFor(() => expect(screen.getByRole('heading', { name: 'Polygon' })).toBeInTheDocument());
    expect(screen.queryByText(/Paid fallback/)).not.toBeInTheDocument();
    expect(screen.queryByText(/quiknode/i)).not.toBeInTheDocument();
  });
});
