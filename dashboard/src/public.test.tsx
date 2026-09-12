import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { MemoryRouter, Route, Routes } from 'react-router-dom';
import { PublicChainPage, PublicHomePage } from './components';
import { CurlExample } from './CurlExample';

const overview = { process: { version: 'x', uptimeSeconds: 1 }, chains: { catalog: 2, pinned: 1, hot: 1, dormant: 0, disabled: 0, serving: 2, available: 2 }, endpoints: { materialized: 2, active: 2 }, traffic: { ingressTotal: 123, cacheHitsTotal: 100, cacheLookupsTotal: 110, upstreamTotal: 23 }, rpc: { pathTemplate: '/rpc/{chainId}' } };
const chains = { total: 2, items: [
  { chainId: 1, name: 'Ethereum', shortName: 'eth', isTestnet: false, nativeSymbol: 'ETH', status: 'active', state: 'available', catalogEndpoints: 2, endpoints: 2, active: 2, head: 100, ingressTotal: 10, cacheHitsTotal: 8, cacheLookupsTotal: 9 },
  { chainId: 143, name: 'Monad', shortName: 'mon', isTestnet: false, nativeSymbol: 'MON', status: 'deprecated', state: 'unverified', catalogEndpoints: 1, endpoints: 1, active: 1, head: 20, ingressTotal: 4, cacheHitsTotal: 3, cacheLookupsTotal: 4 },
] };

function renderWithRouter(element: React.ReactNode, initial = '/') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[initial]}><Routes><Route path="/chain/:id" element={element} /><Route path="*" element={element} /></Routes></MemoryRouter></QueryClientProvider>);
}

describe('public pages', () => {
  beforeEach(() => { vi.restoreAllMocks(); window.history.replaceState({}, '', '/'); });
  it('renders overview tiles and chain rows without Authorization', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input, init) => {
      expect(new Headers(init?.headers).get('Authorization')).toBeNull();
      return new Response(String(input).includes('overview') ? JSON.stringify(overview) : JSON.stringify(chains), { status: 200 });
    });
    renderWithRouter(<PublicHomePage />);
    await waitFor(() => expect(screen.getByText('Ethereum')).toBeInTheDocument());
    expect(screen.getByText('Chains available')).toBeInTheDocument(); expect(screen.getByText('123')).toBeInTheDocument(); expect(screen.getAllByRole('button', { name: 'Copy' }).length).toBeGreaterThan(1); expect(fetchMock).toHaveBeenCalled();
    expect(screen.getByText('Available')).toBeInTheDocument(); expect(screen.getByText('Unverified')).toBeInTheDocument(); expect(screen.getByText('Available (on demand)')).toBeInTheDocument(); expect(screen.getByText('deprecated')).toBeInTheDocument(); expect(screen.getByText(/Replace/)).toBeInTheDocument();
    const search = screen.getByRole('textbox', { name: 'Search chains' }); expect(search).toHaveAttribute('maxLength', '64');
    const before = fetchMock.mock.calls.length; fireEvent.change(search, { target: { value: 'eth' } }); expect(fetchMock).toHaveBeenCalledTimes(before); await waitFor(() => expect(fetchMock.mock.calls.length).toBeGreaterThan(before), { timeout: 1000 });
  });
  it('renders chain curl example with rpc path', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(JSON.stringify({ ...chains.items[0], explorerUrl: 'javascript:alert(1)' }), { status: 200 }));
    renderWithRouter(<PublicChainPage />, '/chain/1');
    await waitFor(() => expect(screen.getAllByText(/\/rpc\/1/).length).toBeGreaterThan(0));
    expect(screen.getByText(/eth_blockNumber/)).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: 'Block explorer' })).not.toBeInTheDocument();
  });
  it('handles clipboard rejection without an unhandled promise', async () => {
    const writeText = vi.fn().mockRejectedValue(new Error('denied')); Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
    render(<CurlExample chainId={1} />); fireEvent.click(screen.getByRole('button', { name: 'Copy' })); await waitFor(() => expect(writeText).toHaveBeenCalledOnce());
  });
});
