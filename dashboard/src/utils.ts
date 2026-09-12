import type { ChainRow, ChainState, Overview } from './api/types';
export const statusMeta = (state: ChainState) => ({ active: ['#0ca30c', 'Active'], hot: ['#0ca30c', 'Hot'], pinned: ['#0ca30c', 'Pinned'], probation: ['#fab219', 'Probation'], cooling: ['#ec835a', 'Cooling'], error: ['#d03b3b', 'Error'], dormant: ['#8b8982', 'Dormant'], disabled: ['#8b8982', 'Disabled'], available: ['#0ca30c', 'Available'], unverified: ['#8b8982', 'Unverified'] } as Record<string, [string,string]>)[state] || ['#8b8982', state];
export const qpsFromSnapshots = (previous: Overview | undefined, current: Overview, elapsedSeconds: number) => previous && elapsedSeconds > 0 ? Math.max(0, current.traffic.ingressTotal - previous.traffic.ingressTotal) / elapsedSeconds : 0;
export const hitRate = (x: Overview['traffic']) => x.cacheLookupsTotal ? x.cacheHitsTotal / x.cacheLookupsTotal * 100 : 0;
export function filterSortChains(rows: ChainRow[], q: string, sort: 'traffic'|'chainId'|'name') {
  const needle=q.toLowerCase(); const filtered=rows.filter(r=>!needle||r.name.toLowerCase().includes(needle)||r.shortName?.toLowerCase().includes(needle)||String(r.chainId).includes(needle));
  return [...filtered].sort((a,b)=>sort==='traffic'?b.ingressTotal-a.ingressTotal:sort==='name'?a.name.localeCompare(b.name):a.chainId-b.chainId);
}
export const rpcUrl = (origin: string, chainId: string | number) => `${origin.replace(/\/+$/, '')}/rpc/${chainId}`;
export const isHttpUrl = (value: string | undefined) => { if (!value) return false; try { return ['http:', 'https:'].includes(new URL(value).protocol) } catch { return false } };
export const curlExample = (origin: string, chainId: string | number, method = 'eth_blockNumber', params: unknown[] = []) => `curl -sS ${rpcUrl(origin, chainId)} \\\n  -H 'content-type: application/json' \\\n  --data '${JSON.stringify({ jsonrpc: '2.0', id: 1, method, params })}'`;
