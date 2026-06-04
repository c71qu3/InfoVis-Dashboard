async function fetchJson(url) {
  const res = await fetch(url);
  return await res.json();
}

export async function fetchConnectionCounts() {
  try {
    const data = await fetchJson('/api/connections');
    console.log('Loaded connection counts for', Object.keys(data || {}).length, 'codes');
    return data || {};
  } catch (e) {
    console.warn('Could not load connection counts', e);
    return {};
  }
}

export async function fetchOutgoingCounts() {
  try {
    const data = await fetchJson('/api/outgoing_connections');
    console.log('Loaded outgoing edge counts for', Object.keys(data || {}).length, 'codes');
    return data || {};
  } catch (e) {
    console.warn('Could not load outgoing edge counts', e);
    return {};
  }
}

export async function fetchYears(iso2) {
  try {
    return await fetchJson(`/api/years/${iso2}`);
  } catch (e) {
    console.warn(e);
    return [];
  }
}

export async function fetchIndicators(iso2, yearMode = 'latest') {
  let url = `/api/indicators/${iso2}`;
  if (yearMode === 'latest') url += '?mode=latest';
  else url += `?mode=${encodeURIComponent(yearMode)}`;

  return await fetchJson(url);
}

export async function fetchEntityGraph(iso3) {
  return await fetchJson(`/api/graph/${iso3}`);
}

export async function fetchJurisdictions(focus = null) {
  const url = focus ? `/api/jurisdictions?focus=${encodeURIComponent(focus)}` : '/api/jurisdictions';
  return await fetchJson(url);
}

export async function fetchEntities(iso3, { limit = 200, offset = 0, q = null } = {}) {
  const params = new URLSearchParams();
  if (limit != null) params.set('limit', String(limit));
  if (offset != null) params.set('offset', String(offset));
  if (q) params.set('q', q);

  const qs = params.toString();
  const url = qs ? `/api/entities/${encodeURIComponent(iso3)}?${qs}` : `/api/entities/${encodeURIComponent(iso3)}`;
  return await fetchJson(url);
}
