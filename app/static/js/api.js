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

// Intermediary list view (for the right-side list panel)
export async function fetchIntermediaries(iso3, { limit = 200, offset = 0, q = null, type = null } = {}) {
  const params = new URLSearchParams();
  if (limit != null) params.set('limit', String(limit));
  if (offset != null) params.set('offset', String(offset));
  if (q) params.set('q', q);
  if (type && type !== 'all') params.set('type', type);

  const qs = params.toString();
  const url = qs
    ? `/api/intermediaries/${encodeURIComponent(iso3)}?${qs}`
    : `/api/intermediaries/${encodeURIComponent(iso3)}`;
  return await fetchJson(url);
}

export async function fetchEntityFocusedGraph(entityId, {
  // Connected-subgraph controls (preferred)
  depth = 4,
  max_nodes = 5000,
  max_rels = 20000,

  // Backward-compatible params (no longer required; server accepts them)
  intermediaries = null,
  per_i = null,
  officers = null,
} = {}) {
  const params = new URLSearchParams();

  if (depth != null) params.set('depth', String(depth));
  if (max_nodes != null) params.set('max_nodes', String(max_nodes));
  if (max_rels != null) params.set('max_rels', String(max_rels));

  if (intermediaries != null) params.set('intermediaries', String(intermediaries));
  if (per_i != null) params.set('per_i', String(per_i));
  if (officers != null) params.set('officers', String(officers));

  const qs = params.toString();
  const url = qs
    ? `/api/entity_graph/${encodeURIComponent(entityId)}?${qs}`
    : `/api/entity_graph/${encodeURIComponent(entityId)}`;

  return await fetchJson(url);
}

export async function fetchIntermediaryFocusedGraph(intermediaryId, {
  // Connected-subgraph controls (preferred)
  depth = 4,
  max_nodes = 5000,
  max_rels = 20000,

  // Backward-compatible params (iso3 is intentionally ignored server-side now)
  iso3 = null,
  entities = null,
  officers = null,
} = {}) {
  const params = new URLSearchParams();

  if (depth != null) params.set('depth', String(depth));
  if (max_nodes != null) params.set('max_nodes', String(max_nodes));
  if (max_rels != null) params.set('max_rels', String(max_rels));

  // Keep these for backward compatibility with older servers/clients.
  if (iso3) params.set('iso3', String(iso3));
  if (entities != null) params.set('entities', String(entities));
  if (officers != null) params.set('officers', String(officers));

  const qs = params.toString();
  const url = qs
    ? `/api/intermediary_graph/${encodeURIComponent(intermediaryId)}?${qs}`
    : `/api/intermediary_graph/${encodeURIComponent(intermediaryId)}`;

  return await fetchJson(url);
}

export async function fetchOfficerFocusedGraph(officerId, {
  // Connected-subgraph controls (preferred)
  depth = 4,
  max_nodes = 5000,
  max_rels = 20000,

  // Backward-compatible params
  entities = null,
  intermediaries = null,
} = {}) {
  const params = new URLSearchParams();

  if (depth != null) params.set('depth', String(depth));
  if (max_nodes != null) params.set('max_nodes', String(max_nodes));
  if (max_rels != null) params.set('max_rels', String(max_rels));

  if (entities != null) params.set('entities', String(entities));
  if (intermediaries != null) params.set('intermediaries', String(intermediaries));

  const qs = params.toString();
  const url = qs
    ? `/api/officer_graph/${encodeURIComponent(officerId)}?${qs}`
    : `/api/officer_graph/${encodeURIComponent(officerId)}`;

  return await fetchJson(url);
}

// Proxy-backed ICIJ Offshore Leaks “Reconciliation extend” details for a node id.
export async function fetchOffshoreNodeDetails(nodeId) {
  return await fetchJson(`/api/offshore_node/${encodeURIComponent(nodeId)}`);
}
