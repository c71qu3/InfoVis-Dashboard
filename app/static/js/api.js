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
