import { fmt } from './js/utils.js';
import { initTooltip } from './js/tooltip.js';
import { buildNameToIso2Map, createIso2Resolver } from './js/countryCodes.js';
import { buildNumericToIso3Map } from './js/isoNumeric.js';
import { fetchConnectionCounts, fetchJurisdictions } from './js/api.js';
import { createInfoPanel } from './js/infoPanel.js';
import { createGraphPanel } from './js/graphPanel.js';
import { initWorldMap } from './js/map.js';

// NOTE: d3 and topojson are loaded from CDN in index.html and exposed as globals.

const tooltip = initTooltip('tooltip');

// Build country-code lookup tables
const nameToIso2 = await buildNameToIso2Map();
const getIso2ForCountryName = createIso2Resolver(nameToIso2);
const numericToIso3 = await buildNumericToIso3Map('/static/data/iso_numeric.json');

async function buildIso3ToCapitalLonLatMap() {
  const m = new Map();
  try {
    const wbRes = await fetch('https://api.worldbank.org/v2/country?format=json&per_page=300');
    const wbData = await wbRes.json();
    if (wbData?.[1]) {
      for (const c of wbData[1]) {
        const iso3 = c?.id;
        const lon = parseFloat(c?.longitude);
        const lat = parseFloat(c?.latitude);
        if (iso3 && /^[A-Z]{3}$/.test(iso3) && Number.isFinite(lon) && Number.isFinite(lat)) {
          m.set(iso3, [lon, lat]);
        }
      }
    }
    console.log('Built ISO3 → capital lon/lat map with', m.size, 'entries');
  } catch (e) {
    console.warn('Could not build capital coordinate map', e);
  }
  return m;
}

const iso3ToCapitalLonLat = await buildIso3ToCapitalLonLatMap();

// Load offshore connection counts (for choropleth colouring)
const iso2Counts = await fetchConnectionCounts();

// Panels
const infoPanel = createInfoPanel({ fmt });
const graphPanel = createGraphPanel({ tooltip });

infoPanel.clear();
graphPanel.clear();

// Map
let arcReq = 0;
const worldMap = await initWorldMap({
  topoJsonUrl: '/static/data/world-topo.json',
  mapPanelId: 'map-panel',
  tooltip,
  getIso2ForCountryName,
  iso2Counts,
  numericToIso3,
  iso3ToCapitalLonLat,
  onSelect: async ({ name, iso2, iso3 }) => {
    const token = ++arcReq;

    // Load arcs (jurisdiction connections) in parallel.
    const jurisPromise = iso3 ? fetchJurisdictions(iso3) : Promise.resolve(null);

    // Knowledge graph uses ISO3 from the map's numeric ids.
    graphPanel.loadEntityGraph(iso3, name);

    // Country indicators use ISO2 from World Bank.
    if (iso2) {
      await infoPanel.loadCountry(name, iso2);
    } else {
      infoPanel.showNoData(name);
    }

    try {
      const juris = await jurisPromise;
      if (token !== arcReq) return;
      if (juris && juris.links && juris.links.length) worldMap.renderJurisdictionArcs(juris, iso3);
      else worldMap.clearJurisdictionArcs();
    } catch (e) {
      if (token !== arcReq) return;
      console.warn('Could not load jurisdiction arcs', e);
      worldMap.clearJurisdictionArcs();
    }
  },
  onClear: () => {
    arcReq++; // invalidate any in-flight arcs request
    worldMap.clearJurisdictionArcs();

    infoPanel.clear();
    graphPanel.clear();
  }
});