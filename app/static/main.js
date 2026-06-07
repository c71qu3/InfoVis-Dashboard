import { fmt, initInfoPopover } from './js/utils.js';
import { initTooltip } from './js/tooltip.js';
import { createIso2Resolver } from './js/countryCodes.js';
import { buildNumericToIso3Map } from './js/isoNumeric.js';
import { fetchConnectionCounts, fetchOutgoingCounts, fetchJurisdictions } from './js/api.js';
import { createInfoPanel } from './js/infoPanel.js';
import { createGraphPanel } from './js/graphPanel.js';
import { createListPanel } from './js/listPanel.js';
import { createCountrySearch } from './js/countrySearch.js';
import { initWorldMap } from './js/map.js';

// NOTE: d3 and topojson are loaded from CDN in index.html and exposed as globals.

const tooltip = initTooltip('tooltip');

// Map "i" button → explanation of the connection arcs.
initInfoPopover('map-info-btn', 'map-info-popover');

// Build country-code lookup tables (single World Bank request)
async function buildWorldBankMaps() {
  const nameToIso2 = new Map();
  const iso3ToCapitalLonLat = new Map();

  try {
    const wbRes = await fetch('https://api.worldbank.org/v2/country?format=json&per_page=300');
    const wbData = await wbRes.json();

    if (wbData?.[1]) {
      for (const c of wbData[1]) {
        const iso2 = c?.iso2Code;
        const iso3 = c?.id;

        // Name → ISO2 (for indicators)
        if (iso2 && /^[A-Z]{2}$/.test(iso2) && c?.capitalCity) {
          nameToIso2.set(c.name, iso2);
        }

        // ISO3 → capital lon/lat (for arcs)
        const lon = parseFloat(c?.longitude);
        const lat = parseFloat(c?.latitude);
        if (iso3 && /^[A-Z]{3}$/.test(iso3) && Number.isFinite(lon) && Number.isFinite(lat)) {
          iso3ToCapitalLonLat.set(iso3, [lon, lat]);
        }
      }
    }

    console.log('Built name→ISO2 map with', nameToIso2.size, 'entries');
    console.log('Built ISO3 → capital lon/lat map with', iso3ToCapitalLonLat.size, 'entries');
  } catch (e) {
    console.warn('Could not load World Bank country metadata', e);
  }

  return { nameToIso2, iso3ToCapitalLonLat };
}

const { nameToIso2, iso3ToCapitalLonLat } = await buildWorldBankMaps();
const getIso2ForCountryName = createIso2Resolver(nameToIso2);
const numericToIso3 = await buildNumericToIso3Map('/static/data/iso_numeric.json');

// Load offshore connection counts (for choropleth colouring)
// Backend returns ISO3 → count.
const iso3Counts = await fetchConnectionCounts();

// Load outgoing cross-border edge counts (for clickability)
const iso3Outgoing = await fetchOutgoingCounts();

// Panels
const infoPanel = createInfoPanel({ fmt });
const graphPanel = createGraphPanel({ tooltip });
const listPanel = createListPanel({
  onSelectIntermediary: (intermediaryId, intermediaryName, iso3) => graphPanel.loadIntermediaryFocusGraph(intermediaryId, intermediaryName, iso3),
  onSelectEntity: (entityId, entityName) => graphPanel.loadEntityFocusGraph(entityId, entityName)
});

infoPanel.clear();
graphPanel.clear();
listPanel.clear();

// Map
let arcReq = 0;
let countrySearch = null; // assigned after the map is built; cleared on deselect
const worldMap = await initWorldMap({
  topoJsonUrl: '/static/data/world-topo.json',
  mapPanelId: 'map-panel',
  tooltip,
  getIso2ForCountryName,
  iso3Counts,
  iso3Outgoing,
  numericToIso3,
  iso3ToCapitalLonLat,
  onSelect: async ({ name, iso2, iso3 }) => {
    const token = ++arcReq;

    // Load arcs (jurisdiction connections) in parallel.
    const jurisPromise = iso3 ? fetchJurisdictions(iso3) : Promise.resolve(null);

    // Knowledge graph uses ISO3 from the map's numeric ids.
    graphPanel.loadEntityGraph(iso3, name);

    // Intermediaries list uses ISO3.
    listPanel.loadIntermediaries(iso3, name);

    // Country indicators use ISO2 from World Bank.
    // Don't await this so arcs/graph can update immediately; infoPanel handles cancellation.
    if (iso2) {
      infoPanel.loadCountry(name, iso2);
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
    listPanel.clear();
    countrySearch?.clear();
  }
});

// Search-to-select: pick a country by name when it's hard to click on the map.
countrySearch = createCountrySearch({
  inputId: 'country-search-input',
  resultsId: 'country-search-results',
  countries: worldMap.getSelectableCountries(),
  onSelect: (c) => worldMap.selectCountryByIso3(c.iso3)
});