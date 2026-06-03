import { fmt } from './js/utils.js';
import { initTooltip } from './js/tooltip.js';
import { buildNameToIso2Map, createIso2Resolver } from './js/countryCodes.js';
import { buildNumericToIso3Map } from './js/isoNumeric.js';
import { fetchConnectionCounts } from './js/api.js';
import { createInfoPanel } from './js/infoPanel.js';
import { createGraphPanel } from './js/graphPanel.js';
import { initWorldMap } from './js/map.js';

// NOTE: d3 and topojson are loaded from CDN in index.html and exposed as globals.

const tooltip = initTooltip('tooltip');

// Build country-code lookup tables
const nameToIso2 = await buildNameToIso2Map();
const getIso2ForCountryName = createIso2Resolver(nameToIso2);
const numericToIso3 = await buildNumericToIso3Map('/static/data/iso_numeric.json');

// Load offshore connection counts (for choropleth colouring)
const iso2Counts = await fetchConnectionCounts();

// Panels
const infoPanel = createInfoPanel({ fmt });
const graphPanel = createGraphPanel({ tooltip });

infoPanel.clear();
graphPanel.clear();

// Map
await initWorldMap({
  topoJsonUrl: '/static/data/world-topo.json',
  mapPanelId: 'map-panel',
  tooltip,
  getIso2ForCountryName,
  iso2Counts,
  numericToIso3,
  onSelect: async ({ name, iso2, iso3 }) => {
    // Knowledge graph uses ISO3 from the map's numeric ids.
    graphPanel.loadEntityGraph(iso3, name);

    // Country indicators use ISO2 from World Bank.
    if (iso2) {
      await infoPanel.loadCountry(name, iso2);
    } else {
      infoPanel.showNoData(name);
    }
  },
  onClear: () => {
    infoPanel.clear();
    graphPanel.clear();
  }
});