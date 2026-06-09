// build a log color scale from the iso3 counts so countries with more offshore connections appear darker 

function buildColorScale(iso3Counts) {
  const countsArray = Object.values(iso3Counts || {}).filter((v) => v > 0);
  const minCount = countsArray.length > 0 ? Math.min(...countsArray) : 1;
  const maxCount = countsArray.length > 0 ? Math.max(...countsArray) : 1;

  let colorScale;
  if (countsArray.length > 1 && minCount < maxCount) {
    colorScale = d3.scaleSequentialLog()
      .domain([minCount, maxCount])
      .interpolator(d3.interpolateBlues);
  } else if (countsArray.length === 1) {
    colorScale = () => '#85c1e9';
  } else {
    colorScale = () => '#1e2a40';
  }

  return { colorScale, countsArray, minCount, maxCount };
}
// gradient legend pinned to top-right of the map svg
function renderLegend({ svg, W, minCount, maxCount, countsArray }) {
  if (!countsArray || countsArray.length === 0) return;

  const legendWidth = 120;
  const legendHeight = 10;
  const legendSvg = svg.append('g')
    .attr('class', 'map-legend')
    .attr('transform', `translate(${W - legendWidth - 20}, 20)`);

  legendSvg
    .append('text')
    .attr('x', 0)
    .attr('y', -5)
    .attr('fill', 'var(--text)')
    .style('font-size', '10px')
    .text('Offshore connections →');

  const defs = svg.append('defs');
  const linearGradient = defs
    .append('linearGradient')
    .attr('id', 'legendGradient')
    .attr('x1', '0%')
    .attr('x2', '100%')
    .attr('y1', '0%')
    .attr('y2', '0%');

  linearGradient
    .selectAll('stop')
    .data(d3.range(0, 1.01, 0.1))
    .join('stop')
    .attr('offset', (d) => `${d * 100}%`)
    .attr('stop-color', (d) => d3.interpolateBlues(d));

  legendSvg
    .append('rect')
    .attr('width', legendWidth)
    .attr('height', legendHeight)
    .style('fill', 'url(#legendGradient)')
    .style('stroke', 'var(--border)')
    .style('stroke-width', '1px');

  legendSvg
    .append('text')
    .attr('x', 0)
    .attr('y', legendHeight + 12)
    .attr('fill', 'var(--text)')
    .style('font-size', '8px')
    .text(`Low → High (${minCount}–${maxCount})`);
}

export async function initWorldMap({
  topoJsonUrl = '/static/data/world-topo.json',
  mapPanelId = 'map-panel',
  tooltip,
  getIso2ForCountryName,
  iso3Counts,
  iso3Outgoing,
  numericToIso3,
  iso3ToCapitalLonLat,
  onSelect,
  onClear
}) {
  const { colorScale, countsArray, minCount, maxCount } = buildColorScale(iso3Counts);

  const NO_NETWORK_COLOR = '#6a5256'; // reddish grey

  function getNetworkStatus(feature) {
    const iso3 = numericToIso3?.get?.(String(feature?.id)) || null;

    const rawCount = iso3 ? Number(iso3Counts?.[iso3] || 0) : 0;
    const outgoingCount = iso3 ? Number(iso3Outgoing?.[iso3] || 0) : 0;

    // clickability is based on *outgoing cross-border edges*, not raw appearances
    const hasOutgoing = Boolean(iso3) && Number.isFinite(outgoingCount) && outgoingCount > 0;

    return {
      iso3,
      rawCount: Number.isFinite(rawCount) ? rawCount : 0,
      outgoingCount: Number.isFinite(outgoingCount) ? outgoingCount : 0,
      hasOutgoing
    };
  }

  function getCountryColor(feature) {
    const { hasOutgoing, rawCount } = getNetworkStatus(feature);

    // grey out anything with no outgoing cross-border edges
    if (!hasOutgoing) return NO_NETWORK_COLOR;

    // if it has outgoing edges but rawCount is 0, fall back to a neutral fill.
    if (!rawCount) return '#1e2a40';

    return colorScale(rawCount);
  }

  const world = await d3.json(topoJsonUrl);
  const features = topojson.feature(world, world.objects.countries).features;

  const mapEl = document.getElementById(mapPanelId);
  let W = mapEl.clientWidth;
  let H = mapEl.clientHeight;

  const projection = d3.geoNaturalEarth1().fitSize([W, H], { type: 'Sphere' });
  const pathGen = d3.geoPath().projection(projection);

  // Precompute projected arc endpoints so we can draw cross-border arcs quickly.
  // Prefer capital coordinates (World Bank), fall back to polygon centroid.
  // Map: ISO3 -> [x,y] in screen coordinates (before zoom/pan transforms).
  const iso3ToXY = new Map();

  function recomputeIso3ToXY() {
    iso3ToXY.clear();
    for (const f of features) {
      const iso3 = numericToIso3?.get(String(f.id));
      if (!iso3) continue;

      // d3 expects [lon, lat]
      const capitalLonLat = iso3ToCapitalLonLat?.get?.(iso3);
      const ll = (capitalLonLat && capitalLonLat.length === 2)
        ? capitalLonLat
        : d3.geoCentroid(f);

      const xy = projection(ll);
      if (!xy || !Number.isFinite(xy[0]) || !Number.isFinite(xy[1])) continue;
      iso3ToXY.set(iso3, xy);
    }
  }

  recomputeIso3ToXY();

  const svg = d3.select(`#${mapPanelId}`).append('svg').attr('viewBox', `0 0 ${W} ${H}`);
  const zoomG = svg.append('g');

  const zoom = d3
    .zoom()
    .scaleExtent([1, 8])
    .translateExtent([[0, 0], [W, H]])
    .on('zoom', (e) => zoomG.attr('transform', e.transform));
  svg.call(zoom);

  renderLegend({ svg, W, minCount, maxCount, countsArray });

  function updateLegendPosition() {
    //  legend pinned to top-right.
    svg.select('.map-legend')
      .attr('transform', `translate(${W - 120 - 20}, 20)`);
  }

  let selectedId = null;

  // Overlay layer for jurisdiction arcs (rendered on top of countries)
  const arcsG = zoomG
    .append('g')
    .attr('class', 'juris-arcs');

  let lastJurisData = null;
  let lastJurisFocus = null;

  // Side list: countries connected to the selected country via the rendered arcs.
  const ARC_LIST_CLASS = 'map-arc-country-list';

  function ensureArcListEl() {
    let el = mapEl.querySelector(`.${ARC_LIST_CLASS}`);
    if (el) return el;

    // Ensure the list positions relative to the map panel.
    const cs = window.getComputedStyle(mapEl);
    if (cs.position === 'static') mapEl.style.position = 'relative';

    el = document.createElement('div');
    el.className = ARC_LIST_CLASS;

    // Minimal inline styling so it works even without dedicated CSS.
    el.style.position = 'absolute';
    el.style.left = '20px';
    el.style.bottom = '20px';
    el.style.maxWidth = '260px';
    el.style.maxHeight = '40%';
    el.style.overflow = 'auto';
    el.style.padding = '8px 10px';
    el.style.border = '1px solid var(--border)';
    el.style.background = 'var(--panel, rgba(15, 20, 30, 0.85))';
    el.style.color = 'var(--text)';
    el.style.borderRadius = '6px';
    el.style.fontSize = '12px';
    el.style.pointerEvents = 'auto';

    mapEl.appendChild(el);
    return el;
  }

  function clearConnectedCountriesList() {
    const el = mapEl.querySelector(`.${ARC_LIST_CLASS}`);
    if (!el) return;
    el.innerHTML = '';
    el.style.display = 'none';
  }

  function renderConnectedCountriesList({ focus, focusLabel, connections }) {
    const el = ensureArcListEl();
    el.style.display = 'block';
    el.innerHTML = '';

    const title = document.createElement('div');
    title.textContent = `Connected to ${focusLabel || focus}`;
    title.style.fontWeight = '600';
    title.style.marginBottom = '6px';

    const sub = document.createElement('div');
    sub.textContent = 'Ordered by shared nodes (weight)';
    sub.style.opacity = '0.85';
    sub.style.fontSize = '11px';
    sub.style.marginBottom = '6px';

    const ol = document.createElement('ol');
    ol.style.margin = '0';
    ol.style.paddingLeft = '18px';

    for (const c of connections) {
      const li = document.createElement('li');
      const w = Number(c.weight) || 0;
      li.textContent = `${c.otherLabel || c.other} · ${w.toLocaleString()}`;
      li.style.margin = '2px 0';
      ol.appendChild(li);
    }

    el.appendChild(title);
    el.appendChild(sub);
    el.appendChild(ol);
  }

  function clearJurisdictionArcs() {
    arcsG.selectAll('*').remove();
    lastJurisData = null;
    lastJurisFocus = null;
    clearConnectedCountriesList();
  }

  function arcPath([x1, y1], [x2, y2]) {
    const dx = x2 - x1;
    const dy = y2 - y1;
    const dist = Math.hypot(dx, dy) || 1;

    const mx = (x1 + x2) / 2;
    const my = (y1 + y2) / 2;

    // Perpendicular offset for a gentle quadratic curve.
    const nx = -dy / dist;
    const ny = dx / dist;
    const bend = Math.min(90, dist * 0.35);

    const cx = mx + nx * bend;
    const cy = my + ny * bend;

    return `M${x1},${y1} Q${cx},${cy} ${x2},${y2}`;
  }

  function renderJurisdictionArcs(jurisData, focusIso3 = null) {
    // Clear current arcs but keep the last data so we can re-render on resize.
    arcsG.selectAll('*').remove();

    const focus = (focusIso3 || jurisData?.focus || '').trim().toUpperCase();
    if (!focus) {
      lastJurisData = null;
      lastJurisFocus = null;
      clearConnectedCountriesList();
      return;
    }

    lastJurisData = jurisData;
    lastJurisFocus = focus;

    const src = iso3ToXY.get(focus);
    if (!src) {
      clearConnectedCountriesList();
      return;
    }

    const labelByIso3 = new Map();
    for (const n of (jurisData?.nodes || [])) {
      if (n?.id) labelByIso3.set(String(n.id).toUpperCase(), n.label || n.id);
    }

    const focusLinks = (jurisData?.links || [])
      .filter((l) => l && (l.source === focus || l.target === focus));

    const drawable = [];
    for (const l of focusLinks) {
      const other = l.source === focus ? l.target : l.source;
      const dst = iso3ToXY.get(other);
      if (!dst) continue;
      drawable.push({
        other,
        otherLabel: labelByIso3.get(other) || other,
        focus,
        focusLabel: labelByIso3.get(focus) || focus,
        weight: l.weight || 1,
        src,
        dst
      });
    }

    if (drawable.length === 0) {
      clearConnectedCountriesList();
      return;
    }

    // Render the side list (aggregate by country, sort by weight desc).
    const agg = new Map();
    for (const d of drawable) {
      const k = d.other;
      const prev = agg.get(k);
      agg.set(k, {
        other: d.other,
        otherLabel: d.otherLabel,
        weight: (prev?.weight || 0) + (Number(d.weight) || 0)
      });
    }

    const connections = [...agg.values()]
      .sort((a, b) => (b.weight - a.weight) || String(a.otherLabel).localeCompare(String(b.otherLabel)));

    renderConnectedCountriesList({
      focus,
      focusLabel: drawable[0]?.focusLabel || focus,
      connections
    });

    const maxW = d3.max(drawable, (d) => d.weight) || 1;

    // Make arcs easier to hover by:
    // 1) enforcing a minimum visible stroke width
    // 2) adding a thicker invisible "hit" stroke for pointer events
    const visibleWScale = d3.scaleLinear().domain([1, maxW]).range([2, 7]);

    const arcWrap = arcsG
      .selectAll('g.juris-arc-wrap')
      .data(drawable, (d) => `${focus}-${d.other}`)
      .join((enter) => {
        const g = enter.append('g').attr('class', 'juris-arc-wrap');
        g.append('path').attr('class', 'juris-arc');
        g.append('path').attr('class', 'juris-arc-hit');
        return g;
      });

    // Visible arc (force full opacity; color comes from CSS)
    // NOTE: use inline style so it overrides the stylesheet rule (.juris-arc { stroke-opacity: ... })
    arcWrap
      .select('path.juris-arc')
      .style('stroke-opacity', 1)
      .attr('stroke-width', (d) => visibleWScale(d.weight))
      .attr('d', (d) => arcPath(d.src, d.dst));

    // Invisible hover target (doesn't change visuals, just makes it easier to hit)
    arcWrap
      .select('path.juris-arc-hit')
      .attr('fill', 'none')
      .attr('stroke', 'transparent')
      .attr('stroke-linecap', 'round')
      .attr('vector-effect', 'non-scaling-stroke')
      // allow hovering/clicking only on the stroke so the map remains usable
      .attr('pointer-events', 'stroke')
      .attr('stroke-width', (d) => Math.max(12, visibleWScale(d.weight) * 3))
      .attr('d', (d) => arcPath(d.src, d.dst))
      .on('mouseover', (e, d) => {
        const w = Number(d.weight) || 0;
        tooltip.show(e, `${d.focusLabel} ↔ ${d.otherLabel} · ${w.toLocaleString()} shared node${w === 1 ? '' : 's'}`);
      })
      .on('mousemove', tooltip.move)
      .on('mouseout', tooltip.hide)
      .on('click', (e) => {
        // prevent arc clicks from clearing/altering selection
        e.stopPropagation();
      });
  }

  const paths = zoomG
    .selectAll('.country')
    .data(features, (d) => d.id)
    .join('path')
    .attr('class', 'country')
    .attr('d', pathGen)
    .attr('fill', (d) => getCountryColor(d))
    .classed('has-data', (d) => getNetworkStatus(d).hasOutgoing)
    .classed('no-network', (d) => !getNetworkStatus(d).hasOutgoing);

  function handleResize() {
    const newW = mapEl.clientWidth;
    const newH = mapEl.clientHeight;
    if (!newW || !newH) return;
    if (newW === W && newH === H) return;

    W = newW;
    H = newH;

    svg.attr('viewBox', `0 0 ${W} ${H}`);

    projection.fitSize([W, H], { type: 'Sphere' });
    // pathGen references the projection object, so updating the projection is enough.
    paths.attr('d', pathGen);

    recomputeIso3ToXY();

    zoom.translateExtent([[0, 0], [W, H]]);
    svg.call(zoom);

    updateLegendPosition();

    if (lastJurisData && lastJurisFocus) {
      renderJurisdictionArcs(lastJurisData, lastJurisFocus);
    }
  }

  // Keep the map responsive to panel resizes (browser resize / responsive layout).
  if (typeof ResizeObserver !== 'undefined') {
    const resizeObserver = new ResizeObserver(handleResize);
    resizeObserver.observe(mapEl);
  } else {
    // Fallback for older browsers
    window.addEventListener('resize', handleResize);
  }

  // Ensure arcs render above countries.
  arcsG.raise();

  // Shared selection logic used by both map clicks and the country search box.
  async function applySelection(d) {
    const countryName = d.properties?.name;
    if (!countryName) return false;

    const { iso3, hasOutgoing } = getNetworkStatus(d);
    if (!hasOutgoing) return false;

    const iso2 = getIso2ForCountryName(countryName);

    selectedId = d.id;
    paths.classed('selected', (f) => f.id === d.id);

    if (onSelect) {
      await onSelect({ id: d.id, name: countryName, iso2, iso3 });
    }
    return true;
  }

  paths
    .on('mouseover', (e, d) => {
      const countryName = d.properties?.name || `ID: ${d.id}`;
      const { hasOutgoing, rawCount } = getNetworkStatus(d);

      if (!hasOutgoing) {
        tooltip.show(e, `No offshore network for ${countryName}`);
        return;
      }

      tooltip.show(e, `${countryName} (${rawCount} offshore links)`);
    })
    .on('mousemove', tooltip.move)
    .on('mouseout', tooltip.hide)
    .on('click', async (e, d) => {
      e.stopPropagation();

      const countryName = d.properties?.name;
      if (!countryName) return;

      const { hasOutgoing } = getNetworkStatus(d);

      // Only allow selecting countries that have outgoing cross-border edges.
      if (!hasOutgoing) {
        tooltip.show(e, `No offshore network for ${countryName}`);
        return;
      }

      // Toggle off if clicking the already-selected country.
      if (selectedId === d.id) {
        selectedId = null;
        paths.classed('selected', false);
        clearJurisdictionArcs();
        if (onClear) onClear();
        return;
      }

      await applySelection(d);
    });

  svg.on('click', () => {
    selectedId = null;
    paths.classed('selected', false);
    clearJurisdictionArcs();
    if (onClear) onClear();
  });

  return {
    clearSelection() {
      selectedId = null;
      paths.classed('selected', false);
      clearJurisdictionArcs();
    },

    // selectable countries , those with an offshore network, for the search box
    getSelectableCountries() {
      const seen = new Set();
      const list = [];
      for (const f of features) {
        const name = f.properties?.name;
        const { iso3, hasOutgoing } = getNetworkStatus(f);
        if (!hasOutgoing || !name || !iso3 || seen.has(iso3)) continue;
        seen.add(iso3);
        list.push({ id: f.id, name, iso2: getIso2ForCountryName(name), iso3 });
      }
      return list;
    },

    // select a country by ISO3 (used by the search box).
    async selectCountryByIso3(iso3) {
      const target = String(iso3 || '').trim().toUpperCase();
      if (!target) return false;

      const d = features.find((f) => {
        const s = getNetworkStatus(f);
        return s.hasOutgoing && s.iso3 === target;
      });
      if (!d) return false;

      return await applySelection(d);
    },

    renderJurisdictionArcs,
    clearJurisdictionArcs
  };
}
