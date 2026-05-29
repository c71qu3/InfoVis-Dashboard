(async () => {
  const fmt = v => {
    if (v == null) return '—';
    const n = +v;
    if (isNaN(n)) return v;
    if (Math.abs(n) >= 1e12) return (n/1e12).toFixed(2)+' T';
    if (Math.abs(n) >= 1e9)  return (n/1e9).toFixed(2)+' B';
    if (Math.abs(n) >= 1e6)  return (n/1e6).toFixed(2)+' M';
    return n.toFixed(2);
  };

  // Tooltip
  const tooltip = document.getElementById('tooltip');
  function showTip(e, text) { tooltip.style.display = 'block'; tooltip.textContent = text; moveTip(e); }
  function moveTip(e) { tooltip.style.left = (e.clientX+12)+'px'; tooltip.style.top = (e.clientY-8)+'px'; }
  function hideTip() { tooltip.style.display = 'none'; }

  // Build name → ISO2 map (from World Bank)
  let nameToIso2 = new Map();
  try {
    const wbRes = await fetch('https://api.worldbank.org/v2/country?format=json&per_page=300');
    const wbData = await wbRes.json();
    if (wbData[1]) {
      wbData[1].forEach(c => {
        const iso2 = c.iso2Code;
        if (iso2 && iso2.match(/^[A-Z]{2}$/) && c.capitalCity) {
          nameToIso2.set(c.name, iso2);
        }
      });
    }
    console.log("Built name→ISO2 map with", nameToIso2.size, "entries");
  } catch(e) { console.warn(e); }

  // Build topojson numeric id → ISO3, used for the offshore graph. Goes straight
  // off the map's own ids so territories the World Bank omits (e.g. Hong Kong) work.
  let numericToIso3 = new Map();
  try {
    const isoNum = await fetch('/static/data/iso_numeric.json').then(r => r.json());
    for (const [iso3, num] of Object.entries(isoNum)) numericToIso3.set(String(num), iso3);
  } catch(e) { console.warn(e); }

  // get ISO2 from country name (with fuzzy matching) 
  function getIso2ForCountryName(name) {
    if (!name) return null;
    if (nameToIso2.has(name)) return nameToIso2.get(name);
    const lowerName = name.toLowerCase();
    for (let [wbName, iso2] of nameToIso2.entries()) {
      if (wbName.toLowerCase() === lowerName) return iso2;
    }
    const aliases = {
      "United States of America": "US", "USA": "US", "United Kingdom": "GB",
      "Russia": "RU", "Czechia": "CZ", "South Korea": "KR", "China": "CN",
      "Vietnam": "VN", "Iran": "IR", "Syria": "SY", "Laos": "LA"
    };
    if (aliases[name]) return aliases[name];
    return null;
  }

  // Fetch connection counts (ISO2 → count)
  let iso2Counts = {};
  try {
    const res = await fetch('/api/connections');
    iso2Counts = await res.json();
    console.log("Loaded connection counts for", Object.keys(iso2Counts).length, "codes");
  } catch(e) {
    console.warn("Could not load connection counts", e);
  }

  // Color scale based on ISO2 counts
  const countsArray = Object.values(iso2Counts).filter(v => v > 0);
  const minCount = countsArray.length > 0 ? Math.min(...countsArray) : 1;
  const maxCount = countsArray.length > 0 ? Math.max(...countsArray) : 1;

  let colorScale;
  if (countsArray.length > 1 && minCount < maxCount) {
    colorScale = d3.scaleSequentialLog()
      .domain([minCount, maxCount])
      .interpolator(d3.interpolateBlues);
  } else if (countsArray.length === 1) {
    colorScale = () => "#85c1e9";
  } else {
    colorScale = () => "#1e2a40";
  }

  function getCountryColor(feature) {
    const name = feature.properties?.name;
    const iso2 = getIso2ForCountryName(name);
    if (!iso2) return "#1e2a40";
    const count = iso2Counts[iso2] || 0;
    if (count === 0) return "#1e2a40";
    return colorScale(count);
  }

  // Load map
  const world = await d3.json('/static/data/world-topo.json');
  const features = topojson.feature(world, world.objects.countries).features;
  const mapEl = document.getElementById('map-panel');
  const W = mapEl.clientWidth, H = mapEl.clientHeight;
  const projection = d3.geoNaturalEarth1().fitSize([W, H], { type: 'Sphere' });
  const pathGen = d3.geoPath().projection(projection);
  const svg = d3.select('#map-panel').append('svg').attr('viewBox', `0 0 ${W} ${H}`);
  const zoomG = svg.append('g');
  const zoom = d3.zoom().scaleExtent([1,8]).translateExtent([[0,0],[W,H]]).on('zoom', e => zoomG.attr('transform', e.transform));
  svg.call(zoom);

  // Legend
  if (countsArray.length > 0) {
    const legendWidth = 120, legendHeight = 10;
    const legendSvg = svg.append('g').attr('transform', `translate(${W - legendWidth - 20}, 20)`);
    legendSvg.append('text').attr('x',0).attr('y',-5).attr('fill','var(--text)').style('font-size','10px').text('Offshore connections →');
    const defs = svg.append('defs');
    const linearGradient = defs.append('linearGradient').attr('id','legendGradient').attr('x1','0%').attr('x2','100%').attr('y1','0%').attr('y2','0%');
    linearGradient.selectAll('stop')
      .data(d3.range(0,1.01,0.1))
      .join('stop')
      .attr('offset', d => `${d*100}%`)
      .attr('stop-color', d => d3.interpolateBlues(d));
    legendSvg.append('rect')
      .attr('width',legendWidth)
      .attr('height',legendHeight)
      .style('fill','url(#legendGradient)')
      .style('stroke','var(--border)')
      .style('stroke-width','1px');
    legendSvg.append('text')
      .attr('x',0)
      .attr('y',legendHeight+12)
      .attr('fill','var(--text)')
      .style('font-size','8px')
      .text(`Low → High (${minCount}–${maxCount})`);
  }

  let selectedId = null;

  const paths = zoomG.selectAll('.country')
    .data(features, d => d.id)
    .join('path')
    .attr('class', 'country')
    .attr('d', pathGen)
    .attr('fill', d => getCountryColor(d))   // colour based on offshore count
    .on('mouseover', (e, d) => {
      let tipText = d.properties?.name || `ID: ${d.id}`;
      const iso2 = getIso2ForCountryName(d.properties?.name);
      if (iso2) {
        const count = iso2Counts[iso2] || 0;
        if (count > 0) tipText += ` (${count} offshore links)`;
      }
      showTip(e, tipText);
    })
    .on('mousemove', moveTip)
    .on('mouseout', hideTip)
    .on('click', async (e, d) => {
      e.stopPropagation();
      const countryName = d.properties?.name;
      if (!countryName) return;
      const iso2 = getIso2ForCountryName(countryName);
      const iso3 = numericToIso3.get(String(d.id));

      if (selectedId === d.id) {
        selectedId = null;
        paths.classed('selected', false);
        clearInfo();
        clearGraph();
        return;
      }
      selectedId = d.id;
      paths.classed('selected', f => f.id === d.id);

      loadGraph(iso3, countryName);

      if (iso2) {
        await loadCountry(countryName, iso2);
      } else {
        clearInfo();
        nameEl.textContent = countryName;
        nameEl.classList.remove('empty');
        hintEl.textContent = `No World Bank data for "${countryName}".`;
      }
    });

  svg.on('click', () => {
    selectedId = null;
    paths.classed('selected', false);
    clearInfo();
    clearGraph();
  });

  // DOM elements for info panel
  const nameEl = document.getElementById('country-name');
  const loadingEl = document.getElementById('loading-indicator');
  const gridEl = document.getElementById('indicators-grid');
  const hintEl = document.getElementById('info-hint');

  function clearInfo() {
    nameEl.textContent = 'Click a country on the map';
    nameEl.classList.add('empty');
    gridEl.innerHTML = '';
    hintEl.style.display = '';
    loadingEl.classList.remove('active');
    const sel = document.getElementById('year-selector');
    if (sel) sel.remove();
  }

  async function loadCountry(name, iso2) {
    nameEl.textContent = name;
    nameEl.classList.remove('empty');
    gridEl.innerHTML = '';
    hintEl.style.display = 'none';
    loadingEl.classList.add('active');

    let years = [];
    try {
      const res = await fetch(`/api/years/${iso2}`);
      years = await res.json();
    } catch(e) { console.warn(e); }

    const existing = document.getElementById('year-selector');
    if (existing) existing.remove();
    const selectorDiv = document.createElement('div');
    selectorDiv.id = 'year-selector';
    selectorDiv.style.marginBottom = '1rem';
    selectorDiv.innerHTML = `
      <label style="font-size:0.7rem; color:var(--muted);">Year: </label>
      <select id="year-dropdown" style="background:#1a2133; color:var(--text); border:1px solid var(--border); padding:0.2rem 0.5rem;">
        <option value="latest">Latest (per indicator)</option>
        ${years.map(y => `<option value="${y}">${y}</option>`).join('')}
      </select>
    `;
    nameEl.parentNode.insertBefore(selectorDiv, nameEl.nextSibling);
    const dropdown = document.getElementById('year-dropdown');

    await loadIndicators(iso2, 'latest');
    loadingEl.classList.remove('active');

    dropdown.addEventListener('change', async (e) => {
      loadingEl.classList.add('active');
      gridEl.innerHTML = '';
      await loadIndicators(iso2, e.target.value);
      loadingEl.classList.remove('active');
    });
  }

  async function loadIndicators(iso2, yearMode) {
    let url = `/api/indicators/${iso2}`;
    if (yearMode === 'latest') {
      url += '?mode=latest';
    } else {
      url += `?mode=${yearMode}`;
    }
    try {
      const res = await fetch(url);
      const data = await res.json();
      renderIndicators(data);
    } catch(err) {
      console.warn(err);
      hintEl.textContent = 'Error loading indicators.';
      hintEl.style.display = '';
    }
  }

  function renderIndicators(data) {
    gridEl.innerHTML = '';
    for (const [label, info] of Object.entries(data)) {
      const card = document.createElement('div');
      card.className = 'indicator-card';
      card.innerHTML = `
        <div class="ind-label">${label}</div>
        <div class="ind-value">${fmt(info.value)}</div>
        ${info.year ? `<div class="ind-year">${info.year}</div>` : ''}
      `;
      gridEl.appendChild(card);
    }
    if (Object.keys(data).length === 0) {
      hintEl.textContent = 'No indicators found.';
      hintEl.style.display = '';
    }
  }

  // ---- Knowledge graph panel ----
  const TYPE_COLORS = {
    Entity: '#4f8ef7', Officer: '#f7a24f',
    Intermediary: '#5fd0a0', Address: '#b07ff7', Node: '#6b7a99'
  };
  const graphHint = document.getElementById('graph-hint');
  let graphSvg = null, graphG = null, graphSim = null, graphZoom = null, graphReq = 0;

  function clearGraph() {
    graphReq++; // invalidate any in-flight request
    if (graphSim) graphSim.stop();
    if (graphG) graphG.selectAll('*').remove();
    graphHint.style.display = '';
    graphHint.textContent = 'Click a country to see its offshore network.';
  }

  async function loadGraph(iso3, name) {
    const token = ++graphReq;
    if (!iso3) {
      clearGraph();
      graphHint.textContent = `No offshore network for ${name || 'this country'}.`;
      return;
    }
    graphHint.style.display = '';
    graphHint.textContent = 'Loading network…';
    let data;
    try {
      const res = await fetch(`/api/graph/${iso3}`);
      data = await res.json();
    } catch(e) {
      if (token !== graphReq) return;
      console.warn(e);
      graphHint.textContent = 'Error loading network.';
      return;
    }
    if (token !== graphReq) return; // a newer selection superseded this one
    if (data.error) {
      clearGraph();
      graphHint.textContent = 'Graph database unavailable.';
      return;
    }
    if (!data.nodes || data.nodes.length === 0) {
      clearGraph();
      graphHint.textContent = `No offshore network for ${name || iso3}.`;
      return;
    }
    graphHint.style.display = 'none';
    renderGraph(data);
  }

  function renderGraph(data) {
    if (graphSim) graphSim.stop(); // halt any previous simulation
    ensureGraphSvg();
    document.getElementById('graph-legend').style.display = 'flex';
    const el = document.getElementById('graph-panel');
    const W = el.clientWidth, H = el.clientHeight;
    graphG.selectAll('*').remove();

    const nodes = data.nodes.map(n => ({ ...n }));

    // collapse repeat links between the same pair; weight = connection frequency
    const linkMap = new Map();
    for (const l of data.links) {
      const key = l.source < l.target ? `${l.source}|${l.target}` : `${l.target}|${l.source}`;
      const existing = linkMap.get(key);
      if (existing) existing.weight++;
      else linkMap.set(key, { source: l.source, target: l.target, rel: l.rel, weight: 1 });
    }
    const links = [...linkMap.values()];

    const maxDeg = d3.max(nodes, d => d.degree) || 1;
    const rScale = d3.scaleSqrt().domain([0, maxDeg]).range([3.5, 15]);
    const maxWeight = d3.max(links, d => d.weight) || 1;
    const wScale = d3.scaleLinear().domain([1, maxWeight]).range([1, 4]);
    const oScale = d3.scaleLinear().domain([1, maxWeight]).range([0.35, 0.85]);

    const link = graphG.append('g').selectAll('line')
      .data(links).join('line')
      .attr('class', 'glink')
      .attr('stroke-width', d => wScale(d.weight))
      .attr('stroke-opacity', d => oScale(d.weight));

    const node = graphG.append('g').selectAll('circle')
      .data(nodes).join('circle')
      .attr('class', 'gnode')
      .attr('r', d => rScale(d.degree))
      .attr('fill', d => TYPE_COLORS[d.type] || TYPE_COLORS.Node)
      .call(nodeDrag())
      .on('mouseover', (e, d) => showTip(e, `${d.label} · ${d.type}${d.degree ? ` · ${d.degree} links` : ''}`))
      .on('mousemove', moveTip)
      .on('mouseout', hideTip);

    // only label the top hubs by degree so the view stays readable
    const hubIds = new Set([...nodes].sort((a, b) => b.degree - a.degree).slice(0, 8).map(d => d.id));
    const hubs = nodes.filter(d => hubIds.has(d.id) && d.degree > 1);
    const label = graphG.append('g').selectAll('text')
      .data(hubs).join('text')
      .attr('class', 'glabel')
      .text(d => d.label.length > 18 ? d.label.slice(0, 17) + '…' : d.label);

    graphSim = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id(d => d.id).distance(34).strength(0.6))
      .force('charge', d3.forceManyBody().strength(-60))
      .force('center', d3.forceCenter(W / 2, H / 2))
      .force('x', d3.forceX(W / 2).strength(0.06))
      .force('y', d3.forceY(H / 2).strength(0.06))
      .force('collide', d3.forceCollide().radius(d => rScale(d.degree) + 3))
      .on('tick', () => {
        link.attr('x1', d => d.source.x).attr('y1', d => d.source.y)
            .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
        node.attr('cx', d => d.x).attr('cy', d => d.y);
        label.attr('x', d => d.x + rScale(d.degree) + 2).attr('y', d => d.y + 3);
      })
      .on('end', () => fitView(nodes, rScale));
  }

  // shared layout helpers (used by both the entity graph and jurisdiction view)
  function fitView(nodes, rScale) {
    if (!nodes.length) return;
    const el = document.getElementById('graph-panel');
    const W = el.clientWidth, H = el.clientHeight, pad = 16;
    const minX = d3.min(nodes, d => d.x - rScale(d.degree));
    const maxX = d3.max(nodes, d => d.x + rScale(d.degree)) + 90; // room for labels
    const minY = d3.min(nodes, d => d.y - rScale(d.degree));
    const maxY = d3.max(nodes, d => d.y + rScale(d.degree)) + 8;
    const gw = maxX - minX || 1, gh = maxY - minY || 1;
    const scale = Math.min((W - pad * 2) / gw, (H - pad * 2) / gh, 2);
    const tx = (W - scale * (minX + maxX)) / 2;
    const ty = (H - scale * (minY + maxY)) / 2;
    graphSvg.transition().duration(400)
      .call(graphZoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
  }

  function nodeDrag() {
    return d3.drag()
      .on('start', (e, d) => { if (!e.active) graphSim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
      .on('drag', (e, d) => { d.fx = e.x; d.fy = e.y; })
      .on('end', (e, d) => { if (!e.active) graphSim.alphaTarget(0); d.fx = null; d.fy = null; });
  }

  function ensureGraphSvg() {
    if (graphSvg) return;
    const el = document.getElementById('graph-panel');
    graphSvg = d3.select('#graph-panel').append('svg').attr('viewBox', `0 0 ${el.clientWidth} ${el.clientHeight}`);
    graphG = graphSvg.append('g');
    graphZoom = d3.zoom().scaleExtent([0.2, 5]).on('zoom', e => graphG.attr('transform', e.transform));
    graphSvg.call(graphZoom);
  }

  // ---- Jurisdiction-flow view: countries as nodes, cross-border links as edges ----
  const LINE_PALETTE = ['#4f8ef7', '#f7a24f', '#5fd0a0', '#b07ff7', '#e0708f', '#5cc8e0', '#d8c45a', '#8c7bf0'];

  async function loadJurisdictions(focus = null) {
    const token = ++graphReq; // a newer selection bumps this and supersedes us
    graphHint.style.display = '';
    graphHint.textContent = 'Loading jurisdiction flows…';
    let data;
    try {
      const url = focus ? `/api/jurisdictions?focus=${focus}` : '/api/jurisdictions';
      data = await fetch(url).then(r => r.json());
    } catch(e) {
      if (token !== graphReq) return;
      console.warn(e);
      graphHint.textContent = 'Error loading flows.';
      return;
    }
    if (token !== graphReq) return; // superseded while we loaded
    if (data.error) { clearGraph(); graphHint.textContent = 'Graph database unavailable.'; return; }
    if (!data.nodes || data.nodes.length === 0) {
      clearGraph();
      graphHint.textContent = focus ? `No cross-border flows for ${focus}.` : 'No jurisdiction data.';
      return;
    }
    graphHint.style.display = 'none';
    renderJurisdictions(data);
  }

  function renderJurisdictions(data) {
    if (graphSim) graphSim.stop();
    ensureGraphSvg();
    document.getElementById('graph-legend').style.display = 'none'; // entity-type legend N/A here
    graphG.selectAll('*').remove();

    const el = document.getElementById('graph-panel');
    const W = el.clientWidth, H = el.clientHeight;
    const nodes = data.nodes.map(n => ({ ...n }));
    const srcColor = new Map();
    let ci = 0;
    const links = data.links.map(l => {
      if (!srcColor.has(l.source)) srcColor.set(l.source, LINE_PALETTE[ci++ % LINE_PALETTE.length]);
      return { ...l, color: srcColor.get(l.source) };
    });

    const maxDeg = d3.max(nodes, d => d.degree) || 1;
    const rScale = d3.scaleSqrt().domain([0, maxDeg]).range([5, 20]);
    const maxW = d3.max(links, d => d.weight) || 1;
    const wScale = d3.scaleLinear().domain([1, maxW]).range([1.2, 6]);

    const link = graphG.append('g').selectAll('path')
      .data(links).join('path')
      .attr('fill', 'none')
      .attr('stroke', d => d.color)
      .attr('stroke-opacity', 0.55)
      .attr('stroke-width', d => wScale(d.weight))
      .attr('stroke-linecap', 'round');

    const node = graphG.append('g').selectAll('circle')
      .data(nodes).join('circle')
      .attr('class', 'gnode')
      .attr('r', d => rScale(d.degree))
      .attr('fill', d => d.focus ? '#f7a24f' : '#cdd6f0')
      .attr('stroke', d => d.focus ? '#fff' : '#0f1117')
      .attr('stroke-width', d => d.focus ? 2 : 1)
      .call(nodeDrag())
      .on('mouseover', (e, d) => showTip(e, `${d.label} · ${d.degree.toLocaleString()} cross-border links`))
      .on('mousemove', moveTip)
      .on('mouseout', hideTip);

    const label = graphG.append('g').selectAll('text')
      .data(nodes).join('text')
      .attr('class', 'glabel')
      .text(d => d.id);

    const ticked = () => {
      link.attr('d', d => {
        const x1 = d.source.x, y1 = d.source.y, x2 = d.target.x, y2 = d.target.y;
        const cx = (x1 + x2) / 2 - (y2 - y1) * 0.15;
        const cy = (y1 + y2) / 2 + (x2 - x1) * 0.15;
        return `M${x1},${y1} Q${cx},${cy} ${x2},${y2}`;
      });
      node.attr('cx', d => d.x).attr('cy', d => d.y);
      label.attr('x', d => d.x + rScale(d.degree) + 3).attr('y', d => d.y + 3);
    };

    graphSim = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id(d => d.id).distance(60).strength(0.2))
      .force('charge', d3.forceManyBody().strength(-150))
      .force('center', d3.forceCenter(W / 2, H / 2))
      .force('x', d3.forceX(W / 2).strength(0.08))
      .force('y', d3.forceY(H / 2).strength(0.08))
      .force('collide', d3.forceCollide().radius(d => rScale(d.degree) + 6))
      .stop();
    for (let i = 0; i < 300; i++) graphSim.tick(); // settle synchronously for a stable layout
    ticked();
    fitView(nodes, rScale);
    graphSim.on('tick', ticked).on('end', () => fitView(nodes, rScale));
  }

})();