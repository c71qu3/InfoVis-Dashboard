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
      let countryName = d.properties?.name;
      if (!countryName) return;
      const iso2 = getIso2ForCountryName(countryName);
      if (!iso2) {
        clearInfo();
        nameEl.textContent = countryName;
        hintEl.textContent = `No World Bank data for "${countryName}".`;
        return;
      }
      if (selectedId === d.id) {
        selectedId = null;
        paths.classed('selected', false);
        clearInfo();
        return;
      }
      selectedId = d.id;
      paths.classed('selected', f => f.id === d.id);
      await loadCountry(countryName, iso2);
    });

  svg.on('click', () => {
    selectedId = null;
    paths.classed('selected', false);
    clearInfo();
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
})();