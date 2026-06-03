function buildColorScale(iso2Counts) {
  const countsArray = Object.values(iso2Counts || {}).filter((v) => v > 0);
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

function renderLegend({ svg, W, minCount, maxCount, countsArray }) {
  if (!countsArray || countsArray.length === 0) return;

  const legendWidth = 120;
  const legendHeight = 10;
  const legendSvg = svg.append('g').attr('transform', `translate(${W - legendWidth - 20}, 20)`);

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
  iso2Counts,
  numericToIso3,
  onSelect,
  onClear
}) {
  const { colorScale, countsArray, minCount, maxCount } = buildColorScale(iso2Counts);

  function getCountryColor(feature) {
    const name = feature.properties?.name;
    const iso2 = getIso2ForCountryName(name);
    if (!iso2) return '#1e2a40';
    const count = iso2Counts?.[iso2] || 0;
    if (count === 0) return '#1e2a40';
    return colorScale(count);
  }

  const world = await d3.json(topoJsonUrl);
  const features = topojson.feature(world, world.objects.countries).features;

  const mapEl = document.getElementById(mapPanelId);
  const W = mapEl.clientWidth;
  const H = mapEl.clientHeight;

  const projection = d3.geoNaturalEarth1().fitSize([W, H], { type: 'Sphere' });
  const pathGen = d3.geoPath().projection(projection);

  const svg = d3.select(`#${mapPanelId}`).append('svg').attr('viewBox', `0 0 ${W} ${H}`);
  const zoomG = svg.append('g');

  const zoom = d3
    .zoom()
    .scaleExtent([1, 8])
    .translateExtent([[0, 0], [W, H]])
    .on('zoom', (e) => zoomG.attr('transform', e.transform));
  svg.call(zoom);

  renderLegend({ svg, W, minCount, maxCount, countsArray });

  let selectedId = null;

  const paths = zoomG
    .selectAll('.country')
    .data(features, (d) => d.id)
    .join('path')
    .attr('class', 'country')
    .attr('d', pathGen)
    .attr('fill', (d) => getCountryColor(d))
    .on('mouseover', (e, d) => {
      let tipText = d.properties?.name || `ID: ${d.id}`;
      const iso2 = getIso2ForCountryName(d.properties?.name);
      if (iso2) {
        const count = iso2Counts?.[iso2] || 0;
        if (count > 0) tipText += ` (${count} offshore links)`;
      }
      tooltip.show(e, tipText);
    })
    .on('mousemove', tooltip.move)
    .on('mouseout', tooltip.hide)
    .on('click', async (e, d) => {
      e.stopPropagation();

      const countryName = d.properties?.name;
      if (!countryName) return;

      const iso2 = getIso2ForCountryName(countryName);
      const iso3 = numericToIso3?.get(String(d.id));

      if (selectedId === d.id) {
        selectedId = null;
        paths.classed('selected', false);
        if (onClear) onClear();
        return;
      }

      selectedId = d.id;
      paths.classed('selected', (f) => f.id === d.id);

      if (onSelect) {
        await onSelect({ id: d.id, name: countryName, iso2, iso3 });
      }
    });

  svg.on('click', () => {
    selectedId = null;
    paths.classed('selected', false);
    if (onClear) onClear();
  });

  return {
    clearSelection() {
      selectedId = null;
      paths.classed('selected', false);
    }
  };
}
