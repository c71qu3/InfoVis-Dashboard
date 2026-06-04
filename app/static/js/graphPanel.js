import { fetchEntityGraph, fetchEntityFocusedGraph, fetchIntermediaryFocusedGraph, fetchJurisdictions } from './api.js';

const TYPE_COLORS = {
  Entity: '#4f8ef7',
  Officer: '#f7a24f',
  Intermediary: '#5fd0a0',
  Address: '#b07ff7',
  Node: '#6b7a99'
};

const LINE_PALETTE = ['#4f8ef7', '#f7a24f', '#5fd0a0', '#b07ff7', '#e0708f', '#5cc8e0', '#d8c45a', '#8c7bf0'];

export function createGraphPanel({ tooltip }) {
  const graphHint = document.getElementById('graph-hint');

  let graphSvg = null;
  let graphG = null;
  let graphSim = null;
  let graphZoom = null;
  let graphReq = 0;

  function ensureGraphSvg() {
    if (graphSvg) return;
    const el = document.getElementById('graph-panel');
    graphSvg = d3.select('#graph-panel')
      .append('svg')
      .attr('viewBox', `0 0 ${el.clientWidth} ${el.clientHeight}`);
    graphG = graphSvg.append('g');
    graphZoom = d3.zoom().scaleExtent([0.2, 5]).on('zoom', (e) => graphG.attr('transform', e.transform));
    graphSvg.call(graphZoom);
  }

  function fitView(nodes, rScale) {
    if (!nodes.length) return;
    const el = document.getElementById('graph-panel');
    const W = el.clientWidth;
    const H = el.clientHeight;
    const pad = 16;

    const minX = d3.min(nodes, (d) => d.x - rScale(d.degree));
    const maxX = d3.max(nodes, (d) => d.x + rScale(d.degree)) + 90; // room for labels
    const minY = d3.min(nodes, (d) => d.y - rScale(d.degree));
    const maxY = d3.max(nodes, (d) => d.y + rScale(d.degree)) + 8;

    const gw = (maxX - minX) || 1;
    const gh = (maxY - minY) || 1;
    const scale = Math.min((W - pad * 2) / gw, (H - pad * 2) / gh, 2);
    const tx = (W - scale * (minX + maxX)) / 2;
    const ty = (H - scale * (minY + maxY)) / 2;

    graphSvg.transition().duration(400)
      .call(graphZoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
  }

  function nodeDrag() {
    return d3.drag()
      .on('start', (e, d) => {
        if (!e.active && graphSim) graphSim.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
      })
      .on('drag', (e, d) => {
        d.fx = e.x;
        d.fy = e.y;
      })
      .on('end', (e, d) => {
        if (!e.active && graphSim) graphSim.alphaTarget(0);
        d.fx = null;
        d.fy = null;
      });
  }

  function clear() {
    graphReq++; // invalidate any in-flight request
    if (graphSim) graphSim.stop();
    if (graphG) graphG.selectAll('*').remove();
    graphHint.style.display = '';
    graphHint.textContent = 'Click a country to see its offshore network.';
  }

  async function loadEntityGraph(iso3, name) {
    const token = ++graphReq;

    if (!iso3) {
      clear();
      graphHint.textContent = `No offshore network for ${name || 'this country'}.`;
      return;
    }

    graphHint.style.display = '';
    graphHint.textContent = 'Loading network…';

    let data;
    try {
      data = await fetchEntityGraph(iso3);
    } catch (e) {
      if (token !== graphReq) return;
      console.warn(e);
      graphHint.textContent = 'Error loading network.';
      return;
    }

    if (token !== graphReq) return;

    if (data?.error) {
      clear();
      graphHint.textContent = 'Graph database unavailable.';
      return;
    }

    if (!data?.nodes || data.nodes.length === 0) {
      clear();
      graphHint.textContent = `No offshore network for ${name || iso3}.`;
      return;
    }

    graphHint.style.display = 'none';
    renderEntityGraph(data);
  }

  async function loadEntityFocusGraph(entityId, entityName = '') {
    const token = ++graphReq;

    const id = (entityId ?? '').toString().trim();
    if (!id) {
      graphHint.style.display = '';
      graphHint.textContent = 'No entity selected.';
      return;
    }

    graphHint.style.display = '';
    graphHint.textContent = `Loading graph for ${entityName || 'entity'}…`;

    let data;
    try {
      data = await fetchEntityFocusedGraph(id);
    } catch (e) {
      if (token !== graphReq) return;
      console.warn(e);
      graphHint.textContent = 'Error loading entity graph.';
      return;
    }

    if (token !== graphReq) return;

    if (data?.error) {
      clear();
      graphHint.textContent = 'Graph database unavailable.';
      return;
    }

    if (!data?.nodes || data.nodes.length === 0) {
      clear();
      graphHint.textContent = `No graph found for ${entityName || id}.`;
      return;
    }

    graphHint.style.display = 'none';
    renderEntityGraph(data, id);
  }

  async function loadIntermediaryFocusGraph(intermediaryId, intermediaryName = '', iso3 = null) {
    const token = ++graphReq;

    const id = (intermediaryId ?? '').toString().trim();
    if (!id) {
      graphHint.style.display = '';
      graphHint.textContent = 'No intermediary selected.';
      return;
    }

    graphHint.style.display = '';
    graphHint.textContent = `Loading graph for ${intermediaryName || 'intermediary'}…`;

    let data;
    try {
      data = await fetchIntermediaryFocusedGraph(id, { iso3 });
    } catch (e) {
      if (token !== graphReq) return;
      console.warn(e);
      graphHint.textContent = 'Error loading intermediary graph.';
      return;
    }

    if (token !== graphReq) return;

    if (data?.error) {
      clear();
      graphHint.textContent = 'Graph database unavailable.';
      return;
    }

    if (!data?.nodes || data.nodes.length === 0) {
      clear();
      graphHint.textContent = `No graph found for ${intermediaryName || id}.`;
      return;
    }

    graphHint.style.display = 'none';
    renderEntityGraph(data, id);
  }

  function renderEntityGraph(data, focusId = null) {
    if (graphSim) graphSim.stop();
    ensureGraphSvg();

    document.getElementById('graph-legend').style.display = 'flex';

    const el = document.getElementById('graph-panel');
    const W = el.clientWidth;
    const H = el.clientHeight;

    graphG.selectAll('*').remove();

    const nodes = data.nodes.map((n) => ({ ...n }));

    // collapse repeat links between the same pair; weight = connection frequency
    const linkMap = new Map();
    for (const l of data.links) {
      const key = l.source < l.target ? `${l.source}|${l.target}` : `${l.target}|${l.source}`;
      const existing = linkMap.get(key);
      if (existing) existing.weight++;
      else linkMap.set(key, { source: l.source, target: l.target, rel: l.rel, weight: 1 });
    }
    const links = [...linkMap.values()];

    const maxDeg = d3.max(nodes, (d) => d.degree) || 1;
    const rScale = d3.scaleSqrt().domain([0, maxDeg]).range([3.5, 15]);
    const maxWeight = d3.max(links, (d) => d.weight) || 1;
    const wScale = d3.scaleLinear().domain([1, maxWeight]).range([1, 4]);
    const oScale = d3.scaleLinear().domain([1, maxWeight]).range([0.35, 0.85]);

    const link = graphG.append('g').selectAll('line')
      .data(links).join('line')
      .attr('class', 'glink')
      .attr('stroke-width', (d) => wScale(d.weight))
      .attr('stroke-opacity', (d) => oScale(d.weight));

    const focus = focusId != null ? String(focusId) : null;

    const node = graphG.append('g').selectAll('circle')
      .data(nodes).join('circle')
      .attr('class', 'gnode')
      .attr('r', (d) => rScale(d.degree))
      .attr('fill', (d) => TYPE_COLORS[d.type] || TYPE_COLORS.Node)
      .attr('stroke', (d) => (focus && String(d.id) === focus ? '#fff' : '#0f1117'))
      .attr('stroke-width', (d) => (focus && String(d.id) === focus ? 2.2 : 1))
      .call(nodeDrag())
      .on('mouseover', (e, d) => tooltip.show(e, `${d.label} · ${d.type}${d.degree ? ` · ${d.degree} links` : ''}`))
      .on('mousemove', tooltip.move)
      .on('mouseout', tooltip.hide);

    // only label the top hubs by degree so the view stays readable
    const hubIds = new Set([...nodes].sort((a, b) => b.degree - a.degree).slice(0, 8).map((d) => d.id));
    const hubs = nodes.filter((d) => hubIds.has(d.id) && d.degree > 1);
    const label = graphG.append('g').selectAll('text')
      .data(hubs).join('text')
      .attr('class', 'glabel')
      .text((d) => (d.label.length > 18 ? d.label.slice(0, 17) + '…' : d.label));

    graphSim = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id((d) => d.id).distance(34).strength(0.6))
      .force('charge', d3.forceManyBody().strength(-60))
      .force('center', d3.forceCenter(W / 2, H / 2))
      .force('x', d3.forceX(W / 2).strength(0.06))
      .force('y', d3.forceY(H / 2).strength(0.06))
      .force('collide', d3.forceCollide().radius((d) => rScale(d.degree) + 3))
      .on('tick', () => {
        link
          .attr('x1', (d) => d.source.x)
          .attr('y1', (d) => d.source.y)
          .attr('x2', (d) => d.target.x)
          .attr('y2', (d) => d.target.y);
        node.attr('cx', (d) => d.x).attr('cy', (d) => d.y);
        label.attr('x', (d) => d.x + rScale(d.degree) + 2).attr('y', (d) => d.y + 3);
      })
      .on('end', () => fitView(nodes, rScale));
  }

  async function loadJurisdictionFlows(focus = null) {
    const token = ++graphReq;

    graphHint.style.display = '';
    graphHint.textContent = 'Loading jurisdiction flows…';

    let data;
    try {
      data = await fetchJurisdictions(focus);
    } catch (e) {
      if (token !== graphReq) return;
      console.warn(e);
      graphHint.textContent = 'Error loading flows.';
      return;
    }

    if (token !== graphReq) return;

    if (data?.error) {
      clear();
      graphHint.textContent = 'Graph database unavailable.';
      return;
    }

    if (!data?.nodes || data.nodes.length === 0) {
      clear();
      graphHint.textContent = focus ? `No cross-border flows for ${focus}.` : 'No jurisdiction data.';
      return;
    }

    graphHint.style.display = 'none';
    renderJurisdictionFlows(data);
  }

  function renderJurisdictionFlows(data) {
    if (graphSim) graphSim.stop();
    ensureGraphSvg();

    document.getElementById('graph-legend').style.display = 'none'; // entity legend N/A here
    graphG.selectAll('*').remove();

    const el = document.getElementById('graph-panel');
    const W = el.clientWidth;
    const H = el.clientHeight;

    const nodes = data.nodes.map((n) => ({ ...n }));

    const srcColor = new Map();
    let ci = 0;
    const links = data.links.map((l) => {
      if (!srcColor.has(l.source)) srcColor.set(l.source, LINE_PALETTE[ci++ % LINE_PALETTE.length]);
      return { ...l, color: srcColor.get(l.source) };
    });

    const maxDeg = d3.max(nodes, (d) => d.degree) || 1;
    const rScale = d3.scaleSqrt().domain([0, maxDeg]).range([5, 20]);
    const maxW = d3.max(links, (d) => d.weight) || 1;
    const wScale = d3.scaleLinear().domain([1, maxW]).range([1.2, 6]);

    const link = graphG.append('g').selectAll('path')
      .data(links).join('path')
      .attr('fill', 'none')
      .attr('stroke', (d) => d.color)
      .attr('stroke-opacity', 0.55)
      .attr('stroke-width', (d) => wScale(d.weight))
      .attr('stroke-linecap', 'round');

    const node = graphG.append('g').selectAll('circle')
      .data(nodes).join('circle')
      .attr('class', 'gnode')
      .attr('r', (d) => rScale(d.degree))
      .attr('fill', (d) => (d.focus ? '#f7a24f' : '#cdd6f0'))
      .attr('stroke', (d) => (d.focus ? '#fff' : '#0f1117'))
      .attr('stroke-width', (d) => (d.focus ? 2 : 1))
      .call(nodeDrag())
      .on('mouseover', (e, d) => tooltip.show(e, `${d.label} · ${d.degree.toLocaleString()} cross-border links`))
      .on('mousemove', tooltip.move)
      .on('mouseout', tooltip.hide);

    const label = graphG.append('g').selectAll('text')
      .data(nodes).join('text')
      .attr('class', 'glabel')
      .text((d) => d.id);

    const ticked = () => {
      link.attr('d', (d) => {
        const x1 = d.source.x;
        const y1 = d.source.y;
        const x2 = d.target.x;
        const y2 = d.target.y;
        const cx = (x1 + x2) / 2 - (y2 - y1) * 0.15;
        const cy = (y1 + y2) / 2 + (x2 - x1) * 0.15;
        return `M${x1},${y1} Q${cx},${cy} ${x2},${y2}`;
      });
      node.attr('cx', (d) => d.x).attr('cy', (d) => d.y);
      label.attr('x', (d) => d.x + rScale(d.degree) + 3).attr('y', (d) => d.y + 3);
    };

    graphSim = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id((d) => d.id).distance(60).strength(0.2))
      .force('charge', d3.forceManyBody().strength(-150))
      .force('center', d3.forceCenter(W / 2, H / 2))
      .force('x', d3.forceX(W / 2).strength(0.08))
      .force('y', d3.forceY(H / 2).strength(0.08))
      .force('collide', d3.forceCollide().radius((d) => rScale(d.degree) + 6))
      .stop();

    for (let i = 0; i < 300; i++) graphSim.tick();
    ticked();
    fitView(nodes, rScale);

    graphSim.on('tick', ticked).on('end', () => fitView(nodes, rScale));
  }

  return {
    clear,
    loadEntityGraph,
    loadEntityFocusGraph,
    loadIntermediaryFocusGraph,
    loadJurisdictionFlows
  };
}
