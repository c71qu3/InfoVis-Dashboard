import { fetchIntermediaries } from './api.js';

export function createListPanel({ onSelectIntermediary, onSelectEntity, onSelectOfficer } = {}) {
  const statusEl = document.getElementById('entities-status');
  const typeDescEl = document.getElementById('entities-type-desc');
  const summaryEl = document.getElementById('entities-summary');
  const listEl = document.getElementById('entities-list');
  const loadMoreBtn = document.getElementById('entities-load-more');
  const searchEl = document.getElementById('entities-search');
  const typeEl = document.getElementById('entities-type');

  let req = 0;
  let currentIso3 = null;
  let currentCountryName = '';
  let currentOffset = 0;
  let currentTotal = 0;
  let currentQuery = '';
  let currentType = 'all';
  const pageSize = 200;

  // Plural noun used in the summary line, depending on the selected type.
  const TYPE_NOUN = {
    all: 'results',
    entity: 'entities',
    officer: 'officers',
    intermediary: 'intermediaries',
  };

  // Short definitions shown under the type dropdown.
  const TYPE_DESC = {
    all: 'All node types (entities, officers, and intermediaries).',
    entity: 'Entity: an offshore company, trust, or fund.',
    officer: 'Officer: a person or company connected to an entity (e.g., director, shareholder, beneficiary).',
    intermediary: 'Intermediary: an agent or firm that helps set up offshore entities.',
  };

  function setStatus(text) {
    if (statusEl) statusEl.textContent = text;
  }

  function setTypeDescription() {
    if (!typeDescEl) return;
    typeDescEl.textContent = TYPE_DESC[currentType] || '';
  }

  function setSummary({ countryName = null, total = null, shown = null } = {}) {
    if (!summaryEl) return;

    if (!countryName) {
      summaryEl.textContent = '';
      return;
    }

    if (total == null || shown == null) {
      summaryEl.textContent = countryName;
      return;
    }

    const totalTxt = Number(total).toLocaleString();
    const shownTxt = Number(shown).toLocaleString();
    const noun = TYPE_NOUN[currentType] || 'results';
    summaryEl.textContent = `${countryName} · ${shownTxt} of ${totalTxt} ${noun}`;
  }

  let selectedIntermediaryId = null;

  function clearList() {
    if (listEl) listEl.innerHTML = '';
    if (loadMoreBtn) loadMoreBtn.style.display = 'none';
    currentOffset = 0;
    currentTotal = 0;
    selectedIntermediaryId = null;
  }

  function clear() {
    req++;
    currentIso3 = null;
    currentCountryName = '';
    setSummary({ countryName: null });
    clearList();
    setStatus('Select a country to view its nodes.');
  }

  function renderItems(items, { append = false } = {}) {
    if (!listEl) return;
    if (!append) listEl.innerHTML = '';

    const frag = document.createDocumentFragment();

    for (const it of (items || [])) {
      const row = document.createElement('button');
      row.type = 'button';
      row.className = 'entity-item'; // keep existing CSS

      const id = it?.id != null ? String(it.id) : '';
      const name = (it?.name || '').trim();
      const label = name || (it?.id != null ? String(it.id) : '(unnamed)');

      row.dataset.intermediaryId = id;
      row.dataset.intermediaryName = label;
      row.dataset.nodeType = (it?.type || '').toLowerCase();
      row.title = id ? `node_id: ${id}` : '';
      row.textContent = label;

      if (selectedIntermediaryId && id && selectedIntermediaryId === id) {
        row.classList.add('selected');
      }

      frag.appendChild(row);
    }

    listEl.appendChild(frag);
  }

  async function loadNextPage(token) {
    if (!currentIso3) return;

    let data;
    try {
      data = await fetchIntermediaries(currentIso3, {
        limit: pageSize,
        offset: currentOffset,
        q: currentQuery || null,
        type: currentType,
      });
    } catch (e) {
      if (token !== req) return;
      console.warn('Could not load list', e);
      setStatus('Error loading results.');
      if (loadMoreBtn) loadMoreBtn.style.display = 'none';
      return;
    }

    if (token !== req) return;

    if (data?.error) {
      setStatus('Graph database unavailable.');
      if (loadMoreBtn) loadMoreBtn.style.display = 'none';
      return;
    }

    const items = data?.items || [];
    currentTotal = Number.isFinite(Number(data?.total)) ? Number(data.total) : currentTotal;

    const isFirst = currentOffset === 0;
    if (isFirst && items.length === 0) {
      clearList();
      setSummary({ countryName: currentCountryName, total: currentTotal, shown: 0 });
      setStatus(currentQuery ? 'No matches for your search.' : 'No results for this country.');
      return;
    }

    renderItems(items, { append: !isFirst });

    currentOffset += items.length;

    setStatus('');
    setSummary({ countryName: currentCountryName, total: currentTotal, shown: currentOffset });

    const hasMore = currentOffset < currentTotal;
    if (loadMoreBtn) loadMoreBtn.style.display = hasMore ? 'block' : 'none';
  }

  async function loadIntermediaries(iso3, countryName = '') {
    const token = ++req;

    currentIso3 = iso3;
    currentCountryName = countryName || '';
    currentOffset = 0;
    currentTotal = 0;

    clearList();

    if (!iso3) {
      setSummary({ countryName: currentCountryName, total: 0, shown: 0 });
      setStatus('No ISO3 code for this selection.');
      return;
    }

    setStatus('Loading…');
    setSummary({ countryName: currentCountryName, total: null, shown: null });

    await loadNextPage(token);
  }

  // Re-run the current query from the top (used by the search box / type filter).
  async function reload() {
    if (!currentIso3) return;
    const token = ++req;
    currentOffset = 0;
    currentTotal = 0;
    clearList();
    setStatus('Loading…');
    setSummary({ countryName: currentCountryName, total: null, shown: null });
    await loadNextPage(token);
  }

  // Backwards-compatible alias (older callers still call loadEntities)
  const loadEntities = loadIntermediaries;

  if (searchEl) {
    let debounce = null;
    searchEl.addEventListener('input', () => {
      currentQuery = searchEl.value.trim();
      clearTimeout(debounce);
      debounce = setTimeout(reload, 250);
    });
  }

  if (typeEl) {
    // Ensure our internal default matches the DOM default.
    currentType = typeEl.value || currentType;

    typeEl.addEventListener('change', () => {
      currentType = typeEl.value || 'all';
      setTypeDescription();
      reload();
    });
  }

  if (loadMoreBtn) {
    loadMoreBtn.addEventListener('click', async () => {
      const token = req;
      loadMoreBtn.disabled = true;
      try {
        await loadNextPage(token);
      } finally {
        loadMoreBtn.disabled = false;
      }
    });
  }

  if (listEl) {
    listEl.addEventListener('click', (e) => {
      const btn = e.target?.closest?.('button.entity-item');
      if (!btn) return;

      const nodeId = btn.dataset.intermediaryId || '';
      const nodeName = btn.dataset.intermediaryName || btn.textContent || '';
      const nodeType = btn.dataset.nodeType || '';
      if (!nodeId) return;

      // UI selected state
      selectedIntermediaryId = nodeId;
      for (const el of listEl.querySelectorAll('button.entity-item.selected')) el.classList.remove('selected');
      btn.classList.add('selected');

      // Open the focused graph that matches the clicked node's type.
      if (nodeType === 'entity' && typeof onSelectEntity === 'function') {
        onSelectEntity(nodeId, nodeName, currentIso3);
      } else if (nodeType === 'intermediary' || !nodeType) {
        if (typeof onSelectIntermediary === 'function') onSelectIntermediary(nodeId, nodeName, currentIso3);
        else if (typeof onSelectEntity === 'function') onSelectEntity(nodeId, nodeName, currentIso3);
      } else if (nodeType === 'officer') {
        if (typeof onSelectOfficer === 'function') onSelectOfficer(nodeId, nodeName, currentIso3);
        else if (typeof onSelectEntity === 'function') onSelectEntity(nodeId, nodeName, currentIso3);
      }
    });
  }

  setTypeDescription();
  clear();

  return {
    clear,
    loadIntermediaries,
    loadEntities,
  };
}
