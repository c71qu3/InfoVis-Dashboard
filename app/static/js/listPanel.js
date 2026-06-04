import { fetchEntities } from './api.js';

export function createListPanel({ onSelectEntity } = {}) {
  const statusEl = document.getElementById('entities-status');
  const summaryEl = document.getElementById('entities-summary');
  const listEl = document.getElementById('entities-list');
  const loadMoreBtn = document.getElementById('entities-load-more');

  let req = 0;
  let currentIso3 = null;
  let currentCountryName = '';
  let currentOffset = 0;
  let currentTotal = 0;
  const pageSize = 200;

  function setStatus(text) {
    if (statusEl) statusEl.textContent = text;
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
    summaryEl.textContent = `${countryName} · ${shownTxt} of ${totalTxt} entities`;
  }

  let selectedEntityId = null;

  function clearList() {
    if (listEl) listEl.innerHTML = '';
    if (loadMoreBtn) loadMoreBtn.style.display = 'none';
    currentOffset = 0;
    currentTotal = 0;
    selectedEntityId = null;
  }

  function clear() {
    req++;
    currentIso3 = null;
    currentCountryName = '';
    setSummary({ countryName: null });
    clearList();
    setStatus('Select a country to view its entities.');
  }

  function renderItems(items, { append = false } = {}) {
    if (!listEl) return;
    if (!append) listEl.innerHTML = '';

    const frag = document.createDocumentFragment();

    for (const it of (items || [])) {
      const row = document.createElement('button');
      row.type = 'button';
      row.className = 'entity-item';

      const id = it?.id != null ? String(it.id) : '';
      const name = (it?.name || '').trim();
      const label = name || (it?.id != null ? String(it.id) : '(unnamed)');

      row.dataset.entityId = id;
      row.dataset.entityName = label;
      row.title = id ? `node_id: ${id}` : '';
      row.textContent = label;

      if (selectedEntityId && id && selectedEntityId === id) {
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
      data = await fetchEntities(currentIso3, { limit: pageSize, offset: currentOffset });
    } catch (e) {
      if (token !== req) return;
      console.warn('Could not load entities', e);
      setStatus('Error loading entities.');
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
      setStatus('No entities found for this country.');
      return;
    }

    renderItems(items, { append: !isFirst });

    currentOffset += items.length;

    setStatus('');
    setSummary({ countryName: currentCountryName, total: currentTotal, shown: currentOffset });

    const hasMore = currentOffset < currentTotal;
    if (loadMoreBtn) loadMoreBtn.style.display = hasMore ? 'block' : 'none';
  }

  async function loadEntities(iso3, countryName = '') {
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

    setStatus('Loading entities…');
    setSummary({ countryName: currentCountryName, total: null, shown: null });

    await loadNextPage(token);
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

      const entityId = btn.dataset.entityId || '';
      const entityName = btn.dataset.entityName || btn.textContent || '';
      if (!entityId) return;

      // UI selected state
      selectedEntityId = entityId;
      for (const el of listEl.querySelectorAll('button.entity-item.selected')) el.classList.remove('selected');
      btn.classList.add('selected');

      if (typeof onSelectEntity === 'function') onSelectEntity(entityId, entityName);
    });
  }

  clear();

  return {
    clear,
    loadEntities
  };
}
