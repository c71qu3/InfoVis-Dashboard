import { fetchIndicators, fetchYears } from './api.js';

export function createInfoPanel({ fmt }) { // AI assisted
  // DOM elements for info panel
  const nameEl = document.getElementById('country-name');
  const loadingEl = document.getElementById('loading-indicator');
  const gridEl = document.getElementById('indicators-grid');
  const hintEl = document.getElementById('info-hint');

  // Request token to keep the panel responsive when users click quickly.
  let infoReq = 0;

  function clear() {
    infoReq++; // invalidate any in-flight load
    nameEl.textContent = 'Click a country on the map, or search above';
    nameEl.classList.add('empty');
    gridEl.innerHTML = '';
    hintEl.style.display = '';
    loadingEl.classList.remove('active');
    const sel = document.getElementById('year-selector');
    if (sel) sel.remove();
  }

  function showNoData(countryName) {
    clear();
    nameEl.textContent = countryName;
    nameEl.classList.remove('empty');
    hintEl.textContent = `No World Bank data for "${countryName}".`;
  }

  async function loadCountry(name, iso2) {
    const token = ++infoReq;

    nameEl.textContent = name;
    nameEl.classList.remove('empty');
    gridEl.innerHTML = '';
    hintEl.style.display = 'none';
    loadingEl.classList.add('active');

    const years = await fetchYears(iso2);
    if (token !== infoReq) return;

    // (re)build year selector
    const existing = document.getElementById('year-selector');
    if (existing) existing.remove();

    const selectorDiv = document.createElement('div');
    selectorDiv.id = 'year-selector';
    selectorDiv.style.marginBottom = '1rem';
    selectorDiv.innerHTML = `
      <label style="font-size:0.7rem; color:var(--muted);">Year: </label>
      <select id="year-dropdown" style="background:#1a2133; color:var(--text); border:1px solid var(--border); padding:0.2rem 0.5rem;">
        <option value="latest">Latest (per indicator)</option>
        ${years.map((y) => `<option value="${y}">${y}</option>`).join('')}
      </select>
    `;
    nameEl.parentNode.insertBefore(selectorDiv, nameEl.nextSibling);

    const dropdown = document.getElementById('year-dropdown');

    await loadIndicatorsIntoGrid(iso2, 'latest', token);
    if (token !== infoReq) return;
    loadingEl.classList.remove('active');

    dropdown.addEventListener('change', async (e) => {
      if (token !== infoReq) return;
      loadingEl.classList.add('active');
      gridEl.innerHTML = '';
      await loadIndicatorsIntoGrid(iso2, e.target.value, token);
      if (token !== infoReq) return;
      loadingEl.classList.remove('active');
    });
  }

  async function loadIndicatorsIntoGrid(iso2, yearMode, token) {
    try {
      const data = await fetchIndicators(iso2, yearMode);
      if (token !== infoReq) return;
      renderIndicators(data);
    } catch (err) {
      if (token !== infoReq) return;
      console.warn(err);
      hintEl.textContent = 'Error loading indicators.';
      hintEl.style.display = '';
    }
  }

  function renderIndicators(data) {
    gridEl.innerHTML = '';
    for (const [label, info] of Object.entries(data || {})) {
      const card = document.createElement('div');
      card.className = 'indicator-card';
      card.innerHTML = `
        <div class="ind-label">${label}</div>
        <div class="ind-value">${fmt(info.value)}</div>
        ${info.year ? `<div class="ind-year">${info.year}</div>` : ''}
      `;
      gridEl.appendChild(card);
    }

    if (!data || Object.keys(data).length === 0) {
      hintEl.textContent = 'No indicators found.';
      hintEl.style.display = '';
    }
  }

  return { clear, loadCountry, showNoData };
}
