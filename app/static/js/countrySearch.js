// Search-to-select dropdown for picking a country by name.
// Complements clicking on the map, useful for small countries (e.g. Hong Kong).
export function createCountrySearch({ inputId, resultsId, countries, onSelect }) {
  const input = document.getElementById(inputId);
  const results = document.getElementById(resultsId);
  if (!input || !results) return { clear() {} };

  const all = [...countries].sort((a, b) => a.name.localeCompare(b.name));

  let matches = [];
  let active = -1;

  function close() {
    results.innerHTML = '';
    results.style.display = 'none';
    matches = [];
    active = -1;
  }

  function highlight() {
    [...results.children].forEach((el, i) => el.classList.toggle('active', i === active));
  }

  function pick(c) {
    if (!c) return;
    input.value = c.name;
    close();
    input.blur();
    onSelect(c);
  }

  function render() {
    const q = input.value.trim().toLowerCase();
    if (!q) { close(); return; }

    matches = all.filter((c) => c.name.toLowerCase().includes(q)).slice(0, 8);

    if (matches.length === 0) {
      results.innerHTML = '<div class="country-search-empty">No matching country with an offshore network</div>';
      results.style.display = 'block';
      active = -1;
      return;
    }

    active = 0;
    results.innerHTML = '';
    matches.forEach((c, i) => {
      const item = document.createElement('button');
      item.type = 'button';
      item.className = 'country-search-item' + (i === active ? ' active' : '');
      item.textContent = c.name;
      // mousedown (not click) so it fires before the input blur closes the list.
      item.addEventListener('mousedown', (e) => { e.preventDefault(); pick(c); });
      item.addEventListener('mouseenter', () => { active = i; highlight(); });
      results.appendChild(item);
    });
    results.style.display = 'block';
  }

  input.addEventListener('input', render);
  input.addEventListener('focus', () => { if (input.value.trim()) render(); });

  input.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (matches.length) { active = (active + 1) % matches.length; highlight(); }
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (matches.length) { active = (active - 1 + matches.length) % matches.length; highlight(); }
    } else if (e.key === 'Enter') {
      e.preventDefault();
      if (active >= 0) pick(matches[active]);
    } else if (e.key === 'Escape') {
      close();
      input.blur();
    }
  });

  // Close when clicking outside the search widget.
  document.addEventListener('click', (e) => {
    if (e.target !== input && !results.contains(e.target)) close();
  });

  return {
    clear() {
      input.value = '';
      close();
    }
  };
}
