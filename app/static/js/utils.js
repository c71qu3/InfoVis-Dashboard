// shared utilities

/**
 * Format numbers for display (k/M/B/T). Returns an em dash for null/undefined.
 * @param {any} v
 */
export function fmt(v) {
  if (v == null) return '—';
  const n = +v;
  if (Number.isNaN(n)) return v;
  if (Math.abs(n) >= 1e12) return (n / 1e12).toFixed(2) + ' T';
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(2) + ' B';
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + ' M';
  return n.toFixed(2);
}

/**
 * Wire a click-to-toggle "i" info popover. Closes on outside click or Escape.
 * @param {string} btnId      id of the toggle button
 * @param {string} popoverId  id of the popover element (uses the `hidden` attr)
 */
export function initInfoPopover(btnId, popoverId) {
  const btn = document.getElementById(btnId);
  const popover = document.getElementById(popoverId);
  if (!btn || !popover) return;

  const setOpen = (open) => {
    popover.hidden = !open;
    btn.setAttribute('aria-expanded', String(open));
  };

  btn.addEventListener('click', (e) => {
    e.stopPropagation();
    setOpen(popover.hidden);
  });
  document.addEventListener('click', (e) => {
    if (!popover.hidden && !popover.contains(e.target)) setOpen(false);
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') setOpen(false);
  });
}
