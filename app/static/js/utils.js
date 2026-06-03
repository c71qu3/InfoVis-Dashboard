// Small shared utilities

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
