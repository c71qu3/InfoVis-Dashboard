/**
 * Tooltip helper.
 * Exposes { show(e, text), move(e), hide() }.
 */
export function initTooltip(tooltipId = 'tooltip') {
  const tooltip = document.getElementById(tooltipId);
  if (!tooltip) {
    // Fail softly so the rest of the UI still works.
    return {
      show() {},
      move() {},
      hide() {}
    };
  }

  function move(e) {
    tooltip.style.left = (e.clientX + 12) + 'px';
    tooltip.style.top = (e.clientY - 8) + 'px';
  }

  function show(e, text) {
    tooltip.style.display = 'block';
    tooltip.textContent = text;
    move(e);
  }

  function hide() {
    tooltip.style.display = 'none';
  }

  return { show, move, hide };
}
