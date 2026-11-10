'use client';
import { MouseEvent } from 'react';

// One shared tooltip node, moved imperatively so mousemove never re-renders the table.
let tipEl: HTMLDivElement | null = null;

function hide() {
  if (tipEl) tipEl.style.display = 'none';
}

function getTip(): HTMLDivElement {
  if (!tipEl) {
    tipEl = document.createElement('div');
    tipEl.setAttribute('role', 'tooltip');
    Object.assign(tipEl.style, {
      position: 'fixed',
      display: 'none',
      pointerEvents: 'none',
      zIndex: '1000',
      maxWidth: '420px',
      padding: '6px 10px',
      borderRadius: '6px',
      fontSize: '12.5px',
      lineHeight: '1.4',
      overflowWrap: 'anywhere',
      boxShadow: '0 4px 14px rgba(0,0,0,0.18)',
    });
    document.body.appendChild(tipEl);
    window.addEventListener('scroll', hide, true);
  }
  return tipEl;
}

/** Handlers that show `full` next to the cursor while the element is cut with an ellipsis (or always). */
export function cutTooltip(
  full: string,
  colors: { bg: string; fg: string },
  always = false
) {
  return {
    onMouseMove: (e: MouseEvent<HTMLElement>) => {
      const el = e.currentTarget;
      if (!full || (!always && el.scrollWidth <= el.clientWidth)) return hide();
      const tip = getTip();
      tip.textContent = full;
      tip.style.background = colors.bg;
      tip.style.color = colors.fg;
      tip.style.display = 'block';
      // flip to the left of the cursor near the right edge of the window
      const flip = e.clientX + 12 + tip.offsetWidth > window.innerWidth - 8;
      tip.style.left = `${flip ? e.clientX - 12 - tip.offsetWidth : e.clientX + 12}px`;
      tip.style.top = `${e.clientY + 18}px`;
    },
    onMouseLeave: hide,
  };
}
