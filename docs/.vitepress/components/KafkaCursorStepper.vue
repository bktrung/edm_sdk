<template>
  <figure
    class="f1-cursor"
    tabindex="0"
    aria-label="Kafka committed offset stepper"
    @keydown.left.prevent="previousStep"
    @keydown.right.prevent="nextStep"
  >
    <div class="f1-cursor__header">
      <strong>One partition, acks out of order</strong>
      <span class="f1-cursor__meta">step {{ stepIndex }} / {{ steps.length - 1 }} &middot; committed position {{ current.cursor }}</span>
    </div>

    <div class="f1-cursor__scroll">
      <svg class="f1-cursor__stage" :viewBox="`0 0 ${width} 150`" role="img" :aria-label="stageLabel">
        <!-- The cursor pointer above the current offset. -->
        <g class="pointer" :transform="`translate(${cellX(current.cursor) + cellWidth / 2}, 0)`">
          <rect x="-34" y="4" width="68" height="22" rx="11" />
          <text x="0" y="19" text-anchor="middle">committed</text>
          <line x1="0" y1="26" x2="0" y2="40" />
        </g>

        <g v-for="offset in offsets" :key="offset" :class="cellClass(offset)">
          <rect class="cell" :x="cellX(offset)" y="44" :width="cellWidth" height="46" rx="8" />
          <text class="cell__label" :x="cellX(offset) + cellWidth / 2" y="73" text-anchor="middle">{{ offset }}</text>
        </g>

        <!-- The settlement attempt of this step, under the offset it names. -->
        <g v-if="current.offset !== null" class="attempt" :class="`attempt--${current.result}`" :transform="`translate(${cellX(current.offset) + cellWidth / 2}, 0)`">
          <line x1="0" y1="112" x2="0" y2="96" />
          <path d="M -5 101 L 0 94 L 5 101 z" />
          <rect x="-46" y="114" width="92" height="24" rx="12" />
          <text x="0" y="130" text-anchor="middle">{{ current.label }}</text>
        </g>
      </svg>
    </div>

    <div class="f1-cursor__legend" aria-hidden="true">
      <span><i class="swatch swatch--committed" />done, below the committed offset</span>
      <span><i class="swatch swatch--current" />current, the next offset to ack</span>
      <span><i class="swatch swatch--waiting" />not yet acked</span>
    </div>

    <p class="f1-cursor__caption" aria-live="polite">{{ current.caption }}</p>

    <div class="f1-cursor__controls">
      <button type="button" :disabled="stepIndex === 0" @click="previousStep">Prev</button>
      <button type="button" :disabled="stepIndex === steps.length - 1" @click="nextStep">Next</button>
      <button type="button" @click="reset">Reset</button>
      <span class="f1-cursor__hint">or focus the figure and use the arrow keys</span>
    </div>
  </figure>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'

type Result = 'advance' | 'refused' | 'requeue'
type Step = { offset: number | null; label: string; result: Result; cursor: number; caption: string }

// A scripted storyboard of the table on this page: each step restates one row.
// Nothing here models the driver, so the figure can only go stale when the
// table does.
const steps: Step[] = [
  { offset: null, label: '', result: 'advance', cursor: 10, caption: 'The partition starts with its committed offset at 10: every offset below it is done.' },
  { offset: 10, label: 'ack', result: 'advance', cursor: 11, caption: 'Offset 10 is acked. It is the current offset, so the committed offset moves to 11.' },
  { offset: 12, label: 'stale ack, refused', result: 'refused', cursor: 11, caption: 'A stale ack for offset 12 arrives from an earlier ownership while 11 is still current. It is refused rather than allowed to skip record 11.' },
  { offset: 11, label: 'requeue', result: 'requeue', cursor: 11, caption: 'Offset 11 is requeued. The record stays unacked and the committed offset does not move.' },
  { offset: 11, label: 'ack', result: 'advance', cursor: 12, caption: 'Offset 11 is acked, so the committed offset moves to 12.' },
  { offset: 12, label: 'ack', result: 'advance', cursor: 13, caption: 'Offset 12 is now current and is acked, so the committed offset moves to 13.' },
  { offset: 13, label: 'nack, no requeue', result: 'advance', cursor: 14, caption: 'Offset 13 is nacked without requeue. A discard moves the committed offset like an ack, to 14.' },
  { offset: 15, label: 'stale ack, refused', result: 'refused', cursor: 14, caption: 'A stale ack for offset 15 arrives from an earlier ownership while 14 is still current, so it is refused.' },
  { offset: 14, label: 'ack', result: 'advance', cursor: 15, caption: 'Offset 14 is acked, so the committed offset moves to 15.' },
  { offset: 15, label: 'ack', result: 'advance', cursor: 16, caption: 'Offset 15 is acked, so the committed offset moves to 16, the next offset Kafka should deliver.' },
]

const offsets = [10, 11, 12, 13, 14, 15, 16]
const cellWidth = 76
const gap = 12
const margin = 20
const width = margin * 2 + offsets.length * cellWidth + (offsets.length - 1) * gap

const stepIndex = ref(0)
const current = computed(() => steps[stepIndex.value])
const stageLabel = computed(() => `Offsets 10 to 16. The committed offset is ${current.value.cursor}.`)

function cellX(offset: number): number {
  return margin + (offset - offsets[0]) * (cellWidth + gap)
}

function cellClass(offset: number): string {
  if (offset < current.value.cursor) return 'state--committed'
  if (offset === current.value.cursor) return 'state--current'
  return 'state--waiting'
}

function previousStep() {
  if (stepIndex.value > 0) stepIndex.value -= 1
}

function nextStep() {
  if (stepIndex.value < steps.length - 1) stepIndex.value += 1
}

function reset() {
  stepIndex.value = 0
}
</script>

<style scoped>
.f1-cursor {
  --f1-ok: #16a34a;
  --f1-back: #d97706;
  margin: 24px 0;
  padding: 16px 18px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg-soft);
  color: var(--vp-c-text-1);
  font-size: 14px;
}

:global(.dark) .f1-cursor {
  --f1-ok: #4ade80;
  --f1-back: #fbbf24;
}

.f1-cursor:focus-visible {
  outline: 2px solid var(--vp-c-brand-1);
  outline-offset: 2px;
}

.f1-cursor__header {
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  align-items: baseline;
  gap: 8px;
}

.f1-cursor__meta {
  color: var(--vp-c-text-2);
  font-family: var(--vp-font-family-mono);
  font-size: 12px;
}

.f1-cursor__scroll {
  margin: 8px 0 0;
  overflow-x: auto;
}

.f1-cursor__stage {
  display: block;
  width: 100%;
  min-width: 480px;
  height: auto;
}

.cell {
  stroke-width: 1.5;
  transition: fill 0.25s ease, stroke 0.25s ease;
}

.cell__label {
  font-family: var(--vp-font-family-mono);
  font-size: 16px;
  font-weight: 600;
}

.state--committed .cell {
  fill: color-mix(in srgb, var(--f1-ok) 22%, transparent);
  stroke: var(--f1-ok);
}

.state--committed .cell__label {
  fill: var(--vp-c-text-1);
}

.state--current .cell {
  fill: var(--vp-c-brand-soft);
  stroke: var(--vp-c-brand-1);
  stroke-width: 2.5;
}

.state--current .cell__label {
  fill: var(--vp-c-brand-1);
}

.state--waiting .cell {
  fill: var(--vp-c-bg);
  stroke: var(--vp-c-divider);
}

.state--waiting .cell__label {
  fill: var(--vp-c-text-3);
}

.pointer {
  transition: transform 0.3s ease;
}

.pointer rect {
  fill: var(--vp-c-brand-1);
}

.pointer text {
  fill: var(--vp-c-white);
  font-family: var(--vp-font-family-mono);
  font-size: 12px;
  font-weight: 700;
}

.pointer line {
  stroke: var(--vp-c-brand-1);
  stroke-width: 2;
}

.attempt {
  --tone: var(--f1-ok);
}

/* Refused is drawn in the text colour, not red, so it never reads as the
   brand-coloured current offset. */
.attempt--refused { --tone: var(--vp-c-text-1); }
.attempt.attempt--refused text { fill: var(--vp-c-bg); }
.attempt--requeue { --tone: var(--f1-back); }

.attempt line {
  stroke: var(--tone);
  stroke-width: 2;
}

.attempt path,
.attempt rect {
  fill: var(--tone);
}

.attempt text {
  fill: #111;
  font-size: 12px;
  font-weight: 700;
  font-family: var(--vp-font-family-base);
}

.f1-cursor__legend {
  display: flex;
  flex-wrap: wrap;
  gap: 14px;
  color: var(--vp-c-text-2);
  font-size: 12px;
}

.f1-cursor__legend span {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.swatch {
  display: inline-block;
  width: 12px;
  height: 12px;
  border: 1.5px solid;
  border-radius: 3px;
}

.swatch--committed {
  border-color: var(--f1-ok);
  background: color-mix(in srgb, var(--f1-ok) 22%, transparent);
}

.swatch--current {
  border-color: var(--vp-c-brand-1);
  background: var(--vp-c-brand-soft);
}

.swatch--waiting {
  border-color: var(--vp-c-divider);
  background: var(--vp-c-bg);
}

.f1-cursor__caption {
  margin: 12px 0 0;
  padding: 10px 12px;
  border-left: 3px solid var(--vp-c-brand-1);
  border-radius: 0 8px 8px 0;
  background: var(--vp-c-bg);
  line-height: 1.55;
}

.f1-cursor__controls {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  margin-top: 12px;
}

.f1-cursor__controls button {
  padding: 4px 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 6px;
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  font-size: 13px;
  line-height: 1.5;
  cursor: pointer;
}

.f1-cursor__controls button:hover:not(:disabled) {
  border-color: var(--vp-c-brand-1);
}

.f1-cursor__controls button:disabled {
  opacity: 0.4;
  cursor: default;
}

.f1-cursor__hint {
  margin-left: auto;
  color: var(--vp-c-text-3);
  font-size: 12px;
}

@media (max-width: 560px) {
  .f1-cursor__hint {
    display: none;
  }
}

@media (prefers-reduced-motion: reduce) {
  .cell,
  .pointer {
    transition: none;
  }
}
</style>
