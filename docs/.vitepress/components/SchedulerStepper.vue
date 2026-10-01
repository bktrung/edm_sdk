<template>
  <figure
    class="f1-scheduler-stepper"
    tabindex="0"
    aria-label="Scheduler trace stepper"
    @keydown.left.prevent="previousStep"
    @keydown.right.prevent="nextStep"
  >
    <div class="f1-scheduler-stepper__header">
      <strong>{{ scenarioData.title }}</strong>
      <span class="f1-scheduler-stepper__meta">
        <span v-if="currentStep.clockOffsetMillis > 0">t = {{ currentStep.clockOffsetMillis }} ms &middot; </span>
        trace step {{ currentStep.number }} / {{ steps.length }}
      </span>
    </div>

    <div class="f1-scheduler-stepper__table" role="table" aria-label="Group scores and queued items">
      <div class="f1-scheduler-stepper__row f1-scheduler-stepper__row--head" role="row">
        <span role="columnheader">group</span>
        <span role="columnheader" class="f1-scheduler-stepper__scale">
          <span>-{{ scoreExtent }}</span><span>0</span><span>+{{ scoreExtent }}</span>
        </span>
        <span role="columnheader" class="f1-scheduler-stepper__num">score</span>
        <span role="columnheader">queued</span>
      </div>
      <div
        v-for="group in scenarioData.groups"
        :key="group"
        role="row"
        class="f1-scheduler-stepper__row"
        :class="[
          `f1-scheduler-stepper__row--${tone(group)}`,
          { 'f1-scheduler-stepper__row--winner': winnerGroup === group, 'f1-scheduler-stepper__row--promoted': winnerGroup === group && currentStep.promoted },
        ]"
      >
        <span role="cell" class="f1-scheduler-stepper__name">
          {{ group }}
          <span v-if="winnerGroup === group" class="f1-scheduler-stepper__badge">{{ currentStep.promoted ? 'jumped the queue' : 'picked' }}</span>
        </span>
        <span role="cell" class="f1-scheduler-stepper__bar" :aria-label="`${group} score ${scoreFor(group)}`">
          <span class="f1-scheduler-stepper__axis" />
          <span class="f1-scheduler-stepper__fill" :style="fillStyle(scoreFor(group))" />
          <span
            v-if="stepIndex > 0 && previousScoreFor(group) !== scoreFor(group)"
            class="f1-scheduler-stepper__ghost"
            :style="{ left: `${position(previousScoreFor(group))}%` }"
            :title="`before this step: ${signed(previousScoreFor(group))}`"
          />
        </span>
        <span role="cell" class="f1-scheduler-stepper__num">{{ signed(scoreFor(group)) }}</span>
        <span role="cell" class="f1-scheduler-stepper__queue" :aria-label="`${depthFor(group)} queued`" :title="`${depthFor(group)} queued`">
          <span v-for="n in Math.min(depthFor(group), queueDrawLimit)" :key="n" class="f1-scheduler-stepper__item" />
          <span v-if="depthFor(group) > queueDrawLimit" class="f1-scheduler-stepper__more">+{{ depthFor(group) - queueDrawLimit }}</span>
          <span v-if="depthFor(group) === 0" class="f1-scheduler-stepper__more">empty</span>
        </span>
      </div>
    </div>

    <div class="f1-scheduler-stepper__history" aria-label="Picks so far">
      <span class="f1-scheduler-stepper__history-label">picks</span>
      <span
        v-for="pick in history"
        :key="pick.index"
        class="f1-scheduler-stepper__chip"
        :class="[`f1-scheduler-stepper__chip--${tone(pick.group)}`, { 'f1-scheduler-stepper__chip--promoted': pick.promoted, 'f1-scheduler-stepper__chip--current': pick.index === stepIndex }]"
        :title="`${pick.group}${pick.promoted ? ' (overdue, jumped the queue)' : ''}`"
      >{{ pick.group.charAt(0).toUpperCase() }}</span>
      <span v-if="history.length === 0" class="f1-scheduler-stepper__history-empty">none yet</span>
    </div>

    <p class="f1-scheduler-stepper__caption" aria-live="polite">{{ currentStep.caption }}</p>

    <div class="f1-scheduler-stepper__controls">
      <button type="button" :disabled="stepIndex === 0" @click="previousStep">Prev</button>
      <button type="button" :disabled="stepIndex === steps.length - 1" @click="nextStep">Next</button>
      <button v-if="!reducedMotion" type="button" :aria-label="playing ? 'Pause the trace' : 'Play the trace'" @click="togglePlay">{{ playing ? 'Pause' : 'Play' }}</button>
      <button type="button" @click="reset">Reset</button>
      <span class="f1-scheduler-stepper__hint">or focus the figure and use the arrow keys</span>
    </div>
  </figure>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import traceDocument from '../data/scheduler-trace.json'

type TraceScore = { group: string; score: number }
type TraceDepth = { lane: string; depth: number }
type TraceLane = { id: string; group: string }
type TraceStep = {
  number: number
  action: string
  clockOffsetMillis: number
  winner: string
  promoted: boolean
  scores: TraceScore[]
  depths: TraceDepth[]
  caption: string
}
type TraceScenario = { id: string; title: string; groups: string[]; lanes: TraceLane[]; steps: TraceStep[] }
type TraceDocument = { scenarios: TraceScenario[] }

const props = defineProps<{ scenario: 'weighted' | 'promotion' }>()
const traces = traceDocument as TraceDocument
const queueDrawLimit = 14
const playMilliseconds = 1200

const scenarioIndex = computed(() => traces.scenarios.findIndex((item) => item.id === props.scenario))
const scenarioData = computed(() => traces.scenarios[scenarioIndex.value >= 0 ? scenarioIndex.value : 0])
const steps = computed(() => scenarioData.value?.steps ?? [])
const stepIndex = ref(0)
const playing = ref(false)
const reducedMotion = ref(false)
let timer: ReturnType<typeof setTimeout> | null = null
let motionQuery: MediaQueryList | null = null

const emptyStep: TraceStep = {
  number: 1,
  action: 'trace',
  clockOffsetMillis: 0,
  winner: '',
  promoted: false,
  scores: [],
  depths: [],
  caption: 'The scheduler trace is loading.',
}
const currentStep = computed(() => steps.value[Math.min(stepIndex.value, Math.max(steps.value.length - 1, 0))] ?? emptyStep)
const previousStep_ = computed(() => (stepIndex.value > 0 ? steps.value[stepIndex.value - 1] : currentStep.value))
const groupOfLane = (laneID: string) => scenarioData.value?.lanes.find((lane) => lane.id === laneID)?.group ?? ''
const winnerGroup = computed(() => groupOfLane(currentStep.value.winner))
const scoreExtent = computed(() =>
  Math.max(1, ...(scenarioData.value?.steps.flatMap((step) => step.scores.map((item) => Math.abs(item.score))) ?? [])),
)

// The pick history is read straight from the recorded steps: every step with a
// winner is one pick. Nothing here recomputes a scheduling decision.
const history = computed(() =>
  steps.value
    .slice(0, stepIndex.value + 1)
    .map((step, index) => ({ index, group: groupOfLane(step.winner), promoted: step.promoted }))
    .filter((pick) => pick.group !== ''),
)

// Colour family per group: retry groups reuse their priority's family.
function tone(group: string): string {
  if (group.includes('high')) return 'high'
  if (group.includes('medium')) return 'medium'
  if (group.includes('low')) return 'low'
  return 'other'
}

function scoreFor(group: string): number {
  return currentStep.value.scores.find((item) => item.group === group)?.score ?? 0
}

function previousScoreFor(group: string): number {
  return previousStep_.value.scores.find((item) => item.group === group)?.score ?? 0
}

function depthFor(group: string): number {
  return scenarioData.value?.lanes
    .filter((lane) => lane.group === group)
    .reduce((depth, lane) => depth + (currentStep.value.depths.find((item) => item.lane === lane.id)?.depth ?? 0), 0) ?? 0
}

function signed(score: number): string {
  return score > 0 ? `+${score}` : `${score}`
}

// Position on the bar in percent, with 0 at the centre axis.
function position(score: number): number {
  return 50 + (score / scoreExtent.value) * 50
}

function fillStyle(score: number): Record<string, string> {
  const width = (Math.abs(score) / scoreExtent.value) * 50
  return score < 0 ? { left: `${50 - width}%`, width: `${width}%` } : { left: '50%', width: `${width}%` }
}

function clearTimer() {
  if (timer !== null) {
    clearTimeout(timer)
    timer = null
  }
}

function previousStep() {
  playing.value = false
  if (stepIndex.value > 0) {
    stepIndex.value -= 1
  }
}

function nextStep() {
  playing.value = false
  if (stepIndex.value < steps.value.length - 1) {
    stepIndex.value += 1
  }
}

function reset() {
  playing.value = false
  stepIndex.value = 0
}

function togglePlay() {
  if (playing.value) {
    playing.value = false
    return
  }
  if (stepIndex.value >= steps.value.length - 1) {
    stepIndex.value = 0
  }
  playing.value = true
}

watch([playing, stepIndex], () => {
  clearTimer()
  if (!playing.value) {
    return
  }
  if (stepIndex.value >= steps.value.length - 1) {
    playing.value = false
    return
  }
  timer = setTimeout(() => {
    stepIndex.value += 1
  }, playMilliseconds)
})

watch(() => props.scenario, reset)

function onMotionPreferenceChange() {
  reducedMotion.value = motionQuery?.matches ?? false
  if (reducedMotion.value) {
    playing.value = false
  }
}

onMounted(() => {
  motionQuery = window.matchMedia('(prefers-reduced-motion: reduce)')
  reducedMotion.value = motionQuery.matches
  motionQuery.addEventListener('change', onMotionPreferenceChange)
})

onBeforeUnmount(() => {
  motionQuery?.removeEventListener('change', onMotionPreferenceChange)
  clearTimer()
})
</script>

<style scoped>
.f1-scheduler-stepper {
  --f1-high: #2563eb;
  --f1-medium: #7c3aed;
  --f1-low: #0d9488;
  --f1-other: var(--vp-c-text-2);
  --f1-promo: #d97706;
  margin: 24px 0;
  padding: 16px 18px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg-soft);
  color: var(--vp-c-text-1);
  font-size: 14px;
}

:global(.dark) .f1-scheduler-stepper {
  --f1-high: #60a5fa;
  --f1-medium: #a78bfa;
  --f1-low: #2dd4bf;
  --f1-promo: #fbbf24;
}

.f1-scheduler-stepper:focus-visible {
  outline: 2px solid var(--vp-c-brand-1);
  outline-offset: 2px;
}

.f1-scheduler-stepper__header {
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  align-items: baseline;
  gap: 8px;
  margin-bottom: 12px;
}

.f1-scheduler-stepper__meta {
  color: var(--vp-c-text-2);
  font-family: var(--vp-font-family-mono);
  font-size: 12px;
}

.f1-scheduler-stepper__table {
  display: grid;
  gap: 4px;
}

.f1-scheduler-stepper__row {
  display: grid;
  grid-template-columns: minmax(96px, 1.1fr) 3fr 44px minmax(96px, 1.6fr);
  align-items: center;
  gap: 12px;
  min-height: 40px;
  padding: 0 10px;
  border-left: 3px solid transparent;
  border-radius: 8px;
  transition: background-color 0.2s ease;
}

.f1-scheduler-stepper__row--head {
  min-height: 20px;
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-mono);
  font-size: 11px;
}

.f1-scheduler-stepper__row--high { --tone: var(--f1-high); }
.f1-scheduler-stepper__row--medium { --tone: var(--f1-medium); }
.f1-scheduler-stepper__row--low { --tone: var(--f1-low); }
.f1-scheduler-stepper__row--other { --tone: var(--f1-other); }

.f1-scheduler-stepper__row--winner {
  border-left-color: var(--tone);
  background: color-mix(in srgb, var(--tone) 12%, transparent);
}

.f1-scheduler-stepper__row--promoted {
  border-left-color: var(--f1-promo);
  background: color-mix(in srgb, var(--f1-promo) 14%, transparent);
}

.f1-scheduler-stepper__name {
  display: flex;
  align-items: center;
  gap: 6px;
  font-weight: 600;
}

.f1-scheduler-stepper__name::before {
  content: '';
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--tone);
  flex: none;
}

.f1-scheduler-stepper__badge {
  padding: 1px 6px;
  border-radius: 999px;
  background: var(--tone);
  color: var(--vp-c-bg);
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.02em;
}

.f1-scheduler-stepper__row--promoted .f1-scheduler-stepper__badge {
  background: var(--f1-promo);
}

.f1-scheduler-stepper__scale {
  display: flex;
  justify-content: space-between;
}

.f1-scheduler-stepper__bar {
  position: relative;
  height: 12px;
  border-radius: 6px;
  background: var(--vp-c-default-soft);
}

.f1-scheduler-stepper__axis {
  position: absolute;
  top: -4px;
  bottom: -4px;
  left: 50%;
  width: 1px;
  background: var(--vp-c-text-3);
}

.f1-scheduler-stepper__fill {
  position: absolute;
  top: 0;
  bottom: 0;
  border-radius: 6px;
  background: var(--tone);
  transition: left 0.3s ease, width 0.3s ease;
}

.f1-scheduler-stepper__ghost {
  position: absolute;
  top: -3px;
  bottom: -3px;
  width: 2px;
  margin-left: -1px;
  border-radius: 1px;
  background: var(--vp-c-text-2);
  opacity: 0.55;
}

.f1-scheduler-stepper__num {
  font-family: var(--vp-font-family-mono);
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.f1-scheduler-stepper__queue {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 2px;
}

.f1-scheduler-stepper__item {
  width: 6px;
  height: 10px;
  border-radius: 1px;
  background: color-mix(in srgb, var(--tone) 70%, transparent);
}

.f1-scheduler-stepper__more {
  margin-left: 3px;
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-mono);
  font-size: 11px;
}

.f1-scheduler-stepper__history {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px;
  margin-top: 14px;
  padding-top: 12px;
  border-top: 1px dashed var(--vp-c-divider);
}

.f1-scheduler-stepper__history-label,
.f1-scheduler-stepper__history-empty {
  margin-right: 4px;
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-mono);
  font-size: 11px;
}

.f1-scheduler-stepper__chip {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  border: 2px solid transparent;
  border-radius: 6px;
  background: var(--tone);
  color: var(--vp-c-bg);
  font-family: var(--vp-font-family-mono);
  font-size: 11px;
  font-weight: 700;
}

.f1-scheduler-stepper__chip--high { --tone: var(--f1-high); }
.f1-scheduler-stepper__chip--medium { --tone: var(--f1-medium); }
.f1-scheduler-stepper__chip--low { --tone: var(--f1-low); }
.f1-scheduler-stepper__chip--other { --tone: var(--f1-other); }

.f1-scheduler-stepper__chip--promoted {
  border-color: var(--f1-promo);
}

.f1-scheduler-stepper__chip--current {
  box-shadow: 0 0 0 2px var(--vp-c-bg-soft), 0 0 0 4px var(--vp-c-text-1);
}

.f1-scheduler-stepper__caption {
  margin: 12px 0 0;
  padding: 10px 12px;
  border-left: 3px solid var(--vp-c-brand-1);
  border-radius: 0 8px 8px 0;
  background: var(--vp-c-bg);
  line-height: 1.55;
}

.f1-scheduler-stepper__controls {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  margin-top: 12px;
}

.f1-scheduler-stepper__controls button {
  padding: 4px 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 6px;
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  font-size: 13px;
  line-height: 1.5;
  cursor: pointer;
}

.f1-scheduler-stepper__controls button:hover:not(:disabled) {
  border-color: var(--vp-c-brand-1);
}

.f1-scheduler-stepper__controls button:disabled {
  opacity: 0.4;
  cursor: default;
}

.f1-scheduler-stepper__hint {
  margin-left: auto;
  color: var(--vp-c-text-3);
  font-size: 12px;
}

@media (max-width: 560px) {
  .f1-scheduler-stepper__row {
    grid-template-columns: 84px 1fr 36px;
    gap: 8px;
  }

  .f1-scheduler-stepper__queue,
  .f1-scheduler-stepper__row--head > :last-child {
    grid-column: 2 / 4;
  }

  .f1-scheduler-stepper__row--head > :last-child {
    display: none;
  }

  .f1-scheduler-stepper__hint,
  .f1-scheduler-stepper__badge {
    display: none;
  }
}

@media (prefers-reduced-motion: reduce) {
  .f1-scheduler-stepper__fill,
  .f1-scheduler-stepper__row {
    transition: none;
  }
}
</style>
