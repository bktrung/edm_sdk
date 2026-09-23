<template>
  <figure class="f1-delivery-path">
    <div class="f1-delivery-path__tabs" role="group" aria-label="Delivery scenarios">
      <button
        v-for="(scenario, index) in scenarios"
        :key="scenario.id"
        type="button"
        class="f1-delivery-path__tab"
        :class="{ 'f1-delivery-path__tab--active': index === scenarioIndex }"
        :aria-pressed="index === scenarioIndex"
        :aria-label="`Show the ${scenario.label} scenario`"
        @click="selectScenario(index)"
      >
        {{ scenario.label }}
      </button>
    </div>

    <div class="f1-delivery-path__scroll">
      <svg
        class="f1-delivery-path__stage"
        viewBox="0 0 760 250"
        preserveAspectRatio="xMidYMid meet"
        role="img"
        :aria-label="stageLabel"
      >
        <defs>
          <marker :id="`${uid}-arrow`" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" class="arrowhead" />
          </marker>
          <marker :id="`${uid}-arrow-hot`" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M 0 0 L 10 5 L 0 10 z" class="arrowhead" :class="`arrowhead--${currentStep.edge ?? 'normal'}`" />
          </marker>
        </defs>

        <!-- Forward path: each station hands the delivery to the next. -->
        <line
          v-for="index in stations.length - 1"
          :key="`link-${index}`"
          class="link"
          :class="{ 'link--done': index <= activeStationIndex }"
          :x1="stations[index - 1].x + boxHalf + 2"
          :y1="stationY"
          :x2="stations[index].x - boxHalf - 3"
          :y2="stationY"
          :marker-end="`url(#${uid}-arrow)`"
        />

        <!-- Return arcs: the ways a delivery goes back to the broker. -->
        <g v-for="edge in edges" :key="edge.id" class="edge" :class="edgeClass(edge.id)">
          <path
            :d="arcPath(edge.from, edge.depth)"
            class="edge__path"
            :marker-end="`url(#${uid}-${activeEdge === edge.id ? 'arrow-hot' : 'arrow'})`"
          />
          <text
            class="edge__label"
            :x="(stationX(edge.from) + stations[0].x) / 2"
            :y="stationY + boxHalfHeight + edge.depth - 6"
            text-anchor="middle"
          >
            {{ edgeLabel(edge.id) }}
          </text>
        </g>

        <g v-for="(station, index) in stations" :key="station.id" :class="stationClass(index)">
          <template v-if="station.id === 'broker'">
            <path class="box" :d="cylinderPath(station.x)" />
            <ellipse class="box-rim" :cx="station.x" :cy="stationY - boxHalfHeight + 6" :rx="boxHalf" ry="6" />
          </template>
          <rect
            v-else
            class="box"
            :x="station.x - boxHalf"
            :y="stationY - boxHalfHeight"
            :width="boxHalf * 2"
            :height="boxHalfHeight * 2"
            rx="8"
          />
          <text class="label" :x="station.x" :y="stationY + 5" text-anchor="middle">{{ station.label }}</text>
        </g>

        <!-- The delivery itself. -->
        <g class="token" :class="`token--${activeTone}`" :transform="`translate(${tokenX}, ${stationY - boxHalfHeight - 22})`">
          <line class="token__stem" x1="0" y1="10" x2="0" y2="20" />
          <rect x="-24" y="-11" width="48" height="22" rx="11" />
          <text x="0" y="4" text-anchor="middle">o-42</text>
        </g>
      </svg>
    </div>

    <p class="f1-delivery-path__status" aria-live="polite">
      <span class="f1-delivery-path__counter">{{ scenarioLabel }} &middot; step {{ stepIndex + 1 }} / {{ steps.length }} &middot; {{ activeStation }}</span>
      <span class="f1-delivery-path__caption">{{ currentStep.caption }}</span>
    </p>

    <div class="f1-delivery-path__controls">
      <button type="button" aria-label="Previous step" :disabled="stepIndex === 0" @click="previousStep">Back</button>
      <button type="button" aria-label="Next step" :disabled="stepIndex === steps.length - 1" @click="nextStep">Step</button>
      <button type="button" :aria-label="playLabel" @click="togglePlay">{{ playing ? 'Pause' : 'Play' }}</button>
      <button type="button" aria-label="Restart the scenario" @click="restart">Restart</button>
    </div>

    <p class="f1-delivery-path__outcome"><strong>Outcome:</strong> {{ scenarioOutcome }}</p>

    <ol class="f1-delivery-path__steps">
      <li
        v-for="(step, index) in steps"
        :key="`${scenarioIndex}-${index}`"
        :class="{ 'f1-delivery-path__step--active': index === stepIndex, 'f1-delivery-path__step--done': index < stepIndex }"
        :aria-current="index === stepIndex ? 'step' : undefined"
      >
        {{ step.caption }}
      </li>
    </ol>
  </figure>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, useId, watch } from 'vue'

// edge names the arc a step uses; tone colours the token: ok for an
// acknowledgement, back for a delivery going back to the broker without an ack.
type Edge = 'ack' | 'publish' | 'release' | 'requeue'
type Step = { station: string; caption: string; edge?: Edge; tone?: 'ok' | 'back' }

// The seven stations of the map in the page above, in the order o-42 meets them.
const stations = [
  { id: 'broker', label: 'broker', x: 52 },
  { id: 'intake', label: 'intake', x: 161 },
  { id: 'channel', label: 'channel', x: 270 },
  { id: 'lanes', label: 'lanes', x: 379 },
  { id: 'pool', label: 'pool', x: 488 },
  { id: 'handler', label: 'handler', x: 597 },
  { id: 'settle', label: 'finish', x: 706 },
]
const stationY = 70
const boxHalf = 42
const boxHalfHeight = 22

// The arcs back to the broker, from the map: a requeue from intake, a nack
// with requeue from the pool, and the settle arc that carries the ack, the
// successor publish, or the consumer release.
const edges: { id: 'requeue' | 'nack' | 'settle'; from: string; depth: number }[] = [
  { id: 'requeue', from: 'intake', depth: 44 },
  { id: 'nack', from: 'pool', depth: 94 },
  { id: 'settle', from: 'settle', depth: 144 },
]

// A scripted storyboard: every step names a station and restates one claim from
// the page. Nothing here is computed from a scheduling or timing model, so the
// figure can only go stale when the prose does.
const scenarios: { id: string; label: string; outcome: string; steps: Step[] }[] = [
  {
    id: 'happy',
    label: 'Happy path',
    outcome: 'The original is acknowledged after the handler succeeds.',
    steps: [
      { station: 'broker', caption: 'The fetch side takes o-42 off the consumer.' },
      { station: 'intake', caption: 'F1 records the delivery in flight before it goes on the channel.' },
      { station: 'channel', caption: 'The dispatch channel holds as many messages as the subscription has workers.' },
      { station: 'lanes', caption: 'The scheduler places the delivery in a lane for its topic, priority and retry step.' },
      { station: 'pool', caption: 'Unordered work enters a shared queue; messages with the same key wait in one worker queue, and different keys run in parallel.' },
      { station: 'handler', caption: 'The handler runs on its own goroutine with a context bounded by the handler timeout.' },
      { station: 'settle', caption: 'The handler returns nil, so F1 acks the original and the broker is told it is finished.', edge: 'ack', tone: 'ok' },
    ],
  },
  {
    id: 'retry',
    label: 'Retry',
    outcome: 'The retry copy is confirmed before the original is acked.',
    steps: [
      { station: 'handler', caption: 'The handler returns a retryable error with attempts left.' },
      { station: 'settle', caption: 'F1 copies the body and the key, adds one to the attempt, and publishes the copy to the retry destination for its retry step.', edge: 'publish' },
      { station: 'broker', caption: 'The broker confirms the retry copy before anything is acked.', edge: 'publish' },
      { station: 'settle', caption: 'Only after that confirmation does F1 ack the original.', edge: 'ack', tone: 'ok' },
    ],
  },
  {
    id: 'publish-fails',
    label: 'Retry publish fails',
    outcome: 'The original stays unacked and the broker can deliver it again.',
    steps: [
      { station: 'settle', caption: 'Publishing the retry copy gets a few quick retries.', edge: 'publish' },
      { station: 'settle', caption: 'When it still fails transiently, F1 does not ack the original.' },
      { station: 'broker', caption: 'F1 releases the consumer and the broker redelivers the original, the same as after a crash.', edge: 'release', tone: 'back' },
    ],
  },
  {
    id: 'shutdown',
    label: 'Shutdown',
    outcome: 'An unaccepted delivery is nacked with requeue.',
    steps: [
      { station: 'channel', caption: 'The runner\'s fetch and dispatch goroutines are stopped while the dispatch channel is full.' },
      { station: 'intake', caption: 'The fetch side stops accepting, drains the consumer within the drain timeout, and forwards what the driver had already yielded.' },
      { station: 'broker', caption: 'A delivery that cannot be accepted is nacked with requeue, and the broker redelivers it later.', edge: 'requeue', tone: 'back' },
    ],
  },
]

const tweenMilliseconds = 550
const stepMilliseconds = 2200
// Marker ids are document-global, so each instance gets its own prefix. useId
// matches between the server render and hydration.
const uid = `f1dp${useId().replace(/[^a-zA-Z0-9-]/g, '')}`

const scenarioIndex = ref(0)
const stepIndex = ref(0)
const playing = ref(false)
const reducedMotion = ref(false)
const tokenX = ref(stations[0].x)

let frame = 0
let timer: ReturnType<typeof setTimeout> | null = null
let motionQuery: MediaQueryList | null = null

const steps = computed(() => scenarios[scenarioIndex.value].steps)
const scenarioLabel = computed(() => scenarios[scenarioIndex.value].label)
const scenarioOutcome = computed(() => scenarios[scenarioIndex.value].outcome)
const currentStep = computed(() => steps.value[stepIndex.value])
const activeStation = computed(() => currentStep.value.station)
const activeStationIndex = computed(() => stations.findIndex((station) => station.id === currentStep.value.station))
const visited = computed(() => new Set(steps.value.slice(0, stepIndex.value).map((step) => step.station)))
const activeEdge = computed(() => {
  const edge = currentStep.value.edge
  if (edge === 'requeue') return 'requeue'
  return edge ? 'settle' : ''
})
const activeTone = computed(() => currentStep.value.tone ?? 'normal')
const playLabel = computed(() => (playing.value ? 'Pause the scenario' : 'Play the scenario'))
const stageLabel = computed(() => `Delivery path with seven stations. The delivery is at the ${activeStation.value} station.`)

function stationX(id: string): number {
  return stations.find((station) => station.id === id)?.x ?? stations[0].x
}

function stationClass(index: number): string[] {
  const classes = ['station']
  if (index === activeStationIndex.value) classes.push('station--active')
  else if (visited.value.has(stations[index].id)) classes.push('station--visited')
  return classes
}

function edgeClass(id: string): string[] {
  return activeEdge.value === id ? ['edge--active', `edge--${currentStep.value.edge}`] : []
}

function edgeLabel(id: string): string {
  if (id === 'requeue') return 'requeue'
  if (id === 'nack') return 'nack, requeue'
  const edge = activeEdge.value === 'settle' ? currentStep.value.edge : undefined
  if (edge === 'ack') return 'ack original'
  if (edge === 'publish') return 'publish copy'
  if (edge === 'release') return 'release'
  return 'ack, publish or release'
}

// A U-shaped arc under the stations, from a station's bottom back to the broker.
function arcPath(from: string, depth: number): string {
  const startX = stationX(from)
  const endX = stations[0].x
  const top = stationY + boxHalfHeight + 2
  const bottom = top + depth
  const r = Math.min(16, depth / 2)
  return [
    `M ${startX} ${top}`,
    `L ${startX} ${bottom - r}`,
    `Q ${startX} ${bottom} ${startX - r} ${bottom}`,
    `L ${endX + r} ${bottom}`,
    `Q ${endX} ${bottom} ${endX} ${bottom - r}`,
    `L ${endX} ${top + 4}`,
  ].join(' ')
}

function cylinderPath(x: number): string {
  const left = x - boxHalf
  const right = x + boxHalf
  const top = stationY - boxHalfHeight + 6
  const bottom = stationY + boxHalfHeight - 6
  return `M ${left} ${top} L ${left} ${bottom} A ${boxHalf} 6 0 0 0 ${right} ${bottom} L ${right} ${top} A ${boxHalf} 6 0 0 0 ${left} ${top} Z`
}

// The token is the only moving element. Reduced motion sets it straight to the
// station instead of tweening, so the figure still tells the story.
function moveToken(target: number, animate: boolean) {
  if (frame !== 0 && typeof window !== 'undefined') {
    cancelAnimationFrame(frame)
    frame = 0
  }
  if (!animate || reducedMotion.value || typeof window === 'undefined') {
    tokenX.value = target
    return
  }
  const start = tokenX.value
  const distance = target - start
  if (distance === 0) {
    return
  }
  const began = performance.now()
  const tick = (now: number) => {
    const progress = Math.min(1, (now - began) / tweenMilliseconds)
    const eased = progress < 0.5 ? 2 * progress * progress : 1 - 2 * (1 - progress) * (1 - progress)
    tokenX.value = start + distance * eased
    frame = progress < 1 ? requestAnimationFrame(tick) : 0
  }
  frame = requestAnimationFrame(tick)
}

function clearTimer() {
  if (timer !== null) {
    clearTimeout(timer)
    timer = null
  }
}

function selectScenario(index: number) {
  scenarioIndex.value = index
  stepIndex.value = 0
  moveToken(stationX(scenarios[index].steps[0].station), false)
  playing.value = !reducedMotion.value
}

function nextStep() {
  playing.value = false
  if (stepIndex.value < steps.value.length - 1) {
    stepIndex.value += 1
  }
}

function previousStep() {
  playing.value = false
  if (stepIndex.value > 0) {
    stepIndex.value -= 1
  }
}

function restart() {
  stepIndex.value = 0
  moveToken(stationX(steps.value[0].station), false)
  playing.value = !reducedMotion.value
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

watch(stepIndex, () => moveToken(stationX(currentStep.value.station), true))

watch([playing, stepIndex, scenarioIndex], () => {
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
  }, stepMilliseconds)
})

// Turning reduced motion on mid-session stops autoplay and parks the token on
// the current station, so the figure never tweens under a reduced-motion reader.
function onMotionPreferenceChange() {
  reducedMotion.value = motionQuery?.matches ?? false
  if (reducedMotion.value) {
    playing.value = false
    moveToken(stationX(currentStep.value.station), false)
  }
}

onMounted(() => {
  const query = window.matchMedia('(prefers-reduced-motion: reduce)')
  motionQuery = query
  reducedMotion.value = query.matches
  query.addEventListener('change', onMotionPreferenceChange)
  playing.value = !reducedMotion.value
})

onBeforeUnmount(() => {
  motionQuery?.removeEventListener('change', onMotionPreferenceChange)
  cancelAnimationFrame(frame)
  clearTimer()
})
</script>

<style scoped>
.f1-delivery-path {
  --f1-ok: #16a34a;
  --f1-back: #d97706;
  margin: 24px 0;
  padding: 16px 18px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg-soft);
  font-size: 14px;
}

:global(.dark) .f1-delivery-path {
  --f1-ok: #4ade80;
  --f1-back: #fbbf24;
}

.f1-delivery-path__tabs {
  display: inline-flex;
  flex-wrap: wrap;
  gap: 2px;
  padding: 3px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  background: var(--vp-c-bg);
}

.f1-delivery-path__tab {
  padding: 4px 12px;
  border-radius: 6px;
  color: var(--vp-c-text-2);
  font-size: 13px;
  line-height: 1.5;
  cursor: pointer;
}

.f1-delivery-path__tab:hover {
  color: var(--vp-c-text-1);
}

.f1-delivery-path__tab--active {
  background: var(--vp-c-brand-soft);
  color: var(--vp-c-brand-1);
  font-weight: 600;
}

.f1-delivery-path__scroll {
  margin: 8px 0 4px;
  overflow-x: auto;
}

.f1-delivery-path__stage {
  display: block;
  width: 100%;
  min-width: 560px;
  height: auto;
}

.link {
  stroke: var(--vp-c-divider);
  stroke-width: 2;
}

.link--done {
  stroke: var(--vp-c-brand-1);
}

.arrowhead {
  fill: var(--vp-c-text-3);
}

.arrowhead--normal,
.arrowhead--publish {
  fill: var(--vp-c-brand-1);
}

.arrowhead--ack {
  fill: var(--f1-ok);
}

.arrowhead--release,
.arrowhead--requeue {
  fill: var(--f1-back);
}

.edge__path {
  fill: none;
  stroke: var(--vp-c-divider);
  stroke-width: 1.5;
  stroke-dasharray: 5 4;
}

.edge__label {
  fill: var(--vp-c-text-3);
  font-size: 13px;
  font-family: var(--vp-font-family-base);
  paint-order: stroke;
  stroke: var(--vp-c-bg-soft);
  stroke-width: 5px;
}

.edge--active .edge__path {
  stroke-width: 2.5;
}

.edge--active .edge__label {
  font-weight: 700;
}

/* Only a return to the broker is dashed; an ack or a successor publish is a
   normal forward send, so its arc is solid while it is in use. */
.edge--ack .edge__path,
.edge--publish .edge__path {
  stroke-dasharray: none;
}

.edge--ack .edge__path { stroke: var(--f1-ok); }
.edge--ack .edge__label { fill: var(--f1-ok); }
.edge--publish .edge__path { stroke: var(--vp-c-brand-1); }
.edge--publish .edge__label { fill: var(--vp-c-brand-1); }
.edge--release .edge__path,
.edge--requeue .edge__path { stroke: var(--f1-back); }
.edge--release .edge__label,
.edge--requeue .edge__label { fill: var(--f1-back); }

.box,
.box-rim {
  fill: var(--vp-c-bg);
  stroke: var(--vp-c-divider);
  stroke-width: 1.5;
}

.label {
  fill: var(--vp-c-text-2);
  font-size: 15px;
  font-family: var(--vp-font-family-base);
}

.station--visited .box,
.station--visited .box-rim {
  fill: var(--vp-c-brand-soft);
  stroke: var(--vp-c-brand-3);
}

.station--visited .label {
  fill: var(--vp-c-text-1);
}

.station--active .box,
.station--active .box-rim {
  fill: var(--vp-c-brand-soft);
  stroke: var(--vp-c-brand-1);
  stroke-width: 2.5;
}

.station--active .label {
  fill: var(--vp-c-brand-1);
  font-weight: 700;
}

.token rect {
  fill: var(--vp-c-brand-1);
}

.token text {
  fill: var(--vp-c-white);
  font-size: 12px;
  font-weight: 700;
  font-family: var(--vp-font-family-mono);
}

.token__stem {
  stroke: var(--vp-c-brand-1);
  stroke-width: 2;
}

.token--ok rect { fill: var(--f1-ok); }
.token--ok .token__stem { stroke: var(--f1-ok); }
.token--back rect { fill: var(--f1-back); }
.token--back .token__stem { stroke: var(--f1-back); }
.token--ok text,
.token--back text { fill: #111; }

.f1-delivery-path__status {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin: 4px 0 0;
  padding: 10px 12px;
  border-left: 3px solid var(--vp-c-brand-1);
  border-radius: 0 8px 8px 0;
  background: var(--vp-c-bg);
}

.f1-delivery-path__counter {
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-mono);
  font-size: 12px;
}

.f1-delivery-path__caption {
  color: var(--vp-c-text-1);
  font-size: 15px;
  line-height: 1.55;
}

.f1-delivery-path__controls {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 10px;
}

.f1-delivery-path__controls button {
  padding: 4px 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 6px;
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  font-size: 13px;
  line-height: 1.5;
  cursor: pointer;
}

.f1-delivery-path__controls button:hover:not(:disabled) {
  border-color: var(--vp-c-brand-1);
}

.f1-delivery-path__controls button:disabled {
  opacity: 0.4;
  cursor: default;
}

.f1-delivery-path__outcome {
  margin: 14px 0 6px;
  color: var(--vp-c-text-2);
  font-size: 14px;
}

.f1-delivery-path__steps {
  margin: 0;
  padding-left: 22px;
  color: var(--vp-c-text-3);
  font-size: 14px;
  line-height: 1.6;
}

.f1-delivery-path__step--done {
  color: var(--vp-c-text-2);
}

.f1-delivery-path__step--active {
  color: var(--vp-c-brand-1);
  font-weight: 600;
}
</style>
