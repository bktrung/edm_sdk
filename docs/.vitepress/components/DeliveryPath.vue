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

    <svg
      class="f1-delivery-path__stage"
      viewBox="0 0 740 190"
      preserveAspectRatio="xMidYMid meet"
      role="img"
      :aria-label="stageLabel"
    >
      <line class="rail" x1="52" y1="130" x2="688" y2="130" />
      <g v-for="(station, index) in stations" :key="station.id">
        <line
          class="tick"
          :class="{ 'tick--active': index === activeStationIndex }"
          :x1="station.x"
          y1="74"
          :x2="station.x"
          y2="130"
        />
        <rect
          class="box"
          :class="{ 'box--active': index === activeStationIndex }"
          :x="station.x - 46"
          y="26"
          width="92"
          height="46"
          rx="8"
        />
        <text
          class="label"
          :class="{ 'label--active': index === activeStationIndex }"
          :x="station.x"
          y="55"
          text-anchor="middle"
        >
          {{ station.label }}
        </text>
      </g>
      <circle class="dot" :cx="dotX" cy="130" r="6" />
    </svg>

    <div class="f1-delivery-path__controls">
      <button type="button" :aria-label="playLabel" @click="togglePlay">{{ playing ? 'Pause' : 'Play' }}</button>
      <button type="button" aria-label="Previous step" @click="previousStep">Back</button>
      <button type="button" aria-label="Next step" @click="nextStep">Step</button>
      <button type="button" aria-label="Restart the scenario" @click="restart">Restart</button>
    </div>

    <p class="f1-delivery-path__status" aria-live="polite">
      {{ scenarioLabel }}, step {{ stepIndex + 1 }} of {{ steps.length }}: {{ activeStation }}
    </p>

    <ol class="f1-delivery-path__steps">
      <li
        v-for="(step, index) in steps"
        :key="`${scenarioIndex}-${index}`"
        :class="{ 'f1-delivery-path__step--active': index === stepIndex }"
        :aria-current="index === stepIndex ? 'step' : undefined"
      >
        {{ step.caption }}
      </li>
    </ol>
  </figure>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'

type Step = { station: string; caption: string }

// The seven stations of the map in the page above, in the order o-42 meets them.
const stations = [
  { id: 'broker', label: 'broker', x: 52 },
  { id: 'intake', label: 'intake', x: 158 },
  { id: 'channel', label: 'channel', x: 264 },
  { id: 'lanes', label: 'lanes', x: 370 },
  { id: 'pool', label: 'pool', x: 476 },
  { id: 'handler', label: 'handler', x: 582 },
  { id: 'settle', label: 'settle', x: 688 },
]

// A scripted storyboard: every step names a station and restates one claim from
// the page. Nothing here is computed from a scheduling or timing model, so the
// figure can only go stale when the prose does.
const scenarios: { id: string; label: string; steps: Step[] }[] = [
  {
    id: 'happy',
    label: 'Happy path',
    steps: [
      { station: 'broker', caption: 'The fetch side takes o-42 off the consumer.' },
      { station: 'intake', caption: 'F1 records the delivery in flight before it goes on the channel.' },
      { station: 'channel', caption: 'The dispatch channel holds as many messages as the subscription has workers.' },
      { station: 'lanes', caption: 'The scheduler places the delivery in a lane for its topic, priority and retry tier.' },
      { station: 'pool', caption: 'The dispatch loop asks for a pick only while the pool can take work, so a free worker runs it.' },
      { station: 'handler', caption: 'The handler runs on its own goroutine with a context bounded by the handler timeout.' },
      { station: 'settle', caption: 'The handler returns nil, so F1 acks the original and the broker is told it is finished.' },
    ],
  },
  {
    id: 'retry',
    label: 'Retry',
    steps: [
      { station: 'handler', caption: 'The handler returns a retryable error with attempts left.' },
      { station: 'settle', caption: 'F1 copies the body and the key, adds one to the attempt, and publishes the copy to the retry destination for its tier.' },
      { station: 'broker', caption: 'The successor publish is confirmed before anything is settled.' },
      { station: 'settle', caption: 'Only after that confirmation does F1 ack the original.' },
    ],
  },
  {
    id: 'publish-fails',
    label: 'Retry publish fails',
    steps: [
      { station: 'settle', caption: 'The successor publish gets a few quick retries.' },
      { station: 'settle', caption: 'When it still fails, F1 does not ack the original.' },
      { station: 'broker', caption: 'F1 releases the consumer and the broker redelivers the original, the same as after a crash.' },
    ],
  },
  {
    id: 'shutdown',
    label: 'Shutdown',
    steps: [
      { station: 'channel', caption: 'The generation is cancelled while the dispatch channel is full.' },
      { station: 'intake', caption: 'The fetch side stops accepting, drains the consumer within the drain timeout, and forwards what the driver had already yielded.' },
      { station: 'broker', caption: 'A delivery that cannot be accepted is nacked with requeue, and the broker redelivers it later.' },
    ],
  },
]

const tweenMilliseconds = 550
const stepMilliseconds = 1800

const scenarioIndex = ref(0)
const stepIndex = ref(0)
const playing = ref(false)
const reducedMotion = ref(false)
const dotX = ref(stations[0].x)

let frame = 0
let timer: ReturnType<typeof setTimeout> | null = null
let motionQuery: MediaQueryList | null = null

const steps = computed(() => scenarios[scenarioIndex.value].steps)
const scenarioLabel = computed(() => scenarios[scenarioIndex.value].label)
const currentStep = computed(() => steps.value[stepIndex.value])
const activeStation = computed(() => currentStep.value.station)
const activeStationIndex = computed(() => stations.findIndex((station) => station.id === currentStep.value.station))
const playLabel = computed(() => (playing.value ? 'Pause the scenario' : 'Play the scenario'))
const stageLabel = computed(() => `Delivery path with seven stations. The marker is at the ${activeStation.value} station.`)

function stationX(id: string): number {
  return stations.find((station) => station.id === id)?.x ?? stations[0].x
}

// The dot is the only moving element. Reduced motion sets it straight to the
// station instead of tweening, so the figure still tells the story.
function moveDot(target: number, animate: boolean) {
  if (frame !== 0 && typeof window !== 'undefined') {
    cancelAnimationFrame(frame)
    frame = 0
  }
  if (!animate || reducedMotion.value || typeof window === 'undefined') {
    dotX.value = target
    return
  }
  const start = dotX.value
  const distance = target - start
  if (distance === 0) {
    return
  }
  const began = performance.now()
  const tick = (now: number) => {
    const progress = Math.min(1, (now - began) / tweenMilliseconds)
    const eased = progress < 0.5 ? 2 * progress * progress : 1 - 2 * (1 - progress) * (1 - progress)
    dotX.value = start + distance * eased
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
  moveDot(stationX(scenarios[index].steps[0].station), false)
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
  moveDot(stationX(steps.value[0].station), false)
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

watch(stepIndex, () => moveDot(stationX(currentStep.value.station), true))

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

// Turning reduced motion on mid-session stops autoplay and parks the dot on the
// current station, so the figure never tweens under a reduced-motion reader.
function onMotionPreferenceChange() {
  reducedMotion.value = motionQuery?.matches ?? false
  if (reducedMotion.value) {
    playing.value = false
    moveDot(stationX(currentStep.value.station), false)
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
  margin: 24px 0;
  padding: 16px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg-soft);
}

.f1-delivery-path__tabs,
.f1-delivery-path__controls {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.f1-delivery-path__tabs {
  margin-bottom: 12px;
}

.f1-delivery-path__controls {
  margin-top: 8px;
}

.f1-delivery-path__tab,
.f1-delivery-path__controls button {
  padding: 6px 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  font-size: 14px;
  line-height: 1.4;
  cursor: pointer;
}

.f1-delivery-path__tab--active {
  border-color: var(--vp-c-brand-1);
  color: var(--vp-c-brand-1);
}

.f1-delivery-path__stage {
  display: block;
  width: 100%;
  height: auto;
}

.rail,
.tick {
  stroke: var(--vp-c-divider);
  stroke-width: 2;
}

.box {
  fill: var(--vp-c-bg);
  stroke: var(--vp-c-divider);
  stroke-width: 1.5;
}

.tick--active,
.box--active {
  stroke: var(--vp-c-brand-1);
}

.box--active {
  stroke-width: 2.5;
}

.label {
  fill: var(--vp-c-text-1);
  font-size: 15px;
  font-family: var(--vp-font-family-base);
}

.label--active {
  fill: var(--vp-c-brand-1);
}

.dot {
  fill: var(--vp-c-brand-1);
}

.f1-delivery-path__status {
  margin: 12px 0 8px;
  color: var(--vp-c-text-1);
  font-size: 14px;
}

.f1-delivery-path__steps {
  margin: 0;
  padding-left: 20px;
  color: var(--vp-c-text-2);
  font-size: 14px;
  line-height: 1.6;
}

.f1-delivery-path__step--active {
  color: var(--vp-c-brand-1);
}
</style>
