<template>
  <figure class="f1-swrr">
    <!-- Playback strip: the reference's control pill, with the clock in simulated time. -->
    <div class="f1-swrr__bar">
      <div class="f1-swrr__group">
        <button type="button" class="f1-swrr__btn is-primary" :aria-label="playing ? 'Pause the run' : 'Play the run'"
          @click="togglePlay">{{ playing ? 'Pause' : 'Play' }}</button>
        <button type="button" class="f1-swrr__btn" aria-label="Advance one simulated tick" @click="stepOnce">Step</button>
        <button type="button" class="f1-swrr__btn" aria-label="Restart this run" @click="loadScenario(scenarioIndex, true)">Restart</button>
      </div>
      <div class="f1-swrr__group" role="group" aria-label="Playback speed">
        <button v-for="option in speedOptions" :key="option" type="button" class="f1-swrr__speed"
          :class="{ 'is-active': speed === option }" :aria-pressed="speed === option"
          :aria-label="`Playback speed ${option} times`" @click="setSpeed(option)">{{ option }}x</button>
      </div>
      <span class="f1-swrr__clock">T = {{ clock }}</span>
    </div>

    <div class="f1-swrr__scenarios">
      <div class="f1-swrr__tabs" role="group" aria-label="Scenarios">
        <button v-for="(item, index) in scenarios" :key="item.id" type="button" class="f1-swrr__tab"
          :class="{ 'is-active': index === scenarioIndex }" :aria-pressed="index === scenarioIndex"
          :aria-label="`Load the ${item.label} scenario`" @click="loadScenario(index)">{{ item.label }}</button>
      </div>
      <p class="f1-swrr__desc">{{ current.description }}</p>
    </div>

    <!-- The stage scrolls sideways inside its own box; the page never does. -->
    <div class="f1-swrr__stage">
      <svg class="f1-swrr__svg" viewBox="0 0 1200 480" role="img"
        :aria-label="`Stage: the high, medium and low lanes, a picker, and ${workers.length} workers`">
        <defs>
          <filter id="f1-swrr-glow" x="-60%" y="-60%" width="220%" height="220%">
            <feGaussianBlur in="SourceGraphic" stdDeviation="2.4" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        <g class="f1-swrr__headings">
          <text x="130" y="30">01 / BROKER INTAKE</text>
          <text x="460" y="30">02 / LANES</text>
          <text x="790" y="30">03 / THE PICK</text>
          <text x="1045" y="30">04 / WORKERS</text>
        </g>

        <path v-for="lane in lanes" :key="`in-${lane.id}`" class="f1-swrr__wire"
          :class="{ 'is-live': lane.flowing }" :d="pathOf(laneGeom.ingest(lane.id))" />
        <text v-for="lane in lanes" :key="`bt-${lane.id}`" x="246" :y="LANE_CY[lane.id] - 10" text-anchor="end"
          class="f1-swrr__wire-label">{{ lane.held ? `${lane.held} at broker` : '' }}</text>
        <path v-for="lane in lanes" :key="`out-${lane.id}`" class="f1-swrr__wire"
          :class="{ 'is-live': lensLane === lane.id }" :d="pathOf(laneGeom.toPicker(lane.id))" />
        <path v-for="(worker, index) in workers" :key="`w-${index}`" class="f1-swrr__wire"
          :class="{ 'is-live': worker.busy }" :d="pathOf(workerWire(index))" />

        <g transform="translate(40, 198)">
          <rect width="116" height="84" rx="10" class="f1-swrr__node" />
          <text x="58" y="36" class="f1-swrr__node-name">broker</text>
          <text x="58" y="59" class="f1-swrr__node-sub">{{ brokerTotal }} waiting</text>
        </g>

        <g v-for="lane in lanes" :key="lane.id" :transform="`translate(250, ${laneGeom.boxY(lane.id)})`">
          <rect width="420" height="74" rx="8" class="f1-swrr__lane" :class="`is-${lane.id}`" />
          <circle cx="18" cy="18" r="4" :class="`f1-swrr__dot is-${lane.id}`" />
          <text x="30" y="21" class="f1-swrr__lane-name">{{ lane.id }} lane</text>
          <text x="408" y="21" text-anchor="end" class="f1-swrr__lane-meta">
            weight {{ lane.weight }}, {{ lane.budget / 1000 }}s budget, cap {{ lane.cap }}
          </text>
          <rect x="18" y="28" width="384" height="18" rx="4" class="f1-swrr__tray" />
          <g transform="translate(26, 37)">
            <rect v-for="(item, index) in lane.items" :key="item.id" :x="index * 22 - 7" y="-7" width="18" height="14"
              rx="4" :class="`f1-swrr__chip is-${lane.id}`" />
          </g>
          <rect x="18" y="56" width="100" height="3" rx="1.5" class="f1-swrr__score-track" />
          <rect x="68" y="55" width="1" height="5" class="f1-swrr__score-zero" />
          <rect :x="scoreBar(lane).x" y="56" :width="scoreBar(lane).width" height="3" rx="1.5"
            :class="`f1-swrr__score-fill is-${lane.id}`" />
          <text x="128" y="60" class="f1-swrr__lane-meta">score {{ lane.score > 0 ? '+' : '' }}{{ lane.score }}</text>
          <text x="408" y="60" text-anchor="end" class="f1-swrr__lane-meta">
            {{ lane.items.length }}/{{ lane.cap }} queued
          </text>
        </g>

        <g transform="translate(790, 240)">
          <circle r="40" class="f1-swrr__lens" />
          <circle r="40" class="f1-swrr__lens-ring" :class="lensLane ? `is-${lensLane}` : ''" />
          <text y="-12" text-anchor="middle" class="f1-swrr__lens-title">the pick</text>
          <text y="8" text-anchor="middle" class="f1-swrr__lens-lane"
            :class="lensPromoted ? 'is-promo' : lensLane ? `is-${lensLane}` : ''">
            {{ lensLane ? (lensPromoted ? 'promoted' : lensLane) : 'idle' }}
          </text>
          <text y="26" text-anchor="middle" class="f1-swrr__lens-sub">{{ lensLane ? 'served' : 'no pick' }}</text>
        </g>

        <g v-for="(worker, index) in workers" :key="`box-${index}`" :transform="`translate(950, ${workerWire(index)[3].y - 26})`">
          <rect width="190" height="52" rx="8" class="f1-swrr__worker" :class="{ 'is-busy': worker.busy }" />
          <text x="16" y="24" class="f1-swrr__worker-name">Worker {{ index }}</text>
          <text x="174" y="24" text-anchor="end" class="f1-swrr__worker-state" :class="{ 'is-busy': worker.busy }">
            {{ worker.busy ? 'busy' : 'free' }}
          </text>
          <rect x="16" y="36" width="158" height="3" rx="1.5" class="f1-swrr__progress-track" />
          <rect x="16" y="36" :width="workerProgress(index)" height="3" rx="1.5" class="f1-swrr__progress-fill"
            :opacity="worker.busy ? 1 : 0" />
        </g>

        <g>
          <circle v-for="particle in particles" :key="particle.id" :cx="particle.x" :cy="particle.y" r="3.4"
            :class="`f1-swrr__particle is-${particle.tone}`" filter="url(#f1-swrr-glow)" />
        </g>
      </svg>
    </div>

    <div class="f1-swrr__hud">
      <div class="f1-swrr__card">
        <span class="f1-swrr__card-label">Pick shares{{ shareWindow ? `, last ${shareWindow} picks` : '' }}</span>
        <span class="f1-swrr__card-value">{{ shareText }}</span>
        <span class="f1-swrr__card-sub">Smooth weighted round robin, weights {{ weightText }}</span>
        <div class="f1-swrr__strip" role="list" aria-label="The last picks, oldest first">
          <span v-for="(pick, index) in recentPicks" :key="index" role="listitem" class="f1-swrr__strip-cell"
            :class="[`is-${pick.lane}`, { 'is-promoted': pick.promoted }]"
            :aria-label="`${pick.lane}${pick.promoted ? ', promoted' : ''}`"></span>
        </div>
      </div>
      <div class="f1-swrr__card">
        <span class="f1-swrr__card-label">Deadline promotion</span>
        <span class="f1-swrr__card-value">{{ promotions }}</span>
        <span class="f1-swrr__card-sub">{{ promote ? 'The most overdue lane is served first' : 'Turned off: weights only' }}</span>
        <div class="f1-swrr__gauge">
          <div class="f1-swrr__gauge-fill is-promo" :style="{ width: `${promotionShare}%` }"></div>
        </div>
      </div>
      <div class="f1-swrr__card">
        <span class="f1-swrr__card-label">Worker pool</span>
        <span class="f1-swrr__card-value">{{ busyWorkers }} / {{ workers.length }} busy</span>
        <span class="f1-swrr__card-sub">A pick happens only while a worker is free</span>
        <div class="f1-swrr__gauge">
          <div class="f1-swrr__gauge-fill is-worker" :style="{ width: `${poolShare}%` }"></div>
        </div>
      </div>
      <div class="f1-swrr__card">
        <span class="f1-swrr__card-label">Broker backlog</span>
        <span class="f1-swrr__card-value">{{ brokerTotal }}</span>
        <span class="f1-swrr__card-sub">{{ brokerState }}</span>
        <div class="f1-swrr__gauge">
          <div class="f1-swrr__gauge-fill is-promo" :style="{ width: `${backlogShare}%` }"></div>
        </div>
      </div>
    </div>

    <div class="f1-swrr__controls">
      <div v-for="lane in lanes" :key="`c-${lane.id}`" class="f1-swrr__control">
        <span class="f1-swrr__control-name" :class="`is-${lane.id}`">{{ lane.id }}</span>
        <label>
          <span>weight</span>
          <input type="range" min="1" max="8" step="1" :value="weights[lane.id]"
            :aria-label="`${lane.id} lane weight`" @input="setWeight(lane.id, $event)" />
          <output>{{ weights[lane.id] }}</output>
        </label>
        <label>
          <span>arrivals</span>
          <input type="range" min="0" :max="RATE_MAX" step="0.5" :value="rates[lane.id]"
            :aria-label="`${lane.id} arrivals per simulated second`" @input="setRate(lane.id, $event)" />
          <output>{{ rates[lane.id] }}/s</output>
        </label>
      </div>
      <div class="f1-swrr__control">
        <span class="f1-swrr__control-name">pool</span>
        <label>
          <span>workers</span>
          <input type="range" min="1" max="4" step="1" :value="workers.length"
            aria-label="Worker count" @input="setWorkers($event)" />
          <output>{{ workers.length }}</output>
        </label>
        <label>
          <span>handler</span>
          <input type="range" min="100" max="5000" step="100" :value="handlerMs"
            aria-label="Handler time in simulated milliseconds" @input="setHandler($event)" />
          <output>{{ handlerMs }} ms</output>
        </label>
        <label class="f1-swrr__toggle">
          <input type="checkbox" :checked="promote" aria-label="Deadline promotion"
            @change="setPromote($event)" />
          <span>deadline promotion</span>
        </label>
      </div>
    </div>

    <p class="f1-swrr__status">{{ statusLine }}</p>

    <figcaption class="f1-swrr__caption">
      Time is simulated: the clock, the handler time and the budgets all count simulated milliseconds, so a
      five-second budget is five simulated seconds and the speed buttons change how fast that clock runs, not what
      happens on it. A lane holds as many messages as its share of the workers, doubled, with a small floor; a
      message that arrives at a full lane waits at the broker, and the clock on its budget starts when it enters
      the lane, not when it arrived. Every scenario replays the same way.
    </figcaption>
  </figure>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'

// The scheduler the figure runs is the one the SDK runs: smooth weighted round
// robin over lanes, with deadline promotion checked first. The visual layer is
// decoration on top of this model and never feeds back into it, so the pick
// order here can be compared against the real scheduler pick for pick.
type Priority = 'high' | 'medium' | 'low'

type Item = { id: number; enteredAt: number }

type Lane = {
  id: Priority
  weight: number; budget: number; cap: number
  items: Item[]; held: number
  score: number; flowing: boolean
}

type Particle = { id: number; tone: string; points: Point[]; progress: number; x: number; y: number }

type Point = { x: number; y: number }

type Scenario = {
  id: string; label: string; description: string
  weights: Record<Priority, number>; rates: Record<Priority, number>
  workers: number; handlerMs: number; promote: boolean
  burst?: { everyMs: number; size: number }
}

const TICK_MS = 20
const BUDGETS: Record<Priority, number> = { high: 5000, medium: 30000, low: 120000 }
const LANES: Priority[] = ['high', 'medium', 'low']
// F1 sorts its lane plan by lane name, so a tie on score goes to high, then low,
// then medium. The stage draws them high, medium, low.
const SLOT_ORDER: Priority[] = ['high', 'low', 'medium']
const LANE_CAPACITY_FLOOR = 3
const LANE_CAPACITY_FACTOR = 2
const PARTICLE_STEP = 0.09
const SHARE_WINDOW = 130
const HISTORY_LIMIT = 4000

// Lane geometry, in the stage's viewBox. One source of truth for the wires and
// for the particles that ride them.
const BROKER_OUT: Point = { x: 156, y: 240 }
const LANE_X = 250
const LANE_W = 420
const LANE_H = 74
const LANE_CY: Record<Priority, number> = { high: 110, medium: 240, low: 370 }
const PICKER_IN: Point = { x: 746, y: 240 }
const PICKER_OUT: Point = { x: 834, y: 240 }
const WORKER_X = 950

function curve(from: Point, to: Point, bendX: number): Point[] {
  return [from, { x: bendX, y: from.y }, { x: bendX, y: to.y }, to]
}

function pathOf(points: Point[]): string {
  const [a, b, c, d] = points
  return `M ${a.x} ${a.y} C ${b.x} ${b.y}, ${c.x} ${c.y}, ${d.x} ${d.y}`
}

// Cubic bezier, so a particle sits exactly on the wire it is drawn along.
function pointOn(points: Point[], t: number): Point {
  const [a, b, c, d] = points
  const u = 1 - t
  const w0 = u * u * u
  const w1 = 3 * u * u * t
  const w2 = 3 * u * t * t
  const w3 = t * t * t
  return {
    x: w0 * a.x + w1 * b.x + w2 * c.x + w3 * d.x,
    y: w0 * a.y + w1 * b.y + w2 * c.y + w3 * d.y,
  }
}

const scenarios: Scenario[] = [
  {
    id: 'steady',
    label: 'Steady (8:4:1)',
    description: 'All three lanes stay full, so the pick shares settle on the weight ratio and the picks interleave.',
    weights: { high: 8, medium: 4, low: 1 }, rates: { high: 12, medium: 7, low: 3 },
    workers: 4, handlerMs: 300, promote: true,
  },
  {
    id: 'bursty',
    label: 'Bursty lane',
    description: 'Low arrives in bursts with gaps between them, and each time its lane empties its score drops back to zero.',
    weights: { high: 4, medium: 2, low: 4 }, rates: { high: 10, medium: 8, low: 0 },
    workers: 4, handlerMs: 300, promote: true,
    burst: { everyMs: 4000, size: 6 },
  },
  {
    id: 'slow',
    label: 'Slow handlers',
    description: 'Three-second handlers: high goes over its five-second budget first and takes most of the promotions, while medium goes over much later and takes the occasional turn.',
    weights: { high: 8, medium: 4, low: 1 }, rates: { high: 3, medium: 2, low: 1 },
    workers: 4, handlerMs: 3000, promote: true,
  },
  {
    id: 'saturation',
    label: 'Saturation',
    description: 'Arrivals far above what four workers can clear: lanes sit at their capacity, the broker count climbs, and the wait inside a lane stays bounded.',
    weights: { high: 8, medium: 4, low: 1 }, rates: { high: 40, medium: 25, low: 15 },
    workers: 4, handlerMs: 300, promote: true,
  },
]

const speedOptions = [0.25, 0.5, 1, 2, 4]
const RATE_MAX = 60

const scenarioIndex = ref(0)
const playing = ref(false)
const speed = ref(1)
const simTime = ref(0)
const lanes = ref<Lane[]>([])
const busyUntil = ref<number[]>([])
const particles = ref<Particle[]>([])
const picks = ref<{ lane: Priority; promoted: boolean }[]>([])
const promotions = ref(0)
const lensLane = ref<Priority | null>(null)
const lensPromoted = ref(false)
const weights = ref<Record<Priority, number>>({ high: 8, medium: 4, low: 1 })
const rates = ref<Record<Priority, number>>({ high: 12, medium: 7, low: 3 })
const handlerMs = ref(300)
const promote = ref(true)

// The model's own state, outside Vue's reactivity: the tick loop reads and
// writes these, and the rendered refs are refreshed once per tick.
let burst: Scenario['burst']
let messageId = 0
let particleId = 0
let burstTimer = 0
let rng = 1
let frameSkip = 0

const current = computed(() => scenarios[scenarioIndex.value])
const clock = computed(() => {
  const total = Math.floor(simTime.value / 1000)
  const minutes = String(Math.floor(total / 60)).padStart(2, '0')
  const seconds = String(total % 60).padStart(2, '0')
  const hundredths = String(Math.floor((simTime.value % 1000) / 10)).padStart(2, '0')
  return `${minutes}:${seconds}.${hundredths}`
})
const brokerTotal = computed(() => lanes.value.reduce((sum, lane) => sum + lane.held, 0))
const brokerState = computed(() => {
  if (brokerTotal.value === 0) {
    return 'Nothing held back: every lane has room'
  }
  return 'Held at the broker while a lane is full'
})
const busyWorkers = computed(() => workers.value.filter((worker) => worker.busy).length)
const workers = computed(() => busyUntil.value.map((until) => ({ busy: until > simTime.value, until })))
const poolShare = computed(() => (workers.value.length === 0 ? 0 : (busyWorkers.value / workers.value.length) * 100))
const recentPicks = computed(() => picks.value.slice(-SHARE_WINDOW))
const shareWindow = computed(() => Math.min(SHARE_WINDOW, picks.value.length))
const shareText = computed(() => {
  const window = recentPicks.value
  if (window.length === 0) {
    return 'no picks yet'
  }
  const scale = Math.max(...LANES.map((id) => window.filter((pick) => pick.lane === id).length))
  if (scale === 0) {
    return 'no picks yet'
  }
  // Scaled to the heaviest lane so the number reads like the weight ratio.
  const base = Math.max(...LANES.map((id) => weights.value[id]))
  return LANES.map((id) => {
    const count = window.filter((pick) => pick.lane === id).length
    return Math.round((count / scale) * base)
  }).join(' : ')
})
const weightText = computed(() => LANES.map((id) => weights.value[id]).join(' : '))
const promotionShare = computed(() => (picks.value.length === 0 ? 0 : (promotions.value / picks.value.length) * 100))
const backlogShare = computed(() => Math.min(100, brokerTotal.value * 4))
const scoreScale = computed(() => Math.max(4, ...lanes.value.map((lane) => Math.abs(lane.score))))
const statusLine = computed(() => {
  const parts = lanes.value.map((lane) => `${lane.id} ${lane.items.length} queued, ${lane.held} at the broker, score ${lane.score}`)
  return `${picks.value.length} picks, ${promotions.value} of them promoted. ${parts.join('. ')}.`
})

const laneGeom = {
  boxY: (id: Priority) => LANE_CY[id] - LANE_H / 2,
  ingest: (id: Priority) => curve(BROKER_OUT, { x: LANE_X, y: LANE_CY[id] }, 200),
  toPicker: (id: Priority) => curve({ x: LANE_X + LANE_W, y: LANE_CY[id] }, PICKER_IN, 715),
}

function workerWire(index: number): Point[] {
  const count = Math.max(busyUntil.value.length, 1)
  const centre = 240 + (index - (count - 1) / 2) * 82
  return curve(PICKER_OUT, { x: WORKER_X, y: centre }, 887)
}

function scoreBar(lane: Lane): { x: number; width: number } {
  const half = 50
  const width = Math.min(half, (Math.abs(lane.score) / scoreScale.value) * half)
  return { x: lane.score < 0 ? 18 + half - width : 18 + half, width }
}

function workerProgress(index: number): number {
  const worker = workers.value[index]
  if (!worker || !worker.busy) {
    return 0
  }
  const remaining = worker.until - simTime.value
  return Math.max(0, Math.min(158, 158 - (remaining / Math.max(handlerMs.value, 1)) * 158))
}

function setWeight(priority: Priority, event: Event): void {
  weights.value = { ...weights.value, [priority]: Number((event.target as HTMLInputElement).value) }
  loadScenario(scenarioIndex.value, true)
}

function setRate(priority: Priority, event: Event): void {
  rates.value = { ...rates.value, [priority]: Number((event.target as HTMLInputElement).value) }
  loadScenario(scenarioIndex.value, true)
}

function setWorkers(event: Event): void {
  const count = Number((event.target as HTMLInputElement).value)
  busyUntil.value = Array.from({ length: count }, () => 0)
  loadScenario(scenarioIndex.value, true)
}

function setHandler(event: Event): void {
  handlerMs.value = Number((event.target as HTMLInputElement).value)
  loadScenario(scenarioIndex.value, true)
}

function setPromote(event: Event): void {
  promote.value = (event.target as HTMLInputElement).checked
  loadScenario(scenarioIndex.value, true)
}

function setSpeed(option: number): void {
  speed.value = option
}

function togglePlay(): void {
  playing.value = !playing.value
}

function stepOnce(): void {
  playing.value = false
  advance(1)
}

// Lane capacity is the runner's: the lane's share of the workers, doubled, with
// a floor so a light lane still holds a few messages.
function capacityFor(weight: number, totalWeight: number, workerCount: number): number {
  const share = Math.ceil((workerCount * weight) / totalWeight)
  return Math.max(share, LANE_CAPACITY_FLOOR) * LANE_CAPACITY_FACTOR
}

function buildLanes(): void {
  const totalWeight = LANES.reduce((sum, id) => sum + Math.max(weights.value[id], 1), 0)
  lanes.value = LANES.map((id) => ({
    id,
    weight: Math.max(weights.value[id], 1),
    budget: BUDGETS[id],
    cap: capacityFor(Math.max(weights.value[id], 1), totalWeight, busyUntil.value.length),
    items: [],
    held: 0,
    score: 0,
    flowing: false,
  }))
}

// Loading a scenario or changing a control restarts the run. A run that was
// playing keeps playing, so a reader who drags a slider sees the effect rather
// than having to press Play again.
function loadScenario(index: number, keepControls = false): void {
  const wasPlaying = playing.value
  const scenario = scenarios[index]
  scenarioIndex.value = index
  if (!keepControls) {
    weights.value = { ...scenario.weights }
    rates.value = { ...scenario.rates }
    handlerMs.value = scenario.handlerMs
    promote.value = scenario.promote
  }
  // Every run starts with an idle pool. Without this a restart inherits the
  // workers that were mid-handler when the clock went back to zero, and the new
  // run takes no pick until each of those stale timestamps passes.
  // A pool always has at least one worker, and every scenario sets a count.
  const poolSize = Math.max(1, keepControls ? busyUntil.value.length : scenario.workers)
  busyUntil.value = Array.from({ length: poolSize }, () => 0)
  simTime.value = 0
  messageId = 0
  burstTimer = 0
  frameSkip = 0
  rng = 0x2545f491
  picks.value = []
  promotions.value = 0
  particles.value = []
  lensLane.value = null
  lensPromoted.value = false
  buildLanes()
  burst = scenario.burst
  playing.value = wasPlaying
}

// The pick rule, in one place: deadline promotion first, then smooth weighted
// round robin, then the lane that won. A lane with nothing waiting has its
// score set back to zero in both passes, which is what makes a bursty lane
// start clean instead of spending credit it banked while it was empty.
function pickNextLane(now: number, promotionOn: boolean): { lane: Lane; promoted: boolean } | null {
  if (promotionOn) {
    let overdue: Lane | null = null
    let worstOverrun = 0
    for (const id of SLOT_ORDER) {
      const lane = lanes.value.find((candidate) => candidate.id === id)
      if (!lane) {
        continue
      }
      if (lane.items.length === 0) {
        lane.score = 0
        continue
      }
      const overrun = now - lane.items[0].enteredAt - lane.budget
      if (overrun < 0) {
        continue
      }
      if (overdue === null || overrun > worstOverrun) {
        overdue = lane
        worstOverrun = overrun
      }
    }
    if (overdue) {
      return { lane: overdue, promoted: true }
    }
  }
  let selected: Lane | null = null
  let totalWeight = 0
  for (const id of SLOT_ORDER) {
    const lane = lanes.value.find((candidate) => candidate.id === id)
    if (!lane) {
      continue
    }
    if (lane.items.length === 0) {
      lane.score = 0
      continue
    }
    lane.score += lane.weight
    totalWeight += lane.weight
    if (selected === null || lane.score > selected.score) {
      selected = lane
    }
  }
  if (selected === null) {
    return null
  }
  selected.score -= totalWeight
  return { lane: selected, promoted: false }
}

// A 32-bit linear congruential generator, seeded per run, so every scenario
// replays the same arrivals. It is drawn exactly once per lane per tick, in lane
// order, whether or not the lane has an arrival rate, so editing one lane's rate
// leaves the other lanes' draws where they were.
function rand01(): number {
  rng = (Math.imul(rng, 1664525) + 1013904223) >>> 0
  return rng / 4294967296
}

function spawn(tone: string, points: Point[]): void {
  if (reducedMotion()) {
    return
  }
  particles.value.push({ id: particleId++, tone, points, progress: 0, x: points[0].x, y: points[0].y })
}

function admit(lane: Lane): void {
  if (lane.items.length >= lane.cap) {
    // The lane is full, so the message waits at the broker. It enters the lane,
    // and starts its budget clock, only when a slot frees, so nothing about a
    // message held here is ever observable: a count is the whole of the state.
    lane.held += 1
    lane.flowing = false
    return
  }
  lane.items.push({ id: messageId++, enteredAt: simTime.value })
  lane.flowing = true
}

function arrive(id: Priority): void {
  const lane = lanes.value.find((candidate) => candidate.id === id)
  if (lane) {
    admit(lane)
  }
}

function advance(ticks: number): void {
  for (let step = 0; step < ticks; step++) {
    tick()
  }
}

function tick(): void {
  simTime.value += TICK_MS

  for (const id of LANES) {
    const rate = rates.value[id]
    const draw = rand01()
    if (rate > 0 && draw < (rate * TICK_MS) / 1000) {
      arrive(id)
      const lane = lanes.value.find((candidate) => candidate.id === id)
      if (lane) {
        spawn(lane.id, laneGeom.ingest(lane.id))
      }
    }
  }

  if (burst) {
    burstTimer += TICK_MS
    if (burstTimer >= burst.everyMs) {
      burstTimer = 0
      for (let index = 0; index < burst.size; index++) {
        arrive('low')
      }
      spawn('low', laneGeom.ingest('low'))
    }
  }

  while (true) {
    const free = busyUntil.value.findIndex((until) => until <= simTime.value)
    if (free < 0) {
      break
    }
    const chosen = pickNextLane(simTime.value, promote.value)
    if (!chosen) {
      break
    }
    const item = chosen.lane.items.shift()
    if (!item) {
      break
    }
    busyUntil.value[free] = simTime.value + handlerMs.value
    picks.value.push({ lane: chosen.lane.id, promoted: chosen.promoted })
    if (picks.value.length > HISTORY_LIMIT) {
      picks.value = picks.value.slice(-HISTORY_LIMIT)
    }
    if (chosen.promoted) {
      promotions.value += 1
    }
    lensLane.value = chosen.lane.id
    lensPromoted.value = chosen.promoted
    spawn(chosen.promoted ? 'promo' : chosen.lane.id, workerWire(free))
  }

  // A lane that freed slots pulls in as many messages as it has room for, up to
  // what the broker holds for it. A lane that is still full takes nothing. Each
  // one enters with a fresh clock, which is why the wait at the broker is off it.
  for (const lane of lanes.value) {
    const moving = Math.min(lane.held, lane.cap - lane.items.length)
    for (let index = 0; index < moving; index++) {
      lane.items.push({ id: messageId++, enteredAt: simTime.value })
    }
    lane.held -= moving
  }

  for (const particle of particles.value) {
    particle.progress += PARTICLE_STEP
    const point = pointOn(particle.points, Math.min(particle.progress, 1))
    particle.x = point.x
    particle.y = point.y
  }
  particles.value = particles.value.filter((particle) => particle.progress < 1.1)
}

let timer: ReturnType<typeof setInterval> | null = null
let observer: IntersectionObserver | null = null
let motionQuery: MediaQueryList | null = null
let autoplayStarted = false

function reducedMotion(): boolean {
  return typeof window !== 'undefined' && typeof window.matchMedia === 'function'
    ? window.matchMedia('(prefers-reduced-motion: reduce)').matches
    : false
}

// One tick is the model's unit, so a slow speed skips frames rather than
// slicing a tick, and a fast one runs several ticks per frame. The frame is one
// tick long, which is what makes 1x mean simulated time runs at real time.
function frame(): void {
  if (!playing.value) {
    return
  }
  if (speed.value < 1) {
    frameSkip += 1
    if (frameSkip < Math.round(1 / speed.value)) {
      return
    }
    frameSkip = 0
  }
  advance(Math.max(1, speed.value))
}

function startTimer(): void {
  stopTimer()
  timer = window.setInterval(frame, TICK_MS)
}

function stopTimer(): void {
  if (timer !== null) {
    window.clearInterval(timer)
    timer = null
  }
}

watch(playing, (running) => {
  if (running) {
    startTimer()
  } else {
    stopTimer()
  }
})

onMounted(() => {
  loadScenario(0)
  if (typeof window === 'undefined') {
    return
  }
  if (typeof IntersectionObserver === 'function') {
    observer = new IntersectionObserver(
      (entries) => {
        const visible = entries.some((entry) => entry.isIntersecting)
        if (visible && !autoplayStarted && !reducedMotion()) {
          autoplayStarted = true
          playing.value = true
        }
        if (!visible) {
          playing.value = false
        }
      },
      { threshold: 0.25 },
    )
    const element = document.querySelector('.f1-swrr')
    if (element) {
      observer.observe(element)
    }
  }
  if (typeof window.matchMedia === 'function') {
    motionQuery = window.matchMedia('(prefers-reduced-motion: reduce)')
    motionQuery.addEventListener('change', handleMotionChange)
  }
})

function handleMotionChange(): void {
  if (reducedMotion()) {
    playing.value = false
    particles.value = []
  }
}

onBeforeUnmount(() => {
  stopTimer()
  observer?.disconnect()
  motionQuery?.removeEventListener('change', handleMotionChange)
})
</script>

<style scoped>
/* The reference is dark only. Here every colour is a variable with a value for
   each theme, and the type comes from the docs site. */
.f1-swrr {
  --swrr-high: #0b6e99; --swrr-medium: #6741d9; --swrr-low: #7c8794; --swrr-promo: #b45309;
  --swrr-worker: #0f766e; --swrr-surface: var(--vp-c-bg-soft); --swrr-panel: var(--vp-c-bg);
  --swrr-line: var(--vp-c-divider); --swrr-text: var(--vp-c-text-1);
  --swrr-dim: var(--vp-c-text-2); --swrr-faint: var(--vp-c-text-3);
  --swrr-track: rgba(125, 135, 150, 0.18); margin: 24px 0; padding: 14px;
  border: 1px solid var(--swrr-line); border-radius: 14px; background: var(--swrr-surface);
  color: var(--swrr-text); font-size: 13px;
}

html.dark .f1-swrr {
  --swrr-high: #38bdf8; --swrr-medium: #818cf8; --swrr-low: #94a3b8; --swrr-promo: #f59e0b;
  --swrr-worker: #10b981; --swrr-track: rgba(255, 255, 255, 0.08);
}

.f1-swrr__bar,
.f1-swrr__group,
.f1-swrr__scenarios,
.f1-swrr__controls,
.f1-swrr__control,
.f1-swrr__control label {
  display: flex; align-items: center; gap: 8px;
}

.f1-swrr__bar {
  flex-wrap: wrap; justify-content: space-between; margin-bottom: 12px;
}

.f1-swrr__group { gap: 6px; }

.f1-swrr__btn,
.f1-swrr__speed,
.f1-swrr__tab {
  padding: 5px 11px; border: 1px solid var(--swrr-line); border-radius: 9999px;
  background: transparent; color: var(--swrr-dim); font-size: 12px; cursor: pointer;
}

.f1-swrr__btn.is-primary { border-color: var(--swrr-high); color: var(--swrr-high); }
.f1-swrr__speed { border-radius: 6px; padding: 4px 8px; }
.f1-swrr__speed.is-active,
.f1-swrr__tab.is-active { border-color: var(--swrr-high); color: var(--swrr-text); background: var(--swrr-track); }

.f1-swrr__clock {
  font-family: var(--vp-font-family-mono); font-size: 12px; font-variant-numeric: tabular-nums;
  color: var(--swrr-dim);
}

.f1-swrr__scenarios {
  flex-wrap: wrap; gap: 10px 14px; margin-bottom: 12px;
}

.f1-swrr__tabs { display: flex; flex-wrap: wrap; gap: 4px; }
.f1-swrr__desc { margin: 0; font-size: 12px; color: var(--swrr-dim); flex: 1 1 260px; }

.f1-swrr__stage {
  overflow-x: auto; border: 1px solid var(--swrr-line); border-radius: 12px;
  background: var(--swrr-panel);
}

.f1-swrr__svg { display: block; width: 100%; min-width: 640px; height: auto; }

.f1-swrr__headings text {
  fill: var(--swrr-faint); font-family: var(--vp-font-family-mono); font-size: 20px;
  font-weight: 600; letter-spacing: 0.08em; text-anchor: middle;
}

.f1-swrr__wire { fill: none; stroke: var(--swrr-track); stroke-width: 1.5; }
.f1-swrr__wire.is-live { stroke: var(--swrr-dim); stroke-dasharray: 4 4; }
.f1-swrr__node { fill: var(--swrr-panel); stroke: var(--swrr-line); }
.f1-swrr__node-name,
.f1-swrr__node-sub,
.f1-swrr__lane-name,
.f1-swrr__lane-meta,
.f1-swrr__worker-name,
.f1-swrr__worker-state,
.f1-swrr__lens-title,
.f1-swrr__lens-lane,
.f1-swrr__lens-sub { font-family: var(--vp-font-family-mono); }

.f1-swrr__node-name { fill: var(--swrr-text); font-size: 21px; font-weight: 600; text-anchor: middle; }
.f1-swrr__node-sub { fill: var(--swrr-dim); font-size: 17px; text-anchor: middle; }
.f1-swrr__lane { fill: var(--swrr-panel); stroke: var(--swrr-line); }
.f1-swrr__lane.is-high { stroke: var(--swrr-high); stroke-opacity: 0.45; }
.f1-swrr__lane.is-medium { stroke: var(--swrr-medium); stroke-opacity: 0.45; }
.f1-swrr__lane.is-low { stroke: var(--swrr-low); stroke-opacity: 0.45; }
.f1-swrr__lane-name { fill: var(--swrr-text); font-size: 17px; font-weight: 600; }
.f1-swrr__lane-meta { fill: var(--swrr-dim); font-size: 15px; }
.f1-swrr__wire-label { fill: var(--swrr-dim); font-size: 12px; }
.f1-swrr__tray { fill: var(--swrr-track); stroke: var(--swrr-line); }
.f1-swrr__chip { fill: var(--swrr-low); }
.f1-swrr__chip.is-high, .f1-swrr__dot.is-high { fill: var(--swrr-high); }
.f1-swrr__chip.is-medium, .f1-swrr__dot.is-medium { fill: var(--swrr-medium); }
.f1-swrr__chip.is-low, .f1-swrr__dot.is-low { fill: var(--swrr-low); }
.f1-swrr__score-track { fill: var(--swrr-track); }
.f1-swrr__score-zero { fill: var(--swrr-faint); opacity: 0.5; }
.f1-swrr__score-fill.is-high { fill: var(--swrr-high); }
.f1-swrr__score-fill.is-medium { fill: var(--swrr-medium); }
.f1-swrr__score-fill.is-low { fill: var(--swrr-low); }
.f1-swrr__lens { fill: var(--swrr-panel); stroke: var(--swrr-line); stroke-width: 1.5; }
.f1-swrr__lens-ring { fill: none; stroke: var(--swrr-faint); stroke-width: 2; }
.f1-swrr__lens-ring.is-high { stroke: var(--swrr-high); }
.f1-swrr__lens-lane.is-high { fill: var(--swrr-high); }
.f1-swrr__lens-ring.is-medium { stroke: var(--swrr-medium); }
.f1-swrr__lens-lane.is-medium { fill: var(--swrr-medium); }
.f1-swrr__lens-ring.is-low { stroke: var(--swrr-low); }
.f1-swrr__lens-lane.is-low { fill: var(--swrr-low); }
.f1-swrr__lens-title { fill: var(--swrr-dim); font-size: 15px; font-weight: 600; }
.f1-swrr__lens-lane { font-size: 18px; font-weight: 600; }
.f1-swrr__lens-lane.is-promo { fill: var(--swrr-promo); }
.f1-swrr__lens-sub { fill: var(--swrr-faint); font-size: 13px; }
.f1-swrr__worker { fill: var(--swrr-panel); stroke: var(--swrr-line); }
.f1-swrr__worker.is-busy { stroke: var(--swrr-worker); }
.f1-swrr__worker-name { fill: var(--swrr-text); font-size: 20px; font-weight: 600; }
.f1-swrr__worker-state { fill: var(--swrr-dim); font-size: 17px; }
.f1-swrr__worker-state.is-busy { fill: var(--swrr-worker); }
.f1-swrr__progress-track { fill: var(--swrr-track); }
.f1-swrr__progress-fill { fill: var(--swrr-worker); }
.f1-swrr__particle.is-high { fill: var(--swrr-high); }
.f1-swrr__particle.is-medium { fill: var(--swrr-medium); }
.f1-swrr__particle.is-low { fill: var(--swrr-low); }
.f1-swrr__particle.is-promo { fill: var(--swrr-promo); }

.f1-swrr__hud {
  display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 10px;
  margin-top: 12px;
}

.f1-swrr__card {
  display: flex; flex-direction: column; gap: 4px; padding: 10px 12px;
  border: 1px solid var(--swrr-line); border-radius: 10px; background: var(--swrr-panel);
}

.f1-swrr__card-label {
  font-family: var(--vp-font-family-mono); font-size: 11px; letter-spacing: 0.05em;
  text-transform: uppercase; color: var(--swrr-faint);
}

.f1-swrr__card-value { font-family: var(--vp-font-family-mono); font-size: 17px; font-weight: 600; }
.f1-swrr__card-sub { font-size: 11px; color: var(--swrr-dim); }

.f1-swrr__gauge { height: 3px; border-radius: 2px; background: var(--swrr-track); overflow: hidden; }
.f1-swrr__gauge-fill { height: 100%; background: var(--swrr-high); }
.f1-swrr__gauge-fill.is-promo { background: var(--swrr-promo); }
.f1-swrr__gauge-fill.is-worker { background: var(--swrr-worker); }

.f1-swrr__strip { display: flex; flex-wrap: wrap; gap: 3px; margin-top: 2px; }
.f1-swrr__strip-cell { width: 12px; height: 12px; border-radius: 3px; background: var(--swrr-low); }
.f1-swrr__strip-cell.is-high { background: var(--swrr-high); }
.f1-swrr__strip-cell.is-medium { background: var(--swrr-medium); }
.f1-swrr__strip-cell.is-promoted { outline: 2px solid var(--swrr-promo); outline-offset: -1px; }

.f1-swrr__controls {
  flex-wrap: wrap; gap: 10px 18px; margin-top: 12px; padding-top: 10px;
  border-top: 1px solid var(--swrr-line);
}

.f1-swrr__control { flex-wrap: wrap; gap: 6px 12px; }
.f1-swrr__control-name { font-family: var(--vp-font-family-mono); font-size: 11px; font-weight: 600; min-width: 52px; }
.f1-swrr__control-name.is-high { color: var(--swrr-high); }
.f1-swrr__control-name.is-medium { color: var(--swrr-medium); }
.f1-swrr__control-name.is-low { color: var(--swrr-low); }
.f1-swrr__control label { gap: 6px; font-size: 11px; color: var(--swrr-dim); }
.f1-swrr__control input[type='range'] { width: 84px; }
.f1-swrr__control output { font-family: var(--vp-font-family-mono); font-size: 11px; min-width: 44px; }
.f1-swrr__toggle { gap: 4px; }

.f1-swrr__status { margin: 10px 0 6px; font-size: 12px; color: var(--swrr-dim); }
.f1-swrr__caption { font-size: 12px; color: var(--swrr-dim); line-height: 1.6; }

@media (prefers-reduced-motion: reduce) {
  .f1-swrr__wire.is-live { stroke-dasharray: none; }
}

@media (max-width: 640px) {
  .f1-swrr__controls { flex-direction: column; align-items: flex-start; }
  .f1-swrr__control { width: 100%; }
  .f1-swrr__control label { flex: 1 1 130px; }
}
</style>
