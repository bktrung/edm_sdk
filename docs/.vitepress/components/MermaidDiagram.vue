<template>
  <div ref="diagramRef" class="f1-mermaid" role="img" aria-label="Mermaid diagram" />
</template>

<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useData } from 'vitepress'
import { createMermaidId } from './mermaid-id'

const props = defineProps<{ code: string }>()
const { isDark } = useData()
const diagramRef = ref<HTMLElement | null>(null)
let renderVersion = 0

// Below this width a scaled-down diagram's labels stop being readable, so a
// wider diagram keeps this floor and its container scrolls on narrow screens.
const minReadableWidth = 600

// Mermaid sizes each label box from text it measures at render time, so the
// font must be the one the page paints with; the colors come from the same
// theme variables so diagrams match the site. Both are read after the
// appearance class has settled, which keeps diagrams in step with the
// light/dark toggle.
function siteThemeVariables(dark: boolean) {
  const style = getComputedStyle(document.documentElement)
  const cssVar = (name: string) => style.getPropertyValue(name).trim()
  return {
    darkMode: dark,
    fontFamily: cssVar('--vp-font-family-base'),
    fontSize: '15px',
    background: cssVar('--vp-c-bg'),
    primaryColor: cssVar('--vp-c-bg-soft'),
    primaryBorderColor: cssVar('--vp-c-brand-1'),
    primaryTextColor: cssVar('--vp-c-text-1'),
    secondaryColor: cssVar('--vp-c-default-soft'),
    tertiaryColor: cssVar('--vp-c-bg-alt'),
    lineColor: cssVar('--vp-c-text-3'),
    textColor: cssVar('--vp-c-text-1'),
    clusterBkg: cssVar('--vp-c-bg-alt'),
    clusterBorder: cssVar('--vp-c-divider'),
    edgeLabelBackground: cssVar('--vp-c-bg'),
    // Sequence diagrams: without these, notes keep Mermaid's bright yellow.
    noteBkgColor: cssVar('--vp-c-default-soft'),
    noteBorderColor: cssVar('--vp-c-divider'),
    noteTextColor: cssVar('--vp-c-text-1'),
    actorBkg: cssVar('--vp-c-bg-soft'),
    actorBorder: cssVar('--vp-c-brand-1'),
    actorTextColor: cssVar('--vp-c-text-1'),
    actorLineColor: cssVar('--vp-c-divider'),
    signalColor: cssVar('--vp-c-text-2'),
    signalTextColor: cssVar('--vp-c-text-1'),
  }
}

async function renderDiagram() {
  const version = ++renderVersion
  await nextTick()

  if (typeof window === 'undefined' || !diagramRef.value) {
    return
  }

  // Measuring against fallback-font metrics produces label boxes too small
  // for the web font that paints a moment later, which clips the last line.
  await document.fonts.ready
  const source = decodeURIComponent(props.code)
  const mermaid = (await import('mermaid')).default

  if (version !== renderVersion || !diagramRef.value) {
    return
  }

  try {
    // Inside the try: the theme is derived from runtime CSS values, and a
    // missing or unparsable one must reach the visible fallback below rather
    // than leave a silently blank diagram.
    mermaid.initialize({
      startOnLoad: false,
      securityLevel: 'strict',
      theme: 'base',
      themeVariables: siteThemeVariables(isDark.value),
    })
    const { svg } = await mermaid.render(createMermaidId(), source)
    if (version === renderVersion && diagramRef.value) {
      diagramRef.value.innerHTML = svg
      const element = diagramRef.value.querySelector('svg')
      if (element) {
        // Mermaid's flowchart viewBox can come out several times larger than
        // the drawing, which scales every label down to a few pixels. Fit the
        // viewBox to the painted content so the diagram renders at its size.
        const box = element.getBBox()
        const padding = 8
        if (box.width > 0 && box.height > 0) {
          element.setAttribute(
            'viewBox',
            `${box.x - padding} ${box.y - padding} ${box.width + 2 * padding} ${box.height + 2 * padding}`,
          )
          element.style.maxWidth = `${box.width + 2 * padding}px`
        }
        // The natural width is now the inline max-width.
        const natural = parseFloat(element.style.maxWidth)
        if (natural > 0) {
          element.style.minWidth = `${Math.min(natural, minReadableWidth)}px`
        }
      }
    }
  } catch (error) {
    if (version === renderVersion && diagramRef.value) {
      diagramRef.value.textContent = 'Unable to render this Mermaid diagram.'
      diagramRef.value.classList.add('f1-mermaid--error')
    }
    console.error('Failed to render Mermaid diagram', error)
  }
}

onMounted(() => void renderDiagram())
watch(isDark, () => void renderDiagram())
onBeforeUnmount(() => {
  renderVersion++
})
</script>
