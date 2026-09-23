import type { Theme } from 'vitepress'
import DefaultTheme from 'vitepress/theme'
import MermaidDiagram from '../components/MermaidDiagram.vue'
import DeliveryPath from '../components/DeliveryPath.vue'
import SchedulerStepper from '../components/SchedulerStepper.vue'
import KafkaCursorStepper from '../components/KafkaCursorStepper.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('F1Mermaid', MermaidDiagram)
    app.component('F1DeliveryPath', DeliveryPath)
    app.component('F1SchedulerStepper', SchedulerStepper)
    app.component('F1KafkaCursorStepper', KafkaCursorStepper)
  },
} satisfies Theme
