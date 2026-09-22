import type { Theme } from 'vitepress'
import DefaultTheme from 'vitepress/theme'
import MermaidDiagram from '../components/MermaidDiagram.vue'
import DeliveryPath from '../components/DeliveryPath.vue'
import SchedulerSim from '../components/SchedulerSim.vue'
import './custom.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('F1Mermaid', MermaidDiagram)
    app.component('F1DeliveryPath', DeliveryPath)
    app.component('F1SchedulerSim', SchedulerSim)
  },
} satisfies Theme
