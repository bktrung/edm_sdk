import { defineConfig } from 'vitepress'
import { mermaidPlugin } from './plugins/mermaid'

export default defineConfig({
  lang: 'en-US',
  title: 'F1 Event-Driven Messaging SDK',
  description: 'Reliable event-driven messaging for Go services.',
  cleanUrls: true,
  markdown: {
    lineNumbers: false,
    config: (md) => {
      md.use(mermaidPlugin)
    },
  },
  themeConfig: {
    // The full title stays in `title` for browser tabs and search results;
    // the header gets a short form that fits beside the nav on small screens.
    siteTitle: 'F1 Messaging',
    appearance: true,
    outline: {
      level: [2, 3],
      label: 'On this page',
    },
    search: {
      provider: 'local',
    },
    // Three reader areas over one directory layout. Paths stay where they are
    // because README, ARCHITECTURE.md and CLAUDE.md link into them; grouping
    // happens here instead of by moving files.
    nav: [
      { text: 'Guide', link: '/learn/what-is-f1', activeMatch: '^/(learn|user-guide)/' },
      { text: 'Concepts', link: '/basics/message', activeMatch: '^/(basics|advanced-topics)/' },
      {
        text: 'Internals',
        link: '/development/architecture',
        activeMatch: '^/(development/|runtime-overview|drivers-and-capabilities)',
      },
      { text: 'Deep dives', link: '/deep-dives/kafka-ack-tracker', activeMatch: '^/deep-dives/' },
    ],
    // One sidebar for every page, in reading order, so a reader always sees
    // where the current page sits in the whole site.
    sidebar: [
      {
        text: 'Introduction',
        items: [
          { text: 'What is F1', link: '/learn/what-is-f1' },
          { text: 'Getting started', link: '/learn/getting-started' },
        ],
      },
      {
        text: 'Guides',
        collapsed: false,
        items: [
          { text: 'Publishing events', link: '/user-guide/publishing-events' },
          { text: 'Consuming events', link: '/user-guide/consuming-events' },
          { text: 'Handling failures', link: '/user-guide/handling-failures' },
          { text: 'Graceful shutdown', link: '/user-guide/graceful-shutdown' },
          { text: 'Testing handlers', link: '/user-guide/testing' },
          { text: 'Driver options', link: '/user-guide/driver-options' },
        ],
      },
      {
        text: 'Concepts',
        collapsed: false,
        items: [
          { text: 'Message', link: '/basics/message' },
          { text: 'Publisher and subscriber', link: '/basics/pubsub' },
          { text: 'Middleware', link: '/basics/middleware' },
          { text: 'Failure handling', link: '/advanced-topics/failure-handling' },
          { text: 'Lifecycle and shutdown', link: '/advanced-topics/lifecycle-and-shutdown' },
          { text: 'Ordering and scheduling', link: '/advanced-topics/ordering-and-scheduling' },
          { text: 'Topology and capabilities', link: '/advanced-topics/topology-and-capabilities' },
        ],
      },
      {
        text: 'Internals',
        collapsed: false,
        items: [
          { text: 'Runtime overview', link: '/runtime-overview' },
          { text: 'Architecture', link: '/development/architecture' },
          { text: 'Publish flow', link: '/development/publish-flow' },
          { text: 'Consume flow', link: '/development/consume-flow' },
          { text: 'Driver contract', link: '/development/driver-contract' },
          { text: 'Driver conformance', link: '/development/driver-conformance' },
          { text: 'Drivers and capabilities', link: '/drivers-and-capabilities' },
          { text: 'Testing strategy', link: '/development/testing' },
          { text: 'Source-reading guide', link: '/development/source-reading-guide' },
          { text: 'Writing style', link: '/development/writing-style' },
        ],
      },
      {
        text: 'Deep dives',
        collapsed: false,
        items: [
          { text: 'Kafka ack tracker', link: '/deep-dives/kafka-ack-tracker' },
          { text: 'Kafka partition assignment', link: '/deep-dives/kafka-lane-balancer' },
          { text: 'RabbitMQ delay ladder', link: '/deep-dives/rabbitmq-delay-ladder' },
          { text: 'Scheduler and worker', link: '/deep-dives/scheduler-and-worker' },
        ],
      },
    ],
    editLink: {
      pattern: 'https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/edit/main/docs/:path',
      text: 'Edit this page',
    },
    footer: {
      message: 'F1 Event-Driven Messaging SDK',
    },
  },
})
