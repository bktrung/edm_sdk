import { defineConfig } from 'vitepress'
import { mermaidPlugin } from './plugins/mermaid'
import { codeBreaksPlugin } from './plugins/code-breaks'

export default defineConfig({
  lang: 'en-US',
  title: 'F1 Event-Driven Messaging SDK',
  description: 'Reliable event-driven messaging for Go services.',
  cleanUrls: true,
  markdown: {
    lineNumbers: false,
    config: (md) => {
      md.use(mermaidPlugin)
      md.use(codeBreaksPlugin)
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
    // The same five areas as the sidebar, in reading order. The nav item for an
    // area points at its first page. Learn is a task-first path from install to
    // a running service; Basics and Advanced split the application-facing model
    // from the decisions that need a capacity or failure plan; Drivers holds the
    // adapter configuration; Development is the reference for maintainers and
    // driver authors.
    nav: [
      { text: 'Learn', link: '/learn/getting-started', activeMatch: '^/learn/' },
      { text: 'Basics', link: '/basics/message', activeMatch: '^/basics/' },
      {
        text: 'Advanced',
        link: '/advanced-topics/failure-handling',
        activeMatch: '^/advanced-topics/',
      },
      {
        text: 'Drivers',
        link: '/drivers-and-capabilities',
        activeMatch: '^/(drivers-and-capabilities|drivers/)',
      },
      { text: 'Development', link: '/development/architecture', activeMatch: '^/development/' },
    ],
    // One sidebar for every page, so a reader always sees where the current
    // page sits in the whole site.
    sidebar: [
      {
        text: 'Learn',
        items: [
          { text: 'Getting started', link: '/learn/getting-started' },
          { text: 'Quickstart', link: '/learn/quickstart' },
          { text: 'Glossary', link: '/learn/glossary' },
        ],
      },
      {
        text: 'Basics',
        collapsed: false,
        items: [
          { text: 'Message', link: '/basics/message' },
          { text: 'Publisher and subscriber', link: '/basics/pubsub' },
          { text: 'Middleware', link: '/basics/middleware' },
          { text: 'Observer', link: '/basics/observer' },
        ],
      },
      {
        text: 'Advanced',
        collapsed: false,
        items: [
          { text: 'Failure handling', link: '/advanced-topics/failure-handling' },
          { text: 'Ordering and scheduling', link: '/advanced-topics/ordering-and-scheduling' },
          { text: 'Lifecycle and shutdown', link: '/advanced-topics/lifecycle-and-shutdown' },
          { text: 'Topology and capabilities', link: '/advanced-topics/topology-and-capabilities' },
          { text: 'Observability', link: '/advanced-topics/observability' },
          { text: 'Alerts', link: '/advanced-topics/alerts' },
          { text: 'Running in production', link: '/advanced-topics/running-in-production' },
          { text: 'Configuration reference', link: '/advanced-topics/configuration' },
          { text: 'Troubleshooting', link: '/advanced-topics/troubleshooting' },
        ],
      },
      {
        text: 'Drivers',
        collapsed: false,
        items: [
          { text: 'Drivers and capabilities', link: '/drivers-and-capabilities' },
          { text: 'In-memory', link: '/drivers/inmem' },
          { text: 'RabbitMQ', link: '/drivers/rabbitmq' },
          { text: 'Kafka', link: '/drivers/kafka' },
        ],
      },
      {
        text: 'Development',
        collapsed: false,
        items: [
          { text: 'Architecture', link: '/development/architecture' },
          { text: 'Publish flow', link: '/development/publish-flow' },
          { text: 'Consume flow', link: '/development/consume-flow' },
          { text: 'Driver contract', link: '/development/driver-contract' },
          { text: 'Driver conformance', link: '/development/driver-conformance' },
          { text: 'Observer events', link: '/development/observer-events' },
          { text: 'Testing strategy', link: '/development/testing' },
          { text: 'Benchmarks', link: '/development/benchmarks' },
          { text: 'API compatibility', link: '/development/api-compatibility' },
          { text: 'Source-reading guide', link: '/development/source-reading-guide' },
          { text: 'Writing style', link: '/development/writing-style' },
        ],
      },
      {
        text: 'Deep dives',
        collapsed: false,
        items: [
          { text: 'Life of a delivery', link: '/deep-dives/life-of-a-delivery' },
          { text: 'Retries and dead letters', link: '/deep-dives/retries-and-dead-letters' },
          { text: 'Reconnecting without mixing up connections', link: '/deep-dives/reconnect-and-generations' },
          { text: 'One owner per runner', link: '/deep-dives/runner-owner-loop' },
          { text: 'Ordered by key', link: '/deep-dives/ordered-by-key' },
          { text: 'How F1 picks the next message', link: '/deep-dives/scheduler' },
          { text: 'Kafka ack tracker', link: '/deep-dives/kafka-ack-tracker' },
          { text: 'Kafka lane balancer', link: '/deep-dives/kafka-lane-balancer' },
          { text: 'RabbitMQ retry parking', link: '/deep-dives/rabbitmq-delay-ladder' },
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
