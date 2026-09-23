# Writing style

How the F1 docs are written: which voice each section uses, the rules every
page follows, and the shape of a deep-dive page. Read it before adding or
rewriting a page under `docs/`.

## One voice per section

| Section | Directories | Voice |
| --- | --- | --- |
| Learn | `learn/` | Second person, task first: "Create a subscription, then call `Run`." |
| Basics | `basics/` | Neutral explanation of what F1 does and why an application cares. |
| Advanced | `advanced-topics/` | The same neutral voice, for decisions that need a capacity or failure plan. |
| Drivers | `drivers-and-capabilities.md` | Neutral and exact. Configuration and provider-specific behavior. |
| Development | `development/` | Neutral and exact. Reference for maintainers and driver authors. |
| Deep dives | `deep-dives/` | First person singular. How a part works inside and the reasons behind it, in present tense. |

Keep the voice of the section a page lives in. A Learn page that drifts into
mechanism and the reasoning behind it belongs in a deep dive; a deep dive that
turns into option tables belongs in Development.

## Rules for every page

### Per concept

Define each concept in one plain sentence. Follow it with a picture or a
concrete example, then at most three short paragraphs. Aim for about 60 words
per paragraph; none may exceed 100 words. Add an admonition only for a real
gotcha.


### ASCII only

The rule in `CLAUDE.md` covers docs too. No em or en dash: use
a comma, a colon, parentheses, or a new sentence. Straight quotes only. Write
an arrow as `->`.

### Open with the subject

The first sentence says something about F1, not
about the page. "Publishing validates the event, builds the envelope, and waits
for the broker to confirm it" tells the reader more than "This page is the
maintainer trace for publishing."

### Say it once, plainly

Prefer `is`, `has` and `does` over `serves as`,
`owns` or `is responsible for` when the plain verb is true. Avoid these words
unless they carry a technical meaning in the sentence: `canonical`, `ensure`,
`robust`, `seamless`, `leverage`, `crucial`, `key` as an adjective, `delve`.
A technical use stays: "the canonical topic" names a real derived value.

### Plain words first, link the term

Outside `development/`, write the plain phrase a Go developer already knows and
link it to the glossary the first time it appears on a page: "F1
[finishes the message](/learn/glossary#settlement) only after the retry copy is
confirmed", not "F1 settles the original after the successor handoff". The
project's own words stay in the glossary, in code, and in config keys, so a
reader can still match a log line or a field name to the page.

| Term | Write instead |
| --- | --- |
| settle, settlement, settled, unsettled | finish the message (ack or nack), finished, not yet finished or not yet acked |
| successor, successor publish | retry copy or dead-letter copy, publishing the retry copy |
| epoch | connection number |
| generation | the runner's current consumer |
| admission, admit | allowed to start, let in |
| backstop | the broker's own dead-letter queue |
| handoff | handing the copy to the broker |
| deadline promotion, promotion | jumping the queue when overdue |
| lane budget | wait limit |
| retry tier, retry ladder | retry step, the list of retry delays |
| cursor (Kafka) | committed offset |
| native, emulated | done by the broker, done by F1 |
| partition-bound, free scaling | limited by partition count, not limited by partitions |
| capability report | the list of features the connected broker supports |
| abandon (a delivery) | give up on |
| physical destination, logical topic | broker queue or topic name, topic name in your code |

Common messaging words stay as they are: ack, nack, requeue, redelivery,
prefetch, partition, consumer group, dead letter, idempotency, envelope, codec,
confirm, quorum queue, poison message, in-flight, drain, rebalance, revoke, and
parking queue. Avoid one-off formal words such as quiescence, incarnation,
interleaving, or invariant; say what happens instead.

### Keep sentences uneven

Real prose alternates short and long sentences.
Three items in a list only when there are three things.

### Headings in sentence case

No emoji, no decoration, no heading that repeats
the page title.

### Bold is rare

Bold a term the reader must not miss. Do not give every
bullet a bold label.

### Teach the model, not the code

Pages outside `development/` explain how F1 behaves and why, in words a reader
keeps after the code changes. Internal names change on every refactor; the
mental model does not.

- Do not name tests, test files, unexported functions, internal types, or
  internal constants. Say what the code does: "F1 registers the delivery before
  it queues it", not "`enqueueDelivery` calls `registry.Add`".
- Name only the public API a user types: `f1.Subscribe`, `f1.Terminal`,
  `Subscription.Prefetch`, config keys, and CLI or `make` targets.
- Do not quote internal numbers that are not configuration. "F1 retries the
  publish a few times over a fraction of a second" survives a tuning change;
  "3 attempts, 100 ms apart" does not. Defaults a user can set are fine.
- Code blocks show how to use F1, never how F1 is implemented.
- Source links are rare: at most a few per page, to a file or package, never a
  line number or a `#L` range. Use the full repository URL form with no line
  anchor:
  `https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/<path>`.

`development/` pages are the exception: their job is to map the code, so they
name functions, tests and files. They still never cite a line number.

### No unresolvable citations

`make verify-self-contained` rejects decision
and task identifiers a reader cannot follow. State the rule instead.

### No plan or phase identifiers

Do not write "Phase 4", a plan id, an audit label or a finding code in a page,
a code comment, or a generated artifact. State the behaviour.

### Diagrams and tables

Use one diagram per core idea that is an order of events, a state machine or a
structure. Use tables for comparisons and worked traces.

### Mermaid vocabulary

The broker is a cylinder `B[(broker)]`; the application handler is a stadium
`H([handler])`; an F1 step is a plain rectangle; a decision is a diamond; a
dashed edge means the message goes back to the broker. Node labels at most three
words. No `classDef` colors and no `style` lines: the site theme handles light
and dark mode.

### Code examples compile

Every example builds against the current API. When the API changes, the
example changes in the same commit.

### Algorithms

Algorithms use a static worked trace, table or timeline, checked against the
code. An interactive figure is allowed only when stepping adds understanding,
and only over a trace taken from the Go code, never a second implementation in
the docs.

### Configuration examples

Use VitePress `code-group` tabs for equivalent configuration across brokers.

### Page endings

Every page ends with a short "Go further" list.

### Signs of AI writing

Read every page against Wikipedia's
[Signs of AI writing](https://en.wikipedia.org/wiki/Wikipedia:Signs_of_AI_writing)
list and cut filler openers, "it's not X, it's Y", rule-of-three lists that are
not three things, and summary sentences that repeat the paragraph.

## Deep dives

A deep dive teaches how one mechanism works and why, like an engineering blog
post from a team that builds a platform. The reader is an outside engineer who
may never use F1: they know Go and roughly what a message broker does, but not
F1 itself. The page does not replace the reference: it links to Concepts or
Internals for what the feature does, and spends its length on how and why.

### Voice

- Write as "I", the first person singular. The author is named at the top of the
  page, in the header `*By <author>.*`.
- Opinions are welcome when they read as opinions: "I think this is the
  weakest part of the design."
- State uncertainty plainly: "I have not measured this under load."
- Prefer the concrete case: real numbers, a real sequence of events. A race is
  explained by its interleaving, which goroutine is where and what the other one
  does in between, as `CLAUDE.md` asks of code comments.
- Pages explain the mechanism and the reasoning as it stands today, in present
  tense. Do not write a design-history narrative or an account of what an
  earlier version did.

### Accuracy

- Every behavioural claim is checked against the source or a test before it
  is written. Link the few that a reader will want to open, not every one: a
  page reads as prose and examples, not as a list of source links.
- Broker behaviour is measured against the local fixture, not taken from
  broker or client documentation. Say when a claim was measured.
- Pages are living and in present tense. They describe the code as it is now,
  carry no date and no commit sha, and are edited in place when the code
  changes.
- Measured numbers live on [Benchmarks](/development/benchmarks) with the
  reproduce command and the machine. A deep dive states the finding in words
  and links there. A number that would need a new harness is a finding, not an
  edit.

### Page template

The headings are a guide. Rename or drop them when the story needs a
different order.

```md
# <A claim or a problem, not a component name>

*By <author>.*

<Lede: one concrete scenario that goes wrong without this mechanism.>

## Background

<Just enough broker detail for a Go engineer.>

## How it works

<One worked example end to end, one diagram.>

## Limits and trade-offs

## Go further

- [<Development page>](/development/<page>) - where the code map for this path lives
```

A deep dive follows "Teach the model, not the code": no test names, no internal
function names, no internal constants. The reader leaves with the mechanism and
the reasons; the Development pages hold the code map.

### Before merging a deep dive

1. Ask someone who did not write the page to read it cold and mark where they
   got lost.
2. Check every claim against the current code, and follow every link.
3. Run `npm run docs:build` and `make verify-self-contained`.

## Go further

- [Source-reading guide](/development/source-reading-guide) - choose evidence before editing.
- [Documentation home](/) - follow the reader paths through the site.
