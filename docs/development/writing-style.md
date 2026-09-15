# Writing style

How the F1 docs are written: which voice each section uses, the rules every
page follows, and the shape of a deep-dive page. Read it before adding or
rewriting a page under `docs/`.

## One voice per section

| Section | Directories | Voice |
| --- | --- | --- |
| Guide | `learn/`, `user-guide/` | Second person, task first: "Create a subscription, then call `Run`." |
| Concepts | `basics/`, `advanced-topics/` | Neutral explanation of what F1 does and why an application cares. |
| Internals | `development/`, `runtime-overview.md`, `drivers-and-capabilities.md` | Neutral and exact. Reference for maintainers and driver authors. |
| Deep dives | `deep-dives/` | First person singular. How a part works inside, the reasons behind it, and what broke on the way. |

Keep the voice of the section a page lives in. A Guide page that drifts into
design history belongs in a deep dive; a deep dive that turns into option
tables belongs in Internals.

## Rules for every page

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

### Keep sentences uneven

Real prose alternates short and long sentences.
Three items in a list only when there are three things.

### Headings in sentence case

No emoji, no decoration, no heading that repeats
the page title.

### Bold is rare

Bold a term the reader must not miss. Do not give every
bullet a bold label.

### Link claims to code

Point at the file or test that shows the behaviour,
using the full repository URL form the other pages use:
`https://fgit.zapps.vn/zatf2026-be-t3/event-driven-messaging-sdk/-/blob/main/<path>`.

### No unresolvable citations

`make verify-self-contained` rejects decision
and task identifiers a reader cannot follow. State the rule instead.

### Diagrams earn their place

Use Mermaid when the reader needs to see an
order of events or a structure that prose makes hard to hold. Keep labels
short; one diagram per idea.

### Code examples compile

Every example builds against the current API. When the API changes, the
example changes in the same commit.

## Deep dives

A deep dive teaches how one mechanism works and why, like an engineering blog
post from a team that builds a platform. The reader is an outside engineer who
may never use F1: they know Go and roughly what a message broker does, but not
F1 itself. The page does not replace the reference: it links to Concepts or
Internals for what the feature does, and spends its length on how and why.

### Voice

- Write as "we", the team voice. The author is still named at the top of the
  page, in the header `*By <author>, <Month YYYY>.*`.
- Opinions are welcome when they read as opinions: "We think this is the
  weakest part of the design."
- State uncertainty plainly: "We have not measured this under load."
- Prefer the concrete case: real numbers, a real sequence of events, a real
  commit. A race is explained by its interleaving, which goroutine is where
  and what the other one does in between, as `CLAUDE.md` asks of code
  comments.
- History appears only when it is real. A design that broke is part of the
  story only when a commit or a red test shows it; cite that commit or test.
  There is no fixed history section and no retrospective of alternatives.

### Accuracy

- Every behavioural claim links to the source or test that shows it.
- Broker behaviour is measured against the local fixture, not taken from
  broker or client documentation. Say when a claim was measured.
- Pages are dated and frozen. They describe the code as it was when written
  and carry no commit sha. A later change adds an `Update (<YYYY-MM-DD>):`
  note, or becomes a new post.
- Numbers follow the benchmark rule: cite an existing test, benchmark, or
  `make bench`, and give the reproduce command, the machine (CPU model,
  cores, Go version), and the date beside each number. A number that would
  need a new harness is a finding, not an edit.

### Page template

The headings are a guide. Rename or drop them when the story needs a
different order.

```md
# <A claim or a problem, not a component name>

*By <author>, <Month YYYY>.*

<Lede: one concrete scenario that goes wrong without this mechanism.>

## Background

<Just enough broker detail for a Go engineer.>

## How it works

<One worked example end to end, one diagram.>

## Measured

<Numbers, how to reproduce, machine, date.>

## Limits and trade-offs

## Read the code

- [`path/to/file.go`](<repository URL>) - what to look for there
- [`path/to/file_test.go`](<repository URL>) - the test that pins the behaviour
```

Add the page to the "Deep dives" sidebar group in
`docs/.vitepress/config.mts`.

### Before merging a deep dive

1. Read the draft against Wikipedia's
   [Signs of AI writing](https://en.wikipedia.org/wiki/Wikipedia:Signs_of_AI_writing)
   list and rewrite what matches. The history exception above still applies.
2. Ask someone who did not write the page to read it cold and mark where they
   got lost.
3. Follow every source link and check it still shows what the page says.
4. Run `npm run docs:build` and `make verify-self-contained`.
