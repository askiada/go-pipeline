# Example Timing Diagrams

These diagrams are approximate and focus on relative timing and behavior. Ordering can change when steps run concurrently.

## quick-start
```
time ->
source:  [0] [1] [2]
step:    [0->item-0] [1->item-1] [2->item-2]
sink:    print in order
```
Behavior: sequential transform (Root -> OneToOne -> Sink).

## one-to-many
```
time ->
source:  [1] [2] [3]
expand:  [1]->(a,b) [2]->(a,b) [3]->(a,b)
sink:    prints 6 items (two outputs per input)
```
Behavior: each input expands into multiple outputs.

## from-chan
```
time ->
source:  [0][1][2][3][4]
format:  stream in  ---->  stream out
sink:    prints records as they arrive
```
Behavior: the step function controls channel reads/writes directly.

## sink
```
time ->
source:  [alpha][beta][gamma]
sink:    saved: alpha, saved: beta, saved: gamma
```
Behavior: sink consumes items and performs side effects.

## sink-from-chan
```
time ->
source:  [0][1][2][3][4][5]
collect: read all -> sort -> print once
```
Behavior: sink drains the channel, then emits a single summary result.

## splitter-merger
```
time ->
source: [0][1][2][3][4]
step-1:  [1][2][3][4][5]
             /---- left:  x10 ----\
split:      /                     \
           \---- right: x100 ----/
merge: interleaves left/right outputs
```
Behavior: each item is duplicated to two branches, then merged.

## splitby-routing
```
time ->
source: [1][2][3][4][5][6]
route:  even?  odd?  mul3?
branch: even: [2][4][6]
branch: odd:  [1][3][5]
branch: mul3: [3][6]
merge: interleaves branch outputs
```
Behavior: items can be routed to multiple branches when multiple predicates match.

## split-merge-metrics
```
time ->
source: [1][2][3][4]
prep:   [5ms each]
            /-- left: 8ms --\
fan-out:   |-- mid:  4ms --|
            \-- right:2ms --/
merge: interleaves based on branch speed
```
Behavior: slower branches delay their own outputs; merge emits as results arrive.

## backpressure-buffering
```
time ->
source: [0][1][2] --- blocked ---
left:   [0]........[1]........[2]   (slow 40ms)
right:  [0]..[1]..[2]               (fast 5ms, but waits on split)
```
Behavior: split duplicates each item; the slow branch and small buffer push back on the source and the fast branch.

## concurrency-aggregate
```
time ->
source:  [job1][job2][job3][job4][job5][job6]
process:
  w1:    [job1]   [job4]
  w2:       [job2]   [job5]
  w3:          [job3]   [job6]
collect: drain all -> sort -> print
```
Behavior: concurrent processing, then a sink collects and prints ordered results once complete.

## metrics-drawer
```
time ->
source: [0][1][2][3][4]
step-1: [15ms each]
step-2: [5ms each]
sink:   [10ms each]
```
Behavior: sequential timing showcases how metrics are captured for each step.

## pipeline-defaults
```
time ->
source: [0][1][2][3]
step:   [double] (defaults: concurrency 2, buffer 2)
split:  left + right (defaults)
right:  concurrency overridden to 1
merge:  interleaves
```
Behavior: defaults apply to all steps unless a step overrides them.

## dry-run
```
time ->
build:  validate steps + links
runs:   (skipped)
drawer: graph emitted (no metrics)
```
Behavior: validates wiring and emits a diagram without executing step functions.

## step-options
```
time ->
source: goroutine emits [0..4] into buffered channel
work (concurrency 2):
  w1:   [task-0]   [task-2]   [task-4]
  w2:      [task-1]   [task-3]
sink: print as tasks finish
```
Behavior: buffered root and concurrent work step smooth throughput.

## retry
```
time ->
item-1: attempt-1 (fail) -> attempt-2 (ok)
item-2: attempt-1 (fail) -> attempt-2 (ok)
```
Behavior: sink retries each item once, then succeeds.

## batching
```
time ->
source: [1][2] ----wait---- [3][4]
batch:  [1,2] (flush by MaxWait)  [3,4] (flush on close)
```
Behavior: window flushes partial batches even when MaxSize is large.

## batching-chan
```
time ->
source: [1][2] ----wait---- [3][4]
batch:  chan{1,2} then chan{3,4}
sink:   drains each batch channel
```
Behavior: batches are streamed via channels to reduce memory usage.

## rate-limit
```
time ->
source: [0][1][2][3][4]
limit:  [0]--30ms--[1]--30ms--[2]--30ms--[3]--30ms--[4]
```
Behavior: shared rate limit caps throughput even with higher concurrency.

## max-inflight
```
time ->
source: [0][1][2][3]
in-flight (max 2):
  active: [0][1] then [2][3]
```
Behavior: only two items run the step function at once, even with more workers.

## timeout-retry
```
time ->
item-1: attempt-1 (40ms fail) -> attempt-2 (20ms ok)
item-2: attempt-1 (40ms fail) -> attempt-2 (20ms ok)
```
Behavior: timeout applies across all attempts for each item.

## step-limits
```
time ->
source: [0][1][2][3][4]
rate:   [0]--20ms--[1]--20ms--[2]--20ms--[3]--20ms--[4]
inflight: only one active compute at a time (max 1)
```
Behavior: rate limit spaces inputs and max in-flight caps active work; concurrency allows output handoff overlap.
