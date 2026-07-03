# Reflection

**Which fault types were hardest to catch, and why?**

Lineage structural faults — specifically `missing_upstream` and `orphan_output` where the
actual upstream/downstream values are non-zero/non-empty but still incorrect. The toolkit
only reveals what *is* in the graph, not what *should* be, so detecting these requires
running statistics that get contaminated by the very faults they're trying to catch.
Feature skew at `subtle` tier was also tricky — the mean-shift sigma sits within 2σ of
clean variance, so a single static threshold can't distinguish it from normal noise.

**What would you change about your cost/coverage tradeoff, if you had another pass?**

I'd invest in a rolling-window baseline that resists contamination — for example, storing
per-pillar running medians and updating only when the current event is *not* flagged as
faulty. That would let lineage structural anomalies be caught as outliers relative to a
clean-only recent history. I'd also apply a dynamic threshold that widens with fewer
samples and narrows as confidence grows, instead of today's fixed multipliers on the
static baselines.
