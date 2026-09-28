# Numeric contract

DriftBench separates computational numbers from human-facing presentation.
Sampling weights, statistical inputs, semantic hashes, cache fingerprints,
benchmark-standard files, SQL literals, currency fields, and timestamps keep
their domain-defined precision. They are never rounded merely for appearance.

Notebook, console, gallery, and other human-facing summaries use at most six
fractional decimal places, trim trailing zeros, turn negative zero into zero,
and use compact scientific notation for tiny non-zero values. Probability maps
are formatted as a group so every displayed value remains between zero and one
and their displayed decimal total remains exactly one. In such a group, a
positive probability smaller than one display quantum can appear as `0.0`;
the canonical probability remains unchanged.

Numeric types follow field meaning rather than appearance:

- counts, rows, indices, ordinals, byte sizes, seeds, and sample sizes are
  strict integers; booleans and integral floats are rejected;
- probabilities, ratios, frequencies, durations, latency, rates, and other
  continuous measurements remain real-valued fields, including values such as
  `0.0` and `1.0`;
- public numbers must be finite;
- counts and byte sizes are non-negative, required sizes and durations are
  positive, ratios are in `[0, 1]`, and percentages are in `[0, 100]`.

Producers own field-specific validation. Presentation never clips or silently
repairs an invalid value. The shared implementation is
`driftbench.numeric_contract`; `present_*` and `format_number` functions are for
display copies only and must not feed computation or canonical serialization.
