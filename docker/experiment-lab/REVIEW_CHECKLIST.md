# Human review checklist

Approval is deliberately outside this change. Reviewers should record answers
without editing `review_only` in place.

## Scope and provenance

- [ ] The experiment ID maps to the intended component in `experiments/`.
- [ ] Each GitHub commit and license is acceptable.
- [ ] Dataset terms permit the intended local/devbox use.
- [ ] Source archive and dataset SHA-256 values are supplied after an explicit
      future prepare step.
- [ ] The workload, seed, sample, warmup, repetitions, and metrics match the
      research question.

## Resource and lifecycle safety

- [ ] The local stack remains at or below 2 CPU and 4 GiB RAM.
- [ ] A devbox-only experiment is not relabelled local-lite.
- [ ] Only one experiment ID can be selected.
- [ ] Every service uses `restart: "no"`, bounded logs, PID limits, and pinned
      images.
- [ ] Host ports bind to `127.0.0.1`; the Docker socket is never mounted.
- [ ] Success, error, and Ctrl-C all lead to `down --remove-orphans`.
- [ ] `stop` preserves disk state, while `destroy` requires explicit run-ID
      confirmation.

## Evidence and observability

- [ ] Runtime state is outside the repository.
- [ ] `run-manifest.json`, raw logs, metrics, and checksums are preserved.
- [ ] Grafana is treated as optional visualization, not the source of truth.
- [ ] PNG export targets only the selected run's `figures/` directory.
- [ ] Runtime Grafana/renderer behavior is validated before being called ready.

## Approval boundary

After this checklist is accepted, create a new implementation slice. That slice
must supply verified source/data hashes, completed runner images, lifecycle
tests, and the three repository gates. Merely changing `review_only: true` to
`false` is insufficient and is rejected by the current launcher.
