# leafrun-platform

## Small shared service tools

This planned library will hold technical helpers that multiple services need: authorization clients, message delivery bookkeeping, idempotent consumers, and telemetry. It should keep repeated transport code out of each domain.

Access still owns permissions; each service still owns its data and business rules. Platform packages must not turn into a shared domain database.

**Status:** this repository is a planned component. It has no runnable implementation yet. The working local renderer is in [leafrun-generation](https://github.com/leafrun/leafrun-generation).

**First roadmap step:** [LEA-25 — Make server jobs reliable](https://linear.app/bulnik/issue/LEA-25/e04-nadyozhnyj-asinhronnyj-servernyj-render). Linear may require workspace access; the public [roadmap](https://github.com/leafrun/leafrun/blob/main/docs/roadmap.md) explains the build order. See the [repository map](https://github.com/leafrun/leafrun/blob/main/docs/repositories.md) and [contribution guide](https://github.com/leafrun/leafrun/blob/main/CONTRIBUTING.md).

No project license has been selected yet.
