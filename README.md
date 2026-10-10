# leafrun-platform

Planned shared technical packages for authorization clients, reliable messaging and telemetry.

**Status: planned component.** This repository currently contains its scope and roadmap links. It does not yet provide a runnable service or package. The existing local rendering prototype lives in [leafrun-generation](https://github.com/leafrun/leafrun-generation).

## Responsibility

Versioned technical packages: auth client, outbox/inbox, idempotency and telemetry; no central business rules.

## Boundaries

This is a versioned library, not a separately deployed service. Consumers pin compatible versions; domain rules and database ownership remain with their owning services.

## Roadmap

- [LEA-25 — Reliable asynchronous server rendering](https://linear.app/bulnik/issue/LEA-25/e04-nadyozhnyj-asinhronnyj-servernyj-render)
- [LEA-41 — Users, organizations, roles and bots](https://linear.app/bulnik/issue/LEA-41/e06-polzovateli-organizacii-roli-i-boty)
- [LEA-114 — Caching, operations and scaling](https://linear.app/bulnik/issue/LEA-114/e15-keshirovanie-ekspluataciya-i-masshtabirovanie)

See the [development roadmap](https://github.com/leafrun/leafrun/blob/main/docs/roadmap.md) for dependencies and acceptance milestones. Creating this repository does not complete its implementation issues.

## Contributing

Use English for code comments, documentation, examples, commit messages and pull requests. See the [contribution guide](https://github.com/leafrun/leafrun/blob/main/CONTRIBUTING.md) and [current architecture](https://github.com/leafrun/leafrun/blob/main/docs/architecture.md).

## License status

A project license has not been selected. No license is assigned by this repository scaffold.
