# Prefect Examples

Progressive Prefect 3 examples -- from hello-world to production patterns.

## What you will learn

This project walks through 113 self-contained flows that cover the full
spectrum of Prefect 3 capabilities:

- **Basics** -- flows, tasks, parameters, return values
- **Control flow** -- branching, state hooks, parameterisation
- **Composition** -- subflows, dynamic task mapping
- **Operational** -- polling, retries, error handling, caching
- **Reuse and events** -- shared task libraries, custom events
- **Data processing** -- file I/O, quality rules, incremental processing
- **Analytics** -- correlation, regression, dimensional modeling
- **DHIS2 integration** -- custom credentials block, API client, metadata and
  analytics pipelines
- **Deployments** -- `flow.serve()`, `flow.deploy()`, `prefect.yaml`, work pools
- **Advanced** -- concurrency limits, variables, transactions, async patterns,
  circuit breakers, and end-to-end pipelines

Every example notes its Airflow equivalent so you can map existing knowledge
directly.

## Navigate

| Page | Description |
|------|-------------|
| [Getting Started](getting-started.md) | Prerequisites, installation, first run |
| [Tutorials](tutorials.md) | Step-by-step walkthroughs for common tasks |
| [Core Concepts](core-concepts.md) | Prefect fundamentals and Airflow comparison |
| [Flow Reference](flow-reference.md) | Detailed walkthrough of all 113 flows |
| [Patterns](patterns.md) | Common patterns and best practices |
| [Feature Coverage](feature-coverage.md) | Prefect 3 feature coverage matrix and gaps |
| [Infrastructure](infrastructure.md) | Docker Compose stack and local dev setup |
| [CLI Reference](cli-reference.md) | Prefect CLI commands used in this project |
| [API Reference](api-reference.md) | Auto-generated docs for `Dhis2Client`, `Dhis2Credentials` |
| [Testing](testing.md) | How to test Prefect flows |

## Links

- [Prefect documentation](https://docs.prefect.io)
- [Prefect GitHub](https://github.com/PrefectHQ/prefect)
- [Source repository](https://github.com/mortenoh/prefect-examples)
