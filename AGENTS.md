# Fabric Agent Project

## Project

This repository implements a Microsoft Fabric Data Engineering project using a Bronze → Silver → Gold architecture.

## Fabric Skills

Use the Microsoft Fabric Skills installed in the Copilot environment for Fabric-specific implementation.

Do not recreate or copy Microsoft Fabric Skills into this repository.

## Technology

- Microsoft Fabric
- OneLake
- Lakehouse
- Delta Lake
- PySpark
- Fabric Pipelines
- GitHub
- GitHub Copilot

## Architecture

Bronze:
Raw source data.

Silver:
Validated and cleaned data.

Gold:
Business-ready aggregated data.

## Development Rules

1. Read the complete GitHub issue before implementation.
2. Follow every acceptance criterion.
3. Inspect existing code before modifying it.
4. Use Fabric Skills for Fabric-specific implementation.
5. Use PySpark for Spark transformations.
6. Use Delta tables for Lakehouse tables.
7. Implement idempotent transformations.
8. Add tests.
9. Never hard-code credentials.
10. Never commit secrets.
11. Run tests before creating a PR.
12. Create a PR only after all acceptance criteria have been verified.

## Pull Requests

Every PR must contain:

- Summary
- Requirements implemented
- Files changed
- Tests executed
- Fabric artifacts changed
- Acceptance criteria checklist