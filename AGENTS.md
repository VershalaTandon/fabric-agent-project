# Fabric Agent Project

## Project

This repository implements a Microsoft Fabric Data Engineering
project using a Bronze → Silver → Gold architecture.

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

1. Read the GitHub issue before implementing anything.
2. Follow all acceptance criteria.
3. Do not modify unrelated files.
4. Use PySpark for Spark transformations.
5. Use Delta tables for Lakehouse tables.
6. Implement idempotent transformations.
7. Add tests for transformations.
8. Do not hard-code credentials.
9. Do not commit secrets.
10. Update documentation when required.

## Pull Requests

Every PR must contain:

- Summary
- Requirements implemented
- Files changed
- Tests executed
- Fabric artifacts changed
- Acceptance criteria checklist