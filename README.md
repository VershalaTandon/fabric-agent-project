# Sales Analytics Lakehouse - Microsoft Fabric

An end-to-end Microsoft Fabric Data Engineering project demonstrating Bronze → Silver → Gold architecture, automated testing, GitHub-based development, and AI-assisted development and QA.

## Project Overview

This project implements a daily sales data pipeline using Microsoft Fabric.

Raw sales data is ingested into a Fabric Lakehouse and transformed through three layers:

```text
Source CSV
    |
    v
Bronze - Raw Sales
    |
    v
Silver - Clean Sales
    |
    v
Gold - Daily Sales Summary
    |
    v
Analytics / ChatBot
```

## Architecture
```text
GitHub Issue
     │
     ▼
Backlog
     │
     ▼
In Progress
     │
     ▼
Developer Agent
     │
     ├── Read issue
     ├── Use Fabric Skills
     ├── Write code
     ├── Create Fabric artifacts
     ├── Run tests
     └── Create PR
             │
             ▼
       Human PR Review
             │
             ▼
            QA
             │
             ▼
         QA Agent
             │
        ┌────┴────┐
        │         │
       FAIL      PASS
        │         │
        ▼         ▼
    Developer    Approve
        │         │
        └──► PR ◄─┘
                │
                ▼
              Merge
                │
                ▼
          Fabric Workspace
                │
                ▼
           Data Agent
                │
                ▼
             ChatBot
```

## Ticket Lifecycle
```text
Backlog
   |
   v
In Progress
   |
   v
PR Review
   |
   v
QA
   |
   +----> Fix Required
   |          |
   |          v
   |      In Progress
   |
   v
Done
```

## Build
```text
Developer Agent
       +
QA Agent
       +
GitHub Issues
       +
GitHub Projects
       +
GitHub PR
       +
Fabric Lakehouse
       +
Fabric Pipeline
       +
PySpark
       +
Delta
```

## Complete ticket lifecycle
```text
                    GitHub Issue
                         │
                         ▼
                    BACKLOG
                         │
                    Start work
                         │
                         ▼
                  IN PROGRESS
                         │
                         ▼
             ┌──────────────────────┐
             │ Fabric Developer     │
             │ Custom Agent         │
             └──────────┬───────────┘
                        │
              Fabric Skills
                        │
                        ▼
                Fabric implementation
                        │
                        ▼
                     Tests
                        │
                        ▼
                   Pull Request
                        │
                        ▼
                  PR REVIEW
                        │
                 HUMAN REVIEW
                        │
                        ▼
                       QA
                        │
                        ▼
                ┌───────────────┐
                │ Fabric QA     │
                │ Agent         │
                └───────┬───────┘
                        │
             ┌──────────┴──────────┐
             │                     │
           FAIL                   PASS
             │                     │
             ▼                     ▼
        PR comments             APPROVE
             │                     │
             ▼                     ▼
       Developer fixes           MERGE
                                   │
                                   ▼
                              Fabric TEST
                                   │
                                   ▼
                              Fabric PROD

                                   +
                                   │
                                   ▼
                            Fabric Data Agent
                                   │
                                   ▼
                              ChatBot
```

# Agent Architecture

## Developer Agent

The Developer Agent:
1. Reads the GitHub issue.
2. Reads acceptance criteria.
3. Inspects the repository.
4. Uses Microsoft Fabric Skills.
5. Implements the requested Fabric changes.
6. Adds tests.
7. Runs tests.
8. Creates a Pull Request.

Development Principles:
1. Use Bronze → Silver → Gold architecture.
2. Use Delta tables for Lakehouse data.
3. Use PySpark for transformations.
4. Keep transformations idempotent.
5. Validate input data.
6. Do not hard-code credentials.
7. Never commit secrets.
8. Add tests for transformation logic.
9. Verify acceptance criteria before creating a Pull Request.

## QA Agent

The QA Agent:
1. Reads the original issue.
2. Reads acceptance criteria.
3. Reviews the Pull Request.
4. Checks changed files.
5. Runs tests.
6. Validates data transformation logic.
7. Checks Fabric-specific requirements.
8. Reports PASS/FAIL for each acceptance criterion.

## ChatBot Agent

The ChatBot will provide natural-language access to the sales analytics data.