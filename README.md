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