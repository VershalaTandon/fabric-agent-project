This is particularly important for your **three-agent requirement**.

# Agent Workflow

## Overview

The project uses three AI agents:

1. Developer Agent
2. QA Agent
3. ChatBot Agent

## Developer Agent

The Developer Agent works on GitHub Issues assigned to it.

Workflow:

```text
GitHub Issue
     |
     v
In Progress
     |
     v
Developer Agent
     |
     v
Read Requirements
     |
     v
Inspect Repository
     |
     v
Implement
     |
     v
Run Tests
     |
     v
Create Pull Request