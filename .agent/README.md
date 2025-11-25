# .agent Directory

This directory contains documentation and guidelines for AI coding agents (like Antigravity) working on the Helio project.

## Contents

- **[cpp_guidelines.md](cpp_guidelines.md)**: Comprehensive C++ coding guidelines for the Helio project
  - Code style and formatting rules
  - Fiber-safe programming patterns
  - Error handling conventions
  - Memory management best practices
  - Logging and debugging guidelines
  - Testing conventions
  - Build system usage

## Purpose

These guidelines help ensure that AI-generated code:
- Follows project conventions and best practices
- Is safe to use with the custom fiber library (fb2)
- Uses appropriate error handling patterns
- Maintains consistency with the existing codebase
- Passes code review standards

## For Developers

While these guidelines are primarily for AI agents, they serve as a useful reference for human developers as well. They complement the existing:
- [.clang-format](../.clang-format) - Code formatting rules
- [README.md](../README.md) - Project overview

## Updating Guidelines

When project conventions change, update the relevant guideline documents to keep AI agents aligned with current best practices.
