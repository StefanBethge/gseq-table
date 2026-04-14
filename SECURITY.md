# Security Policy

## Supported Versions

Security fixes are intended for the current `v1.x` line.

Older versions may not receive fixes.

## Reporting a Vulnerability

If you believe you found a security issue in `gseq-table`, please do not open a public issue first.

Preferred options:

- Open a private GitHub security advisory for this repository
- Contact the maintainer directly if you already have a trusted contact path

Please include, if possible:

- affected package or module
- affected version or commit
- reproduction steps or proof of concept
- impact assessment
- any suggested mitigation

## Response Expectations

Initial acknowledgement is targeted within a few days.

After triage:

- valid reports will be investigated and, when appropriate, fixed in the supported version line
- coordinated disclosure is preferred
- reports that are not security issues may be redirected to the normal issue tracker

## Scope

This repository is a Go library, not a hosted service.

Relevant reports typically include issues such as:

- unsafe parsing behavior with security impact
- data exposure across rows or tables
- unintended execution paths caused by untrusted input
- dependency-related security issues in shipped modules

Non-security bugs, API design concerns, and feature requests should go through the normal issue tracker.
