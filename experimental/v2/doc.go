// Package gtable is the prototype of gseq-table v2: an ETL library for stable
// runs over dirty deliveries. Runs get through, rows that cannot be processed
// are rejected and stay traceable in their original state, and deliveries in
// the tens of gigabytes are processed without being held in memory at once.
//
// # Stability
//
// This module is EXPERIMENTAL and has no stability guarantee (design decision
// D34). Its API may change or disappear in any commit. v2.0.0 will be released
// as its own module with the path /v2 and follow SemVer.
//
// # Design
//
// The authoritative design is the frozen design set in
// docs/explanation/design/v2 at the root of the repository. The code
// implements what is decided there; the prototype scope is
// 40-scope-prototype.md.
//
// # Layout
//
// This package holds the public core types (Table, Column, Expr, Op,
// Pipeline, Result, Sink). Readers and writers for file formats live in the
// sub-packages csv and excel. Engine internals live under internal/
// (decision D67).
package gtable
