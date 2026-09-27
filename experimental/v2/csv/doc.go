// Package csv reads and writes CSV deliveries for the gseq-table v2 prototype.
//
// The reader delivers blocks of raw text columns and records the raw state,
// location (file, line, byte offset) and record_key of every row; lines that
// cannot be split into cells are rejected with their raw bytes (design
// decisions D10, D18, D61). The writer implements the Sink interface of the
// core package gtable (D35).
//
// EXPERIMENTAL: no stability guarantee (D34).
package csv
