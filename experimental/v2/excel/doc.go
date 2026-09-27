// Package excel reads and writes Excel deliveries for the gseq-table v2
// prototype.
//
// Every sheet is its own source (design decision D53). Raw state and working
// column carry the stored cell value in a fixed, locale-independent text form;
// rejected rows also carry the displayed text and the cell address (D52, D62).
// The writer implements the Sink interface of the core package gtable (D35).
//
// EXPERIMENTAL: no stability guarantee (D34).
package excel
