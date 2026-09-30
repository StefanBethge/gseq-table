# gseq-table v2

**Profil:** voll · **Freeze:** 2026-09-27 (3add20d) — ab hier append-only

Design-Set für v2 von gseq-table: eine ETL-Library für Go, die für stabile Läufe über
schmutzige Daten gebaut ist. Läufe kommen durch. Nicht verarbeitbare Datensätze werden
aussortiert und bleiben im Originalzustand nachvollziehbar. Datenmengen im zweistelligen
GB-Bereich lassen sich verarbeiten, ohne vollständig im RAM zu liegen.

Erstes Ziel ist ein Prototyp, der die offenen Grundsatzfragen durch Messung und
Erprobung klärt, bevor ein v2.0-Release geplant wird.

| Dokument | Inhalt | IDs |
|---|---|---|
| [Use Cases](05-use-cases.md) | Akteure und Abläufe, und die Fragen, die sie an das Design stellen | [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung) – [UC8](05-use-cases.md#uc8-eine-lieferung-besteht-aus-mehreren-dateien-oder-sheets) |
| [Design Decisions](10-design-decisions.md) | Nummerierte, begründete Entscheidungen | [D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten) – [D113](10-design-decisions.md#d113-nach-66-bleiben-g5-und-g13-offen-und-d7-wird-erst-nach-einem-folgeslice-fur-textspalten-und-kopien-wieder-aufgemacht) |
| [Feature-Katalog](20-feature-catalogue.md) | Features, jedes auf seine Decisions gemappt | [F1](20-feature-catalogue.md#f1-blocke-aus-typisierten-spalten-mit-nullwerten) – [F24](20-feature-catalogue.md#f24-docs-gates-fur-das-design-set) |
| [Test-Plan](30-test-plan.md) | T-Fälle, jeder an eine D- oder F-Eigenschaft gebunden | [T1](30-test-plan.md#t1-aussortierte-zeilen-tragen-den-rohzustand-nicht-den-arbeitszustand) – [T76](30-test-plan.md#t76-sortieren-und-join-sammeln-jede-zelle-einmal-aus-den-blocken-ihrer-eingabe) |
| [Scope Prototyp](40-scope-prototype.md) | Selektion, Vereinfachungen und Exit-Kriterien des Prototyps | [P1](40-scope-prototype.md#p1-auslagern-nur-fur-sortieren-und-gruppieren) – [P8](40-scope-prototype.md#p8-keine-formelmaskierung-im-csv-writer) |
| [Failure Modes](50-failure-modes.md) | Was bei einem Lauf schiefgehen kann und wie reagiert wird | — |
| [Security Boundaries](60-security-boundaries.md) | Grenzen für nicht vertrauenswürdige Lieferungen und ausgelagerte Daten | — |
| [Gap Ledger](70-gap-ledger.md) | Offene Fragen, Annahmen und Inkonsistenzen | [G1](70-gap-ledger.md#g1-was-der-rohzustand-einer-zeile-umfasst) – [G77](70-gap-ledger.md#g77-eine-textspalte-fasst-in-einem-block-hochstens-4-gib) |
