# gseq-table v2

**Profil:** voll · **Freeze:** noch nicht

Design-Set für v2 von gseq-table: eine ETL-Library für Go, die für stabile Läufe über
schmutzige Daten gebaut ist. Läufe kommen durch. Nicht verarbeitbare Datensätze werden
aussortiert und bleiben im Originalzustand nachvollziehbar. Datenmengen im zweistelligen
GB-Bereich lassen sich verarbeiten, ohne vollständig im RAM zu liegen.

Erstes Ziel ist ein Prototyp, der die offenen Grundsatzfragen durch Messung und
Erprobung klärt, bevor ein v2.0-Release geplant wird.

| Dokument | Inhalt | IDs |
|---|---|---|
| [Use Cases](05-use-cases.md) | Akteure und Abläufe, und die Fragen, die sie an das Design stellen | [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung) – [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline) |
| [Design Decisions](10-design-decisions.md) | Nummerierte, begründete Entscheidungen | [D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten) – [D36](10-design-decisions.md#d36-ein-adapter-wandelt-zwischen-v1-und-v2-tabellen) |
| [Gap Ledger](70-gap-ledger.md) | Offene Fragen, Annahmen und Inkonsistenzen | [G1](70-gap-ledger.md#g1-was-der-rohzustand-einer-zeile-umfasst) – [G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blockgrosse) |

Die Dokumente entstehen in der Reihenfolge der Methode (Use Cases zuerst) und werden
erst in diese Tabelle aufgenommen, wenn sie Inhalt haben.
