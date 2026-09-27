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
| [Design Decisions](10-design-decisions.md) | Nummerierte, begründete Entscheidungen | [D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten) – [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash) |
| [Gap Ledger](70-gap-ledger.md) | Offene Fragen, Annahmen und Inkonsistenzen | [G1](70-gap-ledger.md#g1-was-der-rohzustand-einer-zeile-umfasst) – [G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren) |

Die Dokumente entstehen in der Reihenfolge der Methode (Use Cases zuerst) und werden
erst in diese Tabelle aufgenommen, wenn sie Inhalt haben.
