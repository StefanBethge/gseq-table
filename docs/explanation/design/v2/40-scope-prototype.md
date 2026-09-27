# Scope Prototyp

Der Prototyp soll die Grundsatzfragen klären, die sich nur durch Messung und Erprobung an
echten Lieferungen beantworten lassen:
[G5](70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht),
[G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren),
[G12](70-gap-ledger.md#g12-ab-wann-eine-haufung-von-fehlern-als-formatanderung-gilt) und
[G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange).
Er liegt unter `experimental/v2` ohne Stabilitätszusage
([D34](10-design-decisions.md#d34-der-prototyp-liegt-unter-experimentalv2-ohne-zusage-v200-ist-ein-eigenes-modul-mit-semver)).

## Selektion

| Feature | Begründung |
|---|---|
| [F1](20-feature-catalogue.md#f1-blocke-aus-typisierten-spalten-mit-nullwerten) | Grundlage der Messung für [G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange) |
| [F2](20-feature-catalogue.md#f2-plan-und-blockweise-ausfuhrung) | Grundlage für Streaming und vorab geprüfte Pläne |
| [F3](20-feature-catalogue.md#f3-speicherbudget-und-auslagern) | Nachweis, dass große Lieferungen mit begrenztem Speicher durchkommen, eingeschränkt nach [P1](#p1-auslagern-nur-fur-sortieren-und-gruppieren) |
| [F4](20-feature-catalogue.md#f4-kopieren-oder-andern-an-ort-und-stelle) | Grundlage der Messung für [G5](70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht) |
| [F5](20-feature-catalogue.md#f5-operationen-als-werte-sofort-auf-tabellen-oder-im-plan) | eingeschränkt nach [P2](#p2-teilmenge-der-operationen) |
| [F6](20-feature-catalogue.md#f6-ausdrucke) | eingeschränkt nach [P2](#p2-teilmenge-der-operationen) |
| [F7](20-feature-catalogue.md#f7-operationen-mit-eigener-logik) | Sonderlogik echter Kunden-Pipelines |
| [F8](20-feature-catalogue.md#f8-reader-mit-fundstelle-und-rohzustand) | eingeschränkt nach [P3](#p3-reader-fur-csv-und-excel) |
| [F9](20-feature-catalogue.md#f9-erwarteter-aufbau-und-prufung-des-kopfs) | Erprobung von [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt) an echten Lieferungen |
| [F10](20-feature-catalogue.md#f10-aussortierte-zeilen-als-quelle) | Erprobung von [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet) |
| [F11](20-feature-catalogue.md#f11-aussortierte-zeilen) | Kern des Fehlermodells, Validierung nach [G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren) |
| [F12](20-feature-catalogue.md#f12-herkunft-uber-joins-und-aggregationen) | Herkunft ist ohne Joins und Gruppierung nicht erprobt |
| [F13](20-feature-catalogue.md#f13-fehlerverhalten) | Kern des Fehlermodells |
| [F14](20-feature-catalogue.md#f14-schwelle) | Kern des Fehlermodells |
| [F15](20-feature-catalogue.md#f15-fehlerzweige-split-und-merge) | Erprobung von [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig) |
| [F16](20-feature-catalogue.md#f16-status-zahlungen-trace-und-exit-code) | Signal für den Scheduler |
| [F17](20-feature-catalogue.md#f17-anderungsbericht) | Erprobung von [G12](70-gap-ledger.md#g12-ab-wann-eine-haufung-von-fehlern-als-formatanderung-gilt) |
| [F19](20-feature-catalogue.md#f19-sink-schnittstelle-und-datei-writer) | eingeschränkt nach [P4](#p4-datei-writer-fur-csv-und-excel) |
| [F21](20-feature-catalogue.md#f21-offentliche-api-ohne-gseq-typen) | Die API-Form soll schon im Prototyp gelten, damit die Erprobung aussagekräftig ist |
| [F23](20-feature-catalogue.md#f23-auslieferung-als-experimenteller-prototyp-und-als-v2-modul) | nur der Teil `experimental/v2` |

Nicht im Prototyp:
[F18](20-feature-catalogue.md#f18-profil-und-vergleich-mit-fruheren-laufen) (laut
[D24](10-design-decisions.md#d24-ein-lauf-kann-ein-profil-liefern-das-mit-dem-profil-eines-fruheren-laufs-verglichen-wird)),
[F20](20-feature-catalogue.md#f20-writer-fur-datenbank-und-http) (laut
[D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http))
und [F22](20-feature-catalogue.md#f22-v1-adapter) (laut
[D36](10-design-decisions.md#d36-ein-adapter-wandelt-zwischen-v1-und-v2-tabellen)).

## Lokale Vereinfachungen

### P1 — Auslagern nur für Sortieren und Gruppieren

Der Prototyp lagert beim Sortieren und Gruppieren aus. Bei Joins muss die rechte Seite in
den Speicher passen, sonst scheitert der Lauf mit einer klaren Meldung. Pivot lagert nicht
aus. Produktion: Alle Schritte über alle Zeilen lagern aus
([D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)).

### P2 — Teilmenge der Operationen

Der Prototyp enthält Auswahl, Umbenennen, Filter, abgeleitete Spalten, Umwandeln (auch mit
Datumsformat), Inner und Left Join, Gruppieren mit den v1-Aggregationen, Sortieren, `Split`
und `Merge` sowie die Ausdrucksfunktionen, die diese Operationen und die echten
Kunden-Pipelines brauchen. Produktion: alle Operationen und Helfer aus v1 als Operationen
und Ausdrücke
([D31](10-design-decisions.md#d31-jede-operation-gibt-es-einmal-als-wert-mit-zwei-einstiegen-sofort-auf-einer-tabelle-oder-im-plan),
[D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg)).

### P3 — Reader für CSV und Excel

Der Prototyp liest CSV und Excel, die beiden häufigsten Lieferformate. Produktion: dazu
JSON und NDJSON ([F8](20-feature-catalogue.md#f8-reader-mit-fundstelle-und-rohzustand)).

### P4 — Datei-Writer für CSV und Excel

Der Prototyp schreibt CSV und Excel. Produktion: dazu JSON/NDJSON und Markdown
([D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http)).

## Exit-Kriterien

- Die T-Fälle der selektierten Features sind grün:
  [T1](30-test-plan.md#t1-aussortierte-zeilen-tragen-den-rohzustand-nicht-den-arbeitszustand)–[T17](30-test-plan.md#t17-gehaufte-formatfehler-erscheinen-im-anderungsbericht-mit-beispielen)
  und [T19](30-test-plan.md#t19-ein-fehlerzweig-sieht-den-zustand-vor-dem-schritt-und-fuhrt-verarbeitetes-zuruck)–[T28](30-test-plan.md#t28-eine-closure-die-einen-fehler-meldet-sortiert-die-zeile-mit-codecustom-aus)
  sowie [T31](30-test-plan.md#t31-die-offentliche-api-enthalt-keine-gseq-typen), jeweils
  im Umfang von [P1](#p1-auslagern-nur-fur-sortieren-und-gruppieren)–[P4](#p4-datei-writer-fur-csv-und-excel).
- [G5](70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht) und
  [G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange)
  sind durch Benchmarks gegen v1 (`Table` und `MutableTable`) beantwortet: Laufzeit und
  Spitzenspeicher für Filter, abgeleitete Spalten, Umwandeln, Sortieren, Gruppieren und
  Join auf generierten Lieferungen verschiedener Größe. Die Ergebnisse stehen im Repo, und
  die Gaps sind geschlossen oder mit neuer Decision aufgelöst.
- Eine generierte CSV-Lieferung, die ein Vielfaches des Budgets groß ist, läuft mit
  Sortieren und Gruppieren durch ([T23](30-test-plan.md#t23-ein-lauf-uber-mehr-daten-als-das-budget-halt-das-budget-ein)).
  Größe, Budget und Laufzeit sind festgehalten. Die konkreten Werte legt der Maintainer
  vor Beginn fest ([G16](70-gap-ledger.md#g16-konkrete-werte-fur-die-exit-kriterien-des-prototyps)).
- Mindestens zwei echte Lieferungen von Anbietern (Excel) sind mit einer realen Pipeline
  verarbeitet. Der Maintainer hat die aussortierten Zeilen und den Änderungsbericht geprüft,
  und [G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren) sowie
  [G12](70-gap-ledger.md#g12-ab-wann-eine-haufung-von-fehlern-als-formatanderung-gilt) sind
  geschlossen oder mit neuer Decision aufgelöst.
