# Feature-Katalog

Jedes Feature nennt die Decisions, die es umsetzt. Welche Features ein Release oder der
Prototyp enthält, steht im jeweiligen Scope, nicht hier.

## Datenmodell und Engine

### F1 — Blöcke aus typisierten Spalten mit Nullwerten

Die Grundeinheit der Verarbeitung: ein Block von Zeilen, gespeichert als typisierte
Spalten mit Kennzeichnung der Nullwerte. Rohspalten aus Readern sind Text.
**Setzt um:** [D29](10-design-decisions.md#d29-daten-laufen-in-blocken-typisierter-spalten-rohspalten-bleiben-bis-zum-cast-text), [D30](10-design-decisions.md#d30-es-gibt-echte-nullwerte-getrennt-vom-leeren-text)

### F2 — Plan und blockweise Ausführung

Eine Pipeline wird als Plan aufgebaut, vor dem Lauf geprüft und blockweise ausgeführt,
während noch gelesen wird.
**Setzt um:** [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen), [D31](10-design-decisions.md#d31-jede-operation-gibt-es-einmal-als-wert-mit-zwei-einstiegen-sofort-auf-einer-tabelle-oder-im-plan)

### F3 — Speicherbudget und Auslagern

Schritte über alle Zeilen (Sortieren, Gruppieren, Joins, Pivot) sammeln ihre Blöcke und
lagern bei Erreichen des Budgets auf die Platte aus. Das Budget berücksichtigt
Container-Limits, und ausgelagerte Daten werden nach dem Lauf entfernt.
**Setzt um:** [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen), [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern)

### F4 — Kopieren oder Ändern an Ort und Stelle

Die Engine erkennt, ob Daten geteilt sind, und ändert sie nur dann an Ort und Stelle, wenn
niemand sonst sie sieht. Eine Option erzwingt "immer kopieren" oder "immer ändern". An
Verzweigungen wird immer kopiert, und der Trace vermerkt das.
**Setzt um:** [D7](10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert), [D8](10-design-decisions.md#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert), [D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert)

## Operationen

### F5 — Operationen als Werte, sofort auf Tabellen oder im Plan

Jede Operation (Auswahl, Filter, abgeleitete Spalten, Umwandeln, Joins, Gruppieren,
Sortieren, Fensterfunktionen, Umformen) gibt es einmal als Wert. `Table`-Methoden wenden
sie sofort an, Pipeline-Schritte im Plan.
**Setzt um:** [D31](10-design-decisions.md#d31-jede-operation-gibt-es-einmal-als-wert-mit-zwei-einstiegen-sofort-auf-einer-tabelle-oder-im-plan), [D5](10-design-decisions.md#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen)

### F6 — Ausdrücke

Typisierte Ausdrücke für Berechnungen und Filter, mit Funktionen für Text, Datum und
Rechnen. Sie werden vor dem Lauf geprüft und spaltenweise ausgeführt, mit
SQL-Semantik für Nullwerte.
**Setzt um:** [D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg), [D30](10-design-decisions.md#d30-es-gibt-echte-nullwerte-getrennt-vom-leeren-text)

### F7 — Operationen mit eigener Logik

Operationen, die eine Closure ausführen. Gibt sie einen Fehler zurück, wird die Zeile mit
`code=custom` aussortiert.
**Setzt um:** [D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg)

## Quellen

### F8 — Reader mit Fundstelle und Rohzustand

Reader für CSV, JSON/NDJSON und Excel liefern Blöcke mit Rohspalten und halten für jede
Zeile Rohzustand, Fundstelle, `record_key` und optional `record_hash` fest. Unzerlegbare
Zeilen werden mit ihren Rohbytes aussortiert, statt den Lauf abzubrechen.
**Setzt um:** [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle), [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash), [D29](10-design-decisions.md#d29-daten-laufen-in-blocken-typisierter-spalten-rohspalten-bleiben-bis-zum-cast-text)

### F9 — Erwarteter Aufbau und Prüfung des Kopfs

Eine Quelle kann einen erwarteten Aufbau haben. Der Kopf der Lieferung wird beim Lesen
dagegen geprüft: fehlende, neue und vermutlich umbenannte Spalten.
**Setzt um:** [D22](10-design-decisions.md#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird), [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler)

### F10 — Aussortierte Zeilen als Quelle

Eine Quelle, die aussortierte Zeilen eines früheren Laufs mit ihrer ursprünglichen
Fundstelle einliest und unzerlegbare Zeilen erneut durch den ursprünglichen Reader gibt.
Die Library bewahrt dafür nichts selbst auf.
**Setzt um:** [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle), [D17](10-design-decisions.md#d17-die-library-bewahrt-aussortierte-zeilen-nicht-selbst-auf)

## Fehlermodell

### F11 — Aussortierte Zeilen

Zeilen, die ein Schritt nicht verarbeiten kann, werden im ersten scheiternden Schritt
aussortiert, mit einem Eintrag je betroffener Spalte. Das Ergebnis enthält je Quelle eine
Tabelle mit Rohspalten und Info-Spalten sowie eine Übersicht über alle Quellen.
**Setzt um:** [D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten), [D13](10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen), [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix), [D15](10-design-decisions.md#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-eintrag-je-betroffener-spalte)

### F12 — Herkunft über Joins und Aggregationen

Nach einem Join werden alle beteiligten Quellzeilen aussortiert. Der Rohzustand reicht bis
zum ersten zusammenfassenden Schritt, danach werden aggregierte Zeilen aussortiert. Optional
gibt es volle Herkunft.
**Setzt um:** [D11](10-design-decisions.md#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert), [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert)

### F13 — Fehlerverhalten

Plan-, Liefer- und Datenfehler, feste Fehlercodes und je Fehlerart und Code die Wahl
zwischen "aussortieren" und "stoppen". Die Voreinstellung ist "aussortieren".
**Setzt um:** [D3](10-design-decisions.md#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen), [D5](10-design-decisions.md#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen), [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler)

### F14 — Schwelle

Schwelle absolut oder als Anteil, je Lauf oder je Schritt. Standardmäßig läuft der Lauf zu
Ende, optional bricht er sofort ab.
**Setzt um:** [D4](10-design-decisions.md#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen), [D20](10-design-decisions.md#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen)

### F15 — Fehlerzweige, Split und Merge

Gescheiterte Zeilen eines Schritts laufen durch einen Zweig und fließen zurück. Zweige nach
Bedingung und das Zusammenführen nach Spaltennamen. Erneut gescheiterte Zeilen behalten
Kennung und Weg.
**Setzt um:** [D25](10-design-decisions.md#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck), [D26](10-design-decisions.md#d26-zweige-werden-nach-spaltennamen-zusammengefuhrt-typkonflikte-sind-planfehler), [D27](10-design-decisions.md#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg)

## Laufergebnis

### F16 — Status, Zählungen, Trace und Exit-Code

Das Ergebnis eines Laufs mit Status, Zählungen je Schritt und Fehlercode, Trace der Schritte
und einer Abbildung auf Exit-Codes.
**Setzt um:** [D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst), [D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert)

### F17 — Änderungsbericht

Befunde zu fehlenden, neuen und vermutlich umbenannten Spalten und zu gehäuften
Formatfehlern, mit Beispielen.
**Setzt um:** [D23](10-design-decisions.md#d23-das-laufergebnis-enthalt-einen-anderungsbericht)

### F18 — Profil und Vergleich mit früheren Läufen

Ein Profil der Quellen je Lauf und der Vergleich mit dem Profil eines früheren Laufs.
**Setzt um:** [D24](10-design-decisions.md#d24-ein-lauf-kann-ein-profil-liefern-das-mit-dem-profil-eines-fruheren-laufs-verglichen-wird)

## Ziele

### F19 — Sink-Schnittstelle und Datei-Writer

Gemeinsame Schnittstelle für alle Ziele, mit Writern für CSV, JSON/NDJSON, Excel und
Markdown. Ergebnisse und aussortierte Zeilen gehen über denselben Weg.
**Setzt um:** [D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http), [D2](10-design-decisions.md#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse)

### F20 — Writer für Datenbank und HTTP

Unterpakete für `database/sql` (Insert, Upsert auf `record_key`) und HTTP (JSON/NDJSON,
Batching, Wiederholung).
**Setzt um:** [D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http), [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash)

## API und Auslieferung

### F21 — Öffentliche API ohne gseq-Typen

Signaturen verwenden Standard-Go-Typen und eigene Typen der Library.
**Setzt um:** [D33](10-design-decisions.md#d33-die-offentliche-api-verwendet-standard-go-typen-und-eigene-typen-der-library-keine-gseq-typen)

### F22 — v1-Adapter

Umwandlung zwischen v1- und v2-Tabellen.
**Setzt um:** [D36](10-design-decisions.md#d36-ein-adapter-wandelt-zwischen-v1-und-v2-tabellen)

### F23 — Auslieferung als experimenteller Prototyp und als v2-Modul

Prototyp unter `experimental/v2`, v2.0.0 als `/v2`-Modul mit SemVer.
**Setzt um:** [D34](10-design-decisions.md#d34-der-prototyp-liegt-unter-experimentalv2-ohne-zusage-v200-ist-ein-eigenes-modul-mit-semver)
