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
Container-Limits und gilt je Prozess; `GOMEMLIMIT` setzt die Engine nur auf Wunsch
([D65](10-design-decisions.md#d65-gomemlimit-setzt-die-engine-nur-auf-wunsch-und-das-budget-gilt-je-prozess)). Ausgelagerte Daten werden nach dem Lauf entfernt, Reste vom System beendeter
Läufe beim nächsten Lauf ([D38](10-design-decisions.md#d38-ein-spaterer-lauf-entfernt-verwaiste-ausgelagerte-daten)), im Ergebnis
gehaltene aussortierte Zeilen erst beim Schließen des Ergebnisses ([D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close)).
**Setzt um:** [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen), [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern), [D38](10-design-decisions.md#d38-ein-spaterer-lauf-entfernt-verwaiste-ausgelagerte-daten), [D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close), [D65](10-design-decisions.md#d65-gomemlimit-setzt-die-engine-nur-auf-wunsch-und-das-budget-gilt-je-prozess), [D101](10-design-decisions.md#d101-das-budget-gilt-je-prozess-ein-lauf-kann-darin-eine-eigene-obergrenze-haben-und-die-voreinstellung-ist-vorlaufig-ein-viertel-des-erkannten-limits), [D102](10-design-decisions.md#d102-auf-wunsch-setzt-die-engine-gomemlimit-auf-90-des-erkannten-limits-wenn-es-noch-nicht-gesetzt-ist), [D103](10-design-decisions.md#d103-ein-lauf-markiert-sein-verzeichnis-zum-auslagern-mit-einer-dateisperre-die-das-ergebnis-bis-close-halt)

### F4 — Kopieren oder Ändern an Ort und Stelle

Die Engine erkennt, ob Daten geteilt sind, und ändert sie nur dann an Ort und Stelle, wenn
niemand sonst sie sieht. Roh- und Arbeitsdaten teilen Spalten, bis ein Schritt eine Spalte
ändert ([D55](10-design-decisions.md#d55-roh-und-arbeitsdaten-teilen-spalten-bis-ein-schritt-eine-spalte-andert)). Eine Option erzwingt "immer kopieren" oder "immer ändern". An
Verzweigungen und bei der ersten Änderung einer mit dem Rohzustand geteilten Spalte
([D64](10-design-decisions.md#d64-mit-dem-rohzustand-geteilte-spalten-gelten-als-geteilt-auch-im-modus-immer-andern)) wird immer kopiert, und der Trace vermerkt das.
**Setzt um:** [D7](10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert), [D8](10-design-decisions.md#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert), [D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert), [D55](10-design-decisions.md#d55-roh-und-arbeitsdaten-teilen-spalten-bis-ein-schritt-eine-spalte-andert), [D64](10-design-decisions.md#d64-mit-dem-rohzustand-geteilte-spalten-gelten-als-geteilt-auch-im-modus-immer-andern)

## Operationen

### F5 — Operationen als Werte, sofort auf Tabellen oder im Plan

Jede Operation (Auswahl, Filter, abgeleitete Spalten, Umwandeln, Joins, Gruppieren,
Sortieren, Fensterfunktionen, Umformen) gibt es einmal als Wert. `Table`-Methoden wenden
sie sofort an und tragen aussortierte Zeilen und einen haftenden Fehler ([D50](10-design-decisions.md#d50-eine-tabelle-tragt-ihre-aussortierten-zeilen-und-einen-haftenden-fehler)),
Pipeline-Schritte im Plan.
**Setzt um:** [D31](10-design-decisions.md#d31-jede-operation-gibt-es-einmal-als-wert-mit-zwei-einstiegen-sofort-auf-einer-tabelle-oder-im-plan), [D5](10-design-decisions.md#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen), [D50](10-design-decisions.md#d50-eine-tabelle-tragt-ihre-aussortierten-zeilen-und-einen-haftenden-fehler)

### F6 — Ausdrücke

Typisierte Ausdrücke für Berechnungen und Filter, mit Funktionen für Text, Datum und
Rechnen. Sie werden vor dem Lauf geprüft und spaltenweise ausgeführt, mit
SQL-Semantik für Nullwerte. Scheitert ein Ausdruck erst bei der Ausführung, wird die Zeile
mit `expr` aussortiert ([D54](10-design-decisions.md#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus)).
**Setzt um:** [D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg), [D30](10-design-decisions.md#d30-es-gibt-echte-nullwerte-getrennt-vom-leeren-text), [D54](10-design-decisions.md#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus)

### F7 — Operationen mit eigener Logik

Operationen, die eine Closure ausführen. Gibt sie einen Fehler zurück, wird die Zeile mit
`code=custom` aussortiert, bei einem typisierten Fehler mit einem eigenen Code, der mit
`custom:` beginnt ([D54](10-design-decisions.md#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus)). Eine Panik in der Closure gilt wie ein zurückgegebener
Fehler ([D39](10-design-decisions.md#d39-eine-panik-in-einer-closure-wird-wie-ein-zuruckgegebener-fehler-behandelt)).
**Setzt um:** [D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg), [D39](10-design-decisions.md#d39-eine-panik-in-einer-closure-wird-wie-ein-zuruckgegebener-fehler-behandelt), [D54](10-design-decisions.md#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus)

## Quellen

### F8 — Reader mit Fundstelle und Rohzustand

Reader für CSV und Excel, ab v2.0 auch JSON/NDJSON ([D57](10-design-decisions.md#d57-der-prototyp-liest-csv-und-excel-v20-zusatzlich-json-und-ndjson)), liefern Blöcke mit
Rohspalten und halten für jede Zeile Rohzustand, Fundstelle, `record_key` und optional
`record_hash` fest. Jede Datei und jedes Sheet ist eine Quelle ([D53](10-design-decisions.md#d53-jede-datei-und-jedes-sheet-ist-eine-quelle-gruppen-von-dateien-wirken-als-eine-quelle)), und die Kennung
der Lieferung steht beim Öffnen fest ([D61](10-design-decisions.md#d61-die-kennung-einer-lieferung-ist-ein-fingerabdruck-der-beim-offnen-feststeht)). Bei Excel tragen Rohzustand und
Arbeitsspalte den gespeicherten Wert in fester Textform, die Fundstelle nennt Sheet, Zeile
und Zelle ([D52](10-design-decisions.md#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert), [D62](10-design-decisions.md#d62-bei-excel-tragen-rohzustand-und-arbeitsspalte-den-gespeicherten-wert-in-fester-textform)). Unzerlegbare Zeilen werden mit ihren Rohbytes aussortiert,
statt den Lauf abzubrechen. Grenzen für Feldlänge und entpackten Umfang schützen vor
übergroßen Lieferungen ([D56](10-design-decisions.md#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch)).
**Setzt um:** [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle), [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash), [D29](10-design-decisions.md#d29-daten-laufen-in-blocken-typisierter-spalten-rohspalten-bleiben-bis-zum-cast-text), [D52](10-design-decisions.md#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert), [D53](10-design-decisions.md#d53-jede-datei-und-jedes-sheet-ist-eine-quelle-gruppen-von-dateien-wirken-als-eine-quelle), [D56](10-design-decisions.md#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch), [D57](10-design-decisions.md#d57-der-prototyp-liest-csv-und-excel-v20-zusatzlich-json-und-ndjson), [D61](10-design-decisions.md#d61-die-kennung-einer-lieferung-ist-ein-fingerabdruck-der-beim-offnen-feststeht), [D62](10-design-decisions.md#d62-bei-excel-tragen-rohzustand-und-arbeitsspalte-den-gespeicherten-wert-in-fester-textform)

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
aussortiert. Das Ergebnis enthält je Quelle eine Tabelle mit Rohspalten und Info-Spalten,
in der jede Quellzeile einmal steht, sowie eine Übersicht über alle Quellen mit einem
Eintrag je betroffener Spalte ([D44](10-design-decisions.md#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler)). Writer im Plan schreiben sie während des Laufs,
sonst hält sie das Ergebnis bis `Close` ([D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close)).
**Setzt um:** [D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten), [D13](10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen), [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix), [D15](10-design-decisions.md#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-ubersichtseintrag-je-betroffener-spalte), [D44](10-design-decisions.md#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler), [D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close)

### F12 — Herkunft über Joins und Aggregationen

Scheitert eine Ergebniszeile nach einem Join, werden ihre Quellzeilen aussortiert, bei
1:n-Joins nur die der gescheiterten Ergebniszeile ([D47](10-design-decisions.md#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen)). Der Rohzustand reicht bis
zum ersten zusammenfassenden Schritt, danach stehen aussortierte aggregierte Zeilen in einer
eigenen Tabelle ([D48](10-design-decisions.md#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle)). Optional
gibt es volle Herkunft.
**Setzt um:** [D11](10-design-decisions.md#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert), [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert), [D47](10-design-decisions.md#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen), [D48](10-design-decisions.md#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle)

### F13 — Fehlerverhalten

Plan-, Liefer- und Datenfehler, feste Fehlercodes und je Fehlerart und Code die Wahl
zwischen "aussortieren" und "stoppen". Die Voreinstellung ist "aussortieren". Jeder
Lieferfehler setzt `delivery_error` ([D42](10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error)), ein Schreibfehler bricht mit `sink_error` ab
([D40](10-design-decisions.md#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab)), und im Modus "stoppen" wird der scheiternde Block nicht geschrieben ([D51](10-design-decisions.md#d51-im-modus-stoppen-wird-der-scheiternde-block-nicht-geschrieben-bereits-geschriebene-blocke-bleiben)).
**Setzt um:** [D3](10-design-decisions.md#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen), [D5](10-design-decisions.md#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen), [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler), [D40](10-design-decisions.md#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab), [D42](10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error), [D51](10-design-decisions.md#d51-im-modus-stoppen-wird-der-scheiternde-block-nicht-geschrieben-bereits-geschriebene-blocke-bleiben)

### F14 — Schwelle

Schwelle absolut oder als Anteil, je Lauf oder je Schritt. Standardmäßig läuft der Lauf zu
Ende, optional bricht er sofort ab. Anteile beziehen sich auf bisher gelesene Zeilen und
brechen erst nach einer Mindestzahl ab ([D45](10-design-decisions.md#d45-die-schwelle-bezieht-anteile-auf-bisher-gelesene-zeilen-gilt-bei-einer-der-grenzen-und-bricht-erst-nach-einer-mindestzahl-ab)).
**Setzt um:** [D4](10-design-decisions.md#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen), [D20](10-design-decisions.md#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen), [D45](10-design-decisions.md#d45-die-schwelle-bezieht-anteile-auf-bisher-gelesene-zeilen-gilt-bei-einer-der-grenzen-und-bricht-erst-nach-einer-mindestzahl-ab)

### F15 — Fehlerzweige, Split und Merge

Gescheiterte Zeilen eines Schritts laufen durch einen Zweig und fließen zurück. Zweige nach
Bedingung und das Zusammenführen nach Spaltennamen. Erneut gescheiterte Zeilen behalten
Kennung und Weg, gerettete ihre Geschichte, und die Schwelle zählt nur endgültig
aussortierte ([D46](10-design-decisions.md#d46-gerettete-zeilen-behalten-ihre-geschichte-und-die-schwelle-zahlt-nur-endgultig-aussortierte)).
**Setzt um:** [D25](10-design-decisions.md#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck), [D26](10-design-decisions.md#d26-zweige-werden-nach-spaltennamen-zusammengefuhrt-typkonflikte-sind-planfehler), [D27](10-design-decisions.md#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg), [D46](10-design-decisions.md#d46-gerettete-zeilen-behalten-ihre-geschichte-und-die-schwelle-zahlt-nur-endgultig-aussortierte)

## Laufergebnis

### F16 — Status, Zählungen, Trace und Exit-Code

Das Ergebnis eines Laufs mit Status, Zählungen gelesener, durchgelaufener, aussortierter
und verworfener Quellzeilen ([D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben)) je Schritt und Fehlercode, Trace der Schritte
und einer Abbildung auf Exit-Codes. Treffen mehrere Status zu, gilt der höchste, und das
Ergebnis nennt alle Befunde ([D63](10-design-decisions.md#d63-es-gilt-der-hochste-zutreffende-status-und-das-ergebnis-nennt-alle-befunde)).
**Setzt um:** [D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst), [D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert), [D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben), [D63](10-design-decisions.md#d63-es-gilt-der-hochste-zutreffende-status-und-das-ergebnis-nennt-alle-befunde)

### F17 — Änderungsbericht

Befunde zu fehlenden, neuen und vermutlich umbenannten Spalten und zu gehäuften
Formatfehlern, mit Beispielen. Die Grenze für einen Formatänderungs-Befund ist einstellbar
([D59](10-design-decisions.md#d59-die-grenze-fur-einen-formatanderungs-befund-ist-einstellbar-die-voreinstellung-wird-im-prototyp-festgelegt)).
**Setzt um:** [D23](10-design-decisions.md#d23-das-laufergebnis-enthalt-einen-anderungsbericht), [D59](10-design-decisions.md#d59-die-grenze-fur-einen-formatanderungs-befund-ist-einstellbar-die-voreinstellung-wird-im-prototyp-festgelegt)

### F18 — Profil und Vergleich mit früheren Läufen

Ein Profil der Quellen je Lauf und der Vergleich mit dem Profil eines früheren Laufs.
**Setzt um:** [D24](10-design-decisions.md#d24-ein-lauf-kann-ein-profil-liefern-das-mit-dem-profil-eines-fruheren-laufs-verglichen-wird)

## Ziele

### F19 — Sink-Schnittstelle und Datei-Writer

Gemeinsame Schnittstelle für alle Ziele, mit Writern für CSV, JSON/NDJSON, Excel und
Markdown. Ergebnisse und aussortierte Zeilen gehen über denselben Weg.
**Setzt um:** [D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http), [D2](10-design-decisions.md#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse)

### F20 — Writer für Datenbank und HTTP

Unterpakete für `database/sql` (Insert, Upsert auf `row_key` nach [D47](10-design-decisions.md#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen)) und HTTP (JSON/NDJSON,
Batching, Wiederholung mit Idempotenzschlüssel, Zusage "mindestens einmal" nach [D41](10-design-decisions.md#d41-der-http-writer-liefert-mindestens-einmal-und-schickt-einen-idempotenzschlussel-mit)).
**Setzt um:** [D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http), [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash), [D41](10-design-decisions.md#d41-der-http-writer-liefert-mindestens-einmal-und-schickt-einen-idempotenzschlussel-mit)

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

## Pflege

### F24 — Docs-Gates für das Design-Set

Go-Tests in diesem Repo prüfen das Design-Set: IDs, Links und Anker, Bereichsangaben,
die Verweise zwischen Use Cases und Decisions und den Abgleich der T-Fälle mit
`Proves(t, "T<n>")`.
**Setzt um:** [D37](10-design-decisions.md#d37-die-docs-gates-werden-in-diesem-repo-selbst-gebaut)
