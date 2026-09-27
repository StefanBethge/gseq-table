# Test-Plan

Jeder T-Fall beweist eine Eigenschaft einer Decision oder eines Features, nicht eine
bestimmte Implementierung. Tests, die einen T-Fall beweisen, deklarieren das mit
`Proves(t, "T<n>")`.

## Fehlermodell

### T1 — Aussortierte Zeilen tragen den Rohzustand, nicht den Arbeitszustand

**Beweist:** [D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten), [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
Eine Pipeline verändert eine Spalte in einem frühen Schritt und lässt die Zeile in einem
späteren Schritt scheitern. Die aussortierte Zeile enthält den Wert aus der Lieferung,
nicht den veränderten.

### T2 — Aussortierte Zeilen lassen sich mit den Datei-Writern schreiben und mit dem passenden Reader wieder lesen

**Beweist:** [D2](10-design-decisions.md#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse), [F19](20-feature-catalogue.md#f19-sink-schnittstelle-und-datei-writer)
Die Tabelle aussortierter Zeilen wird mit jedem Datei-Writer geschrieben, zu dem es einen
Reader gibt (CSV und Excel im Prototyp), und mit dem passenden Reader gelesen. Rohspalten
und Info-Spalten bleiben erhalten. Nullwerte in `value` bleiben nur erhalten, wo das Format
sie darstellen kann.

### T3 — Im Modus "stoppen" endet der Lauf beim ersten Datenfehler, im Modus "aussortieren" nicht

**Beweist:** [D3](10-design-decisions.md#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen), [D51](10-design-decisions.md#d51-im-modus-stoppen-wird-der-scheiternde-block-nicht-geschrieben-bereits-geschriebene-blocke-bleiben)
Dieselbe Lieferung mit einem fehlerhaften Wert ergibt im Modus "stoppen" den Status
`aborted`, im Modus "aussortieren" den Status `ok` mit genau einer aussortierten Zeile. Im
Modus "stoppen" steht keine Zeile des Blocks mit dem Fehler im Ziel, zuvor geschriebene
Blöcke bleiben.

### T4 — Die Schwelle markiert den Lauf als fehlgeschlagen und lässt ihn standardmäßig zu Ende laufen

**Beweist:** [D4](10-design-decisions.md#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen), [D20](10-design-decisions.md#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen)
Bei überschrittener Schwelle (absolut, als Anteil, je Lauf und je Schritt) ist der Status
`failed_threshold`, und alle aussortierten Zeilen liegen vor. Mit der Abbruch-Option endet
der Lauf, sobald die Schwelle überschritten ist.

### T5 — Ohne Angaben kommt ein Lauf über eine schmutzige Lieferung durch

**Beweist:** [D5](10-design-decisions.md#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen), [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler)
Eine Pipeline ohne Angaben zum Fehlerverhalten verarbeitet eine Lieferung mit Parse-,
Validierungs- und Zerlegungsfehlern. Der Lauf endet mit `ok`, und jede fehlerhafte Zeile
ist aussortiert.

### T6 — Eine im Schritt scheiternde Zeile steht einmal in ihrer Tabelle je Quelle, hat einen Übersichtseintrag je Spalte und erreicht spätere Schritte nicht

**Beweist:** [D15](10-design-decisions.md#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-ubersichtseintrag-je-betroffener-spalte), [D44](10-design-decisions.md#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler)
Scheitern in einem Schritt drei Spalten einer Zeile, steht sie in ihrer Tabelle je Quelle
einmal mit `error_count` gleich 3, und die Übersicht enthält drei Einträge mit derselben
`reject_id`. Ein späterer Schritt, der jede Zeile zählt, zählt diese Zeile nicht.

### T7 — Aussortierte Zeilen gibt es je Quelle, und die Übersicht stimmt mit ihnen überein

**Beweist:** [D13](10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen), [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix), [D44](10-design-decisions.md#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler)
Bei zwei Quellen mit verschiedenen Spalten hat jede Tabelle aussortierter Zeilen die
Rohspalten ihrer Quelle. Die Übersicht enthält genau die Info-Spalten, mit einem Eintrag je
Fehler der Zeilen aus den Tabellen je Quelle. Ein
geändertes Präfix gilt für alle Info-Spalten, und eine Datenspalte mit Standardpräfix-Namen
wird nicht überschrieben.

### T8 — Scheitert eine Ergebniszeile nach einem Join, sind ihre Quellzeilen mit gemeinsamer Kennung aussortiert

**Beweist:** [D11](10-design-decisions.md#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert), [D47](10-design-decisions.md#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen)
Scheitert eine Ergebniszeile eines Joins, stehen beide Quellzeilen mit derselben
`reject_id` in der Übersicht. Bei einem 1:n-Join, in dem eine von fünf Ergebniszeilen
scheitert, kommen die übrigen vier im Ziel an, die linke Quellzeile zählt als
durchgelaufen, und eine rechte Zeile mit vielen gescheiterten Partnern steht einmal in
ihrer Tabelle je Quelle.

### T9 — Nach einer Gruppierung werden aggregierte Zeilen aussortiert, und der Speicher bleibt im Budget

**Beweist:** [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert), [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern), [D48](10-design-decisions.md#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle)
Eine nach der Gruppierung scheiternde Zeile wird mit Gruppenschlüssel und Anzahl der
Quellzeilen aussortiert. Der Spitzenspeicher eines Laufs mit Gruppierung über eine
Lieferung, die ein Vielfaches des Budgets groß ist, bleibt im Budget
([D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern)). Mit der Option "volle Herkunft" trägt der Eintrag die Kennungen der Quellzeilen; ob die
Quellzeilen auch mit Rohzustand in ihren Tabellen je Quelle stehen, ist offen ([G42](70-gap-ledger.md#g42-volle-herkunft-liefert-nach-einer-gruppierung-keinen-rohzustand)).

## Quellen

### T10 — Unzerlegbare Zeilen werden mit Rohbytes und richtiger Fundstelle aussortiert

**Beweist:** [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle), [F8](20-feature-catalogue.md#f8-reader-mit-fundstelle-und-rohzustand)
Eine CSV-Lieferung mit kaputten Anführungszeichen und falscher Spaltenzahl in bekannten
Zeilen ergibt Einträge mit `raw_line` gleich den Quellbytes, der richtigen Zeilennummer und
dem richtigen Byte-Offset. Die übrigen Zeilen laufen durch.

### T11 — Die Fundstelle bleibt über Sortieren und Filtern richtig

**Beweist:** [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
Nach Sortieren und Filtern zeigt die Fundstelle jeder aussortierten Zeile auf die Zeile in
der Lieferung, aus der ihre Werte stammen.

### T12 — Nachverarbeitung behält ursprüngliche Fundstelle und Schlüssel

**Beweist:** [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle), [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash), [D44](10-design-decisions.md#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler)
Aussortierte Zeilen eines Laufs werden geschrieben, gelesen und als Quelle verwendet.
Durchlaufende Zeilen tragen denselben `record_key` wie im ersten Lauf. Erneut scheiternde
Zeilen zeigen auf die Original-Lieferung. Unzerlegbare Zeilen werden nach geänderter
Reader-Konfiguration verarbeitet. Eine Zeile mit mehreren fehlerhaften Spalten wird einmal
verarbeitet.

### T13 — record_key ist innerhalb einer Lieferung stabil, ein fachlicher Schlüssel darüber hinaus

**Beweist:** [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash)
Dieselbe Lieferung ergibt in zwei Läufen und in der Nachverarbeitung dieselben
`record_key`s. Eine Lieferung mit einer oben eingefügten Zeile und eine neue Lieferung mit
gleichem Dateinamen überschreiben keine Schlüssel der alten. Mit fachlichem Schlüssel ist
`record_key` über Lieferungen hinweg gleich. Dieselbe Lieferung unter anderem Namen ergibt
denselben `record_hash`.

### T14 — Die Prüfung des Kopfs erkennt fehlende, neue und umbenannte Spalten

**Beweist:** [D22](10-design-decisions.md#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird), [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler), [D42](10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error)
Eine fehlende Spalte ergibt den Status `delivery_error` (Vorrang im Modus "stoppen" offen:
[G46](70-gap-ledger.md#g46-vorrang-der-status-und-umfang-von-aborted)), sortiert im Modus "aussortieren" alle Zeilen mit `missing_column` aus und
stoppt im Modus "stoppen". Eine neue Spalte wird gemeldet und durchgereicht. Ein Paar
aus fehlender und ähnlich benannter neuer Spalte wird als vermutliche Umbenennung gemeldet.

### T15 — Planfehler verhindern den Lauf, Liefer- und Datenfehler folgen der Konfiguration je Code

**Beweist:** [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler), [D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg), [D42](10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error), [D54](10-design-decisions.md#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus)
Ein Ausdruck auf eine unbekannte Spalte oder mit Typkonflikt ergibt `plan_error`, bevor
eine Zeile gelesen wird, in jedem Modus. Ist `parse` auf "aussortieren" und
`missing_column` auf "stoppen" gesetzt, verhält sich der Lauf je Code entsprechend. Ein
Ausdruck, der erst bei der Ausführung scheitert (Division durch null), sortiert die Zeile mit
`expr` aus und ergibt keinen `plan_error`.

## Laufergebnis

### T16 — Status und Zählungen sind konsistent und bilden auf Exit-Codes ab

**Beweist:** [D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst), [D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben)
Gelesene Quellzeilen sind gleich durchgelaufene plus aussortierte plus verworfene ([D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben)), auch mit Filtern, Inner Joins ohne Partner und Filtern über Nullwerte. Jeder Status
bildet auf einen eigenen Exit-Code ab.

### T17 — Gehäufte Formatfehler erscheinen im Änderungsbericht mit Beispielen

**Beweist:** [D23](10-design-decisions.md#d23-das-laufergebnis-enthalt-einen-anderungsbericht)
Scheitert in einer Lieferung ein erheblicher Teil der Werte einer Spalte mit demselben
Fehlercode, enthält der Änderungsbericht einen Befund `format_change` mit Quelle, Spalte,
Anzahl und Beispielen der Werte, wie sie jetzt aussehen. Die Grenze für "erheblich" ist
offen ([G12](70-gap-ledger.md#g12-ab-wann-eine-haufung-von-fehlern-als-formatanderung-gilt)).

### T18 — Der Profilvergleich meldet eine Abweichung, ohne dass eine Zeile scheitert

**Beweist:** [D24](10-design-decisions.md#d24-ein-lauf-kann-ein-profil-liefern-das-mit-dem-profil-eines-fruheren-laufs-verglichen-wird)
Ein Lauf liefert ein Profil seiner Quellen. Ein späterer Lauf über eine Lieferung, in der
plötzlich ein Drittel der Werte einer Spalte leer ist, lässt keine Zeile scheitern und
meldet die Abweichung als Befund im Änderungsbericht.

## Zweige

### T19 — Ein Fehlerzweig sieht den Zustand vor dem Schritt und führt Verarbeitetes zurück

**Beweist:** [D25](10-design-decisions.md#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck)
Die Zeilen im Zweig tragen die Ergebnisse aller vorherigen Schritte und die Info-Spalten.
Im Zweig verarbeitete Zeilen erscheinen im Ergebnis ohne Info-Spalten.

### T20 — Zweige werden nach Namen zusammengeführt, und Typkonflikte fallen vor dem Lauf auf

**Beweist:** [D26](10-design-decisions.md#d26-zweige-werden-nach-spaltennamen-zusammengefuhrt-typkonflikte-sind-planfehler)
Zwei Zweige mit teils verschiedenen Spalten werden zusammengeführt. Spalten sind nach Namen
zugeordnet, eine in einem Zweig fehlende Spalte ist mit Nullwerten aufgefüllt, und die
Info-Spalten sind nach dem Zurückführen weg. Haben gleichnamige Spalten verschiedene Typen,
ergibt der Plan `plan_error`, bevor eine Zeile gelesen wird.

### T21 — Eine im Zweig erneut scheiternde Zeile behält Kennung, Weg und vorigen Grund und zählt einmal

**Beweist:** [D27](10-design-decisions.md#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg)
Eine Zeile scheitert im Hauptweg und im Fehlerzweig erneut. Ihr Eintrag behält die
`reject_id`, `step` enthält den ganzen Weg, und `prev_reason` enthält den Grund aus dem
Hauptweg. In den Zählungen erscheint die Quellzeile einmal.

## Engine

### T22 — Dieselbe Pipeline liefert im Speicher und im Streaming dasselbe Ergebnis

**Beweist:** [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen), [D31](10-design-decisions.md#d31-jede-operation-gibt-es-einmal-als-wert-mit-zwei-einstiegen-sofort-auf-einer-tabelle-oder-im-plan), [D50](10-design-decisions.md#d50-eine-tabelle-tragt-ihre-aussortierten-zeilen-und-einen-haftenden-fehler)
Für generierte Lieferungen im Modus "aussortieren" ohne vorzeitigen Abbruch (vgl.
[G55](70-gap-ledger.md#g55-t22-ist-nicht-in-jedem-modus-deterministisch-und-der-erste-fehler-hat-keine-reihenfolge)) sind Ergebnis und aussortierte Zeilen gleich, egal ob der Lauf
im Speicher, blockweise oder mit Auslagern ausgeführt wird, und gleich dem Ergebnis der
`Table`-Methoden mit denselben Operationen.

### T23 — Ein Lauf über mehr Daten als das Budget hält das Budget ein

**Beweist:** [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen), [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern), [D58](10-design-decisions.md#d58-der-prototyp-hat-feste-bestehkriterien-fur-laufzeit-speicher-und-budget), [D60](10-design-decisions.md#d60-der-prototyp-wird-mit-eigenen-beispiel-lieferungen-der-1brc-datei-und-in-docker-mit-verschiedenen-speicher-limits-erprobt)
Sortieren, Gruppieren und Join über eine Lieferung, die ein Vielfaches des Budgets groß
ist, kommen durch, und der gemessene Spitzenwert des Speichers bleibt innerhalb des
Budgets plus 10 %, auch in Docker mit mehreren Speicher-Limits ([D58](10-design-decisions.md#d58-der-prototyp-hat-feste-bestehkriterien-fur-laufzeit-speicher-und-budget)).

### T24 — Nach einem Lauf bleibt nichts neben den Zielen zurück

**Beweist:** [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern), [D17](10-design-decisions.md#d17-die-library-bewahrt-aussortierte-zeilen-nicht-selbst-auf), [D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close), [D38](10-design-decisions.md#d38-ein-spaterer-lauf-entfernt-verwaiste-ausgelagerte-daten)
Nach einem erfolgreichen, einem gestoppten und einem über den Kontext abgebrochenen Lauf
ist das Verzeichnis zum Auslagern leer, bei im Ergebnis gehaltenen aussortierten Zeilen nach dem Schließen des Ergebnisses ([D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close)), und die Library hat keine Dateien außerhalb der
angegebenen Ziele angelegt. Die Reste eines vom System beendeten Laufs entfernt der nächste
Lauf (Abgrenzung zu offenen Ergebnissen offen: [G47](70-gap-ledger.md#g47-aufraumen-verwaister-laufe-gegen-offene-ergebnisse)).

### T25 — Kein Zweig sieht Änderungen eines anderen, in jedem Modus

**Beweist:** [D7](10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert), [D8](10-design-decisions.md#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert), [D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert)
Zwei Zweige verändern dieselbe Spalte unterschiedlich. In den Modi "automatisch", "immer
kopieren" und "immer ändern" sieht jeder Zweig nur seine eigene Änderung, der Rohzustand
ist unverändert, und im Modus "immer ändern" vermerkt der Trace die Kopie an der
Verzweigung.

### T26 — Die Modi für Kopieren und Ändern liefern dasselbe Ergebnis

**Beweist:** [D8](10-design-decisions.md#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert)
Dieselbe Pipeline über dieselbe Lieferung liefert in den Modi "automatisch", "immer
kopieren" und "immer ändern" dasselbe Ergebnis und dieselben aussortierten Zeilen.

### T27 — Nullwerte sind vom leeren Text getrennt und verhalten sich wie in SQL

**Beweist:** [D30](10-design-decisions.md#d30-es-gibt-echte-nullwerte-getrennt-vom-leeren-text)
Ein leeres Feld bleibt als Rohspalte leerer Text und wird beim Umwandeln null. Konfigurierte
Null-Texte werden null. Rechnen mit null ergibt null, Vergleiche mit null sind nicht wahr,
und Aggregationen überspringen Nullwerte.

### T28 — Eine Closure, die einen Fehler meldet, sortiert die Zeile mit code=custom aus

**Beweist:** [D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg), [D39](10-design-decisions.md#d39-eine-panik-in-einer-closure-wird-wie-ein-zuruckgegebener-fehler-behandelt), [D54](10-design-decisions.md#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus)
Eine Closure gibt für bestimmte Zeilen einen Fehler zurück. Diese Zeilen sind mit
`code=custom` und dem Fehlertext als Grund aussortiert, die übrigen laufen durch. Eine
Closure in Panik sortiert die Zeile ebenso aus, mit der Panik-Meldung im Grund. Ein
typisierter Fehler mit eigenem Code ergibt einen Code, der mit `custom:` beginnt.

## Ziele und API

### T29 — Der Datenbank-Writer ist bei Nachverarbeitung idempotent

**Beweist:** [D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http), [D47](10-design-decisions.md#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen)
Ein Lauf und eine anschließende Nachverarbeitung mit Upsert auf `row_key` ergeben keine
doppelten Zeilen im Ziel, auch bei einem 1:n-Join, dessen Ergebniszeilen alle erhalten
bleiben.

### T30 — Der HTTP-Writer liefert jede Zeile trotz vorübergehender Fehler aus

**Beweist:** [D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http), [D41](10-design-decisions.md#d41-der-http-writer-liefert-mindestens-einmal-und-schickt-einen-idempotenzschlussel-mit)
Gegen einen Testserver, der einzelne Anfragen vorübergehend ablehnt, kommt jede Zeile an.
Wiederholte Batches tragen denselben Idempotenzschlüssel ([D41](10-design-decisions.md#d41-der-http-writer-liefert-mindestens-einmal-und-schickt-einen-idempotenzschlussel-mit)).

### T31 — Die öffentliche API enthält keine gseq-Typen

**Beweist:** [D33](10-design-decisions.md#d33-die-offentliche-api-verwendet-standard-go-typen-und-eigene-typen-der-library-keine-gseq-typen)
Eine Prüfung über die exportierten Signaturen aller öffentlichen Pakete findet keinen Typ
aus gseq.

### T32 — Eine v1-Tabelle übersteht den Weg über v2 zurück nach v1 unverändert

**Beweist:** [D36](10-design-decisions.md#d36-ein-adapter-wandelt-zwischen-v1-und-v2-tabellen)
Eine v1-Tabelle, in eine v2-Tabelle gewandelt und zurück, ist gleich der ursprünglichen.

Die Tabelle "welcher Test beweist welchen Fall" entsteht mit den ersten Tests. Ihr Format
gibt der Parser des Docs-Gates vor ([G14](70-gap-ledger.md#g14-docs-gates-aus-dem-archivar-repo-ubernehmen)).
