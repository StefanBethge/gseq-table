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

**Beweist:** [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash), [D61](10-design-decisions.md#d61-die-kennung-einer-lieferung-ist-ein-fingerabdruck-der-beim-offnen-feststeht)
Dieselbe Lieferung ergibt in zwei Läufen und in der Nachverarbeitung dieselben
`record_key`s. Eine Lieferung mit einer oben eingefügten Zeile und eine neue Lieferung mit
gleichem Dateinamen überschreiben keine Schlüssel der alten. Mit fachlichem Schlüssel ist
`record_key` über Lieferungen hinweg gleich. Dieselbe Lieferung unter anderem Namen ergibt
denselben `record_hash`.

### T14 — Die Prüfung des Kopfs erkennt fehlende, neue und umbenannte Spalten

**Beweist:** [D22](10-design-decisions.md#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird), [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler), [D42](10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error), [D63](10-design-decisions.md#d63-es-gilt-der-hochste-zutreffende-status-und-das-ergebnis-nennt-alle-befunde)
Eine fehlende Spalte ergibt den Status `delivery_error` (auch im Modus "stoppen", nach
[D63](10-design-decisions.md#d63-es-gilt-der-hochste-zutreffende-status-und-das-ergebnis-nennt-alle-befunde)), sortiert im Modus "aussortieren" alle Zeilen mit `missing_column` aus und
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

**Beweist:** [D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst), [D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben), [D63](10-design-decisions.md#d63-es-gilt-der-hochste-zutreffende-status-und-das-ergebnis-nennt-alle-befunde)
Gelesene Quellzeilen sind gleich durchgelaufene plus aussortierte plus verworfene ([D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben)), auch mit Filtern, Inner Joins ohne Partner und Filtern über Nullwerte. Jeder Status
bildet auf einen eigenen Exit-Code ab. Treffen mehrere Status zu, bildet der höchste nach
[D63](10-design-decisions.md#d63-es-gilt-der-hochste-zutreffende-status-und-das-ergebnis-nennt-alle-befunde) auf den Exit-Code ab.

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

**Beweist:** [D27](10-design-decisions.md#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg), [D46](10-design-decisions.md#d46-gerettete-zeilen-behalten-ihre-geschichte-und-die-schwelle-zahlt-nur-endgultig-aussortierte)
Eine Zeile scheitert im Hauptweg und im Fehlerzweig erneut. Ihr Eintrag behält die
`reject_id`, `step` enthält den ganzen Weg, und `prev_reason` enthält den Grund aus dem
Hauptweg. In den Zählungen erscheint die Quellzeile einmal. Eine im Zweig gerettete Zeile,
die später im Hauptweg scheitert, behält ebenso ihre `reject_id` und zeigt `prev_reason`
([D46](10-design-decisions.md#d46-gerettete-zeilen-behalten-ihre-geschichte-und-die-schwelle-zahlt-nur-endgultig-aussortierte)). Gerettete Zeilen zählen nicht zur Schwelle.

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

**Beweist:** [D7](10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert), [D8](10-design-decisions.md#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert), [D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert), [D64](10-design-decisions.md#d64-mit-dem-rohzustand-geteilte-spalten-gelten-als-geteilt-auch-im-modus-immer-andern)
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

## Nachträge

### T33 — Ein Ziel, das einen Block ablehnt, beendet den Lauf sofort mit sink_error

**Beweist:** [D40](10-design-decisions.md#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab)
Ein Test-Ziel lehnt einen Block ab. Der Lauf endet sofort mit dem Status `sink_error` und
dem Fehler des Ziels, und es werden keine weiteren Blöcke geschrieben. Dasselbe gilt, wenn
ein Writer für aussortierte Zeilen einen Block ablehnt.

### T34 — Excel-Zellen tragen den gespeicherten Wert in fester Textform, aussortierte Zeilen auch den angezeigten Text

**Beweist:** [D52](10-design-decisions.md#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert), [D62](10-design-decisions.md#d62-bei-excel-tragen-rohzustand-und-arbeitsspalte-den-gespeicherten-wert-in-fester-textform)
Eine Excel-Lieferung enthält eine Datumszelle und eine formatierte Zahl. Rohzustand und
Arbeitsspalte tragen `2026-09-27` und `1234.5`. Eine aussortierte Zeile trägt in `display`
den angezeigten Text, in `cell` die Zelladresse und als Zeilennummer die aus Excel,
einschließlich Kopfzeile.

### T35 — Grenzen für Feldlänge und entpackten Umfang greifen, und ausgelagerte Dateien sind geschützt

**Beweist:** [D56](10-design-decisions.md#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch)
Ein Feld über der eingestellten Grenze sortiert seine Zeile mit `field_too_large` aus. Ein
Excel-Archiv über dem eingestellten entpackten Umfang oder Verhältnis ergibt `unreadable` und
den Status `delivery_error`. Verzeichnis und Dateien zum Auslagern sind nur für den
ausführenden Nutzer zugänglich.

### T36 — Bei mehreren zutreffenden Status gilt der höchste, und das Ergebnis nennt alle Befunde

**Beweist:** [D63](10-design-decisions.md#d63-es-gilt-der-hochste-zutreffende-status-und-das-ergebnis-nennt-alle-befunde), [D42](10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error)
Lieferfehler und überschrittene Schwelle ergeben `delivery_error`, ein Lieferfehler im Modus
"stoppen" ergibt `delivery_error`, und ein Schreibfehler nach einem Lieferfehler ergibt
`sink_error`. In jedem Fall nennt das Ergebnis alle zutreffenden Befunde.

### T37 — Die Kennung einer Lieferung steht beim Öffnen fest und ändert sich mit der Datei

**Beweist:** [D61](10-design-decisions.md#d61-die-kennung-einer-lieferung-ist-ein-fingerabdruck-der-beim-offnen-feststeht), [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash)
Der `record_key` der ersten Zeile ist bekannt, bevor die Datei zu Ende gelesen ist. Dieselbe
unveränderte Datei ergibt dieselben Schlüssel, eine geänderte Datei andere. Gibt die
Pipeline eine eigene Kennung der Lieferung an, gilt diese.

### T38 — Im Modus "immer ändern" wird eine mit dem Rohzustand geteilte Spalte einmal kopiert

**Beweist:** [D64](10-design-decisions.md#d64-mit-dem-rohzustand-geteilte-spalten-gelten-als-geteilt-auch-im-modus-immer-andern), [D55](10-design-decisions.md#d55-roh-und-arbeitsdaten-teilen-spalten-bis-ein-schritt-eine-spalte-andert), [D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert)
Im Modus "immer ändern" ändert ein Schritt eine Rohspalte. Die Engine kopiert sie bei der
ersten Änderung einmal, der Trace vermerkt die Kopie, und der Rohzustand ist unverändert.

### T39 — GOMEMLIMIT wird nur auf Wunsch gesetzt, und Läufe in einem Prozess teilen ein Budget

**Beweist:** [D65](10-design-decisions.md#d65-gomemlimit-setzt-die-engine-nur-auf-wunsch-und-das-budget-gilt-je-prozess), [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern)
Ein bereits gesetztes `GOMEMLIMIT` bleibt unverändert. Ohne die ausdrückliche Option setzt
die Engine es nicht. Zwei gleichzeitige Läufe in einem Prozess bleiben zusammen innerhalb
des einen Budgets.

### T40 — Mit Writern im Plan werden aussortierte Zeilen während des Laufs geschrieben, sonst hält sie das Ergebnis bis Close

**Beweist:** [D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close)
Mit Writern für aussortierte Zeilen im Plan stehen die aussortierten Zeilen schon während
des Laufs im Ziel, und das Ergebnis enthält nur Übersicht und Zählungen. Ohne solche Writer
enthält das Ergebnis die aussortierten Zeilen, und sie bleiben lesbar, bis es geschlossen
wird.

### T41 — Eine Tabelle trägt aussortierte Zeilen und einen haftenden Fehler

**Beweist:** [D50](10-design-decisions.md#d50-eine-tabelle-tragt-ihre-aussortierten-zeilen-und-einen-haftenden-fehler)
Eine `Table`-Methode mit einem Datenfehler liefert eine Tabelle, deren aussortierte Zeilen
die Zeile enthalten. Eine unbekannte Spalte setzt den haftenden Fehler, und folgende
Operationen der Kette laufen nicht. Im Modus "stoppen" setzt der erste Datenfehler den
haftenden Fehler.

### T42 — Zwei Sheets einer Excel-Datei sind zwei Quellen, und ein fehlendes Sheet ist ein Lieferfehler

**Beweist:** [D53](10-design-decisions.md#d53-jede-datei-und-jedes-sheet-ist-eine-quelle-gruppen-von-dateien-wirken-als-eine-quelle), [D42](10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error)
Zwei Sheets einer Excel-Datei ergeben zwei Quellen, `datei.xlsx#Kunden` und
`datei.xlsx#Auftraege`, jede mit eigener Tabelle aussortierter Zeilen. Fehlt ein erwartetes
Sheet, ergibt das `missing_sheet` und den Status `delivery_error`.

### T43 — Die Schwelle bricht bei einem Anteil erst nach der Mindestzahl ab, bei einer absoluten Grenze sofort

**Beweist:** [D45](10-design-decisions.md#d45-die-schwelle-bezieht-anteile-auf-bisher-gelesene-zeilen-gilt-bei-einer-der-grenzen-und-bricht-erst-nach-einer-mindestzahl-ab)
Mit der Option zum sofortigen Abbruch bricht ein überschrittener Anteil den Lauf nicht vor
der Mindestzahl gelesener Zeilen ab, eine überschrittene absolute Grenze sofort. Sind beide
angegeben, genügt eine. Der Abbruch ergibt den Status `failed_threshold`.

### T44 — Ein Join trägt die aussortierten Zeilen und haftenden Fehler beider Tabellen, und gleichnamige Spalten sind ein Planfehler

**Beweist:** [D69](10-design-decisions.md#d69-ein-join-zweier-tabellen-tragt-die-aussortierten-zeilen-und-den-haftenden-fehler-beider-seiten), [D72](10-design-decisions.md#d72-gleichnamige-spalten-beider-seiten-eines-joins-sind-ein-planfehler)
Zwei Tabellen mit je einer aussortierten Zeile werden verbunden. Das Ergebnis trägt beide,
die der linken Seite zuerst. Hat die rechte Tabelle einen haftenden Fehler, läuft der Join
nicht, und das Ergebnis trägt diesen Fehler. Eine Spalte, die auf beiden Seiten gleich heißt
und kein Schlüssel ist, und Schlüssel verschiedener Typen ergeben einen Planfehler. Ein
Nullwert im Schlüssel findet keinen Partner.

### T45 — Ganzzahl und Gleitkomma werden erweitert, die Division ergibt Gleitkomma

**Beweist:** [D71](10-design-decisions.md#d71-ganzzahl-und-gleitkomma-werden-in-ausdrucken-erweitert-und-die-division-ergibt-gleitkomma)
Ganzzahl mal Gleitkomma ergibt Gleitkomma, Ganzzahl plus Ganzzahl Ganzzahl, und `7 / 2`
ergibt `3.5`. Ein Überlauf einer Ganzzahl sortiert die Zeile mit `expr` aus. Text plus Zahl
ergibt einen Planfehler.

### T46 — Nullwerte stehen beim Sortieren hinten, und das Sortieren ist stabil

**Beweist:** [D70](10-design-decisions.md#d70-beim-sortieren-stehen-nullwerte-in-beiden-richtungen-hinten)
Aufsteigend und absteigend sortiert stehen Zeilen mit null in der Sortierspalte am Ende.
Zeilen mit gleichem Schlüssel behalten ihre Reihenfolge.

### T47 — Cast ist ohne Option streng und mit Lenient nachsichtig wie v1

**Beweist:** [D74](10-design-decisions.md#d74-cast-ist-standardmaig-streng-die-option-lenient-verhalt-sich-wie-v1)
Ohne Option scheitern ` 12` als Ganzzahl und `27.09.2026` als Datum mit `parse`. Mit
`Lenient` werden beide gelesen, und ein ausdrückliches `DateFormat` gilt weiterhin, mit
entfernten Leerzeichen.

### T48 — Eine im Code gebaute Tabelle ist eine benannte Quelle, und gleichnamige Quellen in einem Join sind ein Planfehler

**Beweist:** [D75](10-design-decisions.md#d75-eine-im-code-gebaute-tabelle-ist-eine-eigene-quelle-mit-einstellbarem-namen-und-gleichnamige-quellen-sind-ein-planfehler), [D50](10-design-decisions.md#d50-eine-tabelle-tragt-ihre-aussortierten-zeilen-und-einen-haftenden-fehler)
Eine Tabelle aus `NewTable` hat die Quelle `code`, mit `AsSource` den gegebenen Namen, in der
Info-Spalte `source` und als Name ihrer Tabelle aussortierter Zeilen. Die Fundstelle ist der
Zeilenindex, der Rohzustand sind die Werte beim Erstellen. Ein Join zweier Quellen gleichen
Namens ergibt sofort und in einer Pipeline einen Planfehler, ebenso `AsSource` nach einer
Operation.

### T49 — Kennungen sind eindeutig, und row_key verbindet die record_keys eines Joins

**Beweist:** [D76](10-design-decisions.md#d76-kennungen-von-lauf-fehler-und-zeile-haben-eine-feste-form)
`run_id` hat 16 Hex-Zeichen, und `reject_id` bleibt über zwei verbundene Tabellen und zwei
Zweige einer Kette eindeutig. `record_key` ist für gleiche Werte gleich und für andere Werte
verschieden. Der `row_key` einer gescheiterten Join-Zeile verbindet die `record_key`s beider
Seiten, links zuerst.

### T50 — Aggregierte aussortierte Zeilen stehen je Schritt in einer Tabelle und in der Übersicht

**Beweist:** [D77](10-design-decisions.md#d77-aggregierte-aussortierte-zeilen-stehen-je-schritt-in-einer-tabelle-und-auch-in-der-ubersicht), [D48](10-design-decisions.md#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle)
Scheitert eine aggregierte Zeile in einem späteren Schritt, steht sie mit ihren Werten,
ihrem Gruppenschlüssel und `source_rows` in der Tabelle dieses Schritts und mit leerer
Fundstelle in der Übersicht, aber in keiner Tabelle je Quelle. Scheitern zwei Aggregationen
einer Gruppe in der Gruppierung selbst, gibt es zwei Einträge mit derselben `reject_id`.

### T51 — CastAll und WithAll werten jede Spalte gegen die Eingangszeile aus und sortieren eine Zeile einmal aus

**Beweist:** [D78](10-design-decisions.md#d78-castall-und-withall-sind-je-ein-schritt-uber-mehrere-spalten), [D15](10-design-decisions.md#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-ubersichtseintrag-je-betroffener-spalte)
`WithAll` mit zwei Ausdrücken, von denen einer die Spalte des anderen liest, sieht deren Wert
vor dem Schritt. Scheitern in `CastAll` zwei Spalten einer Zeile, fehlt die Zeile im Ergebnis
einmal, und es gibt zwei Einträge mit derselben `reject_id`. Zwei Teiloperationen auf
dieselbe Spalte und eine fremde Teiloperation sind Planfehler.

### T52 — Eine Tabelle mit haftendem Fehler behält die Daten vor der gescheiterten Operation

**Beweist:** [D73](10-design-decisions.md#d73-eine-tabelle-mit-haftendem-fehler-behalt-die-daten-vor-der-gescheiterten-operation)
Nach einer Operation mit unbekannter Spalte hat die Tabelle den haftenden Fehler, und Zeilen,
Spalten und aussortierte Zeilen sind die von vorher. Eine weitere Operation ändert daran
nichts.

### T53 — Excel-Zellen jedes Typs tragen ihre feste Textform

**Beweist:** [D80](10-design-decisions.md#d80-jeder-zelltyp-einer-excel-lieferung-hat-eine-feste-textform)
Eine Excel-Lieferung enthält ein Datum mit Uhrzeit, eine reine Uhrzeit, eine große Zahl, einen
Wahrheitswert, eine Fehlerzelle, eine Formel und eine leere Zeile. Rohzustand und Arbeitsspalte
tragen `2026-09-27T14:30:00`, `14:30:00`, `100000`, `true`, `#DIV/0!` und das Ergebnis der
Formel, die leere Zeile fehlt, und die Zeilennummern der folgenden Zeilen zählen sie mit. Eine
aussortierte Zeile, deren Spalte ein Schritt abgeleitet hat, hat leere `cell` und `display`.

### T54 — Fundstelle und record_key einer CSV-Zeile folgen ihrer physischen Zeile

**Beweist:** [D81](10-design-decisions.md#d81-eine-gelesene-zeile-wird-uber-ihre-physische-zeile-gefunden-und-record_key-besteht-aus-fingerabdruck-und-zeile)
In einer CSV-Lieferung mit einer Leerzeile und einem Feld über zwei Zeilen tragen die Zeilen
danach die Zeilennummer, die ein Editor zeigt, und den Byte-Offset ihres Anfangs. Ihr
`record_key` ist der Fingerabdruck mit dieser Zeilennummer. Dieselben Zellwerte unter anderem
Dateinamen ergeben denselben `record_hash`. Ein Kopf mit doppeltem Spaltennamen ergibt
`unreadable`.

### T55 — Eine vermutliche Umbenennung wird nach Normalisierung und Distanz erkannt

**Beweist:** [D79](10-design-decisions.md#d79-eine-fehlende-und-eine-neue-spalte-gelten-als-vermutlich-umbenannt-wenn-ihre-namen-normalisiert-gleich-oder-nah-beieinander-sind), [D22](10-design-decisions.md#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird)
`Kunden Nr` und `kunden_nr` sowie `Kundennr` und `Kundenr` gelten als vermutlich umbenannt,
`id` und `nr` nicht. Zwei neue Spalten mit derselben kleinsten Distanz ergeben keine Meldung.
Fehlende und neue Spalte werden in jedem Fall gemeldet.

### T56 — Die Quelle aus aussortierten Zeilen behält Schlüssel und Fundstelle

**Beweist:** [D82](10-design-decisions.md#d82-die-quelle-aus-aussortierten-zeilen-ubernimmt-schlussel-und-fundstelle-aus-den-info-spalten), [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle)
Die aussortierten Zeilen eines Laufs über eine CSV-Lieferung werden als Quelle gelesen. Eine
erneut scheiternde Zeile trägt `record_key`, `source`, `line` und `offset` aus dem ersten Lauf,
eine durchlaufende Zeile denselben `record_key`, und die Info-Spalten sind keine Datenspalten.
Eine unzerlegbare Zeile wird mit geänderter Reader-Konfiguration gelesen.

### T57 — Eine Quellzeile zählt einmal, auch nach 1:n-Joins, Gruppierungen und Abbrüchen

**Beweist:** [D84](10-design-decisions.md#d84-eine-quellzeile-zahlt-einmal-aussortiert-vor-durchgelaufen-vor-verworfen-und-nach-einem-abbruch-gibt-es-nicht-verarbeitete-zeilen), [D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben)
Eine linke Zeile, von deren drei Ergebniszeilen eines Joins eine scheitert, zählt einmal als
aussortiert. Eine gescheiterte aggregierte Zeile zählt ihre Quellzeilen als aussortiert. Bricht
ein Lauf ab, stehen die übrigen gelesenen Zeilen unter `nicht verarbeitet`, und die Gleichung
geht für den Lauf und je Schritt auf. Eine Zeile mit zwei Codes zählt bei beiden Codes.

### T58 — Verworfene Zeilen geben den Rohzustand frei, aussortierte behalten ihn als Kopie

**Beweist:** [D87](10-design-decisions.md#d87-ohne-ziel-halt-die-ergebnistabelle-den-rohzustand-ihrer-zeilen-aussortierte-zeilen-behalten-eine-kopie), [D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben)
Ein Lauf über eine gelesene Lieferung verwirft alle Zeilen der ersten Blöcke und sortiert
einige aus. Die Blöcke des Rohzustands ohne durchgelaufene Zeile sind danach freigegeben, die
aussortierten Zeilen zeigen trotzdem ihren Rohzustand, und Zeilen der Ergebnistabelle lassen
sich mit Rohzustand weiter aussortieren.

### T59 — Der Anteil für format_change zählt nur Zeilen, die in den Schritt hineingingen, und ist je Spalte einstellbar

**Beweist:** [D85](10-design-decisions.md#d85-der-anteil-fur-format_change-bezieht-sich-auf-die-zeilen-einer-quelle-die-in-den-schritt-hineingingen-vorlaufig-mit-50), [D59](10-design-decisions.md#d59-die-grenze-fur-einen-formatanderungs-befund-ist-einstellbar-die-voreinstellung-wird-im-prototyp-festgelegt)
Filtert ein Schritt vorher die Hälfte der Zeilen weg, meldet ein Schritt, in dem die Hälfte der
übrigen scheitert, `format_change` mit der vorläufigen Grenze 0,5. Eine höhere Grenze für die
Spalte unterdrückt den Befund. `missing_column` ergibt keinen `format_change`. Der Befund zeigt
höchstens fünf verschiedene Werte.

### T60 — Nach einem Zweig stehen die Zeilen in Eingangsreihenfolge, und Operationen über alle Zeilen im Zweig sind Planfehler

**Beweist:** [D90](10-design-decisions.md#d90-zweige-enthalten-nur-blockweise-schritte-und-zeilen-behalten-nach-dem-zusammenfuhren-ihre-reihenfolge)
Dieselbe Pipeline mit Fehlerzweig und `Split` liefert bei verschiedenen Blocklängen dieselben
Zeilen in der Reihenfolge der Lieferung. Ein Sortieren im Zweig und ein Fehlerzweig an einer
Gruppierung ergeben `plan_error`, bevor eine Zeile gelesen wird.

### T61 — Der Modus "immer ändern" ändert keine Tabelle, die Quelle der Pipeline ist

**Beweist:** [D93](10-design-decisions.md#d93-spalten-einer-tabelle-als-quelle-gelten-als-geteilt-auch-im-modus-immer-andern)
Eine Pipeline im Modus "immer ändern" über eine Tabelle, deren Spalte schon umgewandelt ist,
ändert diese Spalte. Die Tabelle hat danach ihre alten Werte, und der Trace vermerkt die Kopie.

Die Tabelle "welcher Test beweist welchen Fall" entsteht mit den ersten Tests. Ihr Format
gibt der Parser des Docs-Gates vor ([G14](70-gap-ledger.md#g14-docs-gates-aus-dem-archivar-repo-ubernehmen)).

## Welcher Test beweist welchen Fall

Nachtrag zu [F24](20-feature-catalogue.md#f24-docs-gates-fur-das-design-set) nach [D37](10-design-decisions.md#d37-die-docs-gates-werden-in-diesem-repo-selbst-gebaut), 2026-09-27. Jede Zeile nennt einen T-Fall
und die Go-Tests, die ihn mit `Proves(t, "T<n>")` beweisen: Testnamen in Backticks, mehrere
durch Komma getrennt, zum Beispiel `` `TestRejectsKeepRawState` ``. `ausstehend` heißt, dass
es noch keinen Test gibt. `TestPlanMappingConsistency` gleicht die Tabelle in beiden
Richtungen mit den `Proves`-Aufrufen im Modul `experimental/v2` ab: Ein genannter Test muss
den Fall beweisen, und jeder Test, der ihn beweist, muss hier stehen. Eine Zeile mit
`ausstehend` wird ungültig, sobald ein Test den Fall beweist.

| T-Fall | Tests |
|---|---|
| [T1](#t1-aussortierte-zeilen-tragen-den-rohzustand-nicht-den-arbeitszustand) | `TestRejectsCarryRawStateNotWorkingState` |
| [T2](#t2-aussortierte-zeilen-lassen-sich-mit-den-datei-writern-schreiben-und-mit-dem-passenden-reader-wieder-lesen) | ausstehend |
| [T3](#t3-im-modus-stoppen-endet-der-lauf-beim-ersten-datenfehler-im-modus-aussortieren-nicht) | ausstehend |
| [T4](#t4-die-schwelle-markiert-den-lauf-als-fehlgeschlagen-und-lasst-ihn-standardmaig-zu-ende-laufen) | `TestThresholdFailsTheRunAndRunsToTheEnd` |
| [T5](#t5-ohne-angaben-kommt-ein-lauf-uber-eine-schmutzige-lieferung-durch) | `TestDirtyDeliveryRunsThroughWithDefaults` |
| [T6](#t6-eine-im-schritt-scheiternde-zeile-steht-einmal-in-ihrer-tabelle-je-quelle-hat-einen-ubersichtseintrag-je-spalte-und-erreicht-spatere-schritte-nicht) | `TestRowFailingInOneStepIsRejectedOnceWithAnEntryPerColumn` |
| [T7](#t7-aussortierte-zeilen-gibt-es-je-quelle-und-die-ubersicht-stimmt-mit-ihnen-uberein) | `TestRejectsPerSourceAndOverviewAgree` |
| [T8](#t8-scheitert-eine-ergebniszeile-nach-einem-join-sind-ihre-quellzeilen-mit-gemeinsamer-kennung-aussortiert) | `TestFailedJoinRowRejectsItsSourceRowsTogether` |
| [T9](#t9-nach-einer-gruppierung-werden-aggregierte-zeilen-aussortiert-und-der-speicher-bleibt-im-budget) | ausstehend |
| [T10](#t10-unzerlegbare-zeilen-werden-mit-rohbytes-und-richtiger-fundstelle-aussortiert) | `TestUnparseableLinesAreRejectedWithRawBytes` |
| [T11](#t11-die-fundstelle-bleibt-uber-sortieren-und-filtern-richtig) | `TestLocationStaysRightOverSortAndFilter` |
| [T12](#t12-nachverarbeitung-behalt-ursprungliche-fundstelle-und-schlussel) | ausstehend |
| [T13](#t13-record_key-ist-innerhalb-einer-lieferung-stabil-ein-fachlicher-schlussel-daruber-hinaus) | ausstehend |
| [T14](#t14-die-prufung-des-kopfs-erkennt-fehlende-neue-und-umbenannte-spalten) | `TestHeaderCheckFindsMissingNewAndRenamedColumns` |
| [T15](#t15-planfehler-verhindern-den-lauf-liefer-und-datenfehler-folgen-der-konfiguration-je-code) | `TestPlanErrorsStopTheRunAndOtherErrorsFollowTheirCode` |
| [T16](#t16-status-und-zahlungen-sind-konsistent-und-bilden-auf-exit-codes-ab) | `TestStatusAndCountsAreConsistent` |
| [T17](#t17-gehaufte-formatfehler-erscheinen-im-anderungsbericht-mit-beispielen) | `TestFormatChangesAppearInTheChangeReport` |
| [T18](#t18-der-profilvergleich-meldet-eine-abweichung-ohne-dass-eine-zeile-scheitert) | ausstehend |
| [T19](#t19-ein-fehlerzweig-sieht-den-zustand-vor-dem-schritt-und-fuhrt-verarbeitetes-zuruck) | ausstehend |
| [T20](#t20-zweige-werden-nach-namen-zusammengefuhrt-und-typkonflikte-fallen-vor-dem-lauf-auf) | ausstehend |
| [T21](#t21-eine-im-zweig-erneut-scheiternde-zeile-behalt-kennung-weg-und-vorigen-grund-und-zahlt-einmal) | ausstehend |
| [T22](#t22-dieselbe-pipeline-liefert-im-speicher-und-im-streaming-dasselbe-ergebnis) | ausstehend |
| [T23](#t23-ein-lauf-uber-mehr-daten-als-das-budget-halt-das-budget-ein) | ausstehend |
| [T24](#t24-nach-einem-lauf-bleibt-nichts-neben-den-zielen-zuruck) | ausstehend |
| [T25](#t25-kein-zweig-sieht-anderungen-eines-anderen-in-jedem-modus) | ausstehend |
| [T26](#t26-die-modi-fur-kopieren-und-andern-liefern-dasselbe-ergebnis) | ausstehend |
| [T27](#t27-nullwerte-sind-vom-leeren-text-getrennt-und-verhalten-sich-wie-in-sql) | `TestNullsAreSeparateFromEmptyTextAndBehaveLikeSQL` |
| [T28](#t28-eine-closure-die-einen-fehler-meldet-sortiert-die-zeile-mit-codecustom-aus) | `TestClosureErrorRejectsRowWithCodeCustom` |
| [T29](#t29-der-datenbank-writer-ist-bei-nachverarbeitung-idempotent) | ausstehend |
| [T30](#t30-der-http-writer-liefert-jede-zeile-trotz-vorubergehender-fehler-aus) | ausstehend |
| [T31](#t31-die-offentliche-api-enthalt-keine-gseq-typen) | `TestPublicAPIHasNoGseqTypes` |
| [T32](#t32-eine-v1-tabelle-ubersteht-den-weg-uber-v2-zuruck-nach-v1-unverandert) | ausstehend |
| [T33](#t33-ein-ziel-das-einen-block-ablehnt-beendet-den-lauf-sofort-mit-sink_error) | ausstehend |
| [T34](#t34-excel-zellen-tragen-den-gespeicherten-wert-in-fester-textform-aussortierte-zeilen-auch-den-angezeigten-text) | `TestExcelCellsCarryStoredValueAndDisplay` |
| [T35](#t35-grenzen-fur-feldlange-und-entpackten-umfang-greifen-und-ausgelagerte-dateien-sind-geschutzt) | ausstehend |
| [T36](#t36-bei-mehreren-zutreffenden-status-gilt-der-hochste-und-das-ergebnis-nennt-alle-befunde) | ausstehend |
| [T37](#t37-die-kennung-einer-lieferung-steht-beim-offnen-fest-und-andert-sich-mit-der-datei) | `TestDeliveryIDIsFixedAtOpen` |
| [T38](#t38-im-modus-immer-andern-wird-eine-mit-dem-rohzustand-geteilte-spalte-einmal-kopiert) | ausstehend |
| [T39](#t39-gomemlimit-wird-nur-auf-wunsch-gesetzt-und-laufe-in-einem-prozess-teilen-ein-budget) | ausstehend |
| [T40](#t40-mit-writern-im-plan-werden-aussortierte-zeilen-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close) | ausstehend |
| [T41](#t41-eine-tabelle-tragt-aussortierte-zeilen-und-einen-haftenden-fehler) | `TestTableCarriesRejectsAndStickyError` |
| [T42](#t42-zwei-sheets-einer-excel-datei-sind-zwei-quellen-und-ein-fehlendes-sheet-ist-ein-lieferfehler) | `TestTwoSheetsAreTwoSourcesAndAMissingSheetIsADeliveryError` |
| [T43](#t43-die-schwelle-bricht-bei-einem-anteil-erst-nach-der-mindestzahl-ab-bei-einer-absoluten-grenze-sofort) | `TestThresholdAbortsAfterTheMinimumForAShareAndAtOnceForAnAbsoluteLimit` |
| [T44](#t44-ein-join-tragt-die-aussortierten-zeilen-und-haftenden-fehler-beider-tabellen-und-gleichnamige-spalten-sind-ein-planfehler) | `TestJoinCarriesRejectsAndErrorsOfBothTables` |
| [T45](#t45-ganzzahl-und-gleitkomma-werden-erweitert-die-division-ergibt-gleitkomma) | `TestNumbersWidenAndDivisionIsFloat` |
| [T46](#t46-nullwerte-stehen-beim-sortieren-hinten-und-das-sortieren-ist-stabil) | `TestSortPutsNullsLastAndIsStable` |
| [T47](#t47-cast-ist-ohne-option-streng-und-mit-lenient-nachsichtig-wie-v1) | `TestCastIsStrictByDefaultAndLenientLikeV1` |
| [T48](#t48-eine-im-code-gebaute-tabelle-ist-eine-benannte-quelle-und-gleichnamige-quellen-in-einem-join-sind-ein-planfehler) | `TestCodeTableIsANamedSource` |
| [T49](#t49-kennungen-sind-eindeutig-und-row_key-verbindet-die-record_keys-eines-joins) | `TestIdentifiersHaveFixedForms` |
| [T50](#t50-aggregierte-aussortierte-zeilen-stehen-je-schritt-in-einer-tabelle-und-in-der-ubersicht) | `TestAggregatedRejectsStandPerStepAndInTheOverview` |
| [T51](#t51-castall-und-withall-werten-jede-spalte-gegen-die-eingangszeile-aus-und-sortieren-eine-zeile-einmal-aus) | `TestCastAllAndWithAllAreOneStep` |
| [T52](#t52-eine-tabelle-mit-haftendem-fehler-behalt-die-daten-vor-der-gescheiterten-operation) | `TestStickyErrorKeepsTheDataBeforeTheFailedOperation` |
| [T53](#t53-excel-zellen-jedes-typs-tragen-ihre-feste-textform) | `TestExcelCellTypesHaveAFixedTextForm` |
| [T54](#t54-fundstelle-und-record_key-einer-csv-zeile-folgen-ihrer-physischen-zeile) | `TestLocationAndKeyFollowThePhysicalLine` |
| [T55](#t55-eine-vermutliche-umbenennung-wird-nach-normalisierung-und-distanz-erkannt) | `TestProbablyRenamedColumns` |
| [T56](#t56-die-quelle-aus-aussortierten-zeilen-behalt-schlussel-und-fundstelle) | `TestRejectsAsSourceKeepKeysAndLocation` |
| [T57](#t57-eine-quellzeile-zahlt-einmal-auch-nach-1n-joins-gruppierungen-und-abbruchen) | `TestSourceRowCountsOnce` |
| [T58](#t58-verworfene-zeilen-geben-den-rohzustand-frei-aussortierte-behalten-ihn-als-kopie) | `TestDroppedRowsReleaseTheRawState` |
| [T59](#t59-der-anteil-fur-format_change-zahlt-nur-zeilen-die-in-den-schritt-hineingingen-und-ist-je-spalte-einstellbar) | `TestFormatChangeShareCountsTheRowsThatWentIntoTheStep` |
| [T60](#t60-nach-einem-zweig-stehen-die-zeilen-in-eingangsreihenfolge-und-operationen-uber-alle-zeilen-im-zweig-sind-planfehler) | ausstehend |
| [T61](#t61-der-modus-immer-andern-andert-keine-tabelle-die-quelle-der-pipeline-ist) | ausstehend |
