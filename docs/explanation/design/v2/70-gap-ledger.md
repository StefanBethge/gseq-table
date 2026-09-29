# Gap Ledger

Inkonsistenzen, Annahmen und Lücken des Design-Sets. Geschlossene Einträge nennen, was
sie geschlossen hat. Akzeptierte Einträge nennen ihre Revisit-Bedingung.

### G1 — Was der Rohzustand einer Zeile umfasst

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
[D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten)
legt fest, dass aussortierte Zeilen im Rohzustand zurückkommen. Offen ist, ob das die
Zellwerte sind, wie der Reader sie gelesen hat, oder auch die Rohbytes der Quelle, etwa
eine CSV-Zeile, die gar nicht erst in Zellen zerlegt werden konnte. Offen ist auch, wie
der Rohzustand bei großen Lieferungen verfügbar bleibt, ohne dass jede Zeile ihn mitführt
([UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen),
[UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)).

### G2 — Welche Info-Spalten eine aussortierte Zeile trägt

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix) und [D15](10-design-decisions.md#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-ubersichtseintrag-je-betroffener-spalte)
Kandidaten: Schritt, Grund, betroffene Spalte, Rohwert, Fundstelle in der Quelle (Datei,
Sheet, Zeile), Lauf. Offen sind außerdem die Namen der Spalten, damit sie nicht mit
Datenspalten kollidieren, und ob eine Zeile mehrere Gründe tragen kann
([UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)).

### G3 — Bemessung und Wirkung der Schwelle

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D20](10-design-decisions.md#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen) und [D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst)
[D4](10-design-decisions.md#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen)
führt die Schwelle ein. Offen: absolut oder als Anteil, für den ganzen Lauf oder je
Schritt, und ob der Lauf beim Überschreiten sofort stoppt oder bis zum Ende läuft und dann
als fehlgeschlagen gilt. Offen ist auch, wie der Scheduler das Ergebnis erfährt
([UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)).

### G4 — Verarbeitungsmodell für große Lieferungen

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)
[UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)
verlangt, dass dieselbe Pipeline große Lieferungen verarbeitet, ohne sie vollständig zu
laden. Offen ist, wie Streaming und In-Memory-Verarbeitung zusammenspielen und was mit
Schritten passiert, die alle Zeilen brauchen (Sortieren, Gruppieren, Joins). Soll durch
den Prototyp geklärt werden.

### G5 — Ob es eine veränderbare Tabelle braucht

**Type:** Assumption · **Kind:** verify · **Status:** offen
Annahme: Unveränderliche Tabellen mit geteilten Spalten erreichen bei Laufzeit und
Speicher dieselben Ergebnisse wie eine veränderbare Tabelle. Dann entfällt die
veränderbare Variante. Der Maintainer braucht sie nicht, wenn es ohne sie gleich gut geht
([D5](10-design-decisions.md#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen),
[UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)).
Soll durch Messung im Prototyp gegen v1 `MutableTable` geprüft werden.
Seit [D7](10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert)
entscheidet die Engine über Kopieren oder Ändern. Die Annahme lautet damit: Ändern an Ort
und Stelle, wo Daten nicht geteilt sind, erreicht Laufzeit und Speicher von v1
`MutableTable`. Fällt die Messung negativ aus, wird [D7](10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert) über eine neue Decision
wieder aufgemacht.

### G6 — Writer für Datenbank und HTTP

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http)
[D2](10-design-decisions.md#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse)
schreibt aussortierte Zeilen und Ergebnisse über Writer. Offen ist, ob die Library Writer
für Datenbank und HTTP mitbringt oder nur eine Schnittstelle, die der Pipeline-Entwickler
selbst umsetzt ([UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)).

### G7 — Wirkung des harten Schalters für Kopieren und Ändern

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D8](10-design-decisions.md#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert)
[D7](10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert)
sieht einen harten Schalter am Anfang der Pipeline vor. Offen ist, was er festlegt: immer
kopieren (nachvollziehbar, für Debugging), immer an Ort und Stelle ändern (schnell, aber
nur ohne Zweige sicher), oder die Ausführungsart (Speicher, Streaming, automatisch).

### G8 — "Immer ändern" gegen Zweige und Rohzustand

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert)
Der Modus "immer an Ort und Stelle ändern" aus [D8](10-design-decisions.md#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert) verträgt sich nicht ohne Weiteres mit
zwei anderen Zusagen. Erstens garantiert
[D7](10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert),
dass ein Zweig nie Daten verändert, die ein anderer Zweig sieht
([UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)).
Zweitens verlangt
[D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten),
dass aussortierte Zeilen im Rohzustand zurückkommen. Kandidaten: Der Rohzustand wird
getrennt von den Arbeitsdaten gehalten, sodass Ändern ihn nie berührt. An Verzweigungen
kopiert die Engine auch in diesem Modus, oder sie lehnt den Plan vor dem Lauf ab.

### G9 — Rohzustand und Fundstelle nach Aggregation und Join

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D11](10-design-decisions.md#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert) und [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert)
[D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle) ordnet jeder Zeile einen Rohzustand und eine Fundstelle zu. Nach einem Join besteht
eine Ergebniszeile aus zwei Quellzeilen, nach einer Gruppierung aus vielen. Offen ist,
welchen Rohzustand und welche Fundstelle eine solche Zeile trägt, wenn sie in einem
späteren Schritt scheitert, und was nach "durchgekommen" bedeutet, wenn eine Quellzeile in
mehrere Ergebniszeilen eingeht
([UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)).

### G10 — Aussortierte Zeilen mehrerer Quellen: eine Tabelle oder je Quelle

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D13](10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen)
Nach [D11](10-design-decisions.md#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert) können aussortierte Zeilen aus verschiedenen Quellen mit verschiedenen Spalten
stammen. Offen ist, ob es je Quelle eine eigene Tabelle aussortierter Zeilen gibt oder
eine gemeinsame Tabelle mit einer Spalte für die Quelle, in der die Rohspalten je nach
Quelle unterschiedlich belegt sind. Hängt mit
[G2](#g2-welche-info-spalten-eine-aussortierte-zeile-tragt) zusammen.

### G11 — Form der aussortierten Zeilen im Prototyp validieren

**Type:** Assumption · **Kind:** verify · **Status:** offen
[D13](10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen), [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix) und [D15](10-design-decisions.md#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-ubersichtseintrag-je-betroffener-spalte) legen die Form der aussortierten Zeilen fest. Der Maintainer will
sie nach dem Prototyp noch einmal validieren, anhand eigener Beispiel-Lieferungen nach [D60](10-design-decisions.md#d60-der-prototyp-wird-mit-eigenen-beispiel-lieferungen-der-1brc-datei-und-in-docker-mit-verschiedenen-speicher-limits-erprobt): ob die
Info-Spalten reichen, ob "je Quelle plus Übersicht" im Alltag handlich ist, und ob ein
Übersichtseintrag je Spalte nach [D44](10-design-decisions.md#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler) beim Lesen hilft oder stört.

### G12 — Ab wann eine Häufung von Fehlern als Formatänderung gilt

**Type:** Gap · **Kind:** verify · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D59](10-design-decisions.md#d59-die-grenze-fur-einen-formatanderungs-befund-ist-einstellbar-die-voreinstellung-wird-im-prototyp-festgelegt)
[D23](10-design-decisions.md#d23-das-laufergebnis-enthalt-einen-anderungsbericht) meldet `format_change`, wenn ein erheblicher Teil der Werte einer Spalte mit
demselben Fehlercode scheitert. Offen ist die Grenze: fester Anteil, konfigurierbar, oder
relativ zum Profil eines früheren Laufs ([D24](10-design-decisions.md#d24-ein-lauf-kann-ein-profil-liefern-das-mit-dem-profil-eines-fruheren-laufs-verglichen-wird)). Soll an echten Lieferungen im Prototyp
erprobt werden.

### G13 — Vorteil spaltenorientierter Blöcke und Voreinstellungen für Budget und Blocklänge

**Type:** Assumption · **Kind:** verify · **Status:** offen
Annahme: Blöcke typisierter Spalten nach [D29](10-design-decisions.md#d29-daten-laufen-in-blocken-typisierter-spalten-rohspalten-bleiben-bis-zum-cast-text) sind bei Laufzeit und Speicher messbar
besser als die zeilenbasierte Speicherung aus v1. Offen sind außerdem die Voreinstellungen
für den Anteil des Speichers ([D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern)) und die Blockgröße. Ebenfalls offen: ob das Lesen großer Excel-Dateien im Budget bleibt ([D52](10-design-decisions.md#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert)). Soll im Prototyp mit Benchmarks
gegen v1 geklärt werden
([UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)).
Zusätzlich zu messen: Spitzenspeicher und Auslagern mit und ohne Rohzustand ([D55](10-design-decisions.md#d55-roh-und-arbeitsdaten-teilen-spalten-bis-ein-schritt-eine-spalte-andert)).
Zusätzlich zu messen: Spitzenspeicher des Prozesses im Verhältnis zum Budget, mit und ohne
von der Engine gesetztes `GOMEMLIMIT`.

### G14 — Docs-Gates aus dem Archivar-Repo übernehmen

**Type:** Gap · **Kind:** verify · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D37](10-design-decisions.md#d37-die-docs-gates-werden-in-diesem-repo-selbst-gebaut)
Die Methode prüft IDs, Links und die Zuordnung von Tests zu T-Fällen mit
`TestDocsIDConsistency` und `TestPlanMappingConsistency`. Die Referenzfassung liegt in
`internal/testutil/docs_idlint_test.go` im Archivar-Repo, auf das beim Entwurf kein Zugriff
bestand. Bis die Gates laufen, sind die Heading-Anker in den Links von Hand erzeugt und
ungeprüft. Das Format der Tabelle "welcher Test beweist welchen Fall" im
[Test-Plan](30-test-plan.md) wird ebenfalls erst mit dem Parser festgelegt. Das Gate kennt
laut Methode die UC-Familie noch nicht.

### G15 — Zustellzusage des HTTP-Writers

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D41](10-design-decisions.md#d41-der-http-writer-liefert-mindestens-einmal-und-schickt-einen-idempotenzschlussel-mit)
[D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http)
sieht Wiederholung vor. Bleibt nach einer Anfrage unklar, ob der Empfänger sie verarbeitet
hat (Zeitüberschreitung), führt Wiederholung zu doppelter Zustellung. Offen ist, ob der
Writer "mindestens einmal" zusagt und den `record_key` aus
[D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash)
zur Deduplizierung beim Empfänger mitschickt, oder ob er gar nicht wiederholt
([T30](30-test-plan.md#t30-der-http-writer-liefert-jede-zeile-trotz-vorubergehender-fehler-aus)).

### G16 — Konkrete Werte für die Exit-Kriterien des Prototyps

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D60](10-design-decisions.md#d60-der-prototyp-wird-mit-eigenen-beispiel-lieferungen-der-1brc-datei-und-in-docker-mit-verschiedenen-speicher-limits-erprobt)
Der [Scope des Prototyps](40-scope-prototype.md) verlangt einen Lauf über eine Lieferung,
die ein Vielfaches des Budgets groß ist. Größe der Lieferung und Budget legt der
Maintainer vor Beginn des Prototyps fest.

### G17 — Aufräumen nach einem vom System beendeten Lauf

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D38](10-design-decisions.md#d38-ein-spaterer-lauf-entfernt-verwaiste-ausgelagerte-daten)
Wird ein Lauf vom System beendet (Speicher), bleiben ausgelagerte Daten liegen
([Failure Modes](50-failure-modes.md), Zeile "Speicher reicht trotz Budget nicht"). Offen
ist, ob ein späterer Lauf verwaiste Daten erkennt und entfernt, und woran.
[T24](30-test-plan.md#t24-nach-einem-lauf-bleibt-nichts-neben-den-zielen-zuruck) deckt
diesen Fall nicht ab.

### G18 — Verhalten bei Panik in einer Closure

**Type:** Assumption · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D39](10-design-decisions.md#d39-eine-panik-in-einer-closure-wird-wie-ein-zuruckgegebener-fehler-behandelt)
Die [Failure Modes](50-failure-modes.md) nehmen an, dass eine Panik in einer Closure wie
ein zurückgegebener Fehler behandelt wird
([D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg)).
Das verhindert Abbrüche durch fremden Code, kann aber echte Programmierfehler als
Datenfehler tarnen. Nicht vom Maintainer entschieden.

### G19 — Fehler beim Schreiben ins Ziel

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D40](10-design-decisions.md#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab)
Keine Decision legt fest, was passiert, wenn ein Ziel einen Block ablehnt oder das
Schreiben der aussortierten Zeilen scheitert ([Failure Modes](50-failure-modes.md),
Abschnitt Ziele). [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler)
kennt Plan-, Liefer- und Datenfehler, aber keine Fehler des Ziels. Scheitert das Schreiben
der aussortierten Zeilen still, gehen genau die Daten verloren, die
[D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten)
bewahren soll.

### G20 — Grenzen für Feldlänge und entpackten Umfang

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D56](10-design-decisions.md#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch)
Aus den [Security Boundaries](60-security-boundaries.md): Grenzen für einzelne Feldgrößen
und für die entpackte Größe von Excel-Archiven, Dateirechte der ausgelagerten Daten, und
ob der CSV-Writer Werte maskiert, die als Formel interpretiert würden. Keines davon ist
entschieden.

### G21 — Eine Formatänderung meldet mit Voreinstellungen einen erfolgreichen Lauf

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D42](10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error)
Mit den Voreinstellungen aus [D5](10-design-decisions.md#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen) und ohne Schwelle nach [D4](10-design-decisions.md#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen) sortiert eine fehlende Spalte nach [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler) alle Zeilen aus, und der Lauf meldet `ok`. Die Begründung von [D4](10-design-decisions.md#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen) sagt selbst, dass das die Änderung verdeckt, und [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler) verlässt sich auf eine Schwelle, die per Voreinstellung nicht gesetzt ist. Betrifft [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt) und [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline). Optionen: eine Standard-Schwelle (z. B. alle Zeilen einer Quelle), Lieferfehler per Voreinstellung auf "stoppen", oder ein eigener Status für Läufe mit Befunden aus [D23](10-design-decisions.md#d23-das-laufergebnis-enthalt-einen-anderungsbericht).

### G22 — Aussortierte aggregierte Zeilen haben keinen Ort und lassen sich nicht nachverarbeiten

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D48](10-design-decisions.md#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle)
Nach [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert) trägt eine nach der Gruppierung aussortierte Zeile weder Rohspalten noch eine einzelne Quelle. [D13](10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen) verlangt aber Tabellen je Quelle mit deren Rohspalten, und nach Join und Gruppierung umfasst eine Gruppe zwei Quellen. Fundstelle und `record_key` aus [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix) sind für sie undefiniert, [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) kann sie nicht wieder einlesen, und [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet) scheitert für jede Validierung nach einer Aggregation. [D25](10-design-decisions.md#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck) verspricht den Rohzustand im Zweig, den [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert) nach einer Aggregation schon freigegeben hat. Außerdem enthält die Begründung von [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert) die unbelegte Annahme, dass sich Quellzeilen über den Gruppenschlüssel in der Lieferung finden lassen. Das scheitert, wenn die Lieferung nicht mehr vorliegt ([D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle) verwirft genau deshalb das Nachlesen) oder der Schlüssel ein abgeleiteter Wert ist. Optionen: eine eigene Tabelle für aggregierte aussortierte Zeilen, die [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) ausdrücklich ausnimmt; mit voller Herkunft die Quellzeilen in ihre Tabellen zurückgeben; volle Herkunft erzwingen, wenn nach einer Aggregation validiert oder verzweigt wird.

### G23 — Absichtlich verworfene Zeilen fehlen in Zählung und Freigabe des Rohzustands

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben)
Eine Zeile, die ein Filter absichtlich verwirft, ist weder durchgelaufen noch aussortiert. Dasselbe gilt für Zeilen ohne Partner im Inner Join und für Zweige ohne Ziel. Damit gilt die Gleichung aus [T16](30-test-plan.md#t16-status-und-zahlungen-sind-konsistent-und-bilden-auf-exit-codes-ab) nicht mehr, und nach [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert) wird ihr Rohzustand nie freigegeben. Im Streaming wäre das ein Speicherleck. Mit der Null-Semantik aus [D30](10-design-decisions.md#d30-es-gibt-echte-nullwerte-getrennt-vom-leeren-text) verschwinden Zeilen in Filtern still, genau der v1-Mangel, den [D30](10-design-decisions.md#d30-es-gibt-echte-nullwerte-getrennt-vom-leeren-text) beheben soll. Vorschlag: eine gezählte Kategorie "verworfen" je Schritt, und die Freigabe gilt für jedes Verlassen des Plans.

### G24 — 1:n-Joins: Identität der Ergebniszeilen und teilweiser Erfolg

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D47](10-design-decisions.md#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen)
Verbindet ein Join eine linke Zeile mit fünf rechten und scheitert eine der fünf Ergebniszeilen, sortiert [D11](10-design-decisions.md#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert) die linke Quellzeile aus, obwohl vier ihrer Ergebniszeilen schon im Ziel sind. Eine rechte Stammdatenzeile wird je gescheitertem Partner aussortiert und überflutet Tabelle und Schwelle. Eine Nachverarbeitung nach [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) erzeugt alle fünf Ergebniszeilen erneut, und ein Upsert auf `record_key` ([D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http), [T29](30-test-plan.md#t29-der-datenbank-writer-ist-bei-nachverarbeitung-idempotent)) fasst fünf verschiedene Zeilen zu einer zusammen. [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash) sagt nicht, welchen Schlüssel eine Ergebniszeile nach einem Join trägt. [T8](30-test-plan.md#t8-scheitert-eine-ergebniszeile-nach-einem-join-sind-ihre-quellzeilen-mit-gemeinsamer-kennung-aussortiert) deckt 1:n nicht ab.

### G25 — Ein Eintrag je Spalte vervielfacht Rohzeilen und macht Zählungen mehrdeutig

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D44](10-design-decisions.md#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler)
Nach [D15](10-design-decisions.md#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-ubersichtseintrag-je-betroffener-spalte) erzeugt eine in drei Spalten scheiternde Zeile drei Einträge in der Tabelle je Quelle, jeder mit der vollen Rohzeile. [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) liest die Tabelle wieder ein und verarbeitet die Zeile dann dreimal. Die absolute Schwelle aus [D20](10-design-decisions.md#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen) und die Zählungen je Code aus [D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst) lassen offen, ob Einträge oder Quellzeilen gezählt werden. Optionen: je Quellzeile eine Zeile in der Tabelle je Quelle und Einzelheiten nur in der Übersicht; [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) dedupliziert nach `reject_id`; Zählungen immer nach verschiedenen Quellzeilen.

### G26 — Wo die aussortierten Zeilen großer Läufe liegen

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close)
Bei einer umfangreichen Lieferung, in der alle Zeilen scheitern, ist die Tabelle aussortierter Zeilen so groß wie die Lieferung. Liegt sie nach dem Lauf im Ergebnis, muss sie im Speicher liegen (widerspricht [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)) oder in ausgelagerten Dateien, die [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern) und [T24](30-test-plan.md#t24-nach-einem-lauf-bleibt-nichts-neben-den-zielen-zuruck) am Ende löschen. [D2](10-design-decisions.md#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse) sagt nicht, ob der Writer für aussortierte Zeilen vor dem Lauf im Plan hängt und während des Laufs schreibt, oder ob er danach aus dem Ergebnis schreibt. Optionen: Writer für aussortierte Zeilen stehen im Plan und schreiben im Streaming, das Ergebnis hält nur Übersicht und Zählungen; oder das Ergebnis liest aussortierte Zeilen verzögert, und die ausgelagerten Dateien leben bis zu einem `Close`.

### G27 — Schwelle als Anteil im Streaming

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D45](10-design-decisions.md#d45-die-schwelle-bezieht-anteile-auf-bisher-gelesene-zeilen-gilt-bei-einer-der-grenzen-und-bricht-erst-nach-einer-mindestzahl-ab)
Der Anteil aus [D20](10-design-decisions.md#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen) bezieht sich auf die gelesenen Zeilen, deren Gesamtzahl im Streaming nicht vorab bekannt ist. Mit sofortigem Abbruch ergibt die erste aussortierte von einer gelesenen Zeile 100 % und bricht ab. Offen sind der Nenner bei einer Schwelle je Schritt, die Zählung nach Aggregationen, ob "beides" UND oder ODER bedeutet, und ob ein Abbruch an der Schwelle `failed_threshold` oder `aborted` ergibt ([D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst)). Braucht eine Mindestmenge vor einem vorzeitigen Abbruch.

### G28 — Abgeschnittene und unlesbare Lieferungen

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D42](10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error)
Die [Failure Modes](50-failure-modes.md) sortieren bei einer abgeschnittenen Übertragung nur die letzte Zeile aus. Der Lauf meldet `ok`, obwohl die halbe Lieferung fehlt, und [D23](10-design-decisions.md#d23-das-laufergebnis-enthalt-einen-anderungsbericht) kennt keinen Befund dafür. "Datei fehlt" und "Excel beschädigt" stoppen dort ohne Wahl, obwohl [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler) Lieferfehler konfigurierbar macht und sie beim Lesen des Kopfs verortet. Braucht Codes wie `truncated` und `unreadable`, und eine Aussage in [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler), welche Lieferfehler sich nicht aussortieren lassen, weil es keine Zeilen gibt.

### G29 — record_key ist positionsabhängig

**Type:** Assumption · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash)
[D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash) bildet den Schlüssel aus Quelle und Fundstelle. Fügt der Anbieter oben eine Zeile ein, verschieben sich alle späteren Schlüssel, und ein Upsert überschreibt falsche Datensätze. Kommt jeden Tag eine Datei mit demselben Namen, überschreibt Zeile 17 von Tag 2 die von Tag 1. HTTP- und Streaming-Quellen haben weder festen Namen noch Zeilen. Als Dedup-Schlüssel entfernt `record_hash` gleiche, aber legitime Zeilen (zwei gleiche Bestellpositionen). "Stabil" gilt nur für die Nachverarbeitung derselben Lieferung. Optionen: Zusage auf "dieselbe Lieferung" beschränken, eine Lieferungskennung (Hash der Datei) in den Schlüssel aufnehmen, Hash plus Vorkommenszähler, Upsert auf fachliche Schlüssel empfehlen.

### G30 — Nachverarbeitung über mehrere Quellen und mit anderem Präfix

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle)
Nach einem Join-Fehler braucht die Nachverarbeitung der linken aussortierten Zeilen die vollständige rechte Tabelle, nicht nur deren aussortierte Zeilen. [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) spricht nur von einer Quelle und sagt nicht, wie sie im Plan eine von mehreren Quellen ersetzt. Eine Nachverarbeitung mit anderem Präfix nach [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix) erkennt die Info-Spalten nicht.

### G31 — Fehlerweg der sofortigen Table-Methoden

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D50](10-design-decisions.md#d50-eine-tabelle-tragt-ihre-aussortierten-zeilen-und-einen-haftenden-fehler)
Eine sofortige `Table`-Methode nach [D31](10-design-decisions.md#d31-jede-operation-gibt-es-einmal-als-wert-mit-zwei-einstiegen-sofort-auf-einer-tabelle-oder-im-plan) trifft auf einen fehlerhaften Wert. Fehlerverhalten, Schwelle, Status und Tabellen aussortierter Zeilen hängen aber an der Pipeline ([D3](10-design-decisions.md#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen), [D5](10-design-decisions.md#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen), [D13](10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen), [D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst)). Eine im Code gebaute Tabelle hat keinen Rohzustand und keine Fundstelle nach [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle). [T22](30-test-plan.md#t22-dieselbe-pipeline-liefert-im-speicher-und-im-streaming-dasselbe-ergebnis) verlangt gleiche aussortierte Zeilen bei Table-Methoden und Pipeline, was ohne diese Klärung nicht definiert ist. Optionen: Rückgabe von Tabelle, aussortierten Zeilen und Fehler; Table-Methoden stoppen immer; eine Tabelle trägt eine Fehlerkonfiguration.

### G32 — Rohzustand und Fundstelle bei Excel

**Type:** Assumption · **Kind:** verify · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D52](10-design-decisions.md#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert)
Excel ist das häufigste Lieferformat ([UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)), aber [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle) und [D29](10-design-decisions.md#d29-daten-laufen-in-blocken-typisierter-spalten-rohspalten-bleiben-bis-zum-cast-text) sind am Beispiel CSV formuliert. Excel-Zellen sind typisiert: Datum als Seriennummer oder formatierter Text, Formeln mit gespeichertem Wert, Zahlen als `1E+05` oder `100000`. Offen ist, welche Darstellung der Rohzustand ist, ob die Zeilennummer die Sheet-Zeile oder die Datenzeile ist, und dass Byte-Offset und `raw_line` in gezipptem XML keinen Sinn ergeben. Ebenfalls unbelegt: ob Excel-Lesen im Budget aus [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern) bleibt (die Tabelle geteilter Strings liegt im Speicher). [T10](30-test-plan.md#t10-unzerlegbare-zeilen-werden-mit-rohbytes-und-richtiger-fundstelle-aussortiert) und [T11](30-test-plan.md#t11-die-fundstelle-bleibt-uber-sortieren-und-filtern-richtig) prüfen nur CSV.

### G33 — Lieferungen ohne Kopf und aus mehreren Dateien oder Sheets

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D53](10-design-decisions.md#d53-jede-datei-und-jedes-sheet-ist-eine-quelle-gruppen-von-dateien-wirken-als-eine-quelle)
[D22](10-design-decisions.md#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird) setzt einen Kopf voraus. JSON und NDJSON haben keinen, und ein neuer oder fehlender Schlüssel kann erst bei Zeile 1 Mio. auftauchen. CSV ohne Kopf ist nicht abgedeckt. Echte Lieferungen bestehen oft aus mehreren Dateien oder Sheets ([D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle) kennt `sheet`, [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler) ein fehlendes Sheet), aber kein Use Case beschreibt einen Lauf über mehrere Dateien. Offen: ist jede Datei bzw. jedes Sheet eine Quelle im Sinn von [D13](10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen), was passiert, wenn eine von drei Dateien fehlt, und wie wirkt die Schwelle je Quelle.

### G34 — Laufzeitfehler in Ausdrücken und Vollständigkeit der Fehlercodes

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D54](10-design-decisions.md#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus)
[D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg) macht Fehler in Ausdrücken zu Planfehlern. Division durch null, Überlauf und Datumsrechnung außerhalb des Bereichs entstehen aber erst zur Laufzeit je Zeile. Offen ist, ob sie wie in SQL null ergeben oder die Zeile aussortieren. Die Liste der Codes in [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix) enthält `custom` aus [D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg) nicht.

### G35 — Zurückgeführte Zeilen verlieren ihre Geschichte

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D46](10-design-decisions.md#d46-gerettete-zeilen-behalten-ihre-geschichte-und-die-schwelle-zahlt-nur-endgultig-aussortierte)
[D26](10-design-decisions.md#d26-zweige-werden-nach-spaltennamen-zusammengefuhrt-typkonflikte-sind-planfehler) entfernt beim Zurückführen die Info-Spalten. Scheitert die Zeile später im Hauptweg, bekommt sie eine neue `reject_id` ohne `prev_reason`, entgegen [D27](10-design-decisions.md#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg). [D15](10-design-decisions.md#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-ubersichtseintrag-je-betroffener-spalte) sagt, dass spätere Schritte eine gescheiterte Zeile nie sehen, [D25](10-design-decisions.md#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck) führt sie aber zurück. Offen ist auch, ob Schwelle und Zählungen je Schritt ([D20](10-design-decisions.md#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen), [D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst)) Zeilen mitzählen, die ein Zweig gerettet hat.

### G36 — Speicherkosten des Rohzustands beim Sortieren und Joinen

**Type:** Assumption · **Kind:** verify · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D55](10-design-decisions.md#d55-roh-und-arbeitsdaten-teilen-spalten-bis-ein-schritt-eine-spalte-andert); Messung in [G13](#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange)
Nach [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert) wird der Rohzustand erst beim Ziel oder bei einer Aggregation freigegeben. Sortieren und Join brauchen alle Zeilen, fassen aber nicht zusammen. Bei einer umfangreichen Lieferung liegt deshalb der ganze Rohzustand neben den Arbeitsdaten im Speicher oder auf der Platte, was Auslagern und Ein-/Ausgabe grob verdoppelt. [G13](#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange) misst das nicht. Denkbar ist, Roh- und Arbeitsdaten zu teilen, bis eine Spalte geändert wird ([D29](10-design-decisions.md#d29-daten-laufen-in-blocken-typisierter-spalten-rohspalten-bleiben-bis-zum-cast-text), [D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert)).

### G37 — Bestehkriterien und fehlende Festlegungen im Scope des Prototyps

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D57](10-design-decisions.md#d57-der-prototyp-liest-csv-und-excel-v20-zusatzlich-json-und-ndjson) und [D58](10-design-decisions.md#d58-der-prototyp-hat-feste-bestehkriterien-fur-laufzeit-speicher-und-budget)
Die Exit-Kriterien im [Scope](40-scope-prototype.md) verlangen, dass [G5](#g5-ob-es-eine-veranderbare-tabelle-braucht) und [G13](#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange) "beantwortet" sind, ohne Grenze. Damit schließt jedes Ergebnis sie. Die Toleranz in [T23](30-test-plan.md#t23-ein-lauf-uber-mehr-daten-als-das-budget-halt-das-budget-ein) ist nirgends festgelegt. [P1](40-scope-prototype.md#p1-auslagern-nur-fur-sortieren-und-gruppieren) lässt einen Join über dem Budget mit einer Meldung scheitern, ohne Status nach [D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst). Keine Decision legt die Reader-Formate fest, die [F8](20-feature-catalogue.md#f8-reader-mit-fundstelle-und-rohzustand) nennt. [P3](40-scope-prototype.md#p3-reader-fur-csv-und-excel) verweist deshalb auf ein Feature statt auf eine Decision.

### G38 — Panik oder Rückgabe bei Schreibfehlern

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D40](10-design-decisions.md#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab)
[D40](10-design-decisions.md#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab)
beendet den Lauf bei einem Schreibfehler mit einer Panik. Eine Panik in einer Library
beendet das ganze Programm, auch andere Läufe im selben Prozess, und lässt sich von einem
Dienst nur mit `recover` abfangen. Die Alternative ist ein sofortiger Abbruch mit eigenem
Status (z. B. `sink_error` nach [D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst))
und dem Fehler als Rückgabewert. Auch dann bleiben die Daten vorhanden, und nichts geht
still verloren. Mit dem Maintainer zu klären.

### G39 — D37 nennt keinen Use Case

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch Ausnahme in [D37](10-design-decisions.md#d37-die-docs-gates-werden-in-diesem-repo-selbst-gebaut)
[D37](10-design-decisions.md#d37-die-docs-gates-werden-in-diesem-repo-selbst-gebaut) verlangt,
dass jede Decision mindestens einen Use Case nennt, nennt aber selbst keinen, weil die
Docs-Gates die Pflege des Design-Sets betreffen und keinen Ablauf der Library. Optionen:
ein Use Case für den Maintainer, der das Design-Set pflegt, oder eine Ausnahme in
[D37](10-design-decisions.md#d37-die-docs-gates-werden-in-diesem-repo-selbst-gebaut) für
Decisions über das Design-Set selbst.

### G40 — Teilweise geschriebene Blöcke im Modus "stoppen"

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D51](10-design-decisions.md#d51-im-modus-stoppen-wird-der-scheiternde-block-nicht-geschrieben-bereits-geschriebene-blocke-bleiben)
Offen ist, ob im Modus "stoppen" nach
[D3](10-design-decisions.md#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen)
Zeilen vor dem fehlerhaften Wert im selben Block noch geschrieben werden, und was "vor"
nach einem Sortieren bedeutet. [T3](30-test-plan.md#t3-im-modus-stoppen-endet-der-lauf-beim-ersten-datenfehler-im-modus-aussortieren-nicht)
prüft deshalb nur den Status.

### G41 — Umfang des v1-Adapters und Maß für auffällige Abweichungen

**Type:** Gap · **Kind:** design · **Status:** akzeptiert — Adapter geklärt in [D36](10-design-decisions.md#d36-ein-adapter-wandelt-zwischen-v1-und-v2-tabellen); Maß für auffällige Abweichungen: Revisit, wenn [F18](20-feature-catalogue.md#f18-profil-und-vergleich-mit-fruheren-laufen) umgesetzt wird
[D36](10-design-decisions.md#d36-ein-adapter-wandelt-zwischen-v1-und-v2-tabellen) sagt nicht,
ob der Weg von v2 über v1 zurück verlustfrei sein muss. v1 kennt weder Nullwerte noch
Typen. [T32](30-test-plan.md#t32-eine-v1-tabelle-ubersteht-den-weg-uber-v2-zuruck-nach-v1-unverandert)
prüft nur den Weg von v1 aus.
[D24](10-design-decisions.md#d24-ein-lauf-kann-ein-profil-liefern-das-mit-dem-profil-eines-fruheren-laufs-verglichen-wird)
legt nicht fest, was eine auffällige Abweichung ist
([T18](30-test-plan.md#t18-der-profilvergleich-meldet-eine-abweichung-ohne-dass-eine-zeile-scheitert)).

### G42 — Volle Herkunft liefert nach einer Gruppierung keinen Rohzustand

**Type:** Inconsistency · **Kind:** design · **Status:** offen
[D48](10-design-decisions.md#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle) verspricht mit voller Herkunft nachverarbeitbare Quellzeilen in den Tabellen je Quelle. [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert) führt über die Gruppierung aber nur Kennungen mit, und [D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben) gibt den Rohzustand dort frei. Optionen: volle Herkunft hält den Rohzustand (ausgelagert) bis zum Laufende; oder [D48](10-design-decisions.md#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle) liefert nur Kennungen und Fundstellen ohne Nachverarbeitung.

### G43 — Das Zählmodell deckt Teilausfälle, Einheiten und Abbrüche nicht ab

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-29 — aufgelöst durch [D84](10-design-decisions.md#d84-eine-quellzeile-zahlt-einmal-aussortiert-vor-durchgelaufen-vor-verworfen-und-nach-einem-abbruch-gibt-es-nicht-verarbeitete-zeilen)
Nach [D47](10-design-decisions.md#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen) zählt eine linke Zeile mit einer gescheiterten von fünf Ergebniszeilen als durchgelaufen; fehlt so ein Fünftel der Daten, bleibt aussortiert bei 0 und die Schwelle schlägt nicht an. [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert) und [D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben) definieren "durchgekommen" unterschiedlich. Aggregierte aussortierte Zeilen zählen nach [D45](10-design-decisions.md#d45-die-schwelle-bezieht-anteile-auf-bisher-gelesene-zeilen-gilt-bei-einer-der-grenzen-und-bricht-erst-nach-einer-mindestzahl-ab) mit ihren Quellzeilen, was nach 1:n-Joins mehr als gelesen ergeben kann, und der Nenner je Schritt nach einer Gruppierung mischt Einheiten. Bei Abbrüchen ([D51](10-design-decisions.md#d51-im-modus-stoppen-wird-der-scheiternde-block-nicht-geschrieben-bereits-geschriebene-blocke-bleiben), [D40](10-design-decisions.md#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab), Kontext) fallen Zeilen in keine Kategorie, [T16](30-test-plan.md#t16-status-und-zahlungen-sind-konsistent-und-bilden-auf-exit-codes-ab) verlangt die Gleichung aber immer. Offen ist auch, ob Zählungen je Code Einträge oder Zeilen zählen. Optionen: Kategorie "teilweise aussortiert" oder Zählung von Ergebniszeilen, Kategorie "nicht verarbeitet" bei Abbruch, Zählungen je Code als Einträge.

### G44 — Ein Hash über die ganze Lieferung ist im Streaming erst am Ende bekannt

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D61](10-design-decisions.md#d61-die-kennung-einer-lieferung-ist-ein-fingerabdruck-der-beim-offnen-feststeht)
[D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash) bildet `record_key` standardmäßig mit einem Hash über den Inhalt der Lieferung. Nach [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen) werden Zeilen aber schon während des Lesens mit Schlüssel geschrieben ([D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close), [D41](10-design-decisions.md#d41-der-http-writer-liefert-mindestens-einmal-und-schickt-einen-idempotenzschlussel-mit)). Optionen: Hash über Kopf, ersten Block und Größe; eine von der Pipeline gelieferte Kennung als Standard; zwei Lesedurchgänge.

### G45 — Excel hält zwei Werte je Zelle

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D62](10-design-decisions.md#d62-bei-excel-tragen-rohzustand-und-arbeitsspalte-den-gespeicherten-wert-in-fester-textform)
Nach [D52](10-design-decisions.md#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert) ist der Rohzustand der angezeigte Text, umgewandelt wird aber der gespeicherte Wert. Unklar ist, welchen Wert die Arbeitsspalte vor dem Umwandeln trägt. Dann stimmt [D55](10-design-decisions.md#d55-roh-und-arbeitsdaten-teilen-spalten-bis-ein-schritt-eine-spalte-andert) für Excel nicht, und eine Nachverarbeitung nach [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) wandelt den angezeigten Text um und scheitert genau an den Ländereinstellungen, die [D52](10-design-decisions.md#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert) vermeiden wollte. Option: Arbeitsspalte trägt den gespeicherten Wert, der Rohzustand beide.

### G46 — Vorrang der Status und Umfang von aborted

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D63](10-design-decisions.md#d63-es-gilt-der-hochste-zutreffende-status-und-das-ergebnis-nennt-alle-befunde)
[D21](10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst) legt nicht fest, welcher Status gilt, wenn mehrere zutreffen (Lieferfehler und Schwelle; Lieferfehler und Schreibfehler; Lieferfehler im Modus "stoppen"). `aborted` ist nur für Stopp und Kontext definiert, wird aber auch für einen zu großen Join ([D58](10-design-decisions.md#d58-der-prototyp-hat-feste-bestehkriterien-fur-laufzeit-speicher-und-budget)) und "Platte voll" (Failure Modes) verwendet. Option: feste Rangfolge, z. B. `plan_error` > `sink_error` > `delivery_error` > `aborted` > `failed_threshold` > `ok`.

### G47 — Aufräumen verwaister Läufe gegen offene Ergebnisse

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-29 — aufgelöst durch [D103](10-design-decisions.md#d103-ein-lauf-markiert-sein-verzeichnis-zum-auslagern-mit-einer-dateisperre-die-das-ergebnis-bis-close-halt)
Nach [D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close) leben ausgelagerte aussortierte Zeilen bis zum Schließen des Ergebnisses, nach [D38](10-design-decisions.md#d38-ein-spaterer-lauf-entfernt-verwaiste-ausgelagerte-daten) entfernt ein späterer Lauf aber alle Verzeichnisse, deren Lauf nicht mehr lebt, auch die eines fertigen, nicht geschlossenen Ergebnisses. Wie ein Lauf als lebend markiert ist (Prozess-ID, Dateisperre), ist offen, ebenso geteilte Volumes mehrerer Container. [D17](10-design-decisions.md#d17-die-library-bewahrt-aussortierte-zeilen-nicht-selbst-auf) sagt noch, aussortierte Zeilen würden nicht über das Ende eines Laufs hinaus gehalten.

### G48 — row_key über Joins hinaus

**Type:** Gap · **Kind:** design · **Status:** offen
[D47](10-design-decisions.md#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen) bildet `row_key` aus den Quellzeilen eines Joins. Explode und Unpivot erzeugen mehrere Zeilen mit gleichem Schlüssel, aggregierte Zeilen hätten Millionen Quellschlüssel, und bei einem Left Join ändert sich der Schlüssel, wenn bei der Nachverarbeitung ein Partner da ist. [D41](10-design-decisions.md#d41-der-http-writer-liefert-mindestens-einmal-und-schickt-einen-idempotenzschlussel-mit) leitet den Idempotenzschlüssel noch aus `record_key` ab, [F20](20-feature-catalogue.md#f20-writer-fur-datenbank-und-http) upsertet noch auf `record_key`. Option: `row_key` aus Quellschlüsseln plus Ordnungszahl je erzeugendem Schritt, Gruppenschlüssel bei Aggregaten.

### G49 — Nachverarbeitung übernimmt Schlüssel nicht und braucht die Original-Lieferung

**Type:** Inconsistency · **Kind:** design · **Status:** offen
[D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) lässt die Info-Spalten weg, damit würden `record_key`, `record_hash` und `row_key` neu gebildet, entgegen [D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash). Die übrigen Quellen "normal lesen" setzt voraus, dass die Original-Lieferung der anderen Seite noch vorliegt, was [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle) für HTTP- und Streaming-Quellen ausschließt. Eine linke Zeile mit vier angekommenen und einer gescheiterten Ergebniszeile erzeugt bei der Nachverarbeitung alle fünf erneut, was bei Zielen ohne Upsert Duplikate gibt.
**Teilauflösung (2026-09-29):** Dass die Quelle aus aussortierten Zeilen `record_key`, `record_hash` und die Fundstelle aus den Info-Spalten übernimmt, legt [D82](10-design-decisions.md#d82-die-quelle-aus-aussortierten-zeilen-ubernimmt-schlussel-und-fundstelle-aus-den-info-spalten) fest. Die übrigen Fragen bleiben offen.

### G50 — field_too_large hat keinen brauchbaren Rohzustand

**Type:** Inconsistency · **Kind:** design · **Status:** offen
[D56](10-design-decisions.md#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch) kürzt `raw_line`, das es nach [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle) nur für unzerlegbare Zeilen gibt. Eine gekürzte Zeile kann bei der Nachverarbeitung nach [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) als gültige kürzere Zeile gelesen werden. Ein nicht geschlossenes Anführungszeichen in CSV macht das Feld bis zum Dateiende lang, und es gibt keine Regel zum Wiederaufsetzen. Bei Excel gibt es kein `raw_line` ([D52](10-design-decisions.md#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert)).

### G51 — Spalten, die erst im Lauf auftauchen, und optionale Felder

**Type:** Gap · **Kind:** design · **Status:** offen
Neue JSON-Felder, die erst spät auftauchen und nach [D22](10-design-decisions.md#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird) durchgereicht werden, ändern den Aufbau der Blöcke mitten im Lauf, während [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler) und [D26](10-design-decisions.md#d26-zweige-werden-nach-spaltennamen-zusammengefuhrt-typkonflikte-sind-planfehler) feste, vorab geprüfte Typen annehmen. `missing_field` nach [D53](10-design-decisions.md#d53-jede-datei-und-jedes-sheet-ist-eine-quelle-gruppen-von-dateien-wirken-als-eine-quelle) sortiert jeden Datensatz aus, dem ein optionales Feld fehlt; [D22](10-design-decisions.md#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird) kennt kein Pflicht- oder Optional-Merkmal. Ohne erwarteten Aufbau kann die Engine eine unbekannte Spalte erst nach dem Kopf erkennen, [T15](30-test-plan.md#t15-planfehler-verhindern-den-lauf-liefer-und-datenfehler-folgen-der-konfiguration-je-code) verlangt es vor dem Lesen. Für Gruppen fehlt die Angabe der erwarteten Teile, und die Schwelle je Quelle aus [G33](#g33-lieferungen-ohne-kopf-und-aus-mehreren-dateien-oder-sheets) ist nicht beantwortet.

### G52 — Geteilte Rohspalten gegen den Modus "immer ändern"

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D64](10-design-decisions.md#d64-mit-dem-rohzustand-geteilte-spalten-gelten-als-geteilt-auch-im-modus-immer-andern)
Ändert ein Schritt im Modus "immer an Ort und Stelle ändern" ([D8](10-design-decisions.md#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert)) eine Spalte, die er sich nach [D55](10-design-decisions.md#d55-roh-und-arbeitsdaten-teilen-spalten-bis-ein-schritt-eine-spalte-andert) mit dem Rohzustand teilt, würde er den Rohzustand ändern, was [D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert) verbietet. Der Modus müsste also bei jeder ersten Änderung einer Rohspalte kopieren. Das nimmt ihm viel von seinem Nutzen und verzerrt den Vergleich nach [D58](10-design-decisions.md#d58-der-prototyp-hat-feste-bestehkriterien-fur-laufzeit-speicher-und-budget).

### G53 — GOMEMLIMIT wirkt auf den ganzen Prozess

**Type:** Inconsistency · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D65](10-design-decisions.md#d65-gomemlimit-setzt-die-engine-nur-auf-wunsch-und-das-budget-gilt-je-prozess)
[D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern) setzt `GOMEMLIMIT`, eine Einstellung für den ganzen Prozess. Laufen mehrere Läufe in einem Prozess, überschreibt der letzte die Einstellung der anderen, und die Budgets als Anteil des verfügbaren Speichers ergeben zusammen mehr als das Limit. [D40](10-design-decisions.md#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab) verwirft eine Panik gerade, weil sie andere Läufe im selben Prozess trifft. Optionen: nur auf ausdrücklichen Wunsch setzen, ein gemeinsames Budget je Prozess, einen gesetzten Wert nie überschreiben.

### G54 — Lücken bei Writern für aussortierte Zeilen im Plan

**Type:** Gap · **Kind:** design · **Status:** offen
Ein Writer "für alle Quellen" nach [D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close) bekommt Tabellen mit verschiedenen Spalten, was [D13](10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen) verworfen hatte. Die Übersicht mit einem Eintrag je Fehler ([D44](10-design-decisions.md#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler)) bleibt im Speicher, wenn nur Writer je Quelle gesetzt sind. Der Änderungsbericht nach [D23](10-design-decisions.md#d23-das-laufergebnis-enthalt-einen-anderungsbericht) beruht auf aussortierten Zeilen, die weggeschrieben wurden. Offen ist, ob im Modus "stoppen" die aussortierten Zeilen des nicht geschriebenen Blocks nach [D51](10-design-decisions.md#d51-im-modus-stoppen-wird-der-scheiternde-block-nicht-geschrieben-bereits-geschriebene-blocke-bleiben) noch in die Writer gehen, und was bei `sink_error` nach [D40](10-design-decisions.md#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab) mit Ergebnis- und Fehler-Writer geschieht, die unterschiedlich weit geschrieben haben.
**Teilauflösung (2026-09-29):** Wie ein Writer für alle Quellen aussieht und dass die Übersicht im Ergebnis bleibt, legt [D95](10-design-decisions.md#d95-writer-fur-aussortierte-zeilen-im-plan-gibt-es-je-quelle-als-fabrik-fur-alle-quellen-und-fur-die-ubersicht) fest. Was nach einem Stopp oder bei `sink_error` mit den Writern geschieht, legt [D96](10-design-decisions.md#d96-nach-einem-vorzeitigen-ende-wird-in-kein-ziel-mehr-geschrieben-und-nicht-geschriebene-aussortierte-zeilen-bleiben-im-ergebnis) fest. Offen bleibt, wie die Übersicht bei großen Läufen den Speicher verlässt.

### G55 — T22 ist nicht in jedem Modus deterministisch, und der erste Fehler hat keine Reihenfolge

**Type:** Gap · **Kind:** design · **Status:** offen
Im Modus "stoppen" hängt nach [D51](10-design-decisions.md#d51-im-modus-stoppen-wird-der-scheiternde-block-nicht-geschrieben-bereits-geschriebene-blocke-bleiben) von den Blockgrenzen ab, was geschrieben wird, und ein vorzeitiger Abbruch an der Schwelle nach [D45](10-design-decisions.md#d45-die-schwelle-bezieht-anteile-auf-bisher-gelesene-zeilen-gilt-bei-einer-der-grenzen-und-bricht-erst-nach-einer-mindestzahl-ab) vom zeitlichen Ablauf. [T22](30-test-plan.md#t22-dieselbe-pipeline-liefert-im-speicher-und-im-streaming-dasselbe-ergebnis) gilt deshalb nur für "aussortieren" ohne vorzeitigen Abbruch. [D44](10-design-decisions.md#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler) beschreibt in der Tabelle je Quelle den ersten Fehler, ohne Reihenfolge der Spalten festzulegen.

### G56 — Features und Scope für die Decisions ab D37

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch Auswahl im [Scope Prototyp](40-scope-prototype.md) (Maintainer, 2026-09-27)
Keine der Decisions [D37](10-design-decisions.md#d37-die-docs-gates-werden-in-diesem-repo-selbst-gebaut) bis [D60](10-design-decisions.md#d60-der-prototyp-wird-mit-eigenen-beispiel-lieferungen-der-1brc-datei-und-in-docker-mit-verschiedenen-speicher-limits-erprobt) ist einem Feature zugeordnet. Damit wählt der [Scope](40-scope-prototype.md) sie nicht aus, obwohl etwa `delivery_error`, Writer für aussortierte Zeilen, Tabellen mit haftendem Fehler, Quellen je Datei und Sheet und die Grenzen nach [D56](10-design-decisions.md#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch) für den Prototyp wichtig sind. Die [Security Boundaries](60-security-boundaries.md) nennen die Grenzen "enforced", ohne Feature und T-Fall. Zu entscheiden ist, welche davon in den Prototyp kommen.

### G57 — Erkennen einer abgeschnittenen CSV-Lieferung

**Type:** Assumption · **Kind:** verify · **Status:** offen
Eine unvollständige letzte Zeile lässt sich nicht sicher von einer gültigen Datei ohne Zeilenumbruch am Ende unterscheiden, und ein Abbruch genau an einer Zeilengrenze fällt gar nicht auf. `truncated` nach [D42](10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error) kann so fälschlich melden oder nichts erkennen. Denkbar: erwartete Zeilenzahl oder Größe als Angabe der Quelle.

### G58 — Lücken bei sofortigen Tabellen

**Type:** Gap · **Kind:** design · **Status:** offen
[D50](10-design-decisions.md#d50-eine-tabelle-tragt-ihre-aussortierten-zeilen-und-einen-haftenden-fehler) sagt nicht, wie eine aus einem Reader gebaute `Table` Lieferfehler meldet, wann ihr Rohzustand freigegeben wird (sie verlässt keinen Plan, [D43](10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben)), welchen Rohzustand eine Tabelle aus einem Laufergebnis trägt, und wie sich aussortierte Zeilen und haftende Fehler zweier Tabellen bei einem Join verbinden. Der Rohzustand einer im Code gebauten Tabelle sind typisierte Werte, [D29](10-design-decisions.md#d29-daten-laufen-in-blocken-typisierter-spalten-rohspalten-bleiben-bis-zum-cast-text) sieht Text vor.
**Teilauflösung (2026-09-28):** Wie sich aussortierte Zeilen und haftende Fehler zweier Tabellen bei einem Join verbinden, legt [D69](10-design-decisions.md#d69-ein-join-zweier-tabellen-tragt-die-aussortierten-zeilen-und-den-haftenden-fehler-beider-seiten) fest. Die übrigen Fragen bleiben offen.

### G59 — Vergleichbarkeit der Bestehkriterien

**Type:** Assumption · **Kind:** verify · **Status:** offen (der Bezug der 10 % ist durch [D104](10-design-decisions.md#d104-die-unit-tests-messen-das-budget-an-der-zahlung-der-engine-den-speicher-des-prozesses-misst-t23-in-docker) aufgelöst)
v2 führt Rohzustand und aussortierte Zeilen mit ([D9](10-design-decisions.md#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert), [D55](10-design-decisions.md#d55-roh-und-arbeitsdaten-teilen-spalten-bis-ein-schritt-eine-spalte-andert)), v1 `MutableTable` nicht. [D58](10-design-decisions.md#d58-der-prototyp-hat-feste-bestehkriterien-fur-laufzeit-speicher-und-budget) muss festlegen, ob beim Vergleich der Rohzustand mitgeführt wird, und was eine zahlenlastige Lieferung ist. Ob sich "höchstens 10 % über dem Budget" auf den Speicher des Prozesses oder nur auf die Blöcke der Engine bezieht, ist ebenfalls offen ([D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern)).

### G60 — Formel-Maskierung bricht die Nachverarbeitung

**Type:** Inconsistency · **Kind:** design · **Status:** offen
Die Maskierung nach [D56](10-design-decisions.md#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch) erfasst auch ein führendes `-` und verfälscht negative Zahlen. In einem Writer für aussortierte Zeilen verändert sie die Rohspalten, und die Nachverarbeitung nach [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) liest `'-5`. Optionen: auf Writern für aussortierte Zeilen nicht erlaubt; die Nachverarbeitung entfernt die Maskierung; Zahlen ausnehmen.

### G61 — UC8 fachlich prüfen

**Type:** Gap · **Kind:** verify · **Status:** offen
[UC8](05-use-cases.md#uc8-eine-lieferung-besteht-aus-mehreren-dateien-oder-sheets) entstand zusammen mit [D53](10-design-decisions.md#d53-jede-datei-und-jedes-sheet-ist-eine-quelle-gruppen-von-dateien-wirken-als-eine-quelle) und enthält Fragen, die nicht mehrteilige Lieferungen betreffen (Fundstelle in Excel, CSV ohne Kopf), sowie ein Akzeptanzkriterium im Ablauf. Es fehlen die eigentlichen Fragen mehrteiliger Lieferungen: Wann ist eine Lieferung vollständig, wenn Teile zu verschiedenen Zeiten kommen? Was, wenn ein Teil anders aufgebaut ist oder doppelt kommt? Braucht einen Blick des Maintainers auf echte Abläufe.

### G62 — D68 nennt keinen Use Case, fällt aber nicht unter die Ausnahme von D37

**Type:** Inconsistency · **Kind:** design · **Status:** akzeptiert — Revisit, wenn eine weitere Decision über Werkzeuge des Repos entsteht
[D37](10-design-decisions.md#d37-die-docs-gates-werden-in-diesem-repo-selbst-gebaut) nimmt nur Decisions über das Design-Set selbst davon aus, einen Use Case zu nennen. [D68](10-design-decisions.md#d68-mise-ist-der-task-runner-und-mise-run-test-fuhrt-alle-tests-aller-module-aus) regelt den Task-Runner des Repos und betrifft keinen Ablauf der Library, ist also keine solche Decision. Das Docs-Gate führt beide Ausnahmen ausdrücklich in einer Liste, statt jede Decision mit "keine" durchzulassen. Kommen weitere Werkzeug-Decisions hinzu, sollte [D37](10-design-decisions.md#d37-die-docs-gates-werden-in-diesem-repo-selbst-gebaut) per Amendment auf Decisions über die Pflege des Repos erweitert werden (vgl. [G39](70-gap-ledger.md#g39-d37-nennt-keinen-use-case)).

### G63 — Rohwert des Gruppenschlüssels einer aggregierten aussortierten Zeile

**Type:** Gap · **Kind:** design · **Status:** offen
[D48](10-design-decisions.md#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle) verlangt den Gruppenschlüssel "wo vorhanden" auch als Rohwert. Wurde die Schlüsselspalte vor der Gruppierung umgewandelt, können verschiedene Rohtexte (`01`, `1`, ` 1`) in denselben Schlüssel eingehen, und nach [D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert) ist der Rohzustand der Quellzeilen dort schon freigegeben. [D77](10-design-decisions.md#d77-aggregierte-aussortierte-zeilen-stehen-je-schritt-in-einer-tabelle-und-auch-in-der-ubersicht) lässt den Rohwert im Prototyp deshalb weg. Optionen: der Rohwert der ersten Zeile der Gruppe; alle verschiedenen Rohwerte; nur, wenn die Schlüsselspalte unverändert eine Rohspalte ist.

### G64 — Excel-Zeilen mit Werten rechts vom Kopf

**Type:** Gap · **Kind:** design · **Status:** offen
Eine Excel-Zeile kann Werte in Spalten haben, über denen keine Kopfzelle steht. [D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle) nennt eine falsche Spaltenzahl bei CSV eine unzerlegbare Zeile, [D52](10-design-decisions.md#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert) lässt `raw_line` bei Excel leer. Der Prototyp sortiert eine solche Zeile mit `unparseable_line` aus, ohne `raw_line` und mit den Werten der Spalten unter dem Kopf als Rohzustand, damit keine Werte still verloren gehen. Optionen: so lassen; die Werte als neue Spalten melden wie nach [D22](10-design-decisions.md#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird); die Werte verwerfen und zählen.

### G65 — Aggregierte Zeilen aus einem Fehlerzweig zeigen prev_reason nur in der Übersicht

**Type:** Gap · **Kind:** design · **Status:** offen
Scheitert eine aggregierte Zeile im Hauptweg und im Fehlerzweig nach [D25](10-design-decisions.md#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck), trägt ihr Eintrag nach [D27](10-design-decisions.md#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg) `prev_reason`. Die Info-Spalten der Tabelle aggregierter Zeilen legt [D77](10-design-decisions.md#d77-aggregierte-aussortierte-zeilen-stehen-je-schritt-in-einer-tabelle-und-auch-in-der-ubersicht) ohne `prev_reason` fest. Der Prototyp zeigt den vorigen Grund deshalb nur in der Übersicht, `step` zeigt den Weg in beiden. Optionen: `prev_reason` per Amendment zu [D77](10-design-decisions.md#d77-aggregierte-aussortierte-zeilen-stehen-je-schritt-in-einer-tabelle-und-auch-in-der-ubersicht) aufnehmen; so lassen.

### G66 — Eine Datei aussortierter Zeilen lässt sich nur mit einem anderen Präfix zurücklesen

**Type:** Gap · **Kind:** design · **Status:** offen
Schreibt eine Pipeline aussortierte Zeilen nach [D2](10-design-decisions.md#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse) in eine Datei, tragen deren Spalten das Präfix nach [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix). Ein Reader liest die Datei als Lieferung, und eine Rohspalte mit dem reservierten Präfix ist ein Planfehler. Für die Nachverarbeitung nach [D16](10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) muss die Pipeline die Datei deshalb mit einem anderen Präfix lesen, bevor sie die Tabelle an die Quelle aus aussortierten Zeilen gibt. Der Prototyp geht diesen Weg. Optionen: so lassen und dokumentieren; die Quelle aus aussortierten Zeilen nimmt direkt eine Quelle mit Reader statt einer Tabelle; ein Lesen ohne Prüfung des Präfixes für diesen Fall.

### G67 — Buchführung je Quellzeile liegt außerhalb des Budgets

**Type:** Assumption · **Kind:** verify · **Status:** offen
Das Budget nach [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern) zählt die Blöcke der Engine, den Rohzustand und die Kopien aussortierter Zeilen ([D104](10-design-decisions.md#d104-die-unit-tests-messen-das-budget-an-der-zahlung-der-engine-den-speicher-des-prozesses-misst-t23-in-docker)). Der Prototyp hält daneben je Quellzeile Daten im Speicher, die mit der Lieferung wachsen: die Kategorie der Zählung nach [D84](10-design-decisions.md#d84-eine-quellzeile-zahlt-einmal-aussortiert-vor-durchgelaufen-vor-verworfen-und-nach-einem-abbruch-gibt-es-nicht-verarbeitete-zeilen) für den Lauf und je Schritt, Zeile und Offset der Fundstelle ([D10](10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)), die Kennungen der Quellzeilen einer aggregierten Zeile ([D12](10-design-decisions.md#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert)) und die Einträge der Übersicht aussortierter Zeilen. Nach [D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close) lagert der Prototyp nur die Kopien des Rohzustands aussortierter Zeilen aus. Bei 1 Mrd. Zeilen der 1BRC-Datei sind das mehrere GB. [T23](30-test-plan.md#t23-ein-lauf-uber-mehr-daten-als-das-budget-halt-das-budget-ein) zeigt in Docker, ob das trägt. Optionen: Zählung über Bereiche statt je Zeile; Fundstellen auslagern; Kennungen aggregierter Zeilen nur als Anzahl.

### G68 — Beim ausgelagerten Gruppieren muss jede einzelne Gruppe in den Speicher passen

**Type:** Assumption · **Kind:** design · **Status:** offen
Der Prototyp gruppiert nach dem Auslagern, indem er nach Schlüssel sortiert und jede Gruppe mit ihren Zeilen in der ursprünglichen Reihenfolge berechnet. So ergeben sich dieselben Werte wie im Speicher ([D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)), auch bei Summen über Gleitkommazahlen. Eine einzelne Gruppe mit mehr Zeilen, als das Budget fasst, liegt dabei ganz im Speicher. [P1](40-scope-prototype.md#p1-auslagern-nur-fur-sortieren-und-gruppieren) nennt nur die rechte Seite eines Joins als Einschränkung. Optionen: Aggregationen ohne Bedarf an allen Werten (Summe, Anzahl, Minimum, Maximum) fortlaufend berechnen und nur Median, Quantil, CountDistinct und StringJoin je Gruppe halten; eine übergroße Gruppe beendet den Lauf mit `aborted` wie bei einem Join.
