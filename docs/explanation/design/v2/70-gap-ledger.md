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

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix) und [D15](10-design-decisions.md#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-eintrag-je-betroffener-spalte)
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

**Type:** Gap · **Kind:** verify · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)
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
`MutableTable`.

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
[D13](10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen), [D14](10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix) und [D15](10-design-decisions.md#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-eintrag-je-betroffener-spalte) legen die Form der aussortierten Zeilen fest. Der Maintainer will
sie nach dem Prototyp noch einmal validieren, anhand echter Lieferungen: ob die
Info-Spalten reichen, ob "je Quelle plus Übersicht" im Alltag handlich ist, und ob ein
Eintrag je Spalte beim Lesen hilft oder stört.

### G12 — Ab wann eine Häufung von Fehlern als Formatänderung gilt

**Type:** Gap · **Kind:** verify · **Status:** offen
[D23](10-design-decisions.md#d23-das-laufergebnis-enthalt-einen-anderungsbericht) meldet `format_change`, wenn ein erheblicher Teil der Werte einer Spalte mit
demselben Fehlercode scheitert. Offen ist die Grenze: fester Anteil, konfigurierbar, oder
relativ zum Profil eines früheren Laufs ([D24](10-design-decisions.md#d24-ein-lauf-kann-ein-profil-liefern-das-mit-dem-profil-eines-fruheren-laufs-verglichen-wird)). Soll an echten Lieferungen im Prototyp
erprobt werden.

### G13 — Vorteil spaltenorientierter Blöcke und Voreinstellungen für Budget und Blocklänge

**Type:** Assumption · **Kind:** verify · **Status:** offen
Annahme: Blöcke typisierter Spalten nach [D29](10-design-decisions.md#d29-daten-laufen-in-blocken-typisierter-spalten-rohspalten-bleiben-bis-zum-cast-text) sind bei Laufzeit und Speicher messbar
besser als die zeilenbasierte Speicherung aus v1. Offen sind außerdem die Voreinstellungen
für den Anteil des Speichers ([D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern)) und die Blockgröße. Soll im Prototyp mit Benchmarks
gegen v1 geklärt werden
([UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)).

### G14 — Docs-Gates aus dem Archivar-Repo übernehmen

**Type:** Gap · **Kind:** verify · **Status:** offen
Die Methode prüft IDs, Links und die Zuordnung von Tests zu T-Fällen mit
`TestDocsIDConsistency` und `TestPlanMappingConsistency`. Die Referenzfassung liegt in
`internal/testutil/docs_idlint_test.go` im Archivar-Repo, auf das beim Entwurf kein Zugriff
bestand. Bis die Gates laufen, sind die Heading-Anker in den Links von Hand erzeugt und
ungeprüft. Das Format der Tabelle "welcher Test beweist welchen Fall" im
[Test-Plan](30-test-plan.md) wird ebenfalls erst mit dem Parser festgelegt. Das Gate kennt
laut Methode die UC-Familie noch nicht.

### G15 — Zustellzusage des HTTP-Writers

**Type:** Gap · **Kind:** design · **Status:** offen
[D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http)
sieht Wiederholung vor. Bleibt nach einer Anfrage unklar, ob der Empfänger sie verarbeitet
hat (Zeitüberschreitung), führt Wiederholung zu doppelter Zustellung. Offen ist, ob der
Writer "mindestens einmal" zusagt und den `record_key` aus
[D18](10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash)
zur Deduplizierung beim Empfänger mitschickt, oder ob er gar nicht wiederholt
([T30](30-test-plan.md#t30-der-http-writer-liefert-jede-zeile-trotz-vorubergehender-fehler-aus)).

### G16 — Konkrete Werte für die Exit-Kriterien des Prototyps

**Type:** Gap · **Kind:** design · **Status:** offen
Der [Scope des Prototyps](40-scope-prototype.md) verlangt einen Lauf über eine Lieferung,
die ein Vielfaches des Budgets groß ist. Größe der Lieferung und Budget legt der
Maintainer vor Beginn des Prototyps fest.

### G17 — Aufräumen nach einem vom System beendeten Lauf

**Type:** Gap · **Kind:** design · **Status:** offen
Wird ein Lauf vom System beendet (Speicher), bleiben ausgelagerte Daten liegen
([Failure Modes](50-failure-modes.md), Zeile "Speicher reicht trotz Budget nicht"). Offen
ist, ob ein späterer Lauf verwaiste Daten erkennt und entfernt, und woran.
[T24](30-test-plan.md#t24-nach-einem-lauf-bleibt-nichts-neben-den-zielen-zuruck) deckt
diesen Fall nicht ab.

### G18 — Verhalten bei Panik in einer Closure

**Type:** Assumption · **Kind:** design · **Status:** offen
Die [Failure Modes](50-failure-modes.md) nehmen an, dass eine Panik in einer Closure wie
ein zurückgegebener Fehler behandelt wird
([D32](10-design-decisions.md#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg)).
Das verhindert Abbrüche durch fremden Code, kann aber echte Programmierfehler als
Datenfehler tarnen. Nicht vom Maintainer entschieden.

### G19 — Fehler beim Schreiben ins Ziel

**Type:** Gap · **Kind:** design · **Status:** offen
Keine Decision legt fest, was passiert, wenn ein Ziel einen Block ablehnt oder das
Schreiben der aussortierten Zeilen scheitert ([Failure Modes](50-failure-modes.md),
Abschnitt Ziele). [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler)
kennt Plan-, Liefer- und Datenfehler, aber keine Fehler des Ziels. Scheitert das Schreiben
der aussortierten Zeilen still, gehen genau die Daten verloren, die
[D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten)
bewahren soll.

### G20 — Grenzen für Feldlänge und entpackten Umfang

**Type:** Gap · **Kind:** design · **Status:** offen
Aus den [Security Boundaries](60-security-boundaries.md): Grenzen für einzelne Feldgrößen
und für die entpackte Größe von Excel-Archiven, Dateirechte der ausgelagerten Daten, und
ob der CSV-Writer Werte maskiert, die als Formel interpretiert würden. Keines davon ist
entschieden.
