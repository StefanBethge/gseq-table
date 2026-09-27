# Gap Ledger

Inkonsistenzen, Annahmen und Lücken des Design-Sets. Geschlossene Einträge nennen, was
sie geschlossen hat. Akzeptierte Einträge nennen ihre Revisit-Bedingung.

### G1 — Was der Rohzustand einer Zeile umfasst

**Type:** Gap · **Kind:** design · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D10](10-design-decisions.md#d10-rohzustand-heisst-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
[D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten)
legt fest, dass aussortierte Zeilen im Rohzustand zurückkommen. Offen ist, ob das die
Zellwerte sind, wie der Reader sie gelesen hat, oder auch die Rohbytes der Quelle, etwa
eine CSV-Zeile, die gar nicht erst in Zellen zerlegt werden konnte. Offen ist auch, wie
der Rohzustand bei großen Lieferungen verfügbar bleibt, ohne dass jede Zeile ihn mitführt
([UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen),
[UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)).

### G2 — Welche Info-Spalten eine aussortierte Zeile trägt

**Type:** Gap · **Kind:** design · **Status:** offen
Kandidaten: Schritt, Grund, betroffene Spalte, Rohwert, Fundstelle in der Quelle (Datei,
Sheet, Zeile), Lauf. Offen sind außerdem die Namen der Spalten, damit sie nicht mit
Datenspalten kollidieren, und ob eine Zeile mehrere Gründe tragen kann
([UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)).

### G3 — Bemessung und Wirkung der Schwelle

**Type:** Gap · **Kind:** design · **Status:** offen
[D4](10-design-decisions.md#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen)
führt die Schwelle ein. Offen: absolut oder als Anteil, für den ganzen Lauf oder je
Schritt, und ob der Lauf beim Überschreiten sofort stoppt oder bis zum Ende läuft und dann
als fehlgeschlagen gilt. Offen ist auch, wie der Scheduler das Ergebnis erfährt
([UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)).

### G4 — Verarbeitungsmodell für große Lieferungen

**Type:** Gap · **Kind:** verify · **Status:** geschlossen 2026-09-27 — aufgelöst durch [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)
[UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)
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
[UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)).
Soll durch Messung im Prototyp gegen v1 `MutableTable` geprüft werden.
Seit [D7](10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert)
entscheidet die Engine über Kopieren oder Ändern. Die Annahme lautet damit: Ändern an Ort
und Stelle, wo Daten nicht geteilt sind, erreicht Laufzeit und Speicher von v1
`MutableTable`.

### G6 — Writer für Datenbank und HTTP

**Type:** Gap · **Kind:** design · **Status:** offen
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
[D10](10-design-decisions.md#d10-rohzustand-heisst-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle) ordnet jeder Zeile einen Rohzustand und eine Fundstelle zu. Nach einem Join besteht
eine Ergebniszeile aus zwei Quellzeilen, nach einer Gruppierung aus vielen. Offen ist,
welchen Rohzustand und welche Fundstelle eine solche Zeile trägt, wenn sie in einem
späteren Schritt scheitert, und was nach "durchgekommen" bedeutet, wenn eine Quellzeile in
mehrere Ergebniszeilen eingeht
([UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)).

### G10 — Aussortierte Zeilen mehrerer Quellen: eine Tabelle oder je Quelle

**Type:** Gap · **Kind:** design · **Status:** offen
Nach [D11](10-design-decisions.md#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert) können aussortierte Zeilen aus verschiedenen Quellen mit verschiedenen Spalten
stammen. Offen ist, ob es je Quelle eine eigene Tabelle aussortierter Zeilen gibt oder
eine gemeinsame Tabelle mit einer Spalte für die Quelle, in der die Rohspalten je nach
Quelle unterschiedlich belegt sind. Hängt mit
[G2](#g2-welche-info-spalten-eine-aussortierte-zeile-tragt) zusammen.
