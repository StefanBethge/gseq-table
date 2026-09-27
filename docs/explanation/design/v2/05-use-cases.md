# Use Cases

Die Abläufe, für die gseq-table v2 gebaut wird, und die Fragen, die sie an das Design
stellen. Antworten stehen nicht hier, sondern in den Design Decisions. Wo eine Frage
beantwortet ist, verweist sie darauf.

## Akteure

| Akteur | Rolle |
|---|---|
| Pipeline-Entwickler | schreibt und pflegt ETL-Pipelines mit der Library; derzeit hauptsächlich der Maintainer, perspektivisch mehrere |
| Datenlieferant | Kunde oder externer Anbieter, der Rohdaten liefert (häufig Excel, außerdem CSV und JSON); ändert Format oder Inhalt gelegentlich ohne Ankündigung |
| Scheduler | startet Läufe ohne menschliches Zutun, meistens ein Cron |
| Ziel | nimmt das Ergebnis eines Laufs entgegen: Datenbank, Datei oder ein HTTP-Endpunkt, an den JSON gepostet wird |
| Externer Entwickler | nutzt die Library ohne Kontext des Maintainers, eingebunden per `go get` |

### UC1 — Geplanter Lauf über eine Lieferung

**Akteur(e):** Scheduler, Datenlieferant, Ziel
**Auslöser:** Der Scheduler startet den Lauf zu seiner festen Zeit; eine Lieferung des
Datenlieferanten liegt vor (typischerweise eine Excel-Datei).
**Ablauf:** Die Pipeline liest die Lieferung, bereinigt und typisiert die Werte, reichert
sie an und schreibt das Ergebnis ins Ziel. Zeilen, die ein Schritt nicht verarbeiten kann,
werden aussortiert, statt den Lauf abzubrechen. Der Lauf kommt durch, und die verarbeitbaren
Daten kommen im Ziel an, auch wenn ein Teil der Lieferung fehlerhaft ist. Am Ende ist
festgehalten, wie viele Zeilen durchgelaufen sind und wie viele in welchem Schritt aus
welchem Grund aussortiert wurden. Die aussortierten Zeilen schickt die Pipeline über einen
Writer in einem gewünschten Format an ein Ziel.

```mermaid
sequenceDiagram
    participant S as Scheduler
    participant P as Pipeline
    participant L as Lieferung
    participant Z as Ziel
    S->>P: Lauf starten
    P->>L: lesen
    P->>P: bereinigen, typisieren, anreichern
    P-->>P: nicht verarbeitbare Zeilen aussortieren
    P->>Z: verarbeitbare Zeilen schreiben
    P-->>S: Ergebnis: durchgelaufen / aussortiert je Schritt und Grund
```

**Erzwingt Entscheidungen:**
- Was ist ein Datenfehler, der eine Zeile aussortiert, und was ist ein Konfigurationsfehler, der den Lauf verhindert oder abbricht? → offen
- Wann ist ein Lauf erfolgreich: immer, wenn er durchkommt, oder gibt es eine Schwelle (z. B. Anteil aussortierter Zeilen), ab der er als fehlgeschlagen gilt? → [D4](10-design-decisions.md#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen), Bemessung offen: [G3](70-gap-ledger.md#g3-bemessung-und-wirkung-der-schwelle)
- Wie erfährt der Scheduler vom Ergebnis (Exit-Code, Rückgabewert, Bericht)? → offen, [G3](70-gap-ledger.md#g3-bemessung-und-wirkung-der-schwelle)
- Wohin gehen die aussortierten Zeilen eines Laufs, und in welchem Format? → [D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten), [D2](10-design-decisions.md#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse)
- Gehören Writer für Datenbank und HTTP-JSON zur Library, oder nur Datei-Writer? → offen, [G6](70-gap-ledger.md#g6-writer-fur-datenbank-und-http)

### UC2 — Datenlieferant ändert das Lieferformat unangekündigt

**Akteur(e):** Datenlieferant, Pipeline-Entwickler
**Auslöser:** Die Lieferung weicht vom bisherigen Aufbau ab, zum Beispiel eine umbenannte
oder neue Spalte, ein anderes Datumsformat oder ein neuer Wertebereich.
**Ablauf:** Der nächste Lauf verarbeitet die Lieferung wie gewohnt. Die Zeilen, die vom
geänderten Aufbau betroffen sind, werden aussortiert; die übrigen kommen im Ziel an. Der
Pipeline-Entwickler bemerkt die Änderung am Ergebnis des Laufs, sieht, was genau sich
geändert hat, und hat die betroffenen Zeilen vorliegen, um die Pipeline anzupassen.

**Erzwingt Entscheidungen:**
- Woran bemerkt der Pipeline-Entwickler eine Änderung: am Anstieg der aussortierten Zeilen, an einem Abgleich gegen einen erwarteten Aufbau, oder an beidem?
- Fehlt eine erwartete Spalte ganz: Werden alle Zeilen aussortiert, oder ist das ein Konfigurationsfehler, der den Lauf stoppt? → wählbar: [D3](10-design-decisions.md#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen)
- Wie wird eine neue, unerwartete Spalte behandelt (ignorieren, melden, durchreichen)?
- Wie wird "was genau hat sich geändert" dargestellt (Spalte, betroffene Werte, Beispiele)?

### UC3 — Pipeline-Entwickler untersucht aussortierte Zeilen

**Akteur(e):** Pipeline-Entwickler
**Auslöser:** Ein Lauf hat Zeilen aussortiert (nach [UC1](#uc1-geplanter-lauf-uber-eine-lieferung) oder [UC2](#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)).
**Ablauf:** Der Pipeline-Entwickler holt sich die aussortierten Zeilen des Laufs. Jede
liegt im Originalzustand vor, so wie sie in der Lieferung stand, zusammen mit dem Schritt,
der sie aussortiert hat, dem Grund, der betroffenen Spalte und dem Rohwert. Er findet die
Zeile in der Originaldatei wieder und kann den Fehler nachstellen.

**Erzwingt Entscheidungen:**
- Was genau ist der Originalzustand: die Zellwerte, wie der Reader sie gelesen hat, oder auch die Rohbytes (z. B. eine CSV-Zeile mit kaputten Anführungszeichen)? → [D1](10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten), genauer Umfang offen: [G1](70-gap-ledger.md#g1-was-der-rohzustand-einer-zeile-umfasst)
- Wie wird eine Zeile in der Quelle wiedergefunden (Datei, Sheet, Zeilennummer, Byte-Offset), und bleibt das über Sortieren, Filtern und Joins hinweg erhalten?
- Was wird bei großen Lieferungen für jede Zeile mitgeführt, damit der Originalzustand der aussortierten Zeilen verfügbar ist, ohne den Speicherbedarf aller Zeilen zu vervielfachen? → offen, [G1](70-gap-ledger.md#g1-was-der-rohzustand-einer-zeile-umfasst)
- Wird eine Zeile, die in mehreren Schritten scheitern würde, beim ersten Fehler aussortiert, oder werden alle Gründe gesammelt? → offen, [G2](70-gap-ledger.md#g2-welche-info-spalten-eine-aussortierte-zeile-tragt)

### UC4 — Aussortierte Zeilen werden nach einer Anpassung nachverarbeitet

**Akteur(e):** Pipeline-Entwickler, Ziel
**Auslöser:** Der Pipeline-Entwickler hat die Pipeline an eine Änderung angepasst
(nach [UC2](#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)) und will die
Daten, die im Lauf gefehlt haben, nachliefern.
**Ablauf:** Er startet die angepasste Pipeline auf den aussortierten Zeilen eines früheren
Laufs statt auf einer neuen Lieferung. Die jetzt verarbeitbaren Zeilen kommen im Ziel an;
was weiterhin scheitert, wird wieder aussortiert.

**Erzwingt Entscheidungen:**
- Können aussortierte Zeilen direkt als Quelle einer Pipeline dienen, mit ihrem Originalzustand statt ihrem Zustand zum Zeitpunkt des Fehlers?
- Wie und wie lange werden aussortierte Zeilen zwischen Läufen aufbewahrt?
- Wie werden doppelte Einträge im Ziel vermieden, wenn Zeilen nachgeliefert werden?

### UC5 — Nicht verarbeitbare Zeilen laufen im selben Lauf durch einen eigenen Zweig

**Akteur(e):** Pipeline-Entwickler
**Auslöser:** Der Pipeline-Entwickler weiß, dass ein Teil der Zeilen einen Schritt nicht
besteht, und will sie anders behandeln, statt sie nur auszusortieren.
**Ablauf:** In der Pipeline zweigt er die Zeilen ab, die einen Schritt nicht bestehen,
verarbeitet sie mit anderen Regeln weiter und führt sie danach wieder mit den übrigen
zusammen. Was auch im Zweig scheitert, wird aussortiert.

**Erzwingt Entscheidungen:**
- Wie werden die gescheiterten Zeilen eines Schritts innerhalb des Laufs abgezweigt, und in welchem Zustand (Original oder Stand vor dem Schritt)?
- Wie werden Zweige wieder zusammengeführt, wenn sie unterschiedliche Spalten oder Typen haben?
- Wie bleibt bei einer Zeile, die im Zweig scheitert, nachvollziehbar, dass sie zuvor schon im Hauptweg gescheitert war?

### UC6 — Eine große Lieferung wird verarbeitet, ohne vollständig im RAM zu liegen

**Akteur(e):** Pipeline-Entwickler, Scheduler
**Auslöser:** Eine Lieferung ist groß (bis in den zweistelligen GB-Bereich).
**Ablauf:** Dieselbe Pipeline, die für kleine Lieferungen geschrieben wurde, verarbeitet
die große Lieferung, ohne sie vollständig in den Speicher zu laden. Die Verarbeitung
beginnt, während noch gelesen wird. Der Pipeline-Entwickler muss dafür höchstens an einer
Stelle etwas ändern, nicht jeden Schritt umschreiben. Der Lauf ist langsamer als ein
spezialisiertes Werkzeug, aber er kommt mit begrenztem Speicher durch.

**Erzwingt Entscheidungen:**
- Wie wird zwischen "alles im Speicher" und "während des Lesens verarbeiten" umgeschaltet, und muss der Entwickler das überhaupt wählen? → [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)
- Was passiert mit Schritten, die alle Zeilen brauchen (Sortieren, Gruppieren, Joins, Pivot), wenn nicht alles in den Speicher passt: auslagern auf die Platte, verbieten, oder nur für kleine Seiten erlauben? → auslagern: [D6](10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)
- Wie wird der Speicherbedarf begrenzt oder konfiguriert?
- Braucht es eine veränderbare (mutable) Datenstruktur, um schnell und speichersparend genug zu sein, oder erreicht eine unveränderliche mit geteilten Spalten dieselben Ergebnisse? Der Maintainer braucht die mutable Variante nicht, wenn es ohne sie gleich gut geht. → Engine entscheidet: [D7](10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert); Voreinstellung unveränderlich: [D5](10-design-decisions.md#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen); Bedarf offen: [G5](70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht)
- Bringt spaltenorientierte Speicherung hier messbare Vorteile?

### UC7 — Externer Entwickler baut seine erste Pipeline

**Akteur(e):** Externer Entwickler
**Auslöser:** Jemand ohne Kontext des Maintainers will schmutzige Lieferungen, z. B. Excel
von Anbietern, stabil verarbeiten und bindet die Library ein.
**Ablauf:** Er liest eine Datei ein, beschreibt die Schritte, lässt den Lauf laufen und
bekommt Ergebnis und aussortierte Zeilen, ohne vorher das Fehlermodell im Detail verstehen
zu müssen. Die Voreinstellungen führen zu einem stabilen Lauf, nicht zu einem Abbruch beim
ersten schmutzigen Wert.

**Erzwingt Entscheidungen:**
- Welche Voreinstellungen gelten, wenn der Entwickler nichts zum Fehlerverhalten angibt? → [D5](10-design-decisions.md#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen)
- Welche Typen tauchen in der öffentlichen API auf (eigene Typen der Library, Standardtypen, Typen aus gseq)?
- Wie viele Wege gibt es, dieselbe Operation auszudrücken (Methode, Pipeline-Schritt, Ausdruck), und welcher ist der naheliegende?
- Welche Stabilitätszusage gibt v2 gegenüber externen Nutzern?
