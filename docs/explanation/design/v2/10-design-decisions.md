# Design Decisions

Jede Entscheidung nennt, wer sie wann getroffen hat, und die Use Cases, deren Fragen sie
beantwortet. Bis zum Freeze sind die Einträge Entwurf. Danach werden sie nur noch per
Amendment oder Ersetzung geändert.

### D1 — Aussortierte Zeilen sind eine Tabelle aus Rohzustand und Info-Spalten

**Entscheidung:** Die aussortierten Zeilen eines Laufs werden als Tabelle zurückgegeben.
Jede Zeile enthält ihre Werte im Rohzustand, also so, wie sie aus der Quelle kamen, und
nicht im Zustand des Schritts, in dem sie gescheitert ist. Dazu kommen Info-Spalten, die
beschreiben, wo und warum die Zeile aussortiert wurde. Was "Rohzustand" genau umfasst,
ist offen ([G1](70-gap-ledger.md#g1-was-der-rohzustand-einer-zeile-umfasst)). Welche
Info-Spalten es gibt, ebenfalls ([G2](70-gap-ledger.md#g2-welche-info-spalten-eine-aussortierte-zeile-tragt)).
**Begründung:** Eine Tabelle lässt sich mit allen Mitteln der Library weiterverarbeiten:
filtern, in einen Zweig geben, schreiben. Ein eigenes Fehlerformat bräuchte eigene
Werkzeuge. Der Rohzustand ist nötig, damit der Pipeline-Entwickler den Fehler an der
Originallieferung nachstellen kann. Der Zustand zum Fehlerzeitpunkt zeigt nur, was die
Pipeline aus der Zeile gemacht hatte. Die v1-Variante (`etl.ErrorLog` mit `OriginalRow`
als `map[string]string`) war ein Zusatz, den man erst einhängen musste. In v2 ist die
Tabelle Teil jedes Ergebnisses.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)

### D2 — Aussortierte Zeilen werden über dieselben Writer geschrieben wie Ergebnisse

**Entscheidung:** Die Library legt nicht fest, wohin aussortierte Zeilen gehen. Weil sie
nach [D1](#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten)
eine Tabelle sind, schreibt der Pipeline-Entwickler sie mit einem beliebigen Writer in
Format und Ziel seiner Wahl (Datei, Datenbank, HTTP), programmatisch in der Pipeline.
**Begründung:** Die Ziele unterscheiden sich je Pipeline und Kunde: Datei, Datenbank oder
ein HTTP-Endpunkt. Ein fest eingebauter Ablageort würde keins davon gut abdecken. Über
die normalen Writer gibt es für Fehler und Ergebnisse denselben Weg. Ob die Library
Writer für Datenbank und HTTP selbst mitbringt, ist damit nicht entschieden
([G6](70-gap-ledger.md#g6-writer-fur-datenbank-und-http)).
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D3 — Das Fehlerverhalten ist pro Pipeline wählbar: aussortieren oder sofort stoppen

**Entscheidung:** Eine Pipeline kann für Datenfehler festlegen, ob betroffene Zeilen
aussortiert werden und der Lauf weitergeht, oder ob der Lauf beim ersten Datenfehler
stoppt. Diese Wahl ersetzt das `strict`-Build-Tag aus v1. Sie ist eine Laufzeit-Einstellung
der Pipeline und hängt nicht davon ab, wie das Programm gebaut wurde.
**Begründung:** Beides wird gebraucht. Im laufenden Betrieb sollen Daten ankommen, auch
wenn ein Teil fehlerhaft ist. Beim Entwickeln oder bei einer bekannt kritischen Lieferung
soll ein Fehler sofort sichtbar werden. In v1 wechselte das `strict`-Tag das Verhalten
für das ganze Programm und vermischte Programmier- und Datenfehler. Das hat 53 Tests auf
die falsche Seite gebracht (Issue #40).
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D4 — Eine Pipeline kann eine Schwelle für aussortierte Zeilen festlegen

**Entscheidung:** Eine Pipeline kann eine Schwelle definieren. Werden mehr Zeilen
aussortiert, gilt der Lauf als fehlgeschlagen, auch wenn er im Modus "aussortieren" aus
[D3](#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen)
läuft. Ohne Schwelle gilt ein Lauf als erfolgreich, sobald er durchgekommen ist. Wie die
Schwelle bemessen wird und was beim Überschreiten passiert, ist offen
([G3](70-gap-ledger.md#g3-bemessung-und-wirkung-der-schwelle)).
**Begründung:** Ändert ein Datenlieferant sein Format, scheitert oft ein großer Teil der
Zeilen oder alle. Einen solchen Lauf als erfolgreich zu melden, würde die Änderung
verdecken. Die Schwelle macht aus "viele Zeilen aussortiert" ein Signal, das der Scheduler
sieht.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D5 — Voreinstellung: Durchlauf mit Aussortieren, unveränderliche Tabellen

**Entscheidung:** Gibt der Pipeline-Entwickler nichts an, läuft die Pipeline im Modus
"aussortieren" aus [D3](#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen):
Zeilen mit Datenfehlern werden herausgefiltert, der Lauf kommt durch. Tabellen sind
standardmäßig unveränderlich. Ob es zusätzlich eine veränderbare Variante gibt, ist offen
([G5](70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht)).
**Begründung:** Der Zweck der Library ist, schmutzige Lieferungen stabil zu verarbeiten.
Eine Voreinstellung, die beim ersten schmutzigen Wert abbricht, würde externen Nutzern
genau das verwehren. Unveränderliche Tabellen machen Zweige ([UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig))
und den Rohzustand ([D1](#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten))
einfacher, weil kein Schritt die Daten eines anderen verändert.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline), [UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)
