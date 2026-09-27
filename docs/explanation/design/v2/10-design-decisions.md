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

### D6 — Pipelines sind Pläne, die in Blöcken ausgeführt werden und auf die Platte auslagern können

**Entscheidung:** Eine Pipeline beschreibt einen Plan. Ausgeführt wird er erst, wenn das
Ergebnis angefordert wird. Die Engine verarbeitet die Daten in Blöcken von Zeilen, die
schon während des Lesens durch die Schritte fließen. Schritte, die alle Zeilen brauchen
(Sortieren, Gruppieren, Joins, Pivot), sammeln ihre Blöcke und lagern auf die Platte aus,
wenn ein Speicherbudget erreicht ist. Derselbe Pipeline-Code läuft für kleine Lieferungen
im Speicher und für große im Streaming. Die Wahl trifft die Engine, nicht der
Pipeline-Entwickler.
**Begründung:** Das ist das Modell von Polars, DuckDB und DataFusion. Es ist das einzige
der betrachteten Modelle, das [UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)
vollständig erfüllt: große Lieferungen, begrenzter Speicher, kein Umbau der Pipeline.
Verworfen wurden (B) Blöcke mit Speicherbudget ohne Auslagern, bei dem Schritte über
alle Zeilen am Budget scheitern, und (C) blockweise Verarbeitung durch den Entwickler
selbst, die "ohne großen Umbau" widerspricht. Der Plan ermöglicht außerdem, Schritte vor
dem Lauf zu prüfen und zu optimieren. Der Aufwand für das Auslagern ist hoch und wird im
Scope des Prototyps bewusst begrenzt.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Option A aus dem Vergleich mit pandas, Polars, DuckDB, Spark/Dask, DataFusion)
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D7 — Die Engine entscheidet, ob sie Daten kopiert oder an Ort und Stelle ändert

**Entscheidung:** Für den Pipeline-Entwickler sind Tabellen unveränderlich
([D5](#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen)): Ein
Schritt verändert nie Daten, die ein anderer Schritt, ein Zweig oder die aussortierten
Zeilen noch sehen. Intern darf die Engine Daten an Ort und Stelle ändern, wenn niemand
sonst sie sieht, und kopieren, wenn sie geteilt sind. Es gibt keine eigene veränderbare
Tabelle in der API. Zusätzlich gibt es einen harten Schalter am Anfang der Pipeline,
dessen genaue Wirkung offen ist ([G7](70-gap-ledger.md#g7-wirkung-des-harten-schalters-fur-kopieren-und-andern)).
**Begründung:** In v1 gab es jede Operation doppelt, auf `Table` und `MutableTable`. Der
Maintainer hat `MutableTable` nur wegen Tempo und Speicher genutzt, nicht wegen einer
anderen Bedeutung. Entscheidet die Engine anhand dessen, ob Daten geteilt sind, fällt die
doppelte API weg, und der Gewinn bleibt erhalten, wo er ohne Risiko möglich ist. Ob das
genauso schnell und sparsam ist wie v1 `MutableTable`, prüft der Prototyp
([G5](70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht)).
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D8 — Eine Option legt fest, dass die Engine immer kopiert oder immer an Ort und Stelle ändert

**Entscheidung:** Ohne Angabe entscheidet die Engine nach
[D7](#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert) selbst.
Eine Option am Anfang der Pipeline setzt einen von zwei festen Modi: **immer kopieren**
oder **immer an Ort und Stelle ändern**. Der Modus gilt für den ganzen Lauf.
**Begründung:** Die Automatik ist die sichere Voreinstellung. Der Pipeline-Entwickler
behält aber die Kontrolle für die Fälle, in denen er es besser weiß: beim Debuggen will
er jeden Zwischenstand behalten, bei einer großen Lieferung ohne Zweige will er Speicher
und Zeit sparen. Wie "immer ändern" mit Zweigen und dem Rohzustand verträglich bleibt,
ist offen ([G8](70-gap-ledger.md#g8-immer-andern-gegen-zweige-und-rohzustand)).
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)

### D9 — Der Rohzustand wird getrennt gehalten, an Verzweigungen wird immer kopiert

**Entscheidung:** Der Rohzustand einer Zeile wird beim Lesen gesichert und getrennt von
den Arbeitsdaten gehalten. Kein Schritt und kein Modus aus
[D8](#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert)
kann ihn verändern. Wo sich ein Plan verzweigt, kopiert die Engine die Arbeitsdaten auch
im Modus "immer an Ort und Stelle ändern" und vermerkt das im Trace des Laufs.
**Begründung:** So gelten [D1](#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten)
(Rohzustand) und [D7](#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert)
(kein Zweig sieht Änderungen eines anderen) in jedem Modus, und der Modus "immer ändern"
behält seinen Nutzen für lineare Pipelines. Verworfen: Pläne mit Verzweigung im Modus
"immer ändern" vor dem Lauf abzulehnen. Das hätte den Modus für
[UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)
unbrauchbar gemacht. Der Vermerk im Trace macht sichtbar, wo der Modus nicht greift.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G8](70-gap-ledger.md#g8-immer-andern-gegen-zweige-und-rohzustand))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D10 — Rohzustand heißt gelesene Zellwerte, Rohbytes bei unzerlegbaren Zeilen, und immer die Fundstelle

**Entscheidung:** Der Rohzustand einer Zeile besteht aus den Zellwerten, wie der Reader
sie gelesen hat. Konnte der Reader eine Zeile nicht in Zellen zerlegen (z. B. CSV mit
kaputten Anführungszeichen oder falscher Spaltenzahl), enthält der Rohzustand stattdessen
die Rohbytes der Quellzeile. Jede Zeile trägt außerdem immer ihre Fundstelle in der
Quelle: Quelle bzw. Datei, Sheet falls vorhanden, Zeilennummer und, wo die Quelle es
erlaubt, Byte-Offset. Der Rohzustand wird nur so lange mitgeführt, bis feststeht, dass die
Zeile durchgekommen ist. Bei großen Läufen kann die Engine ihn auf die Platte auslagern
([D6](#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)).
**Begründung:** Zellwerte reichen, um den Fehler an der Lieferung nachzustellen, und
ergeben eine Tabelle mit denselben Spalten wie die Lieferung. Rohbytes sind nur dort
nötig, wo es gar keine Zellen gibt. Nur die Fundstelle zu halten und später nachzulesen
wurde verworfen. Das setzt voraus, dass die Quelle noch unverändert vorliegt, und geht bei
HTTP- und Streaming-Quellen nicht. Die Fundstelle kommt trotzdem immer mit, damit der
Pipeline-Entwickler die Zeile in der Originaldatei findet.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G1](70-gap-ledger.md#g1-was-der-rohzustand-einer-zeile-umfasst))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet), [UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D11 — Scheitert eine Zeile nach einem Join, wird jede beteiligte Quellzeile aussortiert

**Entscheidung:** Scheitert eine Zeile, die aus einem Join entstanden ist, landet jede
beteiligte Quellzeile mit ihrem eigenen Rohzustand und ihrer eigenen Fundstelle nach
[D10](#d10-rohzustand-heisst-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
in den aussortierten Zeilen. Eine gemeinsame Kennung (`reject_id`) verbindet die Einträge,
die zu demselben Fehler gehören. Wie die aussortierten Zeilen verschiedener Quellen
zusammen dargestellt werden, ist offen ([G10](70-gap-ledger.md#g10-aussortierte-zeilen-mehrerer-quellen-eine-tabelle-oder-je-quelle)).
**Begründung:** Jede aussortierte Zeile behält die Spalten ihrer eigenen Lieferung und
lässt sich dort wiederfinden und nachverarbeiten
([UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)).
Eine zusammengeführte Rohzeile beider Seiten gäbe es in keiner Quelle.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Teil der Auflösung von [G9](70-gap-ledger.md#g9-rohzustand-und-fundstelle-nach-aggregation-und-join))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D12 — Der Rohzustand reicht bis zum ersten Schritt über alle Zeilen, danach wird die aggregierte Zeile aussortiert

**Entscheidung:** Rohzustand und Fundstelle der Quellzeilen werden bis zum ersten Schritt
mitgeführt, der alle Zeilen braucht und sie zusammenfasst (Gruppieren, Pivot und
Ähnliches). Scheitert eine Zeile nach diesem Schritt, wird sie als aggregierte Zeile
aussortiert: mit ihren Werten, dem Gruppenschlüssel, der Anzahl der eingegangenen
Quellzeilen und dem Grund. Eine Option "volle Herkunft" führt zusätzlich die Kennungen der
Quellzeilen über die Zusammenfassung hinaus mit. Eine Quellzeile gilt als durchgekommen,
sobald sie einen zusammenfassenden Schritt erreicht hat oder alle aus ihr entstandenen
Zeilen im Ziel angekommen sind. Danach wird ihr Rohzustand freigegeben.
**Begründung:** In eine aggregierte Zeile gehen bei großen Lieferungen bis zu Millionen
Quellzeilen ein. Deren Rohzustand mitzuführen, würde den Speicherrahmen aus
[UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)
sprengen. Über den Gruppenschlüssel lassen sich die Quellzeilen bei Bedarf in der Lieferung
finden. Die Option "volle Herkunft" deckt kleine Läufe und das Debuggen ab.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G9](70-gap-ledger.md#g9-rohzustand-und-fundstelle-nach-aggregation-und-join), zusammen mit [D11](#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D13 — Aussortierte Zeilen gibt es je Quelle, dazu eine Übersicht über alle Quellen

**Entscheidung:** Das Ergebnis eines Laufs liefert je Quelle eine Tabelle aussortierter
Zeilen mit den Rohspalten dieser Quelle und den Info-Spalten aus
[D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix). Zusätzlich gibt es
eine Übersicht über alle Quellen, die nur die Info-Spalten enthält. Die Übersicht ist die
Grundlage für Zählungen, Meldungen und die Schwelle aus
[D4](#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen).
**Begründung:** Quellen haben unterschiedliche Spalten. Eine Tabelle je Quelle lässt sich
schreiben ([D2](#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse))
und wieder als Quelle verwenden
([UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)).
Verworfen: eine gemeinsame Tabelle, deren Rohspalten je nach Quelle unterschiedlich belegt
sind. Die ist schwer zu schreiben und nachzuverarbeiten.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G10](70-gap-ledger.md#g10-aussortierte-zeilen-mehrerer-quellen-eine-tabelle-oder-je-quelle); Validierung im Prototyp: [G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D14 — Info-Spalten tragen ein reserviertes, einstellbares Präfix

**Entscheidung:** Die Info-Spalten einer aussortierten Zeile tragen ein reserviertes
Präfix, standardmäßig `_gseq_`, das pro Pipeline einstellbar ist. Es gibt diese Spalten:
`reject_id` (verbindet zusammengehörige Einträge), `run_id`, `record_key` und optional
`record_hash` nach [D18](#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash), die Fundstelle nach
[D10](#d10-rohzustand-heisst-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
(`source`, `sheet`, `line`, `offset`), `step`, `column`, `value` (Wert zum Zeitpunkt des
Fehlers), `reason` (Text), `code` (feste Fehlerart, z. B. `parse`, `missing_column`,
`validation`, `unparseable_line`) und `raw_line` (nur bei Zeilen, die sich nicht zerlegen
ließen).
**Begründung:** Das Präfix verhindert Kollisionen mit Datenspalten. Der feste Code erlaubt
es, ohne Textvergleich zu filtern und Zweige zu bilden
([UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)).
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G2](70-gap-ledger.md#g2-welche-info-spalten-eine-aussortierte-zeile-tragt); Validierung im Prototyp: [G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)

### D15 — Eine Zeile wird im ersten scheiternden Schritt aussortiert, mit einem Eintrag je betroffener Spalte

**Entscheidung:** Eine Zeile wird im ersten Schritt aussortiert, in dem sie scheitert.
Spätere Schritte sehen sie nicht mehr. Scheitern in diesem Schritt mehrere Spalten, gibt
es je Spalte einen Eintrag mit derselben `reject_id` nach
[D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix).
**Begründung:** Der Pipeline-Entwickler sieht so alle Probleme einer Zeile in dem Schritt,
der sie aussortiert hat, und nicht nur das erste. Alle Schritte weiter auf einer
gescheiterten Zeile laufen zu lassen, wurde verworfen. Deren Ergebnisse wären Folgefehler
und kaum aussagekräftig.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Validierung im Prototyp: [G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)

### D16 — Aussortierte Zeilen können Quelle eines Laufs sein und behalten ihre ursprüngliche Fundstelle

**Entscheidung:** Eine eigene Quelle liest eine Tabelle aussortierter Zeilen
([D13](#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen))
als Eingabe eines Laufs. Sie übernimmt die Rohspalten, lässt die Info-Spalten weg und
setzt die ursprüngliche Fundstelle aus
[D10](#d10-rohzustand-heisst-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
als Fundstelle jeder Zeile. Zeilen mit `raw_line` gehen erneut durch den Reader der
ursprünglichen Quelle, mit dessen aktueller Konfiguration.
**Begründung:** So verarbeitet dieselbe, angepasste Pipeline die fehlenden Daten nach,
ohne Sonderweg. Scheitert eine Zeile erneut, zeigt der neue Eintrag weiterhin auf die
Original-Lieferung und nicht auf die Datei mit den aussortierten Zeilen. Wurde der Reader
angepasst (z. B. CSV-Einstellungen), werden bisher unzerlegbare Zeilen jetzt vielleicht
lesbar.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D17 — Die Library bewahrt aussortierte Zeilen nicht selbst auf

**Entscheidung:** Die Library speichert aussortierte Zeilen nicht über das Ende eines
Laufs hinaus. Die Pipeline schreibt sie nach
[D2](#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse)
in ein Ziel ihrer Wahl und liest sie für eine Nachverarbeitung mit dem passenden Reader
wieder ein. Wie lange sie aufbewahrt werden, entscheiden Pipeline und Ziel.
**Begründung:** Eine eigene Ablage in der Library würde mit dem Writer-Weg aus
[D2](#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse)
konkurrieren und Speicherort, Format und Aufbewahrung vorgeben, die je Kunde verschieden
sind.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D18 — Jede Quellzeile trägt einen stabilen Schlüssel, optional einen Inhalts-Hash

**Entscheidung:** Jede Quellzeile trägt einen Schlüssel `record_key`, gebildet aus Quelle
und Fundstelle. Er ist bei einer Nachverarbeitung nach
[D16](#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle)
derselbe wie im ursprünglichen Lauf, steht in Ergebnissen zur Verfügung und ist eine
Info-Spalte der aussortierten Zeilen
([D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix)). Optional kommt
ein Hash über den Rohinhalt dazu (`record_hash`). Doppelte Einträge im Ziel verhindert
die Pipeline selbst, zum Beispiel per Upsert auf diesem Schlüssel.
**Begründung:** Ob und wie ein Ziel dedupliziert, hängt vom Ziel ab (Datenbank, Datei,
HTTP). Die Library kann das nicht für alle Ziele lösen, aber sie kann den Schlüssel
liefern. Der Inhalts-Hash deckt den Fall ab, dass dieselbe Lieferung unter anderem Namen
erneut kommt und der Fundstellen-Schlüssel dann ein anderer wäre.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D19 — Es gibt drei Fehlerarten: Planfehler, Lieferfehler und Datenfehler

**Entscheidung:** Fehler fallen in drei Arten.
**Planfehler** betreffen die Pipeline selbst, zum Beispiel ein Ausdruck auf eine Spalte,
die kein Schritt erzeugt, ein Typkonflikt oder ein ungültiger Parameter. Sie werden vor
dem Lauf geprüft, und der Lauf startet nicht.
**Lieferfehler** betreffen den Aufbau einer Lieferung, zum Beispiel eine fehlende
erwartete Spalte oder ein fehlendes Sheet. Sie werden beim Lesen des Kopfs erkannt.
**Datenfehler** betreffen einzelne Zeilen, zum Beispiel einen nicht parsebaren Wert, eine
verletzte Validierung oder eine unzerlegbare Zeile.
Wie Liefer- und Datenfehler behandelt werden, ist konfigurierbar, je Fehlerart und je
Fehlercode ([D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix)). Zur
Wahl stehen "aussortieren" und "stoppen" nach
[D3](#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen).
Voreinstellung für beide ist die Einstellung der Pipeline, standardmäßig "aussortieren"
([D5](#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen)). Ein
Lieferfehler, der aussortiert wird, sortiert alle betroffenen Zeilen aus (bei einer
fehlenden Spalte: alle Zeilen, Code `missing_column`).
**Begründung:** Planfehler sind Fehler der Pipeline, nicht der Lieferung. Einen Lauf trotz
eines solchen Fehlers durchzuziehen, bringt keine brauchbaren Ergebnisse, deshalb ist ihr
Verhalten nicht konfigurierbar. Bei Liefer- und Datenfehlern will der Maintainer je nach
Pipeline und Kunde unterschiedlich reagieren, deshalb ist ihr Verhalten konfigurierbar.
Sortiert ein Lieferfehler alle Zeilen aus, macht die Schwelle aus
[D20](#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-standardmassig-zu-ende-laufen)
die Formatänderung aus [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt) sichtbar.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt), [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D20 — Die Schwelle ist absolut oder als Anteil, je Lauf oder je Schritt, und lässt den Lauf standardmäßig zu Ende laufen

**Entscheidung:** Die Schwelle aus
[D4](#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen) wird absolut
(Anzahl Zeilen), als Anteil (Prozent der gelesenen Zeilen) oder beides angegeben.
Standardmäßig gilt sie für den ganzen Lauf, optional auch je Schritt. Wird sie
überschritten, läuft der Lauf standardmäßig bis zum Ende und gilt dann als fehlgeschlagen.
Eine Option bricht den Lauf stattdessen sofort ab.
**Begründung:** Läuft der Lauf zu Ende, liegen alle aussortierten Zeilen vor, und der
Pipeline-Entwickler sieht bei einer Formatänderung das ganze Ausmaß. Der sofortige Abbruch
spart bei großen Lieferungen Zeit, wenn das Ausmaß nicht gebraucht wird.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G3](70-gap-ledger.md#g3-bemessung-und-wirkung-der-schwelle))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt), [UC6](05-use-cases.md#uc6-eine-grosse-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D21 — Ein Lauf liefert einen Status und Zählungen, aus denen sich ein Exit-Code ableiten lässt

**Entscheidung:** Das Ergebnis eines Laufs trägt einen Status: `ok`, `failed_threshold`
(Schwelle überschritten), `aborted` (gestoppt nach
[D3](#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen)
oder abgebrochen über den Kontext) oder `plan_error` (Planfehler nach
[D19](#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler)). Dazu
kommen die Zählungen gelesener, durchgelaufener und aussortierter Zeilen, je Schritt und
je Fehlercode. Eine Hilfsfunktion bildet den Status auf einen Exit-Code für den Scheduler ab.
**Begründung:** Der Scheduler braucht ein eindeutiges Signal, der Pipeline-Entwickler die
Zählungen, um zu sehen, wo etwas passiert ist. Den Status als Wert zurückzugeben, statt ihn
nur über einen `error` auszudrücken, erlaubt es, zwischen "Lauf gescheitert" und
"Lauf durch, aber zu viele aussortiert" zu unterscheiden.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G3](70-gap-ledger.md#g3-bemessung-und-wirkung-der-schwelle))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D22 — Eine Quelle kann einen erwarteten Aufbau haben, gegen den die Lieferung beim Lesen geprüft wird

**Entscheidung:** Zu jeder Quelle kann die Pipeline einen erwarteten Aufbau angeben:
Spalten, Typen und bei Bedarf Formate (z. B. ein Datumsformat). Man schreibt ihn von Hand
oder leitet ihn einmal aus einer Referenz-Lieferung ab und legt ihn im Code ab. Beim Lesen
des Kopfs vergleicht die Engine die Lieferung damit. Eine fehlende Spalte ist ein
Lieferfehler nach [D19](#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler).
Für neue, unerwartete Spalten ist konfigurierbar, ob sie ignoriert, gemeldet oder
durchgereicht werden. Standard: melden und durchreichen. Fehlt eine Spalte und ist eine
ähnlich benannte neu, meldet die Engine eine vermutliche Umbenennung.
**Begründung:** Eine Formatänderung soll beim Lesen des Kopfs auffallen und nicht erst
daran, dass viele Zeilen scheitern. Neue Spalten brechen die Pipeline in der Regel nicht,
sollen aber sichtbar werden. Deshalb ist der Standard "melden und durchreichen" und nicht
"ignorieren" oder "Fehler". Ohne erwarteten Aufbau verhält sich eine Quelle wie in v1:
Sie liest, was kommt.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D23 — Das Laufergebnis enthält einen Änderungsbericht

**Entscheidung:** Das Ergebnis eines Laufs enthält eine Tabelle mit einer Zeile je Befund:
Art (`missing_column`, `new_column`, `probably_renamed`, `format_change`), Quelle, Spalte,
Detail, Anzahl und Beispiele. `format_change` entsteht, wenn ein erheblicher Teil der Werte
einer Spalte mit demselben Fehlercode scheitert. Die Beispiele zeigen, wie die Werte jetzt
aussehen.
**Begründung:** Die aussortierten Zeilen
([D13](#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen))
sind die vollständige Grundlage. Der Bericht beantwortet die Frage "was genau hat sich
geändert?" aus [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)
auf einen Blick. Ab welchem Anteil eine Häufung als `format_change` gilt, ist offen
([G12](70-gap-ledger.md#g12-ab-wann-eine-haufung-von-fehlern-als-formatanderung-gilt)).
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt), [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D24 — Ein Lauf kann ein Profil liefern, das mit dem Profil eines früheren Laufs verglichen wird

**Entscheidung:** Ein Lauf kann ein Profil seiner Quellen als Tabelle liefern: je Spalte
Anteil leerer Werte, erkannte Typen, Anzahl verschiedener Werte und Parse-Quote. Die
Pipeline schreibt es weg ([D17](#d17-die-library-bewahrt-aussortierte-zeilen-nicht-selbst-auf)
gilt sinngemäß). Ein späterer Lauf vergleicht sein Profil damit und meldet auffällige
Abweichungen als Befunde im Änderungsbericht aus
[D23](#d23-das-laufergebnis-enthalt-einen-anderungsbericht). Das gehört nicht zum Scope
des Prototyps.
**Begründung:** Manche Änderungen lassen keine Zeile scheitern, zum Beispiel wenn
plötzlich ein Drittel der Kundennummern leer ist. Die fallen nur im Vergleich mit früheren
Läufen auf.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)
