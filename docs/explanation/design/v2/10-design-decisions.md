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
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D6 — Pipelines sind Pläne, die in Blöcken ausgeführt werden und auf die Platte auslagern können

**Entscheidung:** Eine Pipeline beschreibt einen Plan. Ausgeführt wird er erst, wenn das
Ergebnis angefordert wird. Die Engine verarbeitet die Daten in Blöcken von Zeilen, die
schon während des Lesens durch die Schritte fließen. Schritte, die alle Zeilen brauchen
(Sortieren, Gruppieren, Joins, Pivot), sammeln ihre Blöcke und lagern auf die Platte aus,
wenn ein Speicherbudget erreicht ist. Derselbe Pipeline-Code läuft für kleine Lieferungen
im Speicher und für große im Streaming. Die Wahl trifft die Engine, nicht der
Pipeline-Entwickler.
**Begründung:** Das ist das Modell von Polars, DuckDB und DataFusion. Es ist das einzige
der betrachteten Modelle, das [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)
vollständig erfüllt: große Lieferungen, begrenzter Speicher, kein Umbau der Pipeline.
Verworfen wurden (B) Blöcke mit Speicherbudget ohne Auslagern, bei dem Schritte über
alle Zeilen am Budget scheitern, und (C) blockweise Verarbeitung durch den Entwickler
selbst, die "ohne großen Umbau" widerspricht. Der Plan ermöglicht außerdem, Schritte vor
dem Lauf zu prüfen und zu optimieren. Der Aufwand für das Auslagern ist hoch und wird im
Scope des Prototyps bewusst begrenzt.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Option A aus dem Vergleich mit pandas, Polars, DuckDB, Spark/Dask, DataFusion)
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

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
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

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
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)

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
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D10 — Rohzustand bedeutet gelesene Zellwerte, Rohbytes bei unzerlegbaren Zeilen, und immer die Fundstelle

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
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D11 — Scheitert eine Zeile nach einem Join, wird jede beteiligte Quellzeile aussortiert

**Entscheidung:** Scheitert eine Zeile, die aus einem Join entstanden ist, landet jede
beteiligte Quellzeile mit ihrem eigenen Rohzustand und ihrer eigenen Fundstelle nach
[D10](#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
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
[UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)
sprengen. Über den Gruppenschlüssel lassen sich die Quellzeilen bei Bedarf in der Lieferung
finden. Die Option "volle Herkunft" deckt kleine Läufe und das Debuggen ab.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G9](70-gap-ledger.md#g9-rohzustand-und-fundstelle-nach-aggregation-und-join), zusammen mit [D11](#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

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
[D10](#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
(`source`, `sheet`, `line`, `offset`), `step`, `column`, `value` (Wert zum Zeitpunkt des
Fehlers), `reason` (Text), `prev_reason` (Grund aus dem Hauptweg nach [D27](#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg)), `code` (feste Fehlerart, z. B. `parse`, `missing_column`,
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
[D10](#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
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
[D20](#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen)
die Formatänderung aus [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt) sichtbar.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt), [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D20 — Die Schwelle ist absolut oder als Anteil, je Lauf oder je Schritt, und lässt den Lauf per Voreinstellung zu Ende laufen

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
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

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

### D25 — Gescheiterte Zeilen eines Schritts können in einen Zweig gegeben werden und laufen danach zurück

**Entscheidung:** Ein Schritt kann einen Zweig für seine gescheiterten Zeilen haben. Die
Zeilen kommen in dem Zustand in den Zweig, in dem sie in den gescheiterten Schritt
hineingegangen sind, zusammen mit den Info-Spalten aus
[D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix). Ihr Rohzustand
bleibt nach [D9](#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert)
im Hintergrund erhalten. Was der Zweig verarbeiten kann, fließt nach dem Schritt in den
Hauptweg zurück. Was auch im Zweig scheitert, wird aussortiert. Unabhängig davon gibt es
Zweige nach einer Bedingung (`Split`) und das Zusammenführen beliebiger Zweige (`Merge`).
**Begründung:** Der Zweig soll nachholen, was der Schritt nicht geschafft hat. Dafür
braucht er die Zeile mit allen vorherigen Schritten, nicht den Rohzustand. Die
Info-Spalten erlauben es, im Zweig nach Fehlerart zu filtern. Der allgemeine Zweig nach
Bedingung deckt das Umleiten von Zeilen ab, das nichts mit Fehlern zu tun hat.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D26 — Zweige werden nach Spaltennamen zusammengeführt, Typkonflikte sind Planfehler

**Entscheidung:** Beim Zusammenführen von Zweigen werden Spalten nach Namen zugeordnet.
Fehlt eine Spalte in einem Zweig, wird sie mit Nullwerten aufgefüllt. Info-Spalten fallen
beim Zurückführen in den Hauptweg weg. Haben gleichnamige Spalten verschiedene Typen, ist
das ein Planfehler nach [D19](#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler)
und fällt vor dem Lauf auf.
**Begründung:** Der Plan kennt die Typen jeder Spalte in jedem Zweig
([D6](#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)).
Ein Typkonflikt ist deshalb vorher erkennbar und ein Fehler der Pipeline, nicht der
Lieferung. Stilles Umwandeln in einen gemeinsamen Typ würde Fehler verdecken.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)

### D27 — Eine im Zweig erneut gescheiterte Zeile behält ihre Kennung und zeigt ihren Weg

**Entscheidung:** Scheitert eine Zeile, die schon in einem Zweig nach
[D25](#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck)
ist, erneut, behält sie ihre `reject_id`. Die Info-Spalte `step` enthält den ganzen Weg
(z. B. `cast › cast_alt`). Eine zusätzliche Info-Spalte `prev_reason` enthält den Grund aus
dem Hauptweg. Jede Quellzeile wird in den Zählungen nach
[D21](#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst)
nur einmal gezählt.
**Begründung:** Der Pipeline-Entwickler sieht so, dass die Zeile im Hauptweg und im Zweig
gescheitert ist und warum. Mehrfach zu zählen würde die Schwelle aus
[D20](#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen)
verfälschen.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)

### D28 — Ein Lauf hat ein Speicherbudget und ein Verzeichnis zum Auslagern

**Entscheidung:** Jeder Lauf hat ein Speicherbudget, das die Engine nach
[D6](#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)
einhält, indem sie auslagert. Es ist je Lauf einstellbar. Voreinstellung ist ein Anteil
des verfügbaren Speichers, wobei die Engine das Limit eines Containers bzw. der cgroup
berücksichtigt. Das Verzeichnis zum Auslagern ist einstellbar, Standard ist das temporäre
Verzeichnis des Systems. Ausgelagerte Daten werden am Ende des Laufs gelöscht, auch bei
einem Abbruch.
**Begründung:** Läufe starten per Cron, auch in Containern
([UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)). Ein Budget, das das
Container-Limit ignoriert, würde dort zum Abbruch durch das System führen statt zum
Auslagern. Wie groß der Anteil als Voreinstellung sein soll, klärt der Prototyp
([G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange)).
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D29 — Daten laufen in Blöcken typisierter Spalten, Rohspalten bleiben bis zum Cast Text

**Entscheidung:** Die Blöcke aus
[D6](#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)
bestehen aus typisierten Spalten (mindestens Ganzzahl, Gleitkomma, Text, Wahrheitswert,
Zeitpunkt). Die Blockgröße wählt die Engine, sie ist einstellbar. Spalten, die ein Reader
aus einer Lieferung liest, sind Text, bis ein Schritt sie in einen Typ umwandelt.
**Begründung:** Typisierte Spalten sparen Speicher gegenüber Text, lassen sich blockweise
billig weiterreichen und erlauben vektorisierte Verarbeitung, wie sie `experimental/simd`
in v1.2 gemessen hat. Dass Rohspalten Text bleiben, passt zu schmutzigen Lieferungen:
Erst beim Umwandeln entscheidet sich, ob ein Wert passt oder die Zeile aussortiert wird
([D15](#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-eintrag-je-betroffener-spalte)).
Ob der Vorteil gegenüber v1 messbar ist, klärt der Prototyp
([G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange)).
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D30 — Es gibt echte Nullwerte, getrennt vom leeren Text

**Entscheidung:** Jede Spalte kennzeichnet, welche Zellen keinen Wert haben (null). Ein
leerer Text ist ein Wert und nicht null. Beim Lesen wird ein leeres Feld in Rohspalten zu
leerem Text und beim Umwandeln in einen Typ zu null. Welche Texte beim Umwandeln
zusätzlich als null gelten (z. B. `NULL`, `n/a`, `-`), ist konfigurierbar. In Ausdrücken
verhält sich null wie in SQL: Rechnen mit null ergibt null, Vergleiche mit null sind nicht
wahr. Aggregationen überspringen Nullwerte. Das Profil aus
[D24](#d24-ein-lauf-kann-ein-profil-liefern-das-mit-dem-profil-eines-fruheren-laufs-verglichen-wird)
zählt sie.
**Begründung:** In v1 bedeutete `""` gleichzeitig "leer", "fehlt" und "nicht parsebar".
Dadurch sind Fehler still verschwunden. Das Zusammenführen nach
[D26](#d26-zweige-werden-nach-spaltennamen-zusammengefuhrt-typkonflikte-sind-planfehler)
braucht einen Wert für "fehlt". Konfigurierbare Null-Texte decken Platzhalter ab, die in
Anbieter-Excel-Dateien häufig vorkommen.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D31 — Jede Operation gibt es einmal als Wert, mit zwei Einstiegen: sofort auf einer Tabelle oder im Plan

**Entscheidung:** Jede Operation ist genau einmal implementiert, als Wert (`Op`). Es gibt
zwei Einstiege. Eine `Table` ist eine fertige Tabelle im Speicher; ihre Methoden wenden die
Operation sofort an. Eine Pipeline ist ein Plan nach
[D6](#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)
und nimmt dieselben Operationen als Schritte. Beide laufen über dieselbe Engine. Die
Pipeline bietet nur Ablaufsteuerung: Schritte benennen, Zweige
([D25](#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck)),
Fehlerverhalten, Schwelle, Trace und `Run`. Sie bietet keine eigenen Datenoperationen.
Es gibt keine eigene veränderbare Tabelle
([D7](#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert))
und keine Wrapper-Schicht, die Tabellen-Methoden für die Pipeline nachbildet.
**Begründung:** In v1 gab es fast jede Operation viermal (`Table`, `MutableTable`,
`etl.X`, `etl.Mut.X`), und die Pipelines hatten zusätzlich eigene Datenoperationen wie
`TryMap` und `AssertColumns`. Jedes Feature musste bis zu viermal geschrieben werden. Mit
Operationen als Werten gibt es einen Weg, eine Operation auszudrücken, und der naheliegende
Einstieg ergibt sich aus der Frage "kleine Tabelle jetzt" oder "Lieferung im Lauf". Die
Methoden auf `Table` erhalten die v1-Konvention, Tabellen-Operationen als Methoden
anzubieten. Vorbild: Polars mit `DataFrame` und `LazyFrame`.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline), [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D32 — Ausdrücke sind der Standard für Berechnungen, Closures der Ausweg

**Entscheidung:** Berechnungen, Filter und abgeleitete Spalten werden als Ausdrücke
formuliert (z. B. `Col("netto").Mul(Lit(1.19))`). Ausdrücke sind typisiert. Die Engine
prüft sie vor dem Lauf, Fehler darin sind Planfehler nach
[D19](#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler), und sie
führt sie spaltenweise aus. Die String-, Datums- und Rechen-Helfer aus v1 `schema` werden
zu Ausdrucksfunktionen. Für Logik, die sich nicht als Ausdruck fassen lässt, gibt es
Operationen mit Closures. Gibt eine Closure einen Fehler zurück, wird die Zeile mit
`code=custom` und dem Fehlertext als Grund aussortiert.
**Begründung:** Closures (`func(Row) string` in v1) sind für die Engine undurchsichtig:
Sie lassen sich weder vorab prüfen noch spaltenweise oder vektorisiert ausführen. Ohne
Ausweg ließe sich aber Sonderlogik einzelner Kunden nicht ausdrücken. Dass Fehler einer
Closure zu aussortierten Zeilen werden, hält das Fehlermodell auch für eigene Logik
einheitlich.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)

### D33 — Die öffentliche API verwendet Standard-Go-Typen und eigene Typen der Library, keine gseq-Typen

**Entscheidung:** In der öffentlichen API von v2 kommen nur Standard-Go-Typen vor
(`[]string`, `iter.Seq`, Rückgaben der Form `(T, bool)` und `(T, error)`) sowie eigene
Typen der Library (`Table`, `Column`, `Expr`, `Op`, `Result` und Ähnliche). Typen aus gseq
(`slice.Slice`, `option.Option`, `result.Result`) erscheinen nicht in Signaturen. Intern
darf gseq weiter verwendet werden.
**Begründung:** Externe Entwickler ([UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline))
sollen die Library ohne eine zweite Library und deren Idiome nutzen können. Standardtypen
sind in Go vertraut, und Iteratoren gibt es seit Go 1.23 in der Standardbibliothek.
Beibehalten der gseq-Typen wurde verworfen, weil es externe Nutzer an gseq bindet.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D34 — Der Prototyp liegt unter experimental/v2 ohne Zusage, v2.0.0 ist ein eigenes Modul mit SemVer

**Entscheidung:** Der Prototyp entsteht in diesem Repo unter `experimental/v2` und hat
keine Stabilitätszusage. v2.0.0 erscheint als eigenes Modul mit dem Pfad `/v2` und folgt
SemVer. Teile unter `experimental/…` sind von der Zusage ausgenommen, wie in v1
`experimental/simd`. v1 wird weiter mit Fehlerkorrekturen gepflegt, bekommt aber keine
großen neuen Funktionen mehr.
**Begründung:** Der Prototyp soll Grundsatzfragen klären
([G5](70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht),
[G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren),
[G12](70-gap-ledger.md#g12-ab-wann-eine-haufung-von-fehlern-als-formatanderung-gilt),
[G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange))
und darf sich dabei frei ändern. Externe Nutzer brauchen ab v2.0.0 eine verlässliche
Zusage. Bestehende v1-Pipelines laufen weiter.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D35 — Ziele werden über eine Sink-Schnittstelle beschrieben, mit Datei-Writern und kleinen Paketen für Datenbank und HTTP

**Entscheidung:** Alle Writer erfüllen eine gemeinsame `Sink`-Schnittstelle, die Blöcke
entgegennimmt und Fehler meldet. Die Library bringt Datei-Writer mit (CSV, JSON/NDJSON,
Excel, Markdown). Kleine Unterpakete bringen einen Writer für Datenbanken über
`database/sql` mit Insert und Upsert (auch auf `record_key` nach
[D18](#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash))
und einen Writer für HTTP mit JSON bzw. NDJSON, Batching und Wiederholung. Der Prototyp
enthält die Schnittstelle und die Datei-Writer.
**Begründung:** Die Ziele in [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)
sind Datenbank, Datei und HTTP. Datenbank- und HTTP-Writer mitzubringen erspart jedem
Nutzer den Nachbau. Eigene Unterpakete halten die Abhängigkeiten des Kerns klein. Über die
gemeinsame Schnittstelle gilt
[D2](#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse) für
alle Ziele.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G6](70-gap-ledger.md#g6-writer-fur-datenbank-und-http))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D36 — Ein Adapter wandelt zwischen v1- und v2-Tabellen

**Entscheidung:** v2 bringt einen Adapter mit, der eine v1-Tabelle in eine v2-Tabelle
wandelt und zurück. Er ist Teil von v2.0.0, nicht des Prototyps.
**Begründung:** Bestehende Pipelines sollen schrittweise umsteigen können, zum Beispiel
indem ein neuer v2-Teil auf dem Ergebnis eines unveränderten v1-Teils arbeitet.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D37 — Die Docs-Gates werden in diesem Repo selbst gebaut

**Entscheidung:** Die Prüfungen des Design-Sets (Eindeutigkeit und Lückenlosigkeit der IDs,
Mindestzahl je ID-Familie, keine nackten IDs, kanonische Links mit nachgerechneten Ankern,
aktuelle Bereichsangaben, Abgleich der Tabelle "welcher Test beweist welchen Fall" mit
`Proves(t, "T<n>")`) werden als normale Go-Tests in diesem Repo selbst geschrieben. Sie
decken auch die Familie UC ab und prüfen, dass jeder Use Case von mindestens einer Decision
genannt wird und jede Decision mindestens einen Use Case nennt.
**Begründung:** Die Referenzfassung liegt im Archivar-Repo, auf das beim Entwurf kein
Zugriff bestand. Der Maintainer hat sich ausdrücklich für einen eigenen Bau entschieden.
Er folgt der Spezifikation der Methode und kann die dort noch fehlende UC-Familie gleich
mit abdecken.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G14](70-gap-ledger.md#g14-docs-gates-aus-dem-archivar-repo-ubernehmen))
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D38 — Ein späterer Lauf entfernt verwaiste ausgelagerte Daten

**Entscheidung:** Jeder Lauf lagert in ein eigenes Unterverzeichnis aus, das als in
Benutzung markiert ist, solange der Lauf lebt. Beim Start entfernt ein Lauf die
Unterverzeichnisse, deren Lauf nicht mehr lebt. Das deckt auch Läufe ab, die vom System
beendet wurden.
**Begründung:** Ein vom System beendeter Lauf kann nicht selbst aufräumen
([Failure Modes](50-failure-modes.md), Zeile "Speicher reicht trotz Budget nicht"). Ohne
Aufräumen beim nächsten Lauf würden ausgelagerte Lieferungsinhalte liegen bleiben. Das
betrifft Platz und die [Security Boundaries](60-security-boundaries.md).
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G17](70-gap-ledger.md#g17-aufraumen-nach-einem-vom-system-beendeten-lauf))
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D39 — Eine Panik in einer Closure wird wie ein zurückgegebener Fehler behandelt

**Entscheidung:** Gerät eine Closure nach
[D32](#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg) in Panik,
fängt die Engine sie ab und sortiert die Zeile aus wie bei einem zurückgegebenen Fehler
(`code=custom`). Der Grund enthält die Panik-Meldung.
**Begründung:** Eigene Logik einzelner Kunden soll einen Lauf nicht abbrechen. Das
Fehlermodell bleibt einheitlich. Das Risiko, dass ein echter Programmierfehler so als
Datenfehler erscheint, ist in Kauf genommen: Die Panik-Meldung im Grund und die Schwelle
aus [D20](#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen)
machen ihn sichtbar.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G18](70-gap-ledger.md#g18-verhalten-bei-panik-in-einer-closure))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D40 — Ein Fehler beim Schreiben in ein Ziel beendet den Lauf mit einer Panik

**Entscheidung:** Lehnt ein Ziel einen Block ab oder scheitert das Schreiben, gilt das
nicht als Daten-, Liefer- oder Planfehler. Der Lauf wird sofort beendet, und zwar mit einer
Panik. Das gilt für Ergebnisse und für aussortierte Zeilen.
**Begründung:** Ein stiller Schreibfehler würde genau die Daten verlieren, die das
Fehlermodell bewahren soll
([D1](#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten)). Ein harter
Abbruch hält die Daten in der Quelle vorhanden, und der Lauf lässt sich nach Behebung des
Ziels wiederholen. Ob es beim wörtlichen Go-`panic` bleibt oder ein Rückgabewert mit
eigenem Status gewählt wird, ist offen
([G38](70-gap-ledger.md#g38-panik-oder-ruckgabe-bei-schreibfehlern)).
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G19](70-gap-ledger.md#g19-fehler-beim-schreiben-ins-ziel))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)
