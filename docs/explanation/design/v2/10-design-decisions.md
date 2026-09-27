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
