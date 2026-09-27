# Design Decisions

Jede Entscheidung nennt, wer sie wann getroffen hat, und die Use Cases, deren Fragen sie
beantwortet. Bis zum Freeze sind die Einträge Entwurf. Danach werden sie nur noch per
Amendment oder Ersetzung geändert.

### D1 — Aussortierte Zeilen sind eine Tabelle aus Rohzustand und Info-Spalten

**Entscheidung:** Die aussortierten Zeilen eines Laufs werden als Tabelle zurückgegeben.
Jede Zeile enthält ihre Werte im Rohzustand, also so, wie sie aus der Quelle kamen, und
nicht im Zustand des Schritts, in dem sie gescheitert ist. Dazu kommen Info-Spalten, die
beschreiben, wo und warum die Zeile aussortiert wurde. Was "Rohzustand" genau umfasst,
legt [D10](#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle) fest (vgl. [G1](70-gap-ledger.md#g1-was-der-rohzustand-einer-zeile-umfasst)).
Welche Info-Spalten es gibt, legen [D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix) und
[D15](#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-ubersichtseintrag-je-betroffener-spalte) fest
(vgl. [G2](70-gap-ledger.md#g2-welche-info-spalten-eine-aussortierte-zeile-tragt)).
**Begründung:** Eine Tabelle lässt sich mit allen Mitteln der Library weiterverarbeiten:
filtern, in einen Zweig geben, schreiben. Ein eigenes Fehlerformat bräuchte eigene
Werkzeuge. Der Rohzustand ist nötig, damit der Pipeline-Entwickler den Fehler an der
Originallieferung nachstellen kann. Der Zustand zum Fehlerzeitpunkt zeigt nur, was die
Pipeline aus der Zeile gemacht hatte. Die v1-Variante (`etl.ErrorLog` mit `OriginalRow`
als `map[string]string`) war ein Zusatz, den man erst einhängen musste. In v2 ist die
Tabelle Teil jedes Ergebnisses.
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)

### D2 — Aussortierte Zeilen werden über dieselben Writer geschrieben wie Ergebnisse

**Entscheidung:** Die Library legt nicht fest, wohin aussortierte Zeilen gehen. Weil sie
nach [D1](#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten)
eine Tabelle sind, schreibt der Pipeline-Entwickler sie mit einem beliebigen Writer in
Format und Ziel seiner Wahl (Datei, Datenbank, HTTP), programmatisch in der Pipeline.
**Begründung:** Die Ziele unterscheiden sich je Pipeline und Kunde: Datei, Datenbank oder
ein HTTP-Endpunkt. Ein fest eingebauter Ablageort würde keins davon gut abdecken. Über
die normalen Writer gibt es für Fehler und Ergebnisse denselben Weg. Welche Writer die
Library für Datenbank und HTTP selbst mitbringt, legt
[D35](#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http)
fest (vgl. [G6](70-gap-ledger.md#g6-writer-fur-datenbank-und-http)).
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
die falsche Seite gebracht (Issue #40, PR #41).
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D4 — Eine Pipeline kann eine Schwelle für aussortierte Zeilen festlegen

**Entscheidung:** Eine Pipeline kann eine Schwelle definieren. Werden mehr Zeilen
aussortiert, gilt der Lauf als fehlgeschlagen, auch wenn er im Modus "aussortieren" aus
[D3](#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen)
läuft. Ohne Schwelle gilt ein Lauf als erfolgreich, sobald er durchgekommen ist. Wie die
Schwelle bemessen wird und was beim Überschreiten passiert, legen
[D20](#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen)
und [D21](#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst)
fest (vgl. [G3](70-gap-ledger.md#g3-bemessung-und-wirkung-der-schwelle)).
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
standardmäßig unveränderlich. Eine eigene veränderbare Tabelle gibt es in der API nicht;
ob kopiert oder an Ort und Stelle geändert wird, entscheidet die Engine
([D7](#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert)).
Ob das ohne Verlust an Tempo und Speicher gelingt, prüft der Prototyp
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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Option A aus dem Vergleich mit pandas, Polars, DuckDB, Spark/Dask, DataFusion)
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D7 — Die Engine entscheidet, ob sie Daten kopiert oder an Ort und Stelle ändert

**Entscheidung:** Für den Pipeline-Entwickler sind Tabellen unveränderlich
([D5](#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen)): Ein
Schritt verändert nie Daten, die ein anderer Schritt, ein Zweig oder die aussortierten
Zeilen noch sehen. Intern darf die Engine Daten an Ort und Stelle ändern, wenn niemand
sonst sie sieht, und kopieren, wenn sie geteilt sind. Es gibt keine eigene veränderbare
Tabelle in der API. Zusätzlich gibt es einen harten Schalter am Anfang der Pipeline.
Seine Wirkung legt [D8](#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert)
fest (vgl. [G7](70-gap-ledger.md#g7-wirkung-des-harten-schalters-fur-kopieren-und-andern)).
**Begründung:** In v1 gab es jede Operation doppelt, auf `Table` und `MutableTable`. Der
Maintainer hat `MutableTable` nur wegen Tempo und Speicher genutzt, nicht wegen einer
anderen Bedeutung. Entscheidet die Engine anhand dessen, ob Daten geteilt sind, fällt die
doppelte API weg, und der Gewinn bleibt erhalten, wo er ohne Risiko möglich ist. Ob das
genauso schnell und sparsam ist wie v1 `MutableTable`, prüft der Prototyp
([G5](70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht)).
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D8 — Eine Option legt fest, dass die Engine immer kopiert oder immer an Ort und Stelle ändert

**Entscheidung:** Ohne Angabe entscheidet die Engine nach
[D7](#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert) selbst.
Eine Option am Anfang der Pipeline setzt einen von zwei festen Modi: **immer kopieren**
oder **immer an Ort und Stelle ändern**. Der Modus gilt für den ganzen Lauf.
**Begründung:** Die Automatik ist die sichere Voreinstellung. Der Pipeline-Entwickler
behält aber die Kontrolle für die Fälle, in denen er es besser weiß: beim Debuggen will
er jeden Zwischenstand behalten, bei einer großen Lieferung ohne Zweige will er Speicher
und Zeit sparen. Wie "immer ändern" mit Zweigen und dem Rohzustand verträglich bleibt,
legt [D9](#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert)
fest (vgl. [G8](70-gap-ledger.md#g8-immer-andern-gegen-zweige-und-rohzustand)).
**Quelle:** Maintainer im Kickoff, 2026-09-27
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G8](70-gap-ledger.md#g8-immer-andern-gegen-zweige-und-rohzustand))
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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G1](70-gap-ledger.md#g1-was-der-rohzustand-einer-zeile-umfasst))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D11 — Scheitert eine Zeile nach einem Join, wird jede beteiligte Quellzeile aussortiert

**Entscheidung:** Scheitert eine Zeile, die aus einem Join entstanden ist, landet jede
beteiligte Quellzeile mit ihrem eigenen Rohzustand und ihrer eigenen Fundstelle nach
[D10](#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
in den aussortierten Zeilen. Eine gemeinsame Kennung (`reject_id`) verbindet die Einträge,
die zu demselben Fehler gehören. Wie die aussortierten Zeilen verschiedener Quellen
zusammen dargestellt werden, legt
[D13](#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen)
fest (vgl. [G10](70-gap-ledger.md#g10-aussortierte-zeilen-mehrerer-quellen-eine-tabelle-oder-je-quelle)).
**Begründung:** Jede aussortierte Zeile behält die Spalten ihrer eigenen Lieferung und
lässt sich dort wiederfinden und nachverarbeiten
([UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)).
Eine zusammengeführte Rohzeile beider Seiten gäbe es in keiner Quelle.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Teil der Auflösung von [G9](70-gap-ledger.md#g9-rohzustand-und-fundstelle-nach-aggregation-und-join))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)

### D12 — Der Rohzustand reicht bis zum ersten Schritt über alle Zeilen, danach wird die aggregierte Zeile aussortiert

**Entscheidung:** Rohzustand und Fundstelle der Quellzeilen werden bis zum ersten Schritt
mitgeführt, der alle Zeilen braucht und sie zusammenfasst (Gruppieren, Pivot und
Ähnliches). Scheitert eine Zeile nach diesem Schritt, wird sie als aggregierte Zeile
aussortiert: mit ihren Werten, dem Gruppenschlüssel, der Anzahl der eingegangenen
Quellzeilen und dem Grund, in der Tabelle nach [D48](#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle). Eine Option "volle Herkunft" führt zusätzlich die Kennungen der
Quellzeilen über die Zusammenfassung hinaus mit. Wie eine Quellzeile gezählt und wann ihr
Rohzustand freigegeben wird, legt [D43](#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben) fest (offen: [G43](70-gap-ledger.md#g43-das-zahlmodell-deckt-teilausfalle-einheiten-und-abbruche-nicht-ab)).
**Begründung:** In eine aggregierte Zeile gehen bei großen Lieferungen bis zu Millionen
Quellzeilen ein. Deren Rohzustand mitzuführen, würde den Speicherrahmen aus
[UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)
sprengen. Wo sie landen, regelt [D48](#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle). Die Option "volle Herkunft" deckt kleine Läufe und das Debuggen ab.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G9](70-gap-ledger.md#g9-rohzustand-und-fundstelle-nach-aggregation-und-join), zusammen mit [D11](#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)

### D13 — Aussortierte Zeilen gibt es je Quelle, dazu eine Übersicht über alle Quellen

**Entscheidung:** Das Ergebnis eines Laufs liefert je Quelle eine Tabelle aussortierter
Zeilen mit den Rohspalten dieser Quelle und den Info-Spalten aus
[D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix). Zusätzlich gibt es
eine Übersicht über alle Quellen, die nur die Info-Spalten enthält, mit einem Eintrag je
Fehler nach [D44](#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler); sie ist die Grundlage für Meldungen. Zählungen folgen den Quellzeilen nach [D43](#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben); wie die Schwelle aus
[D4](#d4-eine-pipeline-kann-eine-schwelle-fur-aussortierte-zeilen-festlegen) bemessen wird,
legt [D45](#d45-die-schwelle-bezieht-anteile-auf-bisher-gelesene-zeilen-gilt-bei-einer-der-grenzen-und-bricht-erst-nach-einer-mindestzahl-ab) fest.
**Begründung:** Quellen haben unterschiedliche Spalten. Eine Tabelle je Quelle lässt sich
schreiben ([D2](#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse))
und wieder als Quelle verwenden
([UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)).
Verworfen: eine gemeinsame Tabelle, deren Rohspalten je nach Quelle unterschiedlich belegt
sind. Die ist schwer zu schreiben und nachzuverarbeiten.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G10](70-gap-ledger.md#g10-aussortierte-zeilen-mehrerer-quellen-eine-tabelle-oder-je-quelle); Validierung im Prototyp: [G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D14 — Info-Spalten tragen ein reserviertes, einstellbares Präfix

**Entscheidung:** Die Info-Spalten einer aussortierten Zeile tragen ein reserviertes
Präfix, standardmäßig `_gseq_`, das pro Pipeline einstellbar ist. Es gibt diese Spalten:
`reject_id` (verbindet zusammengehörige Einträge), `run_id`, `cell` (betroffene Excel-Zelle nach [D52](#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert)), `record_key`, `row_key` nach [D47](#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen) und optional
`error_count` nach [D44](#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler), `record_hash` nach [D18](#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash), `formula` (Formel einer Excel-Zelle nach [D52](#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert)), die Fundstelle nach
[D10](#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
(`source`, `sheet`, `line`, `offset`), `step`, `column`, `value` (Wert zum Zeitpunkt des
Fehlers), `reason` (Text), `prev_reason` (Grund aus dem Hauptweg nach [D27](#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg)), `code` (feste Fehlerart aus dem festen Kern nach [D54](#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus):
`parse`, `validation`, `unparseable_line`, `aggregate_failed`, `missing_field` nach [D53](#d53-jede-datei-und-jedes-sheet-ist-eine-quelle-gruppen-von-dateien-wirken-als-eine-quelle),
`field_too_large` nach [D56](#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch), `expr` nach [D54](#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus), `custom` nach [D32](#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg) und
[D39](#d39-eine-panik-in-einer-closure-wird-wie-ein-zuruckgegebener-fehler-behandelt), die Lieferfehler `missing_column`, `missing_sheet`, `missing_file`, `unreadable` und
`truncated` nach [D42](#d42-jeder-lieferfehler-setzt-den-status-delivery_error); dazu eigene Codes `custom:<name>` aus Closures nach [D54](#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus)) und `raw_line` (nur bei Zeilen, die sich nicht zerlegen
ließen).
**Begründung:** Das Präfix verhindert Kollisionen mit Datenspalten. Der feste Code erlaubt
es, ohne Textvergleich zu filtern und Zweige zu bilden
([UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)).
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G2](70-gap-ledger.md#g2-welche-info-spalten-eine-aussortierte-zeile-tragt); Validierung im Prototyp: [G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)

### D15 — Eine Zeile wird im ersten scheiternden Schritt aussortiert, mit einem Übersichtseintrag je betroffener Spalte

**Entscheidung:** Eine Zeile wird im ersten Schritt aussortiert, in dem sie scheitert.
Spätere Schritte sehen sie nicht mehr, es sei denn, der Schritt hat einen Fehlerzweig nach
[D25](#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck). Scheitern in diesem Schritt mehrere Spalten, gibt
es in der Übersicht je Spalte einen Eintrag mit derselben `reject_id` nach
[D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix). In der Tabelle je
Quelle steht die Zeile nach [D44](#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler) einmal.
**Begründung:** Der Pipeline-Entwickler sieht so alle Probleme einer Zeile in dem Schritt,
der sie aussortiert hat, und nicht nur das erste. Alle Schritte weiter auf einer
gescheiterten Zeile laufen zu lassen, wurde verworfen. Deren Ergebnisse wären Folgefehler
und kaum aussagekräftig.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Validierung im Prototyp: [G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)

### D16 — Aussortierte Zeilen können Quelle eines Laufs sein und behalten ihre ursprüngliche Fundstelle

**Entscheidung:** Eine eigene Quelle liest eine Tabelle aussortierter Zeilen
([D13](#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen))
als Eingabe eines Laufs. Sie übernimmt die Rohspalten, lässt die Info-Spalten weg und
setzt die ursprüngliche Fundstelle aus
[D10](#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle)
als Fundstelle jeder Zeile. Zeilen mit `raw_line` gehen erneut durch den Reader der
ursprünglichen Quelle, mit dessen aktueller Konfiguration. Die Quelle ersetzt genau eine Quelle im Plan; die übrigen Quellen werden
normal gelesen. Sie erkennt das Standardpräfix der Info-Spalten selbst, ein anderes wird
als Option angegeben. Aggregierte aussortierte Zeilen nach [D48](#d48-nach-einer-gruppierung-aussortierte-zeilen-stehen-in-einer-eigenen-tabelle) lassen sich so nicht
nachverarbeiten.
**Begründung:** So verarbeitet dieselbe, angepasste Pipeline die fehlenden Daten nach,
ohne Sonderweg. Scheitert eine Zeile erneut, zeigt der neue Eintrag weiterhin auf die
Original-Lieferung und nicht auf die Datei mit den aussortierten Zeilen. Wurde der Reader
angepasst (z. B. CSV-Einstellungen), werden bisher unzerlegbare Zeilen jetzt vielleicht
lesbar.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (ergänzt zur Auflösung von [G30](70-gap-ledger.md#g30-nachverarbeitung-uber-mehrere-quellen-und-mit-anderem-prafix))
**Betroffene Use Cases:** [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D17 — Die Library bewahrt aussortierte Zeilen nicht selbst auf

**Entscheidung:** Die Library speichert aussortierte Zeilen nicht über das Ende eines
Laufs hinaus. Ausgenommen sind aussortierte Zeilen, die das Ergebnis nach [D49](#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close) bis
zum Schließen hält (offen: [G47](70-gap-ledger.md#g47-aufraumen-verwaister-laufe-gegen-offene-ergebnisse)). Die Pipeline schreibt sie nach
[D2](#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse)
in ein Ziel ihrer Wahl und liest sie für eine Nachverarbeitung mit dem passenden Reader
wieder ein. Wie lange sie aufbewahrt werden, entscheiden Pipeline und Ziel.
**Begründung:** Eine eigene Ablage in der Library würde mit dem Writer-Weg aus
[D2](#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse)
konkurrieren und Speicherort, Format und Aufbewahrung vorgeben, die je Kunde verschieden
sind.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
**Betroffene Use Cases:** [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D18 — Jede Quellzeile trägt einen stabilen Schlüssel, optional einen Inhalts-Hash

**Entscheidung:** Jede Quellzeile trägt einen Schlüssel `record_key`. Standardmäßig wird er
aus einer Kennung der Lieferung und der Position der Zeile gebildet. Die Kennung legt [D61](#d61-die-kennung-einer-lieferung-ist-ein-fingerabdruck-der-beim-offnen-feststeht) fest. Die Zusage lautet: stabil innerhalb derselben Lieferung, auch
bei einer Nachverarbeitung nach
[D16](#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle).
Für Ziele, die über Lieferungen hinweg deduplizieren, kann eine Quelle einen fachlichen
Schlüssel aus Spalten angeben, aus dem `record_key` dann gebildet wird. Das ist für
Datenbank-Ziele die empfohlene Form. Optional kommt ein Hash über den Rohinhalt der Zeile
dazu (`record_hash`). Er dient dazu, eine doppelt eingespielte Lieferung zu erkennen, nicht
dazu, einzelne Zeilen zu deduplizieren. `record_key` ist eine Info-Spalte nach
[D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix). Doppelte Einträge im
Ziel verhindert die Pipeline selbst, zum Beispiel per Upsert. Nach Joins gilt der
Schlüssel der Ergebniszeile nach [D47](#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen).
**Begründung:** Ob und wie ein Ziel dedupliziert, hängt vom Ziel ab. Die Library kann den
Schlüssel liefern. Ein Schlüssel nur aus Dateiname und Zeilennummer wurde verworfen: Fügt
der Anbieter oben eine Zeile ein, verschieben sich alle Schlüssel, und eine neue Datei mit
gleichem Namen würde die Datensätze der alten überschreiben. Zwei gleiche Zeilen können
legitim sein (zwei gleiche Bestellpositionen), deshalb taugt `record_hash` nicht als
Dedup-Schlüssel für Zeilen.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (überarbeitet zur Auflösung von [G29](70-gap-ledger.md#g29-record_key-ist-positionsabhangig))
**Betroffene Use Cases:** [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D19 — Es gibt drei Fehlerarten: Planfehler, Lieferfehler und Datenfehler

**Entscheidung:** Fehler fallen in drei Arten.
**Planfehler** betreffen die Pipeline selbst, zum Beispiel ein Ausdruck auf eine Spalte,
die kein Schritt erzeugt, ein Typkonflikt oder ein ungültiger Parameter. Sie werden vor
dem Lauf geprüft, und der Lauf startet nicht.
**Lieferfehler** betreffen den Aufbau einer Lieferung, zum Beispiel eine fehlende
erwartete Spalte oder ein fehlendes Sheet. Sie werden beim Lesen des Kopfs erkannt oder, bei `truncated`,
`missing_file` und fehlenden JSON-Feldern, später im Lauf ([D42](#d42-jeder-lieferfehler-setzt-den-status-delivery_error), [D53](#d53-jede-datei-und-jedes-sheet-ist-eine-quelle-gruppen-von-dateien-wirken-als-eine-quelle)).
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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G3](70-gap-ledger.md#g3-bemessung-und-wirkung-der-schwelle))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D21 — Ein Lauf liefert einen Status und Zählungen, aus denen sich ein Exit-Code ableiten lässt

**Entscheidung:** Das Ergebnis eines Laufs trägt einen Status: `ok`, `failed_threshold`
(Schwelle überschritten), `aborted` (gestoppt nach
[D3](#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen)
oder abgebrochen über den Kontext), `plan_error` (Planfehler nach
[D19](#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler)),
`delivery_error` (Lieferfehler nach [D42](#d42-jeder-lieferfehler-setzt-den-status-delivery_error)) oder
`sink_error` (Schreibfehler nach [D40](#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab))
(Vorrang nach [D63](#d63-es-gilt-der-hochste-zutreffende-status-und-das-ergebnis-nennt-alle-befunde)). Dazu kommen die Zählungen gelesener, durchgelaufener,
aussortierter und verworfener Quellzeilen nach [D43](#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben), je Schritt und je Fehlercode. Eine Hilfsfunktion bildet den Status auf einen Exit-Code für den Scheduler ab.
**Begründung:** Der Scheduler braucht ein eindeutiges Signal, der Pipeline-Entwickler die
Zählungen, um zu sehen, wo etwas passiert ist. Den Status als Wert zurückzugeben, statt ihn
nur über einen `error` auszudrücken, erlaubt es, zwischen "Lauf gescheitert" und
"Lauf durch, aber zu viele aussortiert" zu unterscheiden.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G3](70-gap-ledger.md#g3-bemessung-und-wirkung-der-schwelle))
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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
**Betroffene Use Cases:** [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D25 — Gescheiterte Zeilen eines Schritts können in einen Zweig gegeben werden und laufen danach zurück

**Entscheidung:** Ein Schritt kann einen Zweig für seine gescheiterten Zeilen haben. Die
Zeilen kommen in dem Zustand in den Zweig, in dem sie in den gescheiterten Schritt
hineingegangen sind, zusammen mit den Info-Spalten aus
[D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix). Ihr Rohzustand
bleibt nach [D9](#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert)
im Hintergrund erhalten. Nach einem zusammenfassenden Schritt nach
[D12](#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert)
bekommt ein Fehlerzweig aggregierte Zeilen ohne Rohzustand. Was der Zweig verarbeiten kann, fließt nach dem Schritt in den
Hauptweg zurück. Was auch im Zweig scheitert, wird aussortiert. Unabhängig davon gibt es
Zweige nach einer Bedingung (`Split`) und das Zusammenführen beliebiger Zweige (`Merge`).
**Begründung:** Der Zweig soll nachholen, was der Schritt nicht geschafft hat. Dafür
braucht er die Zeile mit allen vorherigen Schritten, nicht den Rohzustand. Die
Info-Spalten erlauben es, im Zweig nach Fehlerart zu filtern. Der allgemeine Zweig nach
Bedingung deckt das Umleiten von Zeilen ab, das nichts mit Fehlern zu tun hat.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
**Betroffene Use Cases:** [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)

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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
**Betroffene Use Cases:** [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)

### D28 — Ein Lauf hat ein Speicherbudget und ein Verzeichnis zum Auslagern

**Entscheidung:** Jeder Lauf hat ein Speicherbudget, das die Engine nach
[D6](#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)
einhält, indem sie auslagert. Es ist je Lauf einstellbar. Voreinstellung ist ein Anteil
des verfügbaren Speichers, wobei die Engine das Limit eines Containers bzw. der cgroup
berücksichtigt. Wann die Engine `GOMEMLIMIT` setzt und dass das Budget je Prozess gilt, regelt [D65](#d65-gomemlimit-setzt-die-engine-nur-auf-wunsch-und-das-budget-gilt-je-prozess). Die Höhe des Anteils wird aus den Messungen des Prototyps festgelegt. Das
Verzeichnis zum Auslagern ist einstellbar, Standard ist das temporäre
Verzeichnis des Systems. Ausgelagerte Daten werden am Ende des Laufs gelöscht, auch bei
einem Abbruch. Ausgenommen sind ausgelagerte aussortierte Zeilen nach
[D49](#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close), die bis zum Schließen des Ergebnisses leben.
**Begründung:** Läufe starten per Cron, auch in Containern
([UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)). Ein Budget, das das
Container-Limit ignoriert, würde dort zum Abbruch durch das System führen statt zum
Auslagern. Das Budget zählt nur die Blöcke der Engine. Speicherbereinigung,
Reader- und Writer-Puffer, Closures und die Go-Runtime brauchen zusätzlich Speicher, und
eine Beendigung durch das System lässt sich nicht abfangen. Deshalb liegt der Anteil
deutlich unter dem verfügbaren Speicher. Wie groß er als Voreinstellung sein soll, klärt der Prototyp
([G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange)).
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D29 — Daten laufen in Blöcken typisierter Spalten, Rohspalten bleiben bis zum Cast Text

**Entscheidung:** Die Blöcke aus
[D6](#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen)
bestehen aus typisierten Spalten (mindestens Ganzzahl, Gleitkomma, Text, Wahrheitswert,
Zeitpunkt). Die Blockgröße wählt die Engine, sie ist einstellbar. Spalten, die ein Reader
aus einer Lieferung liest, sind Text, bis ein Schritt sie in einen Typ umwandelt.
**Begründung:** Typisierte Spalten sparen Speicher gegenüber Text, lassen sich blockweise
billig weiterreichen und erlauben vektorisierte Verarbeitung, wie sie `experimental/simd`
in v1.2 gemessen hat (PR #12, v1.2.0). Dass Rohspalten Text bleiben, passt zu schmutzigen Lieferungen:
Erst beim Umwandeln entscheidet sich, ob ein Wert passt oder die Zeile aussortiert wird
([D15](#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-ubersichtseintrag-je-betroffener-spalte)).
Ob der Vorteil gegenüber v1 messbar ist, klärt der Prototyp
([G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange)).
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
**Betroffene Use Cases:** [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D32 — Ausdrücke sind der Standard für Berechnungen, Closures der Ausweg

**Entscheidung:** Berechnungen, Filter und abgeleitete Spalten werden als Ausdrücke
formuliert (z. B. `Col("netto").Mul(Lit(1.19))`). Ausdrücke sind typisiert. Die Engine
prüft sie vor dem Lauf, Fehler darin sind Planfehler nach
[D19](#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler) (Fehler, die erst
bei der Ausführung auftreten, regelt [D54](#d54-laufzeitfehler-in-ausdrucken-sortieren-die-zeile-mit-dem-code-expr-aus)), und sie
führt sie spaltenweise aus. Die String-, Datums- und Rechen-Helfer aus v1 `schema` werden
zu Ausdrucksfunktionen. Für Logik, die sich nicht als Ausdruck fassen lässt, gibt es
Operationen mit Closures. Gibt eine Closure einen Fehler zurück, wird die Zeile mit
`code=custom` und dem Fehlertext als Grund aussortiert.
**Begründung:** Closures (`func(Row) string` in v1) sind für die Engine undurchsichtig:
Sie lassen sich weder vorab prüfen noch spaltenweise oder vektorisiert ausführen. Ohne
Ausweg ließe sich aber Sonderlogik einzelner Kunden nicht ausdrücken. Dass Fehler einer
Closure zu aussortierten Zeilen werden, hält das Fehlermodell auch für eigene Logik
einheitlich.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D35 — Ziele werden über eine Sink-Schnittstelle beschrieben, mit Datei-Writern und kleinen Paketen für Datenbank und HTTP

**Entscheidung:** Alle Writer erfüllen eine gemeinsame `Sink`-Schnittstelle, die Blöcke
entgegennimmt und Fehler meldet. Die Library bringt Datei-Writer mit (CSV, JSON/NDJSON,
Excel, Markdown). Kleine Unterpakete bringen einen Writer für Datenbanken über
`database/sql` mit Insert und Upsert (auf `row_key` nach [D47](#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen) bzw. `record_key` nach
[D18](#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash))
und einen Writer für HTTP mit JSON bzw. NDJSON, Batching und Wiederholung. Der Prototyp
enthält die Schnittstelle und die Datei-Writer.
**Begründung:** Die Ziele in [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)
sind Datenbank, Datei und HTTP. Datenbank- und HTTP-Writer mitzubringen erspart jedem
Nutzer den Nachbau. Eigene Unterpakete halten die Abhängigkeiten des Kerns klein. Über die
gemeinsame Schnittstelle gilt
[D2](#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse) für
alle Ziele.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G6](70-gap-ledger.md#g6-writer-fur-datenbank-und-http))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D36 — Ein Adapter wandelt zwischen v1- und v2-Tabellen

**Entscheidung:** v2 bringt einen Adapter mit, der eine v1-Tabelle in eine v2-Tabelle
wandelt und zurück. Er ist Teil von v2.0.0, nicht des Prototyps. Zugesagt ist, dass eine v1-Tabelle den Weg über v2 zurück nach v1
unverändert übersteht. Der Weg von v2 nach v1 verliert Nullwerte (sie werden zu leerem
Text) und Typen (sie werden zu formatiertem Text).
**Begründung:** Bestehende Pipelines sollen schrittweise umsteigen können, zum Beispiel
indem ein neuer v2-Teil auf dem Ergebnis eines unveränderten v1-Teils arbeitet.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (ergänzt nach [G41](70-gap-ledger.md#g41-umfang-des-v1-adapters-und-ma-fur-auffallige-abweichungen))
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D37 — Die Docs-Gates werden in diesem Repo selbst gebaut

**Entscheidung:** Die Prüfungen des Design-Sets (Eindeutigkeit und Lückenlosigkeit der IDs,
Mindestzahl je ID-Familie, keine nackten IDs, kanonische Links mit nachgerechneten Ankern,
aktuelle Bereichsangaben, Abgleich der Tabelle "welcher Test beweist welchen Fall" mit
`Proves(t, "T<n>")`) werden als normale Go-Tests in diesem Repo selbst geschrieben. Sie
decken auch die Familie UC ab und prüfen, dass jeder Use Case von mindestens einer Decision
genannt wird und jede Decision mindestens einen Use Case nennt. Ausgenommen sind
Decisions über das Design-Set selbst, wie diese.
**Begründung:** Die Referenzfassung liegt im Archivar-Repo, auf das beim Entwurf kein
Zugriff bestand. Der Maintainer hat sich ausdrücklich für einen eigenen Bau entschieden.
Er folgt der Spezifikation der Methode und kann die dort noch fehlende UC-Familie gleich
mit abdecken.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G14](70-gap-ledger.md#g14-docs-gates-aus-dem-archivar-repo-ubernehmen); Ausnahme ergänzt zur Auflösung von [G39](70-gap-ledger.md#g39-d37-nennt-keinen-use-case))
**Betroffene Use Cases:** keine. Die Decision betrifft die Pflege des Design-Sets und ist nach ihrer eigenen Regel ausgenommen ([G39](70-gap-ledger.md#g39-d37-nennt-keinen-use-case)).

### D38 — Ein späterer Lauf entfernt verwaiste ausgelagerte Daten

**Entscheidung:** Jeder Lauf lagert in ein eigenes Unterverzeichnis aus, das als in
Benutzung markiert ist, solange der Lauf lebt. Beim Start entfernt ein Lauf die
Unterverzeichnisse, deren Lauf nicht mehr lebt. Das deckt auch Läufe ab, die vom System
beendet wurden.
**Begründung:** Ein vom System beendeter Lauf kann nicht selbst aufräumen
([Failure Modes](50-failure-modes.md), Zeile "Speicher reicht trotz Budget nicht"). Ohne
Aufräumen beim nächsten Lauf würden ausgelagerte Lieferungsinhalte liegen bleiben. Das
betrifft Platz und die [Security Boundaries](60-security-boundaries.md).
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G17](70-gap-ledger.md#g17-aufraumen-nach-einem-vom-system-beendeten-lauf))
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

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
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G18](70-gap-ledger.md#g18-verhalten-bei-panik-in-einer-closure))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D40 — Ein Fehler beim Schreiben in ein Ziel bricht den Lauf sofort mit dem Status sink_error ab

**Entscheidung:** Lehnt ein Ziel einen Block ab oder scheitert das Schreiben, gilt das
nicht als Daten-, Liefer- oder Planfehler. Der Lauf bricht sofort ab und liefert den Status
`sink_error` nach [D21](#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst)
zusammen mit dem Fehler des Ziels. Das gilt für Ergebnisse und für aussortierte Zeilen.
Die Library löst keine Panik aus.
**Begründung:** Ein stiller Schreibfehler würde genau die Daten verlieren, die das
Fehlermodell bewahren soll
([D1](#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten)). Der
sofortige Abbruch hält die Daten bei Datei-Quellen in der Quelle vorhanden, und der Lauf lässt sich nach
Behebung des Ziels wiederholen. Eine Panik wurde erwogen und verworfen: In einer Library
beendet sie das ganze Programm, auch andere Läufe im selben Prozess. Der eigene Status
ist für den Scheduler genauso eindeutig. Wie auf einen Schreibfehler reagiert wird, hängt
vom Ziel ab und bleibt Sache der Pipeline.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G19](70-gap-ledger.md#g19-fehler-beim-schreiben-ins-ziel) und [G38](70-gap-ledger.md#g38-panik-oder-ruckgabe-bei-schreibfehlern))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D41 — Der HTTP-Writer liefert mindestens einmal und schickt einen Idempotenzschlüssel mit

**Entscheidung:** Der HTTP-Writer nach
[D35](#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http)
wiederholt eine Anfrage bei vorübergehenden Fehlern (Zeitüberschreitung, Serverfehler,
Überlastung) mit wachsendem Abstand. Jeder Batch trägt einen Idempotenzschlüssel, der aus
den `record_key`s des Batches abgeleitet ist (Schlüssel nach `row_key` offen: [G48](70-gap-ledger.md#g48-row_key-uber-joins-hinaus))
und bei einer Wiederholung gleich bleibt, und
jede Zeile trägt ihren `record_key`
([D18](#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash)).
Die Zusage ist "mindestens einmal". Deduplizieren ist Sache des Empfängers.
Wiederholungsregeln sind je Writer konfigurierbar. Sind die Wiederholungen erschöpft oder
lehnt der Empfänger endgültig ab, gilt
[D40](#d40-ein-fehler-beim-schreiben-in-ein-ziel-bricht-den-lauf-sofort-mit-dem-status-sink_error-ab),
und die weitere Behandlung programmiert der Pipeline-Entwickler.
**Begründung:** "Genau einmal" kann ein Client ohne Mitwirkung des Empfängers nicht
zusichern. Ganz ohne Wiederholung wären Läufe über unzuverlässige Verbindungen zu
empfindlich. Endpunkte verhalten sich unterschiedlich, deshalb sind die Regeln
konfigurierbar und die Reaktion auf ein endgültiges Scheitern liegt beim Entwickler.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G15](70-gap-ledger.md#g15-zustellzusage-des-http-writers))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D42 — Jeder Lieferfehler setzt den Status delivery_error

**Entscheidung:** Lieferfehler nach
[D19](#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler) sind
`missing_column`, `missing_sheet`, `missing_file` (nach [D53](#d53-jede-datei-und-jedes-sheet-ist-eine-quelle-gruppen-von-dateien-wirken-als-eine-quelle)), `unreadable` (Datei fehlt oder ist nicht lesbar bzw.
beschädigt) und `truncated` (Lieferung endet mitten in einer Zeile oder einem Archiv). Jeder
Lieferfehler setzt den Status `delivery_error` nach
[D21](#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst),
unabhängig vom Modus. Im Modus "aussortieren" verarbeitet der Lauf trotzdem, was lesbar ist,
und sortiert betroffene Zeilen aus. Im Modus "stoppen" bricht er ab. Gibt es keine lesbaren
Zeilen (`unreadable`), verarbeitet der Lauf nichts und meldet ebenfalls `delivery_error`.
`truncated` und `unreadable` erscheinen als Befunde im Änderungsbericht
([D23](#d23-das-laufergebnis-enthalt-einen-anderungsbericht)). Eine neue, unerwartete Spalte
ist kein Lieferfehler, sondern ein Befund nach
[D22](#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird).
**Begründung:** Mit den Voreinstellungen hätte eine Formatänderung, die alle Zeilen
aussortiert, den Status `ok` ergeben, und eine abgeschnittene Lieferung wäre bis auf die
letzte Zeile unbemerkt durchgelaufen. Der eigene Status trennt "mit der Lieferung stimmt
etwas nicht" von "zu viele schlechte Werte" (`failed_threshold`) und bleibt im Modus
"aussortieren" mit den Daten vereinbar. Verworfen: eine Standard-Schwelle "alle Zeilen einer
Quelle". Sie erfasst keine abgeschnittene Lieferung und vermischt die beiden Fälle.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G21](70-gap-ledger.md#g21-eine-formatanderung-meldet-mit-voreinstellungen-einen-erfolgreichen-lauf) und [G28](70-gap-ledger.md#g28-abgeschnittene-und-unlesbare-lieferungen))
**Betroffene Use Cases:** [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt), [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC8](05-use-cases.md#uc8-eine-lieferung-besteht-aus-mehreren-dateien-oder-sheets)

### D43 — Gezählt werden Quellzeilen in vier Kategorien, und der Rohzustand wird bei jedem Verlassen des Plans freigegeben

**Entscheidung:** Zählungen nach [D21](#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst) zählen Quellzeilen, nicht Einträge. Es gibt je
Schritt und für den ganzen Lauf vier Kategorien: gelesen, durchgelaufen (im Ziel
angekommen), aussortiert und verworfen. Verworfen sind Zeilen, die ein Schritt absichtlich
weglässt: ein nicht erfüllter Filter, eine Zeile ohne Partner im Inner Join, ein entferntes
Duplikat, ein Zweig ohne Ziel. Es gilt gelesen = durchgelaufen + aussortiert + verworfen.
Zusätzlich zeigt jeder Schritt mit Fehlerzweig, wie viele Zeilen der Zweig gerettet hat.
Der Rohzustand einer Zeile wird freigegeben, sobald sie den Plan auf irgendeinem Weg
verlässt: Ziel, aussortiert, verworfen oder in einem zusammenfassenden Schritt nach [D12](#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert).
Eine Zeile, die ein Filter wegen eines Vergleichs mit null nicht erfüllt ([D30](#d30-es-gibt-echte-nullwerte-getrennt-vom-leeren-text)), ist
verworfen und wird gezählt.
**Begründung:** Ohne die Kategorie "verworfen" ging die Gleichung nicht auf, sobald ein
Filter Zeilen weglässt, und deren Rohzustand wäre bis zum Ende des Laufs gehalten worden,
im Streaming ein Speicherleck. Die Zählung macht auch Zeilen sichtbar, die wegen Nullwerten
aus einem Filter fallen. Genau dieses stille Verschwinden will [D30](#d30-es-gibt-echte-nullwerte-getrennt-vom-leeren-text) verhindern.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G23](70-gap-ledger.md#g23-absichtlich-verworfene-zeilen-fehlen-in-zahlung-und-freigabe-des-rohzustands))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D44 — Die Tabelle je Quelle hat eine Zeile je Quellzeile, die Übersicht einen Eintrag je Fehler

**Entscheidung:** Die Tabelle aussortierter Zeilen je Quelle nach [D13](#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen) enthält jede
Quellzeile genau einmal. Ihre Info-Spalten beschreiben den ersten Fehler, und die
Info-Spalte `error_count` nennt die Zahl der Fehler der Zeile im scheiternden Schritt. Die
Übersicht enthält einen Eintrag je Fehler, verbunden über `reject_id`. Das präzisiert [D15](#d15-eine-zeile-wird-im-ersten-scheiternden-schritt-aussortiert-mit-einem-ubersichtseintrag-je-betroffener-spalte).
**Begründung:** Stünde eine Zeile mit drei fehlerhaften Spalten dreimal in der Tabelle je
Quelle, würde die Nachverarbeitung nach [D16](#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) sie dreimal verarbeiten. Alle Fehler bleiben
über die Übersicht sichtbar.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G25](70-gap-ledger.md#g25-ein-eintrag-je-spalte-vervielfacht-rohzeilen-und-macht-zahlungen-mehrdeutig))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D45 — Die Schwelle bezieht Anteile auf bisher gelesene Zeilen, gilt bei einer der Grenzen und bricht erst nach einer Mindestzahl ab

**Entscheidung:** Für die Schwelle nach [D20](#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen) bezieht sich ein Anteil beim ganzen Lauf auf
die bisher gelesenen Quellzeilen und bei einer Schwelle je Schritt auf die Zeilen, die in
den Schritt hineingegangen sind. Sind eine absolute Grenze und ein Anteil angegeben, ist die
Schwelle überschritten, sobald eine der beiden überschritten ist. Mit der Option zum
sofortigen Abbruch greift ein Anteil erst nach einer einstellbaren Mindestzahl gelesener
Zeilen, eine absolute Grenze sofort. Eine aggregierte aussortierte Zeile zählt mit der
Anzahl ihrer Quellzeilen. Ein Abbruch an der Schwelle ergibt den Status `failed_threshold`.
**Begründung:** Im Streaming ist die Gesamtzahl der Zeilen vorab nicht bekannt. Ohne
Mindestzahl würde die erste aussortierte Zeile einen Anteil von 100 % ergeben und jeden
Lauf sofort abbrechen. "Eine der beiden Grenzen" ist die strengere und damit für ein Signal
die sichere Lesart.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G27](70-gap-ledger.md#g27-schwelle-als-anteil-im-streaming))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D46 — Gerettete Zeilen behalten ihre Geschichte, und die Schwelle zählt nur endgültig aussortierte

**Entscheidung:** Eine Zeile, die ein Fehlerzweig nach [D25](#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck) gerettet hat, behält ihre
`reject_id`, ihren Weg und ihren Grund im Hintergrund, so wie ihren Rohzustand. Die
Info-Spalten entfallen beim Zurückführen nach [D26](#d26-zweige-werden-nach-spaltennamen-zusammengefuhrt-typkonflikte-sind-planfehler) weiterhin. Scheitert die Zeile später im
Hauptweg, zeigt der Eintrag die ganze Geschichte nach [D27](#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg). Die Schwelle zählt nur
endgültig aussortierte Zeilen. Die Zählungen je Schritt zeigen gescheiterte und gerettete
Zeilen getrennt.
**Begründung:** Ohne die Geschichte im Hintergrund bekäme eine gerettete und später erneut
gescheiterte Zeile eine neue Kennung ohne vorigen Grund, entgegen [D27](#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg). Gerettete Zeilen
auf die Schwelle anzurechnen, würde einen Lauf scheitern lassen, dessen Daten vollständig
angekommen sind.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G35](70-gap-ledger.md#g35-zuruckgefuhrte-zeilen-verlieren-ihre-geschichte))
**Betroffene Use Cases:** [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig)

### D47 — Ergebniszeilen tragen einen row_key aus ihren Quellzeilen, und bei 1:n-Joins scheitern nur die betroffenen Ergebniszeilen

**Entscheidung:** Jede Ergebniszeile trägt einen Schlüssel `row_key`, gebildet aus den
`record_key`s aller Quellzeilen, aus denen sie entstanden ist. Ohne Join ist er gleich dem
`record_key`. Writer, die upserten, verwenden `row_key`. Scheitert bei einem 1:n-Join eine
von mehreren Ergebniszeilen einer Quellzeile, wird nur diese Ergebniszeile aussortiert:
ihre Quellzeilen erscheinen nach [D11](#d11-scheitert-eine-zeile-nach-einem-join-wird-jede-beteiligte-quellzeile-aussortiert) mit gemeinsamer `reject_id` in der Übersicht. Eine
Quellzeile zählt nach [D43](#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben) als aussortiert, wenn keine ihrer Ergebniszeilen durchkommt,
sonst als durchgelaufen. In ihrer Tabelle je Quelle steht eine Quellzeile je Lauf höchstens
einmal ([D44](#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler)), auch wenn viele ihrer Partner scheitern; `error_count` zählt die Fehler.
**Begründung:** Ein Upsert auf `record_key` würde die Ergebniszeilen eines 1:n-Joins, die
alle denselben Schlüssel der linken Zeile tragen, zu einer zusammenfassen. Eine rechte
Stammdatenzeile, die mit sehr vielen Zeilen verbunden wird, würde sonst die Tabelle und die
Schwelle überfluten. Nur die tatsächlich gescheiterten Ergebniszeilen auszusortieren, hält
angekommene Daten im Ziel.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G24](70-gap-ledger.md#g24-1n-joins-identitat-der-ergebniszeilen-und-teilweiser-erfolg))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D48 — Nach einer Gruppierung aussortierte Zeilen stehen in einer eigenen Tabelle

**Entscheidung:** Zeilen, die nach einem zusammenfassenden Schritt nach
[D12](#d12-der-rohzustand-reicht-bis-zum-ersten-schritt-uber-alle-zeilen-danach-wird-die-aggregierte-zeile-aussortiert)
scheitern, stehen im Laufergebnis in einer eigenen Tabelle aggregierter aussortierter
Zeilen: Werte der Zeile, Gruppenschlüssel als abgeleiteter Wert und, wo vorhanden, als
Rohwert, Anzahl der Quellzeilen, Schritt, Grund und Code. Diese Tabelle lässt sich nicht
nach [D16](#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle)
nachverarbeiten. Mit der Option "volle Herkunft" stehen zusätzlich die einzelnen
Quellzeilen in ihren Tabellen je Quelle, mit dem Code `aggregate_failed` und derselben
`reject_id`. Diese lassen sich nachverarbeiten.
**Begründung:** Eine aggregierte Zeile hat keine einzelne Quelle und keine Rohspalten und
passt deshalb in keine Tabelle je Quelle. Ohne volle Herkunft kennt die Engine ihre
Quellzeilen nicht mehr. Die Annahme, man finde sie über den Gruppenschlüssel in der
Lieferung, trägt nicht, weil die Lieferung nicht mehr vorliegen muss und der Schlüssel ein
abgeleiteter Wert sein kann.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G22](70-gap-ledger.md#g22-aussortierte-aggregierte-zeilen-haben-keinen-ort-und-lassen-sich-nicht-nachverarbeiten))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet)

### D49 — Aussortierte Zeilen umfangreicher Läufe werden über Writer im Plan während des Laufs geschrieben, sonst hält sie das Ergebnis bis Close

**Entscheidung:** Eine Pipeline kann im Plan Writer für aussortierte Zeilen angeben, je
Quelle oder für alle Quellen, und einen Writer für die Übersicht. Diese schreiben während
des Laufs im Streaming, und das Ergebnis enthält dann nur Übersicht und Zählungen. Ohne
solche Writer hält das Ergebnis die aussortierten Zeilen. Übersteigen sie das Budget nach
[D28](#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern), lagert die Engine sie aus, und sie bleiben lesbar, bis das Ergebnis geschlossen
wird. Erst dann wird das Verzeichnis zum Auslagern geleert. Für geplante Läufe empfiehlt die
Dokumentation Writer im Plan.
**Begründung:** Bei einer umfangreichen Lieferung, in der viele Zeilen scheitern, ist die
Tabelle aussortierter Zeilen selbst umfangreich. Im Speicher würde sie
[UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen) brechen, und ausgelagert würde sie nach [D28](#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern) am Ende des Laufs gelöscht.
Writer im Plan lösen das für große Läufe. Das Ergebnis mit `Close` bleibt für kleine Läufe
und Tests bequem.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G26](70-gap-ledger.md#g26-wo-die-aussortierten-zeilen-groer-laufe-liegen))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D50 — Eine Tabelle trägt ihre aussortierten Zeilen und einen haftenden Fehler

**Entscheidung:** Eine sofortige `Table` nach [D31](#d31-jede-operation-gibt-es-einmal-als-wert-mit-zwei-einstiegen-sofort-auf-einer-tabelle-oder-im-plan) trägt die aussortierten Zeilen der
auf sie angewandten Operationen und einen haftenden Fehler mit sich. Datenfehler werden
nach der Voreinstellung aus [D5](#d5-voreinstellung-durchlauf-mit-aussortieren-unveranderliche-tabellen) aussortiert. Planfehler, zum Beispiel eine unbekannte
Spalte, setzen den haftenden Fehler, und folgende Operationen in der Kette laufen nicht
mehr. Ein Fehlerverhalten lässt sich für eine Tabelle festlegen. Im Modus "stoppen" setzt
der erste Datenfehler den haftenden Fehler. Eine im Code gebaute Tabelle hat als Fundstelle
die Quelle `code` mit dem Zeilenindex und als Rohzustand die Werte beim Erstellen.
**Begründung:** Ohne eigenen Fehlerweg hätten sofortige Methoden entweder Fehler still
verschluckt oder keine aussortierten Zeilen geliefert, und
[T22](30-test-plan.md#t22-dieselbe-pipeline-liefert-im-speicher-und-im-streaming-dasselbe-ergebnis)
(gleiche Ergebnisse sofort und im Plan) wäre nicht definiert. Der haftende Fehler hält
Ketten wie in v1 lesbar.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G31](70-gap-ledger.md#g31-fehlerweg-der-sofortigen-table-methoden))
**Betroffene Use Cases:** [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D51 — Im Modus stoppen wird der scheiternde Block nicht geschrieben, bereits geschriebene Blöcke bleiben

**Entscheidung:** Stoppt ein Lauf im Modus "stoppen" nach [D3](#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen), bleiben Blöcke, die
schon an ein Ziel gegangen sind, dort. Der Block, in dem der Fehler auftritt, wird nicht
geschrieben, auch nicht seine Zeilen vor dem Fehler. "Vor" meint die
Verarbeitungsreihenfolge, auch nach einem Sortieren. Ein gestoppter Lauf kann ein Ziel also
teilweise befüllt haben. Wer das ausschließen will, verwendet Transaktionen im Ziel oder ein
Upsert auf `row_key` nach [D47](#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen).
**Begründung:** Blockweise ganz oder gar nicht passt zur blockweisen Ausführung nach
[D6](#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen) und ist einfach zu erklären. Ein Zurückrollen bereits geschriebener Blöcke kann die
Library nicht für alle Ziele leisten.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G40](70-gap-ledger.md#g40-teilweise-geschriebene-blocke-im-modus-stoppen))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)

### D52 — Bei Excel ist der Rohzustand der angezeigte Zellinhalt, umgewandelt wird der gespeicherte Wert

**Entscheidung:** Bei Excel-Lieferungen ist der Rohzustand einer Zelle nach [D10](#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle) ihr
Inhalt, wie Excel ihn anzeigt, also formatiert. Das Umwandeln in einen Typ verwendet den
gespeicherten Wert, bei Datumszellen also die Seriennummer. Nur Zellen, die in Excel Text
sind, werden als Text geparst. Bei Formeln gilt der gespeicherte Ergebniswert; die Formel
kann optional als Info mitgeführt werden. Die Fundstelle besteht aus Datei, Sheet und der
Zeilennummer, wie Excel sie zeigt (beginnend bei 1, einschließlich Kopfzeile). Die
Info-Spalte `cell` nennt die betroffene Zelle (z. B. `C17`). Byte-Offset und `raw_line`
bleiben bei Excel leer. Ob das Lesen großer Excel-Dateien im Budget bleibt, misst der
Prototyp ([G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange)).
**Begründung:** Excel ist das häufigste Lieferformat ([UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)). Der angezeigte Inhalt
ist das, was der Pipeline-Entwickler in der Datei wiederfindet. Würde der formatierte Text
umgewandelt, scheiterten echte Datumszellen an Ländereinstellungen. Byte-Offsets ergeben in
einem gezippten XML-Archiv keinen Sinn.
Den Rohzustand legt inzwischen [D62](#d62-bei-excel-tragen-rohzustand-und-arbeitsspalte-den-gespeicherten-wert-in-fester-textform) fest.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G32](70-gap-ledger.md#g32-rohzustand-und-fundstelle-bei-excel))
**Betroffene Use Cases:** [UC8](05-use-cases.md#uc8-eine-lieferung-besteht-aus-mehreren-dateien-oder-sheets), [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen)

### D53 — Jede Datei und jedes Sheet ist eine Quelle, Gruppen von Dateien wirken als eine Quelle

**Entscheidung:** Jede Datei und jedes Sheet ist eine eigene Quelle im Sinn von
[D13](#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen), mit einem Namen aus Datei und Sheet (z. B. `lieferung.xlsx#Kunden`). Eine
Gruppe von Dateien nach Muster wirkt als eine logische Quelle; die Datei steht dann in der
Fundstelle. Fehlt eine erwartete Datei einer Gruppe, ist das der Lieferfehler `missing_file`
nach [D42](#d42-jeder-lieferfehler-setzt-den-status-delivery_error). Bei CSV ohne Kopfzeile kommen die Spaltennamen aus dem erwarteten
Aufbau nach [D22](#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird) und werden nach Position zugeordnet. Bei JSON und NDJSON ist ein
fehlendes Feld in einem Datensatz der Datenfehler `missing_field`. Fehlt ein erwartetes Feld
in der ganzen Lieferung, ergibt das am Ende des Laufs `missing_column`. Neue Felder werden
beim ersten Auftreten als Befund gemeldet.
**Begründung:** Lieferungen bestehen oft aus mehreren Teilen. Als eigene Quellen behalten
sie getrennte Tabellen aussortierter Zeilen mit ihren eigenen Spalten. Die Gruppe deckt
Teile mit gleichem Aufbau ab. JSON hat keine Kopfzeile, deshalb lässt sich ein fehlendes
Feld erst am Ende sicher feststellen.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G33](70-gap-ledger.md#g33-lieferungen-ohne-kopf-und-aus-mehreren-dateien-oder-sheets))
**Betroffene Use Cases:** [UC8](05-use-cases.md#uc8-eine-lieferung-besteht-aus-mehreren-dateien-oder-sheets), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D54 — Laufzeitfehler in Ausdrücken sortieren die Zeile mit dem Code expr aus

**Entscheidung:** Scheitert ein Ausdruck nach [D32](#d32-ausdrucke-sind-der-standard-fur-berechnungen-closures-der-ausweg) erst bei der Ausführung für eine
Zeile (Division durch null, Überlauf, Datum außerhalb des gültigen Bereichs), wird die Zeile
mit dem Code `expr` aussortiert. Soll ein solcher Fehler null ergeben, schreibt der
Pipeline-Entwickler das ausdrücklich an den Ausdruck (z. B. `OrNull()`). Die Codes aus
[D14](#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix) sind ein fester Kern. Closures können über einen typisierten Fehler einen eigenen
Code setzen, der mit `custom:` beginnt.
**Begründung:** Ein stilles null würde Fehler verschwinden lassen, gegen den Zweck des
Fehlermodells. Das ausdrückliche `OrNull()` deckt die Fälle ab, in denen SQL-Verhalten
gewollt ist. Eigene Codes erlauben es, Zweige nach
[D25](#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck) fachlich zu bilden, ohne Texte zu vergleichen.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G34](70-gap-ledger.md#g34-laufzeitfehler-in-ausdrucken-und-vollstandigkeit-der-fehlercodes))
**Betroffene Use Cases:** [UC5](05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC7](05-use-cases.md#uc7-externer-entwickler-baut-seine-erste-pipeline)

### D55 — Roh- und Arbeitsdaten teilen Spalten, bis ein Schritt eine Spalte ändert

**Entscheidung:** Rohzustand nach [D9](#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert) und Arbeitsdaten teilen sich die Spalten eines
Blocks, bis ein Schritt eine Spalte ändert. Erst dann erhält die Arbeitsseite eine eigene
Spalte. Der Rohzustand kostet damit nur für geänderte Spalten zusätzlichen Speicher.
**Begründung:** Beim Sortieren und Joinen werden alle Zeilen gehalten oder ausgelagert. Eine
vollständige Kopie des Rohzustands würde Speicher und Auslagern grob verdoppeln. Rohspalten
bleiben nach [D29](#d29-daten-laufen-in-blocken-typisierter-spalten-rohspalten-bleiben-bis-zum-cast-text) ohnehin Text, bis ein Schritt sie umwandelt. Ob das im Budget
reicht, misst der Prototyp ([G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange)).
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G36](70-gap-ledger.md#g36-speicherkosten-des-rohzustands-beim-sortieren-und-joinen))
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D56 — Die Library begrenzt Feldlänge und entpackten Umfang, schützt ausgelagerte Dateien und maskiert Formeln in CSV auf Wunsch

**Entscheidung:** Ein einzelnes Feld darf standardmäßig höchstens 16 MiB groß sein; die
Grenze ist einstellbar. Längere Felder sortieren die Zeile mit dem Code `field_too_large`
aus, und `raw_line` wird auf die Grenze gekürzt. Für Excel-Archive gibt es eine Grenze für
den entpackten Umfang und für das Verhältnis zwischen gepacktem und entpacktem Umfang
(standardmäßig höchstens 100:1), beide einstellbar. Wird eine überschritten, ist das der
Lieferfehler `unreadable` nach [D42](#d42-jeder-lieferfehler-setzt-den-status-delivery_error). Das Verzeichnis zum Auslagern ist nur für den
ausführenden Nutzer zugänglich (Verzeichnis `0700`, Dateien `0600`). Der CSV-Writer
maskiert Werte, die als Formel gelesen würden (beginnend mit `=`, `+`, `-`, `@`), nur auf
Wunsch durch ein vorangestelltes `'`. Standardmäßig ist das aus.
**Begründung:** Lieferungen kommen von externen Anbietern und sind nicht vertrauenswürdig
([Security Boundaries](60-security-boundaries.md)). Das Budget nach [D28](#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern) gilt für
Blöcke, nicht für einzelne Werte oder das Entpacken eines Archivs. Formeln standardmäßig zu
maskieren würde Werte beim Datenaustausch verfälschen, deshalb ist es eine Option.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G20](70-gap-ledger.md#g20-grenzen-fur-feldlange-und-entpackten-umfang))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D57 — Der Prototyp liest CSV und Excel, v2.0 zusätzlich JSON und NDJSON

**Entscheidung:** Der Prototyp bringt Reader für CSV und Excel mit, v2.0.0 zusätzlich für
JSON und NDJSON. Ob und wann ein Reader für Parquet dazukommt, ist nicht Teil von v2.0.0.
**Begründung:** Excel und CSV sind die häufigsten Lieferformate ([UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)). JSON und
NDJSON kommen aus v1 und aus HTTP-Quellen. Parquet setzt die spaltenorientierte Speicherung
voraus, deren Nutzen der Prototyp erst messen soll ([G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange)).
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G37](70-gap-ledger.md#g37-bestehkriterien-und-fehlende-festlegungen-im-scope-des-prototyps))
**Betroffene Use Cases:** [UC8](05-use-cases.md#uc8-eine-lieferung-besteht-aus-mehreren-dateien-oder-sheets)

### D58 — Der Prototyp hat feste Bestehkriterien für Laufzeit, Speicher und Budget

**Entscheidung:** [G5](70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht) gilt als bestanden, wenn v2 mit automatischer Wahl nach
[D7](#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert) bei Filter, Umwandeln, Sortieren, Gruppieren und Join höchstens das 1,2-Fache der
Laufzeit und höchstens den Spitzenspeicher von v1 `MutableTable` braucht. Andernfalls wird
[D7](#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert) durch eine neue Decision wieder aufgemacht. [G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange) gilt als bestanden, wenn v2
bei zahlenlastigen Lieferungen nicht langsamer als v1 `Table` ist und höchstens 70 % von
dessen Spitzenspeicher braucht. Die Voreinstellungen für den Anteil des Speichers und die
Blocklänge werden aus diesen Messungen festgelegt. Die Toleranz in
[T23](30-test-plan.md#t23-ein-lauf-uber-mehr-daten-als-das-budget-halt-das-budget-ein) beträgt 10 % über dem Budget. Überschreitet im
Prototyp die rechte Seite eines Joins das Budget ([P1](40-scope-prototype.md#p1-auslagern-nur-fur-sortieren-und-gruppieren)),
endet der Lauf mit `aborted` und einem Grund, der die Einschränkung nennt.
**Begründung:** Ohne Grenze würde jedes Messergebnis die Gaps schließen, und der Prototyp
könnte seine Fragen nicht wirklich beantworten.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G37](70-gap-ledger.md#g37-bestehkriterien-und-fehlende-festlegungen-im-scope-des-prototyps))
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D59 — Die Grenze für einen Formatänderungs-Befund ist einstellbar, die Voreinstellung wird im Prototyp festgelegt

**Entscheidung:** Ab welchem Anteil gleichartig scheiternder Werte einer Spalte der
Änderungsbericht nach [D23](#d23-das-laufergebnis-enthalt-einen-anderungsbericht) eine Formatänderung meldet, legt der Pipeline-Entwickler
fest, je Pipeline und bei Bedarf je Spalte. Die Library bringt eine Voreinstellung mit, die
im Prototyp an den Beispiel-Lieferungen nach
[D60](#d60-der-prototyp-wird-mit-eigenen-beispiel-lieferungen-der-1brc-datei-und-in-docker-mit-verschiedenen-speicher-limits-erprobt) festgelegt wird.
**Begründung:** Der Entwickler kennt seine Daten besser als eine feste Grenze. Eine
Voreinstellung braucht es trotzdem, damit der Bericht ohne Konfiguration etwas meldet.
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G12](70-gap-ledger.md#g12-ab-wann-eine-haufung-von-fehlern-als-formatanderung-gilt))
**Betroffene Use Cases:** [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D60 — Der Prototyp wird mit eigenen Beispiel-Lieferungen, der 1BRC-Datei und in Docker mit verschiedenen Speicher-Limits erprobt

**Entscheidung:** Die Form der aussortierten Zeilen und der Änderungsbericht werden an
eigens gebauten Beispiel-Lieferungen erprobt, die typische Probleme von Anbieter-Dateien
nachbilden (Excel und CSV mit falschen Formaten, fehlenden und umbenannten Spalten,
Platzhaltern, kaputten Zeilen). Echte Kundenlieferungen sind dafür nicht nötig. Für den
Großlauf dient die vorhandene 1BRC-Datei (`~/1brc/golang/measurements.txt`, 13,8 GB,
Semikolon-getrennt, ohne Kopfzeile, Station und Messwert); bei Bedarf wird zusätzlich eine
breitere Lieferung erzeugt. Der Großlauf läuft in Docker mit mehreren Speicher-Limits,
damit das Budget nach [D28](#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern) gegen echte Container-Limits geprüft wird. Die Testdaten
für [G13](70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange) sind generierte Lieferungen mit 1 und 10 Millionen Zeilen, jeweils
zahlenlastig und textlastig, dazu eine Excel-Lieferung.
**Begründung:** Eigene Beispiele lassen sich gezielt auf die Fehlerarten zuschneiden und
ohne Datenschutzfragen ins Repo legen. Die 1BRC-Datei liegt schon vor, ist groß genug für
den zweistelligen GB-Bereich und passt zu Gruppieren und Sortieren aus
[P1](40-scope-prototype.md#p1-auslagern-nur-fur-sortieren-und-gruppieren). Docker mit Limits bildet den Betrieb per Cron in
Containern ab ([UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung)).
**Quelle:** Maintainer im Kickoff, 2026-09-27 (Auflösung von [G16](70-gap-ledger.md#g16-konkrete-werte-fur-die-exit-kriterien-des-prototyps); ändert das Exit-Kriterium zu [G11](70-gap-ledger.md#g11-form-der-aussortierten-zeilen-im-prototyp-validieren))
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D61 — Die Kennung einer Lieferung ist ein Fingerabdruck, der beim Öffnen feststeht

**Entscheidung:** Die Kennung einer Lieferung für `record_key` nach [D18](#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash) ist ein
Fingerabdruck aus Quellname, Größe, Änderungszeit und einem Hash über die ersten 64 KiB. Er
steht fest, sobald die Quelle geöffnet ist. Gibt die Pipeline eine eigene Kennung an (z. B.
bei HTTP-Quellen), gilt diese. `record_hash` wird je Zeile beim Lesen gebildet. Ändert sich
eine Datei nachträglich, gilt sie als andere Lieferung, und ihre Schlüssel ändern sich.
**Begründung:** Ein Hash über die ganze Lieferung wäre erst am Ende bekannt, Zeilen werden
nach [D6](#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen) und [D49](#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close) aber schon während des Lesens mit Schlüssel geschrieben.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G44](70-gap-ledger.md#g44-ein-hash-uber-die-ganze-lieferung-ist-im-streaming-erst-am-ende-bekannt))
**Betroffene Use Cases:** [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D62 — Bei Excel tragen Rohzustand und Arbeitsspalte den gespeicherten Wert in fester Textform

**Entscheidung:** Bei Excel-Lieferungen enthalten Rohzustand und Arbeitsspalte den
gespeicherten Wert einer Zelle in einer festen, von Ländereinstellungen unabhängigen
Textform: Datum als `2026-09-27`, Zahl als `1234.5`, bei Formeln das Ergebnis. Der von Excel
angezeigte Text steht in den aussortierten Zeilen zusätzlich in der Info-Spalte `display`.
Das ersetzt die Festlegung des Rohzustands in [D52](#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert).
**Begründung:** Mit dem angezeigten Text als Rohzustand hätten Roh- und Arbeitsspalte von
Anfang an verschiedene Werte, [D55](#d55-roh-und-arbeitsdaten-teilen-spalten-bis-ein-schritt-eine-spalte-andert) hätte für Excel nicht gegolten, und eine
Nachverarbeitung nach [D16](#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle) hätte den angezeigten Text umgewandelt und wäre an den
Ländereinstellungen gescheitert. Die Info-Spalte `display` lässt die Zelle trotzdem
wiedererkennen.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G45](70-gap-ledger.md#g45-excel-halt-zwei-werte-je-zelle))
**Betroffene Use Cases:** [UC3](05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [UC4](05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet), [UC8](05-use-cases.md#uc8-eine-lieferung-besteht-aus-mehreren-dateien-oder-sheets)

### D63 — Es gilt der höchste zutreffende Status, und das Ergebnis nennt alle Befunde

**Entscheidung:** Treffen auf einen Lauf mehrere Status nach [D21](#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst) zu, gilt der
höchste in der Rangfolge `plan_error` > `sink_error` > `delivery_error` > `aborted` >
`failed_threshold` > `ok`. Das Ergebnis nennt zusätzlich alle zutreffenden Befunde.
`aborted` umfasst einen Stopp nach [D3](#d3-das-fehlerverhalten-ist-pro-pipeline-wahlbar-aussortieren-oder-sofort-stoppen), einen Abbruch über den Kontext und
Ressourcenfehler wie eine volle Platte beim Auslagern oder eine zu große rechte Seite eines
Joins im Prototyp nach [D58](#d58-der-prototyp-hat-feste-bestehkriterien-fur-laufzeit-speicher-und-budget). Ein Lieferfehler im Modus "stoppen" ergibt
`delivery_error`.
**Begründung:** Der Scheduler braucht genau einen Status für den Exit-Code. Die Rangfolge
stellt Fehler der Pipeline und des Ziels vor Fehler der Lieferung und diese vor Abbrüche
und Schwellen. Die Liste aller Befunde verhindert, dass ein höherer Status einen anderen
verdeckt.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G46](70-gap-ledger.md#g46-vorrang-der-status-und-umfang-von-aborted))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC2](05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt)

### D64 — Mit dem Rohzustand geteilte Spalten gelten als geteilt, auch im Modus immer ändern

**Entscheidung:** Eine Spalte, die sich Arbeitsdaten und Rohzustand nach [D55](#d55-roh-und-arbeitsdaten-teilen-spalten-bis-ein-schritt-eine-spalte-andert) teilen,
gilt als geteilt im Sinn von [D7](#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert). Auch im Modus "immer an Ort und Stelle ändern" nach
[D8](#d8-eine-option-legt-fest-dass-die-engine-immer-kopiert-oder-immer-an-ort-und-stelle-andert) kopiert die Engine sie bei der ersten Änderung einmal und vermerkt das im Trace,
wie an Verzweigungen nach [D9](#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert). Die Vergleiche nach [D58](#d58-der-prototyp-hat-feste-bestehkriterien-fur-laufzeit-speicher-und-budget) messen v2 mit
mitgeführtem Rohzustand.
**Begründung:** Sonst würde eine Änderung an Ort und Stelle den Rohzustand verändern, was
[D9](#d9-der-rohzustand-wird-getrennt-gehalten-an-verzweigungen-wird-immer-kopiert) ausschließt. Der Modus behält seinen Nutzen für alle weiteren Änderungen und für
abgeleitete Spalten. Der Vergleich mit Rohzustand zeigt die ehrlichen Kosten des
Fehlermodells.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G52](70-gap-ledger.md#g52-geteilte-rohspalten-gegen-den-modus-immer-andern))
**Betroffene Use Cases:** [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)

### D65 — GOMEMLIMIT setzt die Engine nur auf Wunsch, und das Budget gilt je Prozess

**Entscheidung:** Die Engine setzt `GOMEMLIMIT` nur, wenn die Pipeline das ausdrücklich
verlangt, und überschreibt einen gesetzten Wert nie. Das Speicherbudget nach [D28](#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern) gilt
für den ganzen Prozess. Laufen mehrere Läufe in einem Prozess, teilen sie es sich. Das
ersetzt das automatische Setzen von `GOMEMLIMIT` in [D28](#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern).
**Begründung:** `GOMEMLIMIT` wirkt auf den ganzen Prozess. Setzt es jeder Lauf selbst,
überschreibt der letzte die anderen, und Budgets je Lauf ergäben zusammen mehr als das
Limit. Für den häufigen Fall "ein Lauf je Prozess" per Cron bleibt das Setzen auf Wunsch
einfach.
**Quelle:** Vorschlag im Kickoff, vom Maintainer bestätigt, 2026-09-27 (Auflösung von [G53](70-gap-ledger.md#g53-gomemlimit-wirkt-auf-den-ganzen-prozess))
**Betroffene Use Cases:** [UC1](05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC6](05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen)
