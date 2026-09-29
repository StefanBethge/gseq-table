# Messergebnisse des Prototyps

Messungen zu [G5](../../../docs/explanation/design/v2/70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht) und [G13](../../../docs/explanation/design/v2/70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange) nach den Bestehkriterien aus [D58](../../../docs/explanation/design/v2/10-design-decisions.md#d58-der-prototyp-hat-feste-bestehkriterien-fur-laufzeit-speicher-und-budget), mit den Lieferungen aus
[D60](../../../docs/explanation/design/v2/10-design-decisions.md#d60-der-prototyp-wird-mit-eigenen-beispiel-lieferungen-der-1brc-datei-und-in-docker-mit-verschiedenen-speicher-limits-erprobt), sowie die Läufe zu [T23](../../../docs/explanation/design/v2/30-test-plan.md#t23-ein-lauf-uber-mehr-daten-als-das-budget-halt-das-budget-ein) in Docker. Gemessen am 2026-09-29 (#53). Die Rohdaten
stehen unter [results/](results/), das Werkzeug ist `bench` in diesem Verzeichnis.

## Ergebnis

| Frage | Kriterium ([D58](../../../docs/explanation/design/v2/10-design-decisions.md#d58-der-prototyp-hat-feste-bestehkriterien-fur-laufzeit-speicher-und-budget)) | Ergebnis |
|---|---|---|
| [G5](../../../docs/explanation/design/v2/70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht): v2 automatisch mit Rohzustand gegen v1 `MutableTable` | höchstens 1,2-fache Laufzeit und höchstens der Spitzenspeicher, bei Filter, Umwandeln, Sortieren, Gruppieren und Join | **nicht bestanden, Neumessung nach #66**. Zeit: nur Sortieren und zahlenlastiges Umwandeln im Kriterium, sonst das 1,3- bis 2,2-Fache. Speicher: nur abgeleitete Spalten im Kriterium, Sortieren das 2,4- bis 2,9-Fache |
| [G13](../../../docs/explanation/design/v2/70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange): typisierte Blöcke gegen v1 `Table`, zahlenlastig | nicht langsamer und höchstens 70 % des Spitzenspeichers | **nicht bestanden, Neumessung nach #66**. Nur Umwandeln erfüllt beides (0,67- bis 0,68-fache Zeit, 0,53- bis 0,58-facher Speicher); Sortieren ist schneller, braucht aber das Doppelte an Speicher |
| [G13](../../../docs/explanation/design/v2/70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange): Blocklänge | Voreinstellung aus den Messungen | 16384 Zeilen ([D106](../../../docs/explanation/design/v2/10-design-decisions.md#d106-die-blocklange-ist-standardmaig-16384-zeilen-0-wahlt-sie)); 4096 bis 262144 liegen innerhalb weniger Prozent |
| [G13](../../../docs/explanation/design/v2/70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange): Anteil des Speichers | Voreinstellung aus den Messungen | 10 % des erkannten Limits ([D107](../../../docs/explanation/design/v2/10-design-decisions.md#d107-der-anteil-des-speichers-fur-das-budget-ist-ein-zehntel-des-erkannten-limits)); 25 % endeten ohne `GOMEMLIMIT` in jedem Limit durch das System |
| [G13](../../../docs/explanation/design/v2/70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange): Rohzustand an und aus | messen | ohne Rohzustand ([D105](../../../docs/explanation/design/v2/10-design-decisions.md#d105-ein-interner-mess-schalter-lasst-den-rohzustand-fur-die-benchmarks-weg)) sinkt der Spitzenspeicher beim Filtern um rund 40 %, sonst meist um weniger als 10 %; die Laufzeit bleibt gleich |
| [G13](../../../docs/explanation/design/v2/70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange): Excel lesen | im Budget | 1 Mio. Zeilen (58 MB) mit Ziel in 136 MiB Prozessspeicher |
| [T23](../../../docs/explanation/design/v2/30-test-plan.md#t23-ein-lauf-uber-mehr-daten-als-das-budget-halt-das-budget-ein): 1BRC-Datei in Docker mit 1g, 2g, 4g | Lauf kommt durch, Spitze im Budget plus 10 % | **nicht bestanden, Neumessung nach #66**. Die volle Datei endet bei jedem Limit durch das System; die Engine lagert rechtzeitig aus, aber die Buchführung je Quellzeile außerhalb des Budgets ([G67](../../../docs/explanation/design/v2/70-gap-ledger.md#g67-buchfuhrung-je-quellzeile-liegt-auerhalb-des-budgets)) wächst mit der Lieferung. Der Unit-Test zu [T23](../../../docs/explanation/design/v2/30-test-plan.md#t23-ein-lauf-uber-mehr-daten-als-das-budget-halt-das-budget-ein) ist grün |

[G5](../../../docs/explanation/design/v2/70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht) und [G13](../../../docs/explanation/design/v2/70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange) bleiben offen. [D7](../../../docs/explanation/design/v2/10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert) wird erst wieder aufgemacht, wenn die Neumessung nach dem Umbau der
Buchführung ([G67](../../../docs/explanation/design/v2/70-gap-ledger.md#g67-buchfuhrung-je-quellzeile-liegt-auerhalb-des-budgets), #66) das Kriterium weiter verfehlt.

## Maschine

- Host: Apple M4 Pro, 14 Kerne, 48 GiB, macOS 26.6.2, Go 1.27.1 (darwin/arm64)
- Docker: Docker Desktop 28.5.1, VM mit 7,65 GiB und 14 CPUs, Kernel 6.10.14-linuxkit, Image `busybox:1.37.0`
  mit statisch gebauten Linux-Binärdateien (`CGO_ENABLED=0 GOOS=linux`), `--memory-swap` gleich
  `--memory`, also ohne Swap
- v1: `github.com/stefanbethge/gseq-table` v1.3.0, v2: dieser Stand von `experimental/v2`

## Methode

Jede Messung läuft in einem eigenen Kindprozess (`bench measure`). Gemessen werden die Wandzeit und die
Spitze des residenten Speichers des Kindprozesses (`maxrss`), also Spitzenspeicher des ganzen
Prozesses, nicht die Zählung der Engine ([D104](../../../docs/explanation/design/v2/10-design-decisions.md#d104-die-unit-tests-messen-das-budget-an-der-zahlung-der-engine-den-speicher-des-prozesses-misst-t23-in-docker)). Die Tabellen zeigen den Median aus mindestens drei
Wiederholungen. Jede Messung liest die Lieferung aus einer CSV-Datei und wendet eine Operation an;
das Ergebnis bleibt im Speicher. `read` ist das Lesen allein.

- `v1t`: v1 `Table`. `v1m`: v1 `MutableTable` über `MutableView` ohne Kopie, Operationen an Ort und
  Stelle; Gruppieren und Join gibt es in v1 nur auf `Table` und laufen auf `FreezeView`.
  Umwandeln in v1 heißt: den Text parsen und neu formatieren, wie die nachsichtigen Umwandlungen von
  v1 ([D74](../../../docs/explanation/design/v2/10-design-decisions.md#d74-cast-ist-standardmaig-streng-die-option-lenient-verhalt-sich-wie-v1)).
- `v2`: automatische Wahl nach [D7](../../../docs/explanation/design/v2/10-design-decisions.md#d7-die-engine-entscheidet-ob-sie-daten-kopiert-oder-an-ort-und-stelle-andert), mit Rohzustand nach [D64](../../../docs/explanation/design/v2/10-design-decisions.md#d64-mit-dem-rohzustand-geteilte-spalten-gelten-als-geteilt-auch-im-modus-immer-andern), Blocklänge nach [D106](../../../docs/explanation/design/v2/10-design-decisions.md#d106-die-blocklange-ist-standardmaig-16384-zeilen-0-wahlt-sie).
  Filter, Gruppieren und abgeleitete Zahlenspalten wandeln die Spalten vorher um, weil Rohspalten
  Text sind ([D29](../../../docs/explanation/design/v2/10-design-decisions.md#d29-daten-laufen-in-blocken-typisierter-spalten-rohspalten-bleiben-bis-zum-cast-text)). `v2noraw`: wie `v2`, ohne Rohzustand ([D105](../../../docs/explanation/design/v2/10-design-decisions.md#d105-ein-interner-mess-schalter-lasst-den-rohzustand-fur-die-benchmarks-weg)).
- Zahlenlastige Lieferung: `id`, Textschlüssel `code` mit 1000 Werten, fünf Gleitkommaspalten, eine
  Ganzzahlspalte (57 MB bei 1 Mio. Zeilen). Textlastige Lieferung: `id`, `code`, fünf Textspalten
  (Stadt, Name, Straße, E-Mail, Kommentar), ein Betrag (153 MB bei 1 Mio. Zeilen). `bench gen` erzeugt
  sie deterministisch. Die Fälle: Filter `f1 > 0` bzw. `amount > 0`, Umwandeln aller Zahlenspalten,
  abgeleitete Spalte `f1 * f2` bzw. Verkettung zweier Texte, Sortieren nach `code` bzw. `name`,
  Gruppieren nach `code` mit Summe, Mittelwert und Anzahl, Join mit einer Tabelle von 1000 Codes.
  Alle Implementierungen liefern dieselbe Zahl an Ergebniszeilen (`TestCasesAgreeAcrossImplementations`).

Bei den ersten drei Wiederholungen von Sortieren, Gruppieren und Join über 10 Mio. Zeilen war der
Host gestört: Alle Implementierungen waren zugleich bis zu fünfmal langsamer. Diese Läufe stehen in
[results/compare-disturbed.jsonl](results/compare-disturbed.jsonl); die Tabellen nutzen die fünf
Wiederholungen danach, deren Zeiten um weniger als 25 % streuen.

## [G5](../../../docs/explanation/design/v2/70-gap-ledger.md#g5-ob-es-eine-veranderbare-tabelle-braucht): v2 gegen v1 MutableTable

Zahlenlastig, 1M Zeilen:

| Fall | v1m s | v2 s | Zeit × | v1m MiB | v2 MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 0.26 | 0.44 | 1.68 | 355 | 388 | 1.09 | **nein** | **nein** |
| filter | 0.31 | 0.61 | 1.96 | 331 | 456 | 1.38 | **nein** | **nein** |
| cast | 0.77 | 0.73 | 0.95 | 335 | 498 | 1.49 | ja | **nein** |
| derive | 0.47 | 0.61 | 1.29 | 468 | 443 | 0.95 | **nein** | ja |
| sort | 1.92 | 1.18 | 0.61 | 331 | 945 | 2.86 | ja | **nein** |
| groupby | 0.41 | 0.80 | 1.93 | 418 | 736 | 1.76 | **nein** | **nein** |
| join | 0.38 | 0.81 | 2.14 | 622 | 1066 | 1.71 | **nein** | **nein** |

Zahlenlastig, 10M Zeilen:

| Fall | v1m s | v2 s | Zeit × | v1m MiB | v2 MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 2.40 | 4.25 | 1.77 | 3466 | 3942 | 1.14 | **nein** | **nein** |
| filter | 2.80 | 5.99 | 2.13 | 3471 | 4982 | 1.44 | **nein** | **nein** |
| cast | 7.46 | 7.20 | 0.97 | 3794 | 4866 | 1.28 | ja | **nein** |
| derive | 4.34 | 5.97 | 1.37 | 4682 | 4620 | 0.99 | **nein** | ja |
| sort | 15.36 | 10.49 | 0.68 | 3448 | 9080 | 2.63 | ja | **nein** |
| groupby | 2.83 | 5.88 | 2.08 | 4372 | 7504 | 1.72 | **nein** | **nein** |
| join | 2.72 | 6.03 | 2.21 | 6364 | 10657 | 1.67 | **nein** | **nein** |

Textlastig, 1M Zeilen:

| Fall | v1m s | v2 s | Zeit × | v1m MiB | v2 MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 0.31 | 0.49 | 1.60 | 465 | 511 | 1.10 | **nein** | **nein** |
| filter | 0.36 | 0.66 | 1.85 | 427 | 569 | 1.33 | **nein** | **nein** |
| cast | 0.44 | 0.59 | 1.34 | 428 | 514 | 1.20 | **nein** | **nein** |
| derive | 0.39 | 0.59 | 1.52 | 645 | 542 | 0.84 | **nein** | ja |
| sort | 1.99 | 1.26 | 0.64 | 442 | 1081 | 2.44 | ja | **nein** |
| groupby | 0.44 | 0.81 | 1.86 | 514 | 820 | 1.60 | **nein** | **nein** |
| join | 0.42 | 0.83 | 1.99 | 778 | 1182 | 1.52 | **nein** | **nein** |

Textlastig, 10M Zeilen:

| Fall | v1m s | v2 s | Zeit × | v1m MiB | v2 MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 2.25 | 3.50 | 1.55 | 4402 | 4888 | 1.11 | **nein** | **nein** |
| filter | 2.55 | 4.72 | 1.85 | 4402 | 5794 | 1.32 | **nein** | **nein** |
| cast | 3.19 | 4.29 | 1.34 | 4408 | 5223 | 1.18 | **nein** | **nein** |
| derive | 2.83 | 4.32 | 1.53 | 6376 | 5926 | 0.93 | **nein** | ja |
| sort | 15.29 | 10.70 | 0.70 | 4402 | 10534 | 2.39 | ja | **nein** |
| groupby | 3.15 | 6.27 | 1.99 | 5411 | 9327 | 1.72 | **nein** | **nein** |
| join | 3.03 | 6.41 | 2.12 | 7408 | 12530 | 1.69 | **nein** | **nein** |

## [G13](../../../docs/explanation/design/v2/70-gap-ledger.md#g13-vorteil-spaltenorientierter-blocke-und-voreinstellungen-fur-budget-und-blocklange): v2 gegen v1 Table

Zahlenlastig, 1M Zeilen:

| Fall | v1t s | v2 s | Zeit × | v1t MiB | v2 MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 0.26 | 0.44 | 1.68 | 331 | 388 | 1.17 | **nein** | **nein** |
| filter | 0.31 | 0.61 | 1.96 | 464 | 456 | 0.98 | **nein** | **nein** |
| cast | 1.09 | 0.73 | 0.67 | 939 | 498 | 0.53 | ja | ja |
| derive | 0.48 | 0.61 | 1.28 | 498 | 443 | 0.89 | **nein** | **nein** |
| sort | 2.09 | 1.18 | 0.56 | 428 | 945 | 2.21 | ja | **nein** |
| groupby | 0.40 | 0.80 | 2.00 | 333 | 736 | 2.21 | **nein** | **nein** |
| join | 0.35 | 0.81 | 2.28 | 636 | 1066 | 1.67 | **nein** | **nein** |

Zahlenlastig, 10M Zeilen:

| Fall | v1t s | v2 s | Zeit × | v1t MiB | v2 MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 2.39 | 4.25 | 1.78 | 3243 | 3942 | 1.22 | **nein** | **nein** |
| filter | 2.90 | 5.99 | 2.06 | 3851 | 4982 | 1.29 | **nein** | **nein** |
| cast | 10.56 | 7.20 | 0.68 | 8374 | 4866 | 0.58 | ja | ja |
| derive | 4.26 | 5.97 | 1.40 | 4931 | 4620 | 0.94 | **nein** | **nein** |
| sort | 16.80 | 10.49 | 0.62 | 4032 | 9080 | 2.25 | ja | **nein** |
| groupby | 2.55 | 5.88 | 2.31 | 3565 | 7504 | 2.11 | **nein** | **nein** |
| join | 2.58 | 6.03 | 2.34 | 6147 | 10657 | 1.73 | **nein** | **nein** |

Textlastig, ohne Kriterium:

Textlastig, 1M Zeilen:

| Fall | v1t s | v2 s | Zeit × | v1t MiB | v2 MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 0.31 | 0.49 | 1.57 | 427 | 511 | 1.20 | – | – |
| filter | 0.36 | 0.66 | 1.86 | 498 | 569 | 1.14 | – | – |
| cast | 0.51 | 0.59 | 1.16 | 740 | 514 | 0.70 | – | – |
| derive | 0.38 | 0.59 | 1.57 | 601 | 542 | 0.90 | – | – |
| sort | 2.13 | 1.26 | 0.59 | 488 | 1081 | 2.21 | – | – |
| groupby | 0.41 | 0.81 | 1.97 | 445 | 820 | 1.84 | – | – |
| join | 0.41 | 0.83 | 2.02 | 750 | 1182 | 1.58 | – | – |

Textlastig, 10M Zeilen:

| Fall | v1t s | v2 s | Zeit × | v1t MiB | v2 MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 2.05 | 3.50 | 1.70 | 4171 | 4888 | 1.17 | – | – |
| filter | 2.51 | 4.72 | 1.88 | 4986 | 5794 | 1.16 | – | – |
| cast | 3.75 | 4.29 | 1.14 | 7310 | 5223 | 0.71 | – | – |
| derive | 2.78 | 4.32 | 1.56 | 5934 | 5926 | 1.00 | – | – |
| sort | 16.31 | 10.70 | 0.66 | 4631 | 10534 | 2.27 | – | – |
| groupby | 3.09 | 6.27 | 2.03 | 4266 | 9327 | 2.19 | – | – |
| join | 3.13 | 6.41 | 2.05 | 7556 | 12530 | 1.66 | – | – |

### Rohzustand an und aus

`v2noraw` gegen `v2`. Der Rohzustand kostet vor allem beim Filtern Speicher, weil er die Rohspalten
aller Zeilen hält, auch der verworfenen, bis die Ergebnistabelle ihn nach [D87](../../../docs/explanation/design/v2/10-design-decisions.md#d87-ohne-ziel-halt-die-ergebnistabelle-den-rohzustand-ihrer-zeilen-aussortierte-zeilen-behalten-eine-kopie) übernimmt.

Zahlenlastig, 1M Zeilen:

| Fall | v2 s | v2noraw s | Zeit × | v2 MiB | v2noraw MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 0.44 | 0.43 | 0.99 | 388 | 388 | 1.00 | – | – |
| filter | 0.61 | 0.61 | 1.00 | 456 | 284 | 0.62 | – | – |
| cast | 0.73 | 0.71 | 0.98 | 498 | 362 | 0.73 | – | – |
| derive | 0.61 | 0.60 | 0.99 | 443 | 404 | 0.91 | – | – |
| sort | 1.18 | 1.21 | 1.02 | 945 | 860 | 0.91 | – | – |
| groupby | 0.80 | 0.79 | 0.99 | 736 | 713 | 0.97 | – | – |
| join | 0.81 | 0.80 | 1.00 | 1066 | 1012 | 0.95 | – | – |

Zahlenlastig, 10M Zeilen:

| Fall | v2 s | v2noraw s | Zeit × | v2 MiB | v2noraw MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 4.25 | 4.23 | 1.00 | 3942 | 4104 | 1.04 | – | – |
| filter | 5.99 | 5.85 | 0.98 | 4982 | 2815 | 0.56 | – | – |
| cast | 7.20 | 7.08 | 0.98 | 4866 | 3801 | 0.78 | – | – |
| derive | 5.97 | 5.91 | 0.99 | 4620 | 4002 | 0.87 | – | – |
| sort | 10.49 | 10.36 | 0.99 | 9080 | 8947 | 0.99 | – | – |
| groupby | 5.88 | 5.86 | 1.00 | 7504 | 6993 | 0.93 | – | – |
| join | 6.03 | 6.06 | 1.01 | 10657 | 10288 | 0.97 | – | – |

Textlastig, 1M Zeilen:

| Fall | v2 s | v2noraw s | Zeit × | v2 MiB | v2noraw MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 0.49 | 0.49 | 1.00 | 511 | 525 | 1.03 | – | – |
| filter | 0.66 | 0.66 | 0.99 | 569 | 346 | 0.61 | – | – |
| cast | 0.59 | 0.59 | 0.99 | 514 | 498 | 0.97 | – | – |
| derive | 0.59 | 0.59 | 1.00 | 542 | 578 | 1.07 | – | – |
| sort | 1.26 | 1.28 | 1.02 | 1081 | 1010 | 0.93 | – | – |
| groupby | 0.81 | 0.81 | 1.00 | 820 | 863 | 1.05 | – | – |
| join | 0.83 | 0.84 | 1.01 | 1182 | 1147 | 0.97 | – | – |

Textlastig, 10M Zeilen:

| Fall | v2 s | v2noraw s | Zeit × | v2 MiB | v2noraw MiB | Speicher × | Zeit ok | Speicher ok |
|---|---:|---:|---:|---:|---:|---:|---|---|
| read | 3.50 | 3.49 | 1.00 | 4888 | 5045 | 1.03 | – | – |
| filter | 4.72 | 4.69 | 0.99 | 5794 | 3471 | 0.60 | – | – |
| cast | 4.29 | 4.21 | 0.98 | 5223 | 4992 | 0.96 | – | – |
| derive | 4.32 | 4.30 | 1.00 | 5926 | 5912 | 1.00 | – | – |
| sort | 10.70 | 10.78 | 1.01 | 10534 | 9951 | 0.94 | – | – |
| groupby | 6.27 | 6.16 | 0.98 | 9327 | 8319 | 0.89 | – | – |
| join | 6.41 | 6.25 | 0.98 | 12530 | 11541 | 0.92 | – | – |

### Mit Ziel statt Ergebnis im Speicher

Mit einem Ziel hält die Ergebnistabelle keine Zeilen ([D94](../../../docs/explanation/design/v2/10-design-decisions.md#d94-mit-einem-ziel-fur-ergebnisse-halt-die-ergebnistabelle-keine-zeilen)). Die blockweisen Schritte über 10 Mio.
Zeilen brauchen dann rund 700 MiB, ohne dass die Engine mehr als wenige Blöcke hält. Das ist vor allem
die Buchführung je Quellzeile ([G67](../../../docs/explanation/design/v2/70-gap-ledger.md#g67-buchfuhrung-je-quellzeile-liegt-auerhalb-des-budgets)). Sortieren, Gruppieren und Join halten weiter alle Zeilen, weil
das Budget des Hosts (ein Viertel von 48 GiB vor [D107](../../../docs/explanation/design/v2/10-design-decisions.md#d107-der-anteil-des-speichers-fur-das-budget-ist-ein-zehntel-des-erkannten-limits)) nicht erreicht wird.

| Art | Fall | Impl | Zeilen | Block | Ziel | Budget MiB | GOMEMLIMIT | s | MiB | Wdh. | Fehlgeschlagen | Ergebnis |
|---|---|---|---:|---:|---|---:|---|---:|---:|---:|---:|---|
| num | cast | v2 | 10M | 0 | true | 0 | false | 8.11 | 723 | 3 | 0 | 10000000 |
| num | cast | v2noraw | 10M | 0 | true | 0 | false | 8.24 | 697 | 3 | 0 | 10000000 |
| num | derive | v2 | 10M | 0 | true | 0 | false | 7.14 | 745 | 3 | 0 | 10000000 |
| num | derive | v2noraw | 10M | 0 | true | 0 | false | 7.06 | 695 | 3 | 0 | 10000000 |
| num | filter | v2 | 10M | 0 | true | 0 | false | 6.97 | 737 | 3 | 0 | 4998744 |
| num | filter | v2noraw | 10M | 0 | true | 0 | false | 6.54 | 728 | 3 | 0 | 4998744 |
| num | groupby | v2 | 10M | 0 | true | 0 | false | 10.80 | 7822 | 3 | 0 | 1000 |
| num | groupby | v2noraw | 10M | 0 | true | 0 | false | 10.21 | 7102 | 3 | 0 | 1000 |
| num | join | v2 | 10M | 0 | true | 0 | false | 13.07 | 11026 | 3 | 0 | 10000000 |
| num | join | v2noraw | 10M | 0 | true | 0 | false | 12.37 | 9942 | 3 | 0 | 10000000 |
| num | read | v2 | 10M | 0 | true | 0 | false | 6.87 | 719 | 3 | 0 | 10000000 |
| num | read | v2noraw | 10M | 0 | true | 0 | false | 6.96 | 681 | 3 | 0 | 10000000 |
| num | sort | v2 | 10M | 0 | true | 0 | false | 21.76 | 9107 | 3 | 0 | 10000000 |
| num | sort | v2noraw | 10M | 0 | true | 0 | false | 20.98 | 8910 | 3 | 0 | 10000000 |
| text | cast | v2 | 10M | 0 | true | 0 | false | 6.71 | 695 | 3 | 0 | 10000000 |
| text | cast | v2noraw | 10M | 0 | true | 0 | false | 6.59 | 711 | 3 | 0 | 10000000 |
| text | derive | v2 | 10M | 0 | true | 0 | false | 6.48 | 706 | 3 | 0 | 10000000 |
| text | derive | v2noraw | 10M | 0 | true | 0 | false | 6.50 | 711 | 3 | 0 | 10000000 |
| text | filter | v2 | 10M | 0 | true | 0 | false | 7.21 | 747 | 3 | 0 | 4995817 |
| text | filter | v2noraw | 10M | 0 | true | 0 | false | 7.09 | 713 | 3 | 0 | 4995817 |
| text | groupby | v2 | 10M | 0 | true | 0 | false | 13.72 | 9348 | 3 | 0 | 1000 |
| text | groupby | v2noraw | 10M | 0 | true | 0 | false | 13.38 | 8653 | 3 | 0 | 1000 |
| text | join | v2 | 10M | 0 | true | 0 | false | 16.48 | 12837 | 3 | 0 | 10000000 |
| text | join | v2noraw | 10M | 0 | true | 0 | false | 15.67 | 11701 | 3 | 0 | 10000000 |
| text | read | v2 | 10M | 0 | true | 0 | false | 5.85 | 682 | 3 | 0 | 10000000 |
| text | read | v2noraw | 10M | 0 | true | 0 | false | 5.65 | 647 | 3 | 0 | 10000000 |
| text | sort | v2 | 10M | 0 | true | 0 | false | 17.57 | 10794 | 3 | 0 | 10000000 |
| text | sort | v2noraw | 10M | 0 | true | 0 | false | 16.61 | 10297 | 3 | 0 | 10000000 |

### Auslagern unter einem Budget

Sortieren und Gruppieren über 10 Mio. Zeilen mit einer Obergrenze des Laufs von 256 MiB. Mit Ziel
bleibt der Prozess bei 1,6 bis 1,9 GiB, dem 6,6- bis 7,4-Fachen der Obergrenze ([G67](../../../docs/explanation/design/v2/70-gap-ledger.md#g67-buchfuhrung-je-quellzeile-liegt-auerhalb-des-budgets)). Ohne Ziel hält
die Ergebnistabelle die sortierten Zeilen samt Rohzustand im Speicher.

| Art | Fall | Impl | Zeilen | Block | Ziel | Budget MiB | GOMEMLIMIT | s | MiB | Wdh. | Fehlgeschlagen | Ergebnis |
|---|---|---|---:|---:|---|---:|---|---:|---:|---:|---:|---|
| num | groupby | v2 | 10M | 0 | false | 256 | false | 16.80 | 1682 | 3 | 0 | 1000 |
| num | groupby | v2 | 10M | 0 | true | 256 | false | 17.31 | 1838 | 3 | 0 | 1000 |
| num | groupby | v2noraw | 10M | 0 | false | 256 | false | 17.85 | 2041 | 3 | 0 | 1000 |
| num | groupby | v2noraw | 10M | 0 | true | 256 | false | 17.52 | 1870 | 3 | 0 | 1000 |
| num | sort | v2 | 10M | 0 | false | 256 | false | 37.24 | 6436 | 3 | 0 | 10000000 |
| num | sort | v2 | 10M | 0 | true | 256 | false | 32.73 | 1885 | 3 | 0 | 10000000 |
| num | sort | v2noraw | 10M | 0 | false | 256 | false | 30.95 | 4662 | 3 | 0 | 10000000 |
| num | sort | v2noraw | 10M | 0 | true | 256 | false | 18.10 | 1904 | 3 | 0 | 10000000 |
| text | groupby | v2 | 10M | 0 | false | 256 | false | 19.98 | 1612 | 3 | 0 | 1000 |
| text | groupby | v2 | 10M | 0 | true | 256 | false | 19.75 | 1783 | 3 | 0 | 1000 |
| text | groupby | v2noraw | 10M | 0 | false | 256 | false | 20.05 | 1811 | 3 | 0 | 1000 |
| text | groupby | v2noraw | 10M | 0 | true | 256 | false | 20.17 | 1685 | 3 | 0 | 1000 |
| text | sort | v2 | 10M | 0 | false | 256 | false | 24.12 | 8838 | 3 | 0 | 10000000 |
| text | sort | v2 | 10M | 0 | true | 256 | false | 21.86 | 1769 | 3 | 0 | 10000000 |
| text | sort | v2noraw | 10M | 0 | false | 256 | false | 19.48 | 5899 | 3 | 0 | 10000000 |
| text | sort | v2noraw | 10M | 0 | true | 256 | false | 20.75 | 1770 | 3 | 0 | 10000000 |

### Excel lesen

Numerische Zellen, acht Spalten. Mit Ziel bleibt das Lesen klein.

| Art | Fall | Impl | Zeilen | Block | Ziel | Budget MiB | GOMEMLIMIT | s | MiB | Wdh. | Fehlgeschlagen | Ergebnis |
|---|---|---|---:|---:|---|---:|---|---:|---:|---:|---:|---|
| num | excel | v2 | 100000 | 0 | false | 0 | false | 1.59 | 86 | 3 | 0 | 100000 |
| num | excel | v2 | 100000 | 0 | true | 0 | false | 1.59 | 32 | 3 | 0 | 100000 |
| num | excel | v2 | 1M | 0 | false | 0 | false | 16.06 | 695 | 3 | 0 | 1000000 |
| num | excel | v2 | 1M | 0 | true | 0 | false | 15.43 | 136 | 3 | 0 | 1000000 |

### Blocklänge ([D106](../../../docs/explanation/design/v2/10-design-decisions.md#d106-die-blocklange-ist-standardmaig-16384-zeilen-0-wahlt-sie))

10 Mio. zahlenlastige Zeilen, Ergebnis im Speicher.

| Art | Fall | Impl | Zeilen | Block | Ziel | Budget MiB | GOMEMLIMIT | s | MiB | Wdh. | Fehlgeschlagen | Ergebnis |
|---|---|---|---:|---:|---|---:|---|---:|---:|---:|---:|---|
| num | cast | v2 | 10M | 1024 | false | 0 | false | 7.36 | 5028 | 3 | 0 | 10000000 |
| num | cast | v2 | 10M | 16384 | false | 0 | false | 7.14 | 4829 | 3 | 0 | 10000000 |
| num | cast | v2 | 10M | 262144 | false | 0 | false | 7.10 | 4813 | 3 | 0 | 10000000 |
| num | cast | v2 | 10M | 4096 | false | 0 | false | 7.17 | 4655 | 3 | 0 | 10000000 |
| num | cast | v2 | 10M | 65536 | false | 0 | false | 7.12 | 4989 | 3 | 0 | 10000000 |
| num | filter | v2 | 10M | 1024 | false | 0 | false | 6.16 | 4843 | 3 | 0 | 4998744 |
| num | filter | v2 | 10M | 16384 | false | 0 | false | 5.92 | 4665 | 3 | 0 | 4998744 |
| num | filter | v2 | 10M | 262144 | false | 0 | false | 5.92 | 4649 | 3 | 0 | 4998744 |
| num | filter | v2 | 10M | 4096 | false | 0 | false | 5.96 | 4526 | 3 | 0 | 4998744 |
| num | filter | v2 | 10M | 65536 | false | 0 | false | 5.94 | 4686 | 3 | 0 | 4998744 |
| num | groupby | v2 | 10M | 1024 | false | 0 | false | 8.74 | 8003 | 3 | 0 | 1000 |
| num | groupby | v2 | 10M | 16384 | false | 0 | false | 9.53 | 7845 | 3 | 0 | 1000 |
| num | groupby | v2 | 10M | 262144 | false | 0 | false | 8.49 | 7545 | 3 | 0 | 1000 |
| num | groupby | v2 | 10M | 4096 | false | 0 | false | 9.96 | 7811 | 3 | 0 | 1000 |
| num | groupby | v2 | 10M | 65536 | false | 0 | false | 8.46 | 7522 | 3 | 0 | 1000 |
| num | read | v2 | 10M | 1024 | false | 0 | false | 4.27 | 4277 | 3 | 0 | 10000000 |
| num | read | v2 | 10M | 16384 | false | 0 | false | 4.24 | 4048 | 3 | 0 | 10000000 |
| num | read | v2 | 10M | 262144 | false | 0 | false | 4.20 | 3908 | 3 | 0 | 10000000 |
| num | read | v2 | 10M | 4096 | false | 0 | false | 4.26 | 3863 | 3 | 0 | 10000000 |
| num | read | v2 | 10M | 65536 | false | 0 | false | 4.34 | 4176 | 3 | 0 | 10000000 |
| num | sort | v2 | 10M | 1024 | false | 0 | false | 15.65 | 9045 | 3 | 0 | 10000000 |
| num | sort | v2 | 10M | 16384 | false | 0 | false | 15.54 | 8552 | 3 | 0 | 10000000 |
| num | sort | v2 | 10M | 262144 | false | 0 | false | 15.53 | 9381 | 3 | 0 | 10000000 |
| num | sort | v2 | 10M | 4096 | false | 0 | false | 15.46 | 9066 | 3 | 0 | 10000000 |
| num | sort | v2 | 10M | 65536 | false | 0 | false | 14.85 | 8952 | 3 | 0 | 10000000 |

## Budget und GOMEMLIMIT in Docker ([D107](../../../docs/explanation/design/v2/10-design-decisions.md#d107-der-anteil-des-speichers-fur-das-budget-ist-ein-zehntel-des-erkannten-limits))

Sortieren von 10 Mio. zahlenlastigen Zeilen in ein Ziel, das die Zeilen verwirft
(`bench/docker.sh <dir> sort`). Budget „25 %“ ist die vorläufige Voreinstellung aus [D101](../../../docs/explanation/design/v2/10-design-decisions.md#d101-das-budget-gilt-je-prozess-ein-lauf-kann-darin-eine-eigene-obergrenze-haben-und-die-voreinstellung-ist-vorlaufig-ein-viertel-des-erkannten-limits), mit der diese
Reihe lief; „GOMEMLIMIT“ heißt, die Engine setzt es nach [D102](../../../docs/explanation/design/v2/10-design-decisions.md#d102-auf-wunsch-setzt-die-engine-gomemlimit-auf-90-des-erkannten-limits-wenn-es-noch-nicht-gesetzt-ist) auf 90 % des Limits. „Abbruch“
heißt, das System hat den Prozess am Limit beendet (OOM, Exit durch Signal).

| Limit | Rohzustand | Budget | GOMEMLIMIT | Ergebnis | Zeit s | Spitze MiB |
|---|---|---:|---|---|---:|---:|
| 1g | ja | 25 % (256 MiB) | nein | Abbruch | 4,1 | 992 |
| 1g | ja | 25 % (256 MiB) | ja | ok | 129,1 | 982 |
| 1g | nein | 25 % (256 MiB) | nein | Abbruch | 32,1 | 1016 |
| 1g | nein | 25 % (256 MiB) | ja | ok | 71,9 | 989 |
| 1g | ja | 10 % (102 MiB) | nein | Abbruch | 25,4 | 866 |
| 1g | ja | 12,5 % (128 MiB) | nein | Abbruch | 45,9 | 945 |
| 2g | ja | 25 % (512 MiB) | nein | Abbruch | 57,8 | 2035 |
| 2g | ja | 25 % (512 MiB) | ja | ok | 44,3 | 1825 |
| 2g | nein | 25 % (512 MiB) | nein | Abbruch | 22,2 | 2036 |
| 2g | nein | 25 % (512 MiB) | ja | ok | 26,9 | 1839 |
| 2g | ja | 3 % (64 MiB) | nein | ok | 128,9 | 878 |
| 2g | ja | 10 % (205 MiB) | nein | ok | 27,7 | 1411 |
| 2g | ja | 12,5 % (256 MiB) | nein | ok | 28,9 | 1455 |
| 4g | ja | 25 % (1 GiB) | nein | Abbruch | 29,3 | 4020 |
| 4g | ja | 25 % (1 GiB) | ja | ok | 24,3 | 3683 |
| 4g | nein | 25 % (1 GiB) | nein | Abbruch | 9,9 | 3922 |
| 4g | nein | 25 % (1 GiB) | ja | ok | 19,8 | 3676 |
| 4g | ja | 1,6 % (64 MiB) | nein | ok | 103,3 | 884 |
| 4g | ja | 6,25 % (256 MiB) | nein | ok | 22,1 | 1774 |
| 4g | ja | 10 % (410 MiB) | nein | ok | 22,2 | 2091 |

Ohne `GOMEMLIMIT` wächst der Heap nach der Regel der Go-Runtime auf ein Mehrfaches des Lebenden,
und mit 25 % reicht das in keinem Limit. Mit 10 % laufen 2g und 4g durch, mit einer Spitze von 69 %
und 51 % des Limits. Kleinere Budgets tragen auch, lagern aber so oft aus, dass der Lauf vier- bis
fünfmal länger dauert. Mit `GOMEMLIMIT` tragen auch 25 %. Weil [D65](../../../docs/explanation/design/v2/10-design-decisions.md#d65-gomemlimit-setzt-die-engine-nur-auf-wunsch-und-das-budget-gilt-je-prozess) `GOMEMLIMIT` nur auf Wunsch setzt,
muss die Voreinstellung ohne es tragen, daher 10 % ([D107](../../../docs/explanation/design/v2/10-design-decisions.md#d107-der-anteil-des-speichers-fur-das-budget-ist-ein-zehntel-des-erkannten-limits)). Bei 1g reicht kein Anteil ohne `GOMEMLIMIT`:
schon die Buchführung von 10 Mio. Quellzeilen füllt das Limit ([G67](../../../docs/explanation/design/v2/70-gap-ledger.md#g67-buchfuhrung-je-quellzeile-liegt-auerhalb-des-budgets)).

## [T23](../../../docs/explanation/design/v2/30-test-plan.md#t23-ein-lauf-uber-mehr-daten-als-das-budget-halt-das-budget-ein): 1BRC in Docker

`examples/onebrc_budget` (Gruppieren mit Minimum, Mittelwert und Maximum, dann Sortieren) über die
1BRC-Datei `~/1brc/golang/measurements.txt` (13,8 GB, 1 Mrd. Zeilen, schreibgeschützt eingebunden,
nie im Repo), mit `GOMEMLIMIT` durch die Engine und dem Budget nach [D107](../../../docs/explanation/design/v2/10-design-decisions.md#d107-der-anteil-des-speichers-fur-das-budget-ist-ein-zehntel-des-erkannten-limits)
(`bench/docker.sh <dir> onebrc <datei>`). Die Ausschnitte sind die ersten 20, 50 und 100 Mio. Zeilen.

| Lieferung | Limit | Budget | Ergebnis | Zeit s | Spitze MiB |
|---|---|---:|---|---:|---:|
| volle Datei | 1g | 102 MiB | Abbruch | 13,9 | 915 |
| volle Datei | 2g | 205 MiB | Abbruch | 33,3 | 2030 |
| volle Datei | 4g | 410 MiB | Abbruch | 66,2 | 4070 |
| 20 Mio. Zeilen | 1g | 102 MiB | Abbruch | 32,2 | 1011 |
| 20 Mio. Zeilen | 2g | 205 MiB | ok | 41,1 | 1844 |
| 20 Mio. Zeilen | 4g | 410 MiB | ok | 26,4 | 2963 |
| 50 Mio. Zeilen | 1g | 102 MiB | Abbruch | 21,2 | 919 |
| 50 Mio. Zeilen | 2g | 205 MiB | Abbruch | 32,4 | 1989 |
| 50 Mio. Zeilen | 4g | 410 MiB | ok | 69,9 | 3682 |
| 100 Mio. Zeilen | 1g | 102 MiB | Abbruch | 15,3 | 927 |
| 100 Mio. Zeilen | 2g | 205 MiB | Abbruch | 32,6 | 1975 |
| 100 Mio. Zeilen | 4g | 410 MiB | Abbruch | 73,2 | 3953 |

Die Tabelle bestätigt [G67](../../../docs/explanation/design/v2/70-gap-ledger.md#g67-buchfuhrung-je-quellzeile-liegt-auerhalb-des-budgets), das Ziel von #66: 20 Mio. Zeilen laufen bei 2g und 4g durch, 50 Mio. nur bei
4g, 100 Mio. und die volle Datei enden bei jedem Limit durch das System. Die Engine lagert rechtzeitig
aus; was wächst, ist die Buchführung je Quellzeile außerhalb des Budgets (Zählung je Zeile, Zeile und
Offset der Fundstelle, Kennungen der Quellzeilen je Gruppe). Die Zeit bis zum Abbruch wächst ungefähr
mit dem Limit. Nach #66 werden diese Läufe wiederholt. Der Unit-Test
`TestSortAndGroupByOverMoreDataThanTheBudgetKeepTheBudget` beweist [T23](../../../docs/explanation/design/v2/30-test-plan.md#t23-ein-lauf-uber-mehr-daten-als-das-budget-halt-das-budget-ein) im Umfang von [P1](../../../docs/explanation/design/v2/40-scope-prototype.md#p1-auslagern-nur-fur-sortieren-und-gruppieren) an der Zählung der
Engine: Die Spitze liegt bei 108 % des Budgets, das Ergebnis gleicht dem Lauf im Speicher.

Ein früher Lauf über 20 Mio. Zeilen bei 1g lief in 85 s durch; er hatte noch kein `--memory-swap` und
durfte Swap nutzen. Er zählt nicht.

### Die 1BRC-Datei ist beschädigt

Die Datei selbst enthält kaputte Zeilen. Schon die ersten 20 Mio. Zeilen haben 9057 Zeilen (rund
0,045 %), die nicht `name;-?Ziffern.Ziffer` entsprechen:
`head -n 20000000 measurements.txt | grep -cvE '^[^;]+;-?[0-9]+\.[0-9]$'`. Beispiele sind zwei ineinander
geschobene Zeilen (`Kankan;40Flores,  Petén;33.9`), abgeschnittene Zeilen (`Kuop.0`) und Stationsnamen
aus Stücken anderer (`Ho Chi Minh CMexicali;28.1`, Zeile 16199224; `Ho Chi Minh City;318.8`, Zeile
14698223). Die erste kaputte Zeile ist Zeile 8699036; die Zeilen davor sind sauber.

v2 geht damit um wie entworfen: Der Lauf über 20 Mio. Zeilen sortiert 7529 Zeilen aus, die sich nicht
in zwei Felder zerlegen lassen (`unparseable_line`), und 554, deren Wert keine Zahl ist (`parse`),
gruppiert die übrigen so, wie sie in der Datei stehen, und endet mit Status `ok`. Daher kommen die
seltsamen Stationsnamen im Ergebnis; ein Fehler des Readers ist es nicht.

## Ursachen, soweit gemessen

- Der Rohzustand erklärt den Abstand zu v1 nicht: Ohne ihn ändern sich Laufzeit und Speicher außer
  beim Filtern kaum.
- Lesen ist in v2 um das 1,55- bis 1,8-Fache langsamer als in v1, und jeder Fall trägt diesen Abstand.
  v1 liest mit `encoding/csv` `ReadAll` in einem Stück; v2 baut je Zeile Fundstelle, Zählung und
  Herkunft auf ([D10](../../../docs/explanation/design/v2/10-design-decisions.md#d10-rohzustand-bedeutet-gelesene-zellwerte-rohbytes-bei-unzerlegbaren-zeilen-und-immer-die-fundstelle), [D43](../../../docs/explanation/design/v2/10-design-decisions.md#d43-gezahlt-werden-quellzeilen-in-vier-kategorien-und-der-rohzustand-wird-bei-jedem-verlassen-des-plans-freigegeben), [D84](../../../docs/explanation/design/v2/10-design-decisions.md#d84-eine-quellzeile-zahlt-einmal-aussortiert-vor-durchgelaufen-vor-verworfen-und-nach-einem-abbruch-gibt-es-nicht-verarbeitete-zeilen)).
- Gruppieren sammelt alle Zeilen, bevor es rechnet, auch für Summe, Anzahl, Minimum und Maximum
  ([G68](../../../docs/explanation/design/v2/70-gap-ledger.md#g68-beim-ausgelagerten-gruppieren-muss-jede-einzelne-gruppe-in-den-speicher-passen)). Das kostet beim Gruppieren das Doppelte der Zeit und des Speichers von v1.
- Sortieren und Join bauen die Ergebnisblöcke neu auf, während Eingabe, Rohzustand und Herkunft je
  Zeile weiter leben. v1 sortiert Zeiger auf Zeilen.
- Außerhalb des Budgets wächst die Buchführung je Quellzeile mit der Lieferung ([G67](../../../docs/explanation/design/v2/70-gap-ledger.md#g67-buchfuhrung-je-quellzeile-liegt-auerhalb-des-budgets)). Das ist
  der Grund, warum die 1BRC-Datei in keinem Limit durchläuft.

## Nachmessen

```sh
cd experimental/v2
go build -o /tmp/bench ./bench
/tmp/bench suite -dir /tmp/bench-data -plan block -rows 10000000 -out block.jsonl
/tmp/bench suite -dir /tmp/bench-data -plan compare -out compare.jsonl
/tmp/bench suite -dir /tmp/bench-data -plan sink -rows 10000000 -out sink.jsonl
/tmp/bench suite -dir /tmp/bench-data -plan spill -rows 10000000 -out spill.jsonl
/tmp/bench suite -dir /tmp/bench-data -plan excel -rows 100000,1000000 -out excel.jsonl
/tmp/bench gen -dir /tmp/data -kind num -rows 10000000
bench/docker.sh /tmp sort       # liest /tmp/data
bench/docker.sh /tmp onebrc ~/1brc/golang/measurements.txt
/tmp/bench report compare.jsonl block.jsonl sink.jsonl spill.jsonl excel.jsonl
```
