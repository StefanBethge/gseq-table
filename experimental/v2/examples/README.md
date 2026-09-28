# Example pipelines (v2 prototype)

Runnable example pipelines that show whether the v2 API works well day to day: reading a
dirty delivery, looking at what was rejected, and running the same pipeline again. They are
written the way a pipeline developer would write them, not as tests of single functions.

## How they run

Each example is a `package main` directory in this module, with a `main_test.go` that runs
the pipeline on its own small data. The `v2` CI job runs `go test ./...` and `go vet ./...`
in `experimental/v2`, so it builds and runs every example that exists without any extra CI
configuration. The slice that makes an example runnable adds it, and that slice counts as
done only once the example runs in CI. Until then the example is listed below as planned.

## Data

Example data is small, synthetic and deliberately dirty (wrong formats, placeholders,
missing and renamed columns, broken lines), following [D60](../../../docs/explanation/design/v2/10-design-decisions.md#d60-der-prototyp-wird-mit-eigenen-beispiel-lieferungen-der-1brc-datei-und-in-docker-mit-verschiedenen-speicher-limits-erprobt). It lives in a
`testdata/` directory next to the example. No real customer data goes into the repo. Large
files such as the 1BRC measurements are never committed; the example takes their path from
an environment variable or a flag.

## Examples

API names below are sketches. The behavior follows the linked decisions.

| Example | Status | Runnable with |
|---|---|---|
| `eager_table` | runnable | slice 2 (#46) |
| `inspect_rejects` | planned | slice 4 (#48) |
| `fail_branch` | planned | slice 6 (#50) |
| `onebrc_budget` | planned | slice 7 (#51), Docker runs in slice 9 (#53) |
| `scheduled_excel` | planned | slice 8 (#52) |
| `reprocess_rejects` | planned | slice 8 (#52) |

### eager_table

A `Table` used directly, without a pipeline: operations applied one after another, the
rows the table rejected, and the sticky error that stops the chain after an unknown column.
Run it with `go run ./examples/eager_table` in `experimental/v2`. Until the CSV reader of
slice 4 (#48) exists, it reads its delivery with `encoding/csv` into raw text columns.
Design: [D31](../../../docs/explanation/design/v2/10-design-decisions.md#d31-jede-operation-gibt-es-einmal-als-wert-mit-zwei-einstiegen-sofort-auf-einer-tabelle-oder-im-plan), [D50](../../../docs/explanation/design/v2/10-design-decisions.md#d50-eine-tabelle-tragt-ihre-aussortierten-zeilen-und-einen-haftenden-fehler).

### inspect_rejects

Inspecting the rejected rows of a run: the table per source with the raw state and the
`_gseq_` info columns, and the overview with one entry per error. For an Excel source the
rows also carry `display` and `cell`.
Design: [UC3](../../../docs/explanation/design/v2/05-use-cases.md#uc3-pipeline-entwickler-untersucht-aussortierte-zeilen), [D1](../../../docs/explanation/design/v2/10-design-decisions.md#d1-aussortierte-zeilen-sind-eine-tabelle-aus-rohzustand-und-info-spalten), [D13](../../../docs/explanation/design/v2/10-design-decisions.md#d13-aussortierte-zeilen-gibt-es-je-quelle-dazu-eine-ubersicht-uber-alle-quellen), [D14](../../../docs/explanation/design/v2/10-design-decisions.md#d14-info-spalten-tragen-ein-reserviertes-einstellbares-prafix), [D44](../../../docs/explanation/design/v2/10-design-decisions.md#d44-die-tabelle-je-quelle-hat-eine-zeile-je-quellzeile-die-ubersicht-einen-eintrag-je-fehler), [D52](../../../docs/explanation/design/v2/10-design-decisions.md#d52-bei-excel-ist-der-rohzustand-der-angezeigte-zellinhalt-umgewandelt-wird-der-gespeicherte-wert), [D62](../../../docs/explanation/design/v2/10-design-decisions.md#d62-bei-excel-tragen-rohzustand-und-arbeitsspalte-den-gespeicherten-wert-in-fester-textform).

### fail_branch

An `OnFail` branch that retries a failed date cast with an alternative format. One row is
recovered and flows back into the main path. Another fails again and shows its whole path in
`step` and the main-path reason in `prev_reason`.
Design: [UC5](../../../docs/explanation/design/v2/05-use-cases.md#uc5-nicht-verarbeitbare-zeilen-laufen-im-selben-lauf-durch-einen-eigenen-zweig), [D25](../../../docs/explanation/design/v2/10-design-decisions.md#d25-gescheiterte-zeilen-eines-schritts-konnen-in-einen-zweig-gegeben-werden-und-laufen-danach-zuruck), [D26](../../../docs/explanation/design/v2/10-design-decisions.md#d26-zweige-werden-nach-spaltennamen-zusammengefuhrt-typkonflikte-sind-planfehler), [D27](../../../docs/explanation/design/v2/10-design-decisions.md#d27-eine-im-zweig-erneut-gescheiterte-zeile-behalt-ihre-kennung-und-zeigt-ihren-weg), [D46](../../../docs/explanation/design/v2/10-design-decisions.md#d46-gerettete-zeilen-behalten-ihre-geschichte-und-die-schwelle-zahlt-nur-endgultig-aussortierte).

### onebrc_budget

The 1BRC file (CSV with `;`, no header, station and measurement) grouped with
min/mean/max via `GroupByAgg` and sorted, under a memory budget, with `WithManagedMemory`
to opt in to `GOMEMLIMIT`. In CI it runs on a small generated file in the same format. The
real file is not committed; its path comes from an environment variable or a flag, and the
README of the example documents running it in Docker with `--memory=1g`, `2g` and `4g`.
Design: [UC6](../../../docs/explanation/design/v2/05-use-cases.md#uc6-eine-umfangreiche-lieferung-wird-verarbeitet-ohne-vollstandig-im-ram-zu-liegen), [D6](../../../docs/explanation/design/v2/10-design-decisions.md#d6-pipelines-sind-plane-die-in-blocken-ausgefuhrt-werden-und-auf-die-platte-auslagern-konnen), [D28](../../../docs/explanation/design/v2/10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern), [D60](../../../docs/explanation/design/v2/10-design-decisions.md#d60-der-prototyp-wird-mit-eigenen-beispiel-lieferungen-der-1brc-datei-und-in-docker-mit-verschiedenen-speicher-limits-erprobt), [D65](../../../docs/explanation/design/v2/10-design-decisions.md#d65-gomemlimit-setzt-die-engine-nur-auf-wunsch-und-das-budget-gilt-je-prozess).

### scheduled_excel

A scheduled run over an Excel delivery: an expected schema and a business key for the
source, `Cast`/`With`/`Where` steps, a threshold, a reject writer in the plan, a sink for the
results, the change report, and an exit code for cron. The prototype writes to a CSV sink;
a database sink needs the writers of [F20](../../../docs/explanation/design/v2/20-feature-catalogue.md#f20-writer-fur-datenbank-und-http), which are not part of the prototype.
Design: [UC1](../../../docs/explanation/design/v2/05-use-cases.md#uc1-geplanter-lauf-uber-eine-lieferung), [UC2](../../../docs/explanation/design/v2/05-use-cases.md#uc2-datenlieferant-andert-das-lieferformat-unangekundigt), [D18](../../../docs/explanation/design/v2/10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash), [D20](../../../docs/explanation/design/v2/10-design-decisions.md#d20-die-schwelle-ist-absolut-oder-als-anteil-je-lauf-oder-je-schritt-und-lasst-den-lauf-per-voreinstellung-zu-ende-laufen), [D21](../../../docs/explanation/design/v2/10-design-decisions.md#d21-ein-lauf-liefert-einen-status-und-zahlungen-aus-denen-sich-ein-exit-code-ableiten-lasst), [D22](../../../docs/explanation/design/v2/10-design-decisions.md#d22-eine-quelle-kann-einen-erwarteten-aufbau-haben-gegen-den-die-lieferung-beim-lesen-gepruft-wird), [D23](../../../docs/explanation/design/v2/10-design-decisions.md#d23-das-laufergebnis-enthalt-einen-anderungsbericht), [D42](../../../docs/explanation/design/v2/10-design-decisions.md#d42-jeder-lieferfehler-setzt-den-status-delivery_error), [D45](../../../docs/explanation/design/v2/10-design-decisions.md#d45-die-schwelle-bezieht-anteile-auf-bisher-gelesene-zeilen-gilt-bei-einer-der-grenzen-und-bricht-erst-nach-einer-mindestzahl-ab), [D49](../../../docs/explanation/design/v2/10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close), [D63](../../../docs/explanation/design/v2/10-design-decisions.md#d63-es-gilt-der-hochste-zutreffende-status-und-das-ergebnis-nennt-alle-befunde).

### reprocess_rejects

Reprocessing the rejected rows of an earlier run after fixing the pipeline:
`ReplaceSource` with `FromRejects`, stable `record_key` and `row_key`, and an upsert on
`row_key` that leaves no duplicates in the target. Because the prototype has no database
writer ([F20](../../../docs/explanation/design/v2/20-feature-catalogue.md#f20-writer-fur-datenbank-und-http)), the example upserts into a small sink it defines itself on the `Sink`
interface of [D35](../../../docs/explanation/design/v2/10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http).
Design: [UC4](../../../docs/explanation/design/v2/05-use-cases.md#uc4-aussortierte-zeilen-werden-nach-einer-anpassung-nachverarbeitet), [D16](../../../docs/explanation/design/v2/10-design-decisions.md#d16-aussortierte-zeilen-konnen-quelle-eines-laufs-sein-und-behalten-ihre-ursprungliche-fundstelle), [D18](../../../docs/explanation/design/v2/10-design-decisions.md#d18-jede-quellzeile-tragt-einen-stabilen-schlussel-optional-einen-inhalts-hash), [D47](../../../docs/explanation/design/v2/10-design-decisions.md#d47-ergebniszeilen-tragen-einen-row_key-aus-ihren-quellzeilen-und-bei-1n-joins-scheitern-nur-die-betroffenen-ergebniszeilen), [D61](../../../docs/explanation/design/v2/10-design-decisions.md#d61-die-kennung-einer-lieferung-ist-ein-fingerabdruck-der-beim-offnen-feststeht).
