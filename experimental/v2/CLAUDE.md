# gseq-table v2 (Prototyp)

Prototyp von gseq-table v2, einer ETL-Library für stabile Läufe über schmutzige
Lieferungen, für Pipeline-Entwickler in Go. Ohne Stabilitätszusage
([D34](../../docs/explanation/design/v2/10-design-decisions.md#d34-der-prototyp-liegt-unter-experimentalv2-ohne-zusage-v200-ist-ein-eigenes-modul-mit-semver)).

## Befehle

- `mise run test` im Wurzelverzeichnis des Repos: alle Tests aller Module inklusive Docs-Gates
- `go test ./...` in `experimental/v2`: Tests dieses Moduls inklusive Docs-Gates
- `go vet ./...` in `experimental/v2`

## Design-Stand

Autoritativ ist [docs/explanation/design/v2](../../docs/explanation/design/v2/index.md) (Profil: voll, Methode nach
RFC-0009). Der Code setzt um, was dort entschieden ist; was der Prototyp enthält, steht im
[Scope](../../docs/explanation/design/v2/40-scope-prototype.md).

- Baue nichts, was keine Decision deckt. Fehlt eine, halt an: frag nach oder leg einen G-Eintrag an.
- Neue Anforderung: Use Case anhängen → Decision(s) → T-Fall → testgetrieben implementieren. Die drei Artefakte zeigen aufeinander.
- IDs sind append-only (Freeze: siehe [Index](../../docs/explanation/design/v2/index.md)). Nie umnummerieren, nie löschen. Decisions werden ersetzt oder per Amendment mit G-Zitat ergänzt; Verworfenes bleibt mit `!!! failure`-Marker stehen.
- Jede ID-Erwähnung ist ein Link. PR-Beschreibungen zitieren die IDs, die sie umsetzen.
- Tests, die einen T-Fall beweisen, rufen `testutil.Proves(t, "T<n>")` auf und stehen in der Tabelle [Welcher Test beweist welchen Fall](../../docs/explanation/design/v2/30-test-plan.md#welcher-test-beweist-welchen-fall) im Test-Plan.
- Neue IDs heben im selben Commit `familyFloors` in `internal/testutil/docsgate_test.go` und die Bereichsangaben im Index an.
- Was offen ist, steht im [Gap Ledger](../../docs/explanation/design/v2/70-gap-ledger.md), nicht in dieser Datei.

## Konventionen

- Eigenes Go-Modul, unabhängig von den v1-Modulen ([D66](../../docs/explanation/design/v2/10-design-decisions.md#d66-der-prototyp-ist-ein-eigenes-go-modul-unter-experimentalv2)). v1-Pakete werden für v2 nicht geändert.
- Kernpaket `gtable` im Modul-Wurzelverzeichnis, Formatpakete `csv` und `excel`, Engine-Interna und Test-Helfer unter `internal/` ([D67](../../docs/explanation/design/v2/10-design-decisions.md#d67-ein-kernpaket-gtable-formatpakete-fur-csv-und-excel-engine-interna-unter-internal)).
- Die öffentliche API enthält keine gseq-Typen ([D33](../../docs/explanation/design/v2/10-design-decisions.md#d33-die-offentliche-api-verwendet-standard-go-typen-und-eigene-typen-der-library-keine-gseq-typen)).
- Task-Runner ist mise ([D68](../../docs/explanation/design/v2/10-design-decisions.md#d68-mise-ist-der-task-runner-und-mise-run-test-fuhrt-alle-tests-aller-module-aus)); CI ruft `go` direkt auf.
