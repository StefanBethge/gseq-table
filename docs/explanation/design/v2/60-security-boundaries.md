# Security Boundaries

gseq-table ist eine Library und kein Dienst. Sie hat keine eigenen Benutzer, Rechte oder
Netzwerk-Endpunkte. Die Grenzen betreffen die Daten, die sie liest, auslagert und schreibt.
Lieferungen kommen von externen Anbietern und sind nicht vertrauenswürdig.

| Grenze | Status | Bemerkung |
|---|---|---|
| Übergroße oder manipulierte Lieferungen (Zip-Bombe in Excel, einzelne riesige Felder) | not-provided im Prototyp | Das Budget aus [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern) gilt für Blöcke, nicht für einzelne Werte oder das Entpacken eines Archivs. Grenzen für Feldgröße und entpackte Größe sind offen ([G20](70-gap-ledger.md#g20-grenzen-fur-feldlange-und-entpackten-umfang)) |
| Ausgelagerte Daten enthalten Lieferungsinhalte | enforced | Das Verzeichnis zum Auslagern wird nach dem Lauf geleert ([D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern)). Die Dateien sollen nur für den ausführenden Nutzer lesbar sein; das ist als Anforderung festgehalten ([G20](70-gap-ledger.md#g20-grenzen-fur-feldlange-und-entpackten-umfang)) |
| Aussortierte Zeilen enthalten Rohdaten, gegebenenfalls personenbezogene | not-provided | Wohin sie geschrieben werden und wie lange sie liegen, entscheidet die Pipeline ([D2](10-design-decisions.md#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse), [D17](10-design-decisions.md#d17-die-library-bewahrt-aussortierte-zeilen-nicht-selbst-auf)) |
| Formel-Einschleusung beim Schreiben (Werte wie `=HYPERLINK(...)` in CSV/Excel) | enforced für Excel, not-provided für CSV | Der Excel-Writer schreibt Formeln als Text wie in v1.3. Für CSV ist Maskierung offen ([G20](70-gap-ledger.md#g20-grenzen-fur-feldlange-und-entpackten-umfang)) |
| Zugangsdaten für Datenbank und HTTP | inherited | Kommen aus `database/sql` bzw. dem übergebenen `http.Client`; die Library speichert und protokolliert sie nicht |
