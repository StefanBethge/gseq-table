# Security Boundaries

gseq-table ist eine Library und kein Dienst. Sie hat keine eigenen Benutzer, Rechte oder
Netzwerk-Endpunkte. Die Grenzen betreffen die Daten, die sie liest, auslagert und schreibt.
Lieferungen kommen von externen Anbietern und sind nicht vertrauenswürdig.

| Grenze | Status | Bemerkung |
|---|---|---|
| Übergroße oder manipulierte Lieferungen (Zip-Bombe in Excel, einzelne riesige Felder) | enforced | Grenzen für Feldlänge, entpackten Umfang und Packverhältnis nach [D56](10-design-decisions.md#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch) |
| Ausgelagerte Daten enthalten Lieferungsinhalte | enforced | Verzeichnis nur für den ausführenden Nutzer, Leeren nach dem Lauf bzw. Schließen des Ergebnisses und Aufräumen verwaister Läufe ([D56](10-design-decisions.md#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch), [D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern), [D38](10-design-decisions.md#d38-ein-spaterer-lauf-entfernt-verwaiste-ausgelagerte-daten), [D49](10-design-decisions.md#d49-aussortierte-zeilen-umfangreicher-laufe-werden-uber-writer-im-plan-wahrend-des-laufs-geschrieben-sonst-halt-sie-das-ergebnis-bis-close)) |
| Aussortierte Zeilen enthalten Rohdaten, gegebenenfalls personenbezogene | not-provided | Wohin sie geschrieben werden und wie lange sie liegen, entscheidet die Pipeline ([D2](10-design-decisions.md#d2-aussortierte-zeilen-werden-uber-dieselben-writer-geschrieben-wie-ergebnisse), [D17](10-design-decisions.md#d17-die-library-bewahrt-aussortierte-zeilen-nicht-selbst-auf)) |
| Formel-Einschleusung beim Schreiben (Werte wie `=HYPERLINK(...)` in CSV/Excel) | enforced für Excel, auf Wunsch für CSV | Der Excel-Writer schreibt Formeln als Text wie in v1.3; der CSV-Writer maskiert auf Wunsch ([D56](10-design-decisions.md#d56-die-library-begrenzt-feldlange-und-entpackten-umfang-schutzt-ausgelagerte-dateien-und-maskiert-formeln-in-csv-auf-wunsch)) |
| Zugangsdaten für Datenbank und HTTP | inherited | Kommen aus `database/sql` bzw. dem übergebenen `http.Client`; die Library speichert und protokolliert sie nicht |
