# Failure Modes

Was bei einem Lauf schiefgehen kann, wie es erkannt wird und wie die Library reagiert. Die
Zeilen haben keine IDs und werden in Prosa zitiert ("die Zeile Platte voll").
Datenfehler in Lieferungen sind hier nicht aufgeführt, das ist der Normalfall des
Fehlermodells ([D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler)).

## Reader

| Failure | Detection | Response |
|---|---|---|
| Datei fehlt oder ist nicht lesbar | Fehler beim Öffnen | Lieferfehler nach [D19](10-design-decisions.md#d19-es-gibt-drei-fehlerarten-planfehler-lieferfehler-und-datenfehler); eine fehlende Datei hat keine Zeilen zum Aussortieren und stoppt den Lauf mit `aborted` |
| Datei bricht mitten im Lesen ab (abgeschnittene Übertragung) | Lesefehler oder unvollständige letzte Zeile | Bis dahin gelesene Zeilen laufen durch; die unvollständige Zeile wird mit `unparseable_line` aussortiert; der Abbruch erscheint im Änderungsbericht ([D23](10-design-decisions.md#d23-das-laufergebnis-enthalt-einen-anderungsbericht)) |
| Excel-Datei ist beschädigt oder kein gültiges Archiv | Fehler beim Öffnen des Archivs | Lieferfehler, der Lauf stoppt |

## Engine

| Failure | Detection | Response |
|---|---|---|
| Platte voll beim Auslagern | Schreibfehler im Verzeichnis zum Auslagern | Lauf endet mit `aborted`, ausgelagerte Daten werden entfernt ([D28](10-design-decisions.md#d28-ein-lauf-hat-ein-speicherbudget-und-ein-verzeichnis-zum-auslagern)) |
| Speicher reicht trotz Budget nicht (Budget zu nah am Container-Limit, einzelne riesige Werte) | Das System beendet den Prozess | Nicht abfangbar. Ausgelagerte Daten bleiben zurück, bis der nächste Lauf sie entfernt ([G17](70-gap-ledger.md#g17-aufraumen-nach-einem-vom-system-beendeten-lauf)) |
| Lauf wird über den Kontext abgebrochen | Kontext meldet Abbruch | Lauf endet mit `aborted`; bisher geschriebene Daten bleiben im Ziel; Zählungen und aussortierte Zeilen gelten bis zum Abbruch |
| Closure gerät in Panik | `recover` im Schritt | Die Zeile wird mit `code=custom` aussortiert, der Lauf geht weiter (Annahme, [G18](70-gap-ledger.md#g18-verhalten-bei-panik-in-einer-closure)) |

## Ziele

| Failure | Detection | Response |
|---|---|---|
| Ziel nimmt einen Block nicht an (Datenbank-Constraint, HTTP 4xx) | Fehler vom Sink | Offen, ob die Zeilen des Blocks aussortiert werden oder der Lauf stoppt ([G19](70-gap-ledger.md#g19-fehler-beim-schreiben-ins-ziel)) |
| Ziel ist vorübergehend nicht erreichbar | Fehler vom Sink | Wiederholung beim HTTP-Writer ([D35](10-design-decisions.md#d35-ziele-werden-uber-eine-sink-schnittstelle-beschrieben-mit-datei-writern-und-kleinen-paketen-fur-datenbank-und-http)), Zusage offen ([G15](70-gap-ledger.md#g15-zustellzusage-des-http-writers)) |
| Schreiben der aussortierten Zeilen scheitert | Fehler vom Sink der aussortierten Zeilen | Offen ([G19](70-gap-ledger.md#g19-fehler-beim-schreiben-ins-ziel)); ohne Behandlung gingen genau die Daten verloren, für die das Fehlermodell da ist |
