# Tankstellen-Monitor Leun – GitHub Pages Version

Diese Version läuft **ohne eigenes Backend** auf GitHub Pages und aktualisiert die Daten **alle 30 Minuten** über GitHub Actions.

## Warum diese Variante?

- Kein laufendes MacBook nötig.
- Kein Terminal offen nötig.
- Keine Portfreigabe und keine lokale IP nötig.
- Für diesen Anwendungsfall sehr gut mit GitHub Pages + GitHub Actions geeignet. GitHub dokumentiert kostenlose Nutzung von Actions auf öffentlichen Repositories auf Standard-Runnern; bei privaten Repositories gelten Freiminuten. [web:154][web:151]

GitHub Actions unterstützt Zeitpläne bis herunter zu 5 Minuten, daher ist ein halbstündlicher Zeitplan problemlos möglich. [web:162]

Tankerkönig verlangt, dass Abfragen nicht öfter als alle 5 Minuten erfolgen; halbstündlich ist also unkritisch. [web:1]

## Enthalten

- `index.html` – die statische Web-App
- `fetch_data.py` – holt halbstündlich Daten von Tankerkönig
- `.github/workflows/update-fuel-data.yml` – GitHub-Actions-Workflow
- `data/*.json` – automatisch erzeugte Datendateien

## GitHub-Einrichtung

### 1. Neues Repository erstellen

Erstellen Sie auf GitHub ein neues Repository, z. B. `tankstellen-monitor-leun`.

### 2. Dateien hochladen

Laden Sie den kompletten Inhalt dieses Pakets in das Repository hoch.

### 3. GitHub Secret anlegen

Unter **Settings → Secrets and variables → Actions → New repository secret**:

- Name: `TANKERKOENIG_API_KEY`
- Value: Ihr echter Tankerkönig-API-Key

### 4. Repository Variables anlegen

Unter **Settings → Secrets and variables → Actions → Variables** diese Variablen anlegen:

- `LOCATION_LAT` → Ihre korrekte Breite
- `LOCATION_LNG` → Ihre korrekte Länge
- `LOCATION_NAME` → `Justengarten 4A, 35638 Leun`
- `SEARCH_RADIUS_KM` → `16`

### 5. GitHub Pages aktivieren

Unter **Settings → Pages**:

- Source: **Deploy from a branch**
- Branch: **main**
- Folder: **/(root)**

Danach ist die Seite unter einer GitHub-Pages-URL erreichbar.

### 6. Ersten Datenlauf manuell starten

Unter **Actions → Update fuel data → Run workflow**.

Danach sollten die JSON-Dateien in `data/` erzeugt werden.

## Zeitplan

Der Workflow läuft mit:

```yaml
cron: '7,37 * * * *'
```

Das bedeutet: täglich um Minute 07 und 37 jeder Stunde. Leicht versetzte Zeiten sind sinnvoll, damit man nicht genau auf volle und halbe Stunde fällt. Tankerkönig empfiehlt, Abrufe nicht unnötig aggressiv zu takten. [web:1]

## Kostenlos?

- **Öffentliches Repository:** GitHub Actions auf Standard-Runnern kostenlos. [web:154][web:151]
- **Privates Repository:** GitHub Free enthält Freiminuten, daher ist halbstündlich oft noch realistisch, solange der Workflow schlank bleibt. [web:154][web:151]

## Hinweis

Geplante GitHub-Workflows starten nicht immer sekundengenau; kleine Verzögerungen sind normal. [web:167]
