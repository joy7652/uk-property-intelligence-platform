# Power BI

Two projects read the serving layer.

| Project | Source | Contents |
|---|---|---|
| `property_market` | `uk_property_intel.gold`, plus `quality.rule_result` | Semantic model over the Gold star; four screens in build |
| `pipeline_health` | `uk_property_intel.quality` | Run detail, metric coverage, rules and freshness |

## Project format

Each project is saved as a Power BI Project: a `.pbip` at the root, a `.Report` folder, and a `.SemanticModel` folder holding one TMDL file per table. The model definition is text, so a changed measure or a new relationship shows up in a diff.

A `.pbix` gives none of that. It is a sealed binary carrying the imported data, and `property_market` weighs 102 MB once 36.1 million fact rows are compressed into it. GitHub blocks pushes above 100 MB. Git LFS would clear that block and store a fresh 102 MB copy on every commit, against a 1 GB free allowance.

## What is not committed

`.gitignore` in this folder excludes three things:

- `**/.pbi/cache.abf` — the local data cache, and where `property_market`'s 102 MB actually sits.
- `**/.pbi/localSettings.json` — per-machine state.
- `*.pbix` — kept locally as a fallback through the conversion.

A clone therefore gets the model definition and no data.

## Opening a project

Open the `.pbip` at the project root. Power BI Desktop reads the two sibling folders from there. Opening a `.tmdl` or a `.json` directly gets you a text file.

The `.pbip` save option and TMDL both sit under File → Options and settings → Options → Preview features, on Power BI Desktop 2.157.1354.0.

## Refreshing

Refresh needs access to the Databricks SQL warehouse. Credentials are per machine and never travel with the repo.

1. Take the server hostname and HTTP path from the warehouse connection details.
2. Authenticate with client credentials, using the client ID and an OAuth secret for the `bi_readers` service principal. `databricks_src/setup/05_grant_serving_access.py` creates the grants that principal relies on.
3. Storage mode is Import throughout, so every refresh rebuilds every table.

Warm refresh of `property_market` takes about 30 seconds over 148.9 MiB. Two measurements at different sizes fit roughly 11.8 seconds of fixed overhead plus 0.12 seconds per MiB, so statement count dominates below about 96 MiB and bytes dominate above it. `pipeline_health` takes about 32 seconds over 237 KiB, which is all overhead.

## Measures

`property_market` holds every measure in `_Measures`, a table with no columns, organised into display folders. Eight of roughly twenty read two or three fact tables, so no single fact is their home.

Four measures exist to verify the model against figures `databricks_src/gold/exploration/05_verify_cross_source.py` already publishes: `Cost cells` at 45,677, `Price cells` at 124,362, and the two index-comparison ratios at 1.0107 and 1.1757. A hidden `_verify` page carries them as cards. If one of those four moves, the model and the notebook have diverged and the dashboard figures can no longer be trusted against the notebook's.
