# Slowly Changing Dimensions (SCD)

## Current decision

SCD processing is **not implemented in this project at present**.

The current Olist source is being treated as the available dataset snapshot. The project has not established a business requirement or source-history mechanism that requires maintaining historical versions of dimension attributes. The Gold star schema can be built from the validated Silver tables without introducing SCD processing.

The notebooks in this folder are intentionally retained as documented placeholders. They are not production transformation steps and must not be added to a workflow as if they were implemented.

## Type 1

Use Type 1 only when a documented requirement says that selected dimension attributes should be overwritten with their latest values and prior values do not need to be queried.

Before implementation, define the target dimension and business key, the attributes eligible for overwrite, the expected behavior for new and existing keys, and how reruns remain idempotent.

See `NB_04_SCD_Type1.sql`.

## Type 2

Use Type 2 only when the business needs to query historical attribute values as they were valid at a point in time, and the source provides enough reliable change information to detect those changes.

Before implementation, define the dimension and business key, historically tracked attributes, change-detection strategy, effective start/end timestamps, current-row indicator, and handling for late-arriving changes, duplicate input, and reruns.

See `NB_05_SCD_Type2.sql`.

## Reconsideration criteria

Revisit SCD when a concrete analytics or operational requirement needs historical dimension state, or when a source/change-data feed becomes available that can reliably support it.

Do not add SCD solely because it is a standard data-warehouse pattern. Keep the design aligned with demonstrated requirements.

## Current pipeline status

- Bronze: ingested source data into Delta.
- Silver: cleaned and validated current-state entities.
- Gold: star-schema modelling is the next planned stage.
- SCD: deferred; neither Type 1 nor Type 2 is currently implemented.
