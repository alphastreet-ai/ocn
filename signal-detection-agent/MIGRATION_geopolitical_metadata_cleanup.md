# Migration — drop removed metadata keys from `geopolitical_signal` rows

One-off data cleanup. **Run once per environment at deploy time** (staging, then
production). Not a schema change — none of these was ever a column, only a key inside
`agent_classifications.metadata` (JSONB).

## Why this is needed

Stage C writes its tags with a JSONB *merge*:

```sql
SET metadata = metadata || (%s::jsonb) || jsonb_build_object('stage_c_classified_at', NOW())
```

A merge adds and overwrites keys but never removes one. So a row tagged before this
release keeps every key the old code wrote, indefinitely, even after a re-tag. The code
change alone does not clean existing rows.

## Keys removed, and what replaced each

| Key | Replaced by | Why |
|---|---|---|
| `one_line` | `summary` | A 20-word restatement of the headline. `summary` carries the document's filed abstract verbatim, or a generated 3-5 sentence summary from the body. |
| `actors` | `entities_json` (`type: "country"`) | Free text with no vocabulary — it stored `"European states"`, which names no state. Countries are now entities, validated against the article text and carrying a reason. |
| `impacted_companies_direct` | `entities_json` | Matched the HEADLINE only, so it was null on every geopolitical row ever stored — these headlines name countries and institutions, while companies appear in the body. |
| `impacted_companies_by_category` | `entities_json` | Expanded a matched category into every ticker in that sector. A UK airbase story naming no company carried 11 defence tickers; an EU sanctions row carried 25 of 123, alphabetically. Not one could be explained. |
| `impacted_companies_by_category_total` | — | The "25 of 123" counter for the list above. |
| `drop_reason` | `signal_reason` (column) | A second reason field holding canned text keyed off the label, beside a real column holding the model's own sentence. |

`entities_json` is a real column, not a metadata key, and holds only entities the article
itself names — each with a reason quoting the text and a ticker where one exists.

## The statement

```sql
UPDATE agent_classifications
SET metadata = metadata
             - 'one_line'
             - 'actors'
             - 'impacted_companies_direct'
             - 'impacted_companies_by_category'
             - 'impacted_companies_by_category_total'
             - 'drop_reason'
WHERE source_type = 'geopolitical_signal'
  AND (metadata ?| ARRAY['one_line','actors','impacted_companies_direct',
                         'impacted_companies_by_category',
                         'impacted_companies_by_category_total','drop_reason']);
```

The `-` operator removes a key and returns the rest of the object — it is not replacing
the object, so every other key survives. Verify afterwards that `channel`, `grade`,
`coverage` and `summary` are intact on the signal rows.

The `?|` guard means the statement touches only rows that actually carry one of these
keys, so re-running it is free and idempotent.

## Verification

```sql
-- expect 0
SELECT count(*) FROM agent_classifications
WHERE source_type = 'geopolitical_signal'
  AND metadata ?| ARRAY['one_line','actors','impacted_companies_direct',
                        'impacted_companies_by_category',
                        'impacted_companies_by_category_total','drop_reason'];

-- expect the pre-migration counts, unchanged
SELECT count(*) FILTER (WHERE metadata ? 'channel')  AS channel,
       count(*) FILTER (WHERE metadata ? 'grade')    AS grade,
       count(*) FILTER (WHERE metadata ? 'summary')  AS summary,
       count(*) FILTER (WHERE metadata ? 'coverage') AS coverage
FROM agent_classifications WHERE source_type = 'geopolitical_signal';
```

## Also: stale Stage C tags

Rows tagged by the previous release carry tags the current Stage C would produce
differently — `entities_json` without country/product types, `assets` spelled `semis`
and `autos` rather than `semiconductors` and `automotive`. Clearing their timestamp
makes the next scheduled run re-tag them at one model call each:

```sql
-- OPTIONAL, costs one Stage C model call per row
UPDATE agent_classifications
SET metadata = metadata - 'stage_c_classified_at'
WHERE source_type = 'geopolitical_signal'
  AND metadata ? 'stage_c_classified_at';
```

Skip this where the cost is unwelcome: the old tags are stale, not wrong, and every new
row is tagged correctly either way. The asset rename is handled separately, below.

## Asset vocabulary rename

`semis` → `semiconductors` and `autos` → `automotive`. The validator accepts the old
spellings and rewrites them, so nothing breaks unmigrated — but stored rows keep the old
strings until updated:

```sql
UPDATE agent_classifications
SET metadata = jsonb_set(metadata, '{assets}', (
      SELECT jsonb_agg(CASE a WHEN 'semis' THEN 'semiconductors'
                              WHEN 'autos' THEN 'automotive'
                              ELSE a END)
      FROM jsonb_array_elements_text(metadata->'assets') a))
WHERE source_type = 'geopolitical_signal'
  AND (metadata->'assets' @> '["semis"]'::jsonb
    OR metadata->'assets' @> '["autos"]'::jsonb);
```

Note `pipeline/macro_signal_interpret.py` keeps its own asset list and still spells it
`semis`. The two vocabularies overlap but are not the same set, and this rename is
deliberately scoped to geopolitical only.

## Scope

Only `source_type = 'geopolitical_signal'` ever wrote these keys. The `source_type`
predicate is belt-and-braces, not load-bearing, and is kept so the statement cannot touch
another domain's rows if one ever adopts the same key name.

## Run record

| Environment | Date | By |
|---|---|---|
| local (`ocn-postgres-signal-1`) | 2026-10-15 | verified: 0 remaining; `channel`/`grade`/`summary`/`coverage` intact |
| staging | — | |
| production | — | |

## Rollback

None, and none is wanted. Every removed key was either null on every row
(`impacted_companies_direct`), a restatement of a field that already exists
(`one_line`, `drop_reason`), or a list nothing could explain
(`impacted_companies_by_category`). Nothing downstream reads any of them after this
release — the `impacted_ticker` API filter now reads `entities_json`.
