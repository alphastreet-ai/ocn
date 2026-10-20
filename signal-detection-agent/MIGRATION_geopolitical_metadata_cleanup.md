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
| `impacted_categories` | `category` (column, `TEXT[]`) | One value under two names. The `category` column sat NULL on every row of this domain while the same list lived in metadata, so a query written against `category` silently missed geopolitical entirely. **Needs the schema change below, not just a key drop.** |
| `er_concepts`, `er_sentiment`, `er_uri` | `concepts`, `sentiment`, `uri` | Renamed on copy from news-retrieval, so one value had two names depending on which service's table you read. `er_uri` was worse than cosmetic: it read a key no source writes (news-retrieval stores it as `uri`), so it never copied anything at all. |
| `grade`, `corroborated`, `corroborating_domains`, `primary_source`, `specific` | `corroboration` | Stage D no longer grades. All three checks had collapsed against the real two-source pool — `primary_source` fired on 0 of 75 rows, `specific` on 74 of 75 — so every row graded the same. |
| `judged_on: same_event`, `judged_via_event` | — | Both restated what `event_uri` and `corroboration` already record. `judged_on` survives only for its `headline_only` value. |
| `body_excerpt` | `body` | An 8,000-character slice stored beside the field it was sliced from. Stage B still caps what it sends the model; the cap belongs at the call site, not in storage. |

`entities_json` is a real column, not a metadata key, and holds only entities the article
itself names — each with a reason quoting the text and a ticker where one exists.

## Schema change — `category` becomes `TEXT[]`

Needed before the key drop below, because `impacted_categories` holds a list and the
`category` column was `TEXT`. It is **one shared column**: `news` writes exactly one value,
`geopolitical_signal` writes 0–3.

```sql
ALTER TABLE agent_classifications
  ALTER COLUMN category TYPE text[]
  USING CASE WHEN category IS NULL THEN NULL ELSE ARRAY[category] END;
```

Verified locally against 4,305 live `news` rows: all converted to one-element arrays, 29
distinct values preserved, none lost or left empty.

**Deploy order matters.** `signal-herald` reads this column. Its code accepts both a string
and a list, so it can be deployed before or after the ALTER — but it must be deployed. The
old code ran `if category not in config.CATEGORIES` against what is now a list, which is
never true, meaning **every article is skipped and an empty digest is sent with no error
raised anywhere**.

Carrying existing geopolitical rows across is optional — re-running Stage C rewrites them —
but if wanted:

```sql
UPDATE agent_classifications
SET category = ARRAY(SELECT jsonb_array_elements_text(metadata->'impacted_categories'))
WHERE source_type = 'geopolitical_signal'
  AND metadata ? 'impacted_categories';
```

## The statement

Run this **after** the ALTER above, so `impacted_categories` is carried across before it is
dropped.

```sql
UPDATE agent_classifications
SET metadata = metadata
             - 'one_line'
             - 'actors'
             - 'impacted_companies_direct'
             - 'impacted_companies_by_category'
             - 'impacted_companies_by_category_total'
             - 'drop_reason'
             - 'impacted_categories'
             - 'er_concepts'
             - 'er_sentiment'
             - 'er_uri'
             - 'grade'
             - 'corroborated'
             - 'corroborating_domains'
             - 'primary_source'
             - 'specific'
             - 'judged_via_event'
             - 'body_excerpt'
WHERE source_type = 'geopolitical_signal'
  AND (metadata ?| ARRAY['one_line','actors','impacted_companies_direct',
                         'impacted_companies_by_category',
                         'impacted_companies_by_category_total','drop_reason',
                         'impacted_categories','er_concepts','er_sentiment','er_uri',
                         'grade','corroborated','corroborating_domains',
                         'primary_source','specific','judged_via_event','body_excerpt']);
```

`judged_on` is **not** dropped: its `same_event` value is gone, but `headline_only` is still
written and still means something. To clear only the dead value:

```sql
UPDATE agent_classifications
SET metadata = metadata - 'judged_on'
WHERE source_type = 'geopolitical_signal'
  AND metadata->>'judged_on' = 'same_event';
```

The `-` operator removes a key and returns the rest of the object — it is not replacing
the object, so every other key survives. Verify afterwards that `channel`, `corroboration`,
`coverage` and `summary` are intact on the signal rows, and that `category` is populated.

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

## Also: remove the retired geopolitical sources (news-retrieval DB)

**Mandatory, and easy to miss.** `seed()` is additive: it inserts sources
and refreshes their `config`, but it NEVER deletes one that has been
removed from `seed.py`. So commenting out `GDELT_SOURCE` and
`GEOPOLITICAL_GOOGLE_NEWS_SOURCE` stops a *fresh* database getting them
and does nothing at all to an existing one - the rows stay, and the next
scheduled run fetches from them exactly as before.

Confirmed live: staging currently holds both, and they came back locally
within minutes of being deleted because one `trigger` run re-seeded from
an older image.

Run against **news-retrieval's** database (not signal-detection's):

```sql
DELETE FROM sources s
USING domains d
WHERE s.domain_id = d.id
  AND d.slug = 'geopolitical_news'
  AND s.source_type IN ('gdelt', 'google_news');
```

Verify - this should list only `federal_register` and the five
`eventregistry` rows:

```sql
SELECT s.source_type, s.name
FROM sources s JOIN domains d ON d.id = s.domain_id
WHERE d.slug = 'geopolitical_news'
ORDER BY s.source_type, s.name;
```

**Order matters:** deploy the new image FIRST, then delete. A delete
against the old image is undone by the next run's `seed()`.

### Required secret

The Event Registry sources are skipped with a warning unless
`EVENT_REGISTRY_API_KEY` is set. Add it to
`ocn/{env}/news-retrieval` in Secrets Manager and to the task
definition's env before deploying, or the domain fetches only Federal
Register (~16 articles/day).
