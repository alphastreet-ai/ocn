# Geopolitical signal — staging cutover runbook

One-time procedure to move staging onto the two-source `geopolitical_news`
pipeline (`federal_register` + `eventregistry`) and clear the classifications
produced under the old source mix and the old prompts.

After this runs once, the domain is self-sustaining: the daily `--days-back 1`
fetch and the five-stage classifier keep it current with no further manual work.

**Read this first.** Removing a source from `src/seed.py` does **not** remove it
from an already-seeded database, so GDELT and Google News keep being fetched in
staging until their rows are deleted by hand. The aggregate `SOURCES` list in
`seed.py` registers only `FEDERAL_REGISTER_SOURCES` and
`EVENT_REGISTRY_SOURCES`; the other two sit commented out two lines below. But
`load_sources()` selects *every* `sources` row for the domain, with no active
flag and no reference to this file:

```sql
SELECT s.url, f.min_days_back, s.no_fetch, s.source_type, s.config
FROM sources s JOIN frequencies f ON f.id = s.frequency_id
JOIN domains d ON d.slug = ?   -- no is_active, no no_fetch filter
```

`no_fetch` is not the lever: it only skips Trafilatura body extraction, the
source is still fetched. The rows must be deleted.

---

## Order of operations, and why

Three constraints fix the sequence. Each is a way the cutover silently undoes
itself if the steps are run in the wrong order.

**1. Deploy before deleting sources.** `seed()` runs at the start of *every*
`trigger` invocation and on `serve` startup, and it re-inserts every entry in
`SOURCES`. Deleting the GDELT row while the old image is still deployed means
the next fetch — scheduled or manual — seeds it straight back. The deletion only
sticks once the running code is the version with those two entries commented
out. So: **deploy (step 1) → verify the deployed code (step 2) → delete
(step 4)**, never the reverse.

**2. Delete sources before classifications.** Otherwise the 02:00 UTC fetch can
land a fresh batch of GDELT articles in the gap, and you clear classifications
that a still-live source immediately recreates.

**3. Delete articles as well as classifications.** `articles.source` is plain
TEXT with no foreign key to `sources`, so removing a source cascades nothing.
Stage A re-pools any surviving article inside its date window, so old GDELT rows
reappear in the classifications right after you cleared them.

Which gives:

```
1  deploy image + terraform apply
2  verify deployed code and key        ← proves 1 landed before anything is deleted
3  record starting state
4  delete retired sources              ← safe only after 2
5  delete classifications + articles   ← after 4, so nothing live recreates them
6  first clean fetch + classify
7  verify
8  steady state — nothing to run, the existing schedules take over
```

Steps 3–5 are safe at any hour. Step 6 is best not straddling 02:00 UTC, though
if it does there is no harm — the scheduled run just does the work step 6 would
have.

---

## 1. Deploy

- [ ] Both images built `--platform linux/amd64` (Mac builds ARM64 by default;
      ECS needs AMD64) and pushed; both task definitions point at the new tag.
- [ ] `terraform apply` run, so the news-retrieval task definition actually
      injects `EVENT_REGISTRY_API_KEY`. Without it the fetch does not fail; it
      logs `EVENT_REGISTRY_API_KEY not set - skipping N eventregistry source(s)`
      and silently runs on Federal Register alone.
- [ ] `EVENT_REGISTRY_API_KEY` present in `ocn/staging/news-retrieval`
      — **done**, version `abce2f94`, validated against the live API.

---

## 2. Verify what is actually deployed  ← do this before deleting anything

Two things to prove, both against the *running* task rather than against disk or
Secrets Manager. Step 4's deletion is only durable if this passes.

```bash
aws ecs run-task --cluster staging-ocn \
  --task-definition staging-news-retrieval \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[<private-subnet>],securityGroups=[<news-retrieval-sg>]}" \
  --overrides '{"containerOverrides":[{"name":"news-retrieval","command":["python","-c","import os;k=os.environ.get(\"EVENT_REGISTRY_API_KEY\");print(\"KEY\",\"SET len=%d\"%len(k) if k else \"MISSING\");s=open(\"/app/src/seed.py\").read();b=s[s.index(chr(10)+\"SOURCES: list\"):];b=b[:b.index(chr(10)+\"]\"+chr(10))];L=[l.strip() for l in b.splitlines()];print(\"GDELT_SOURCE active:\",any(l.startswith(\"GDELT_SOURCE\") for l in L));print(\"GOOGLE_NEWS active:\",any(l.startswith(\"GEOPOLITICAL_GOOGLE_NEWS_SOURCE\") for l in L))"]}]}'
```

The `chr(10)+"SOURCES: list"` anchor is deliberate: a bare `"SOURCES: list"`
matches `NEWSAPI_SOURCES: list` hundreds of lines earlier and extracts the wrong
block, which reports `active: False` for everything and passes even on the old
image. Tested both ways — the form above returns `False/False` on the current
code and `True/True` against a simulated old image.

Required output:

```
KEY SET len=36
GDELT_SOURCE active: False
GOOGLE_NEWS active: False
```

- `KEY MISSING` → the apply has not landed. Stop.
- either `active: True` → the **old image** is still deployed. Stop. Deleting
  the source rows now would be undone by the next `seed()`, which runs on every
  `trigger` invocation.

Checking the deployed `seed.py` rather than the local one is the point: the
stale-image trap has bitten this pipeline before, where prompts and code on disk
were not what the container was running.

---

## 3. Record the starting state

So step 4's effect is provable rather than assumed.

```sql
SELECT s.source_type, count(*) AS source_rows
FROM sources s JOIN domains d ON d.id = s.domain_id
WHERE d.slug = 'geopolitical_news'
GROUP BY 1 ORDER BY 1;

SELECT source_type, count(*) AS articles
FROM articles
WHERE run_id IN (SELECT r.id FROM runs r JOIN domains d ON d.id = r.domain_id
                 WHERE d.slug = 'geopolitical_news')
GROUP BY 1 ORDER BY 2 DESC;

SELECT signal_detection, count(*) FROM agent_classifications
WHERE source_type = 'geopolitical_signal' GROUP BY 1;
```

Keep this output. Step 7 compares against it.

---

## 4. Delete the retired sources  ← only after step 2 passed

Removes GDELT and Google News from staging. Everything not in the two keepers
goes, so a source seeded earlier and since forgotten is caught too.

```sql
BEGIN;

-- Look before deleting.
SELECT s.id, s.source_type, s.url, s.name
FROM sources s JOIN domains d ON d.id = s.domain_id
WHERE d.slug = 'geopolitical_news'
  AND s.source_type NOT IN ('federal_register', 'eventregistry')
ORDER BY s.source_type, s.url;

DELETE FROM sources s
USING domains d
WHERE s.domain_id = d.id
  AND d.slug = 'geopolitical_news'
  AND s.source_type NOT IN ('federal_register', 'eventregistry');

-- Must return exactly federal_register and eventregistry.
SELECT s.source_type, count(*)
FROM sources s JOIN domains d ON d.id = s.domain_id
WHERE d.slug = 'geopolitical_news' GROUP BY 1 ORDER BY 1;

COMMIT;
```

Expect 5 `eventregistry` rows (one per query — the free tier caps a query at
15 keywords, and a multi-word phrase counts per word) plus the
`federal_register` rows. If the final count shows anything else, `ROLLBACK`.

This is scoped to `geopolitical_news` by design. Taiwan and Korea have their own
GDELT sources and must not be touched.

---

## 5. Clear the old classifications and their articles

Two tables in two databases. The classifications are the point; the articles
matter because a stale article row would otherwise be re-pooled by Stage A.

**5a — classifications** (`signal_detection_db`):

```sql
SELECT count(*) FROM agent_classifications WHERE source_type = 'geopolitical_signal';
DELETE FROM agent_classifications WHERE source_type = 'geopolitical_signal';
```

**5b — articles** (`news_retrieval_db`):

```sql
DELETE FROM articles
WHERE run_id IN (
  SELECT r.id FROM runs r JOIN domains d ON d.id = r.domain_id
  WHERE d.slug = 'geopolitical_news'
);

DELETE FROM runs r USING domains d
WHERE r.domain_id = d.id AND d.slug = 'geopolitical_news';
```

`articles.source` is plain TEXT with no foreign key to `sources`, so step 4
deletes nothing here on its own — this step has to be explicit.

Leaving 5b out is survivable but messy: the old GDELT articles stay in the
table, and Stage A re-pools any that fall inside its date window, so GDELT rows
reappear in the classifications after you just cleared them.

---

## 6. First clean fetch and classify

The scheduled rules already do this daily (`--days-back 1`, 02:00 UTC fetch,
webhook-triggered classify). Run them once by hand to confirm the cutover
rather than waiting for tomorrow.

```bash
# 6a. Fetch — same arguments as the daily schedule.
aws ecs run-task --cluster staging-ocn \
  --task-definition staging-news-retrieval \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[<private-subnet>],securityGroups=[<news-retrieval-sg>]}" \
  --overrides '{"containerOverrides":[{"name":"news-retrieval","command":["python","__main__.py","trigger","--domain","geopolitical_news","--days-back","1"]}]}'
```

Before moving on, confirm both sources actually contributed:

```sql
SELECT source_type, count(*) FROM articles
WHERE run_id = (SELECT max(r.id) FROM runs r JOIN domains d ON d.id = r.domain_id
                WHERE d.slug = 'geopolitical_news')
GROUP BY 1;
```

Both `eventregistry` and `federal_register` must appear, and nothing else.
`eventregistry` missing means the API key is not reaching the task — back to
step 2. A `gdelt` row appearing here means step 4 was run against the old image
and `seed()` has re-inserted the source — redo steps 2 and 4.

```bash
# 6b. Classify — Stage A -> B -> C -> D -> E in one task.
aws ecs run-task --cluster staging-ocn \
  --task-definition staging-signal-detection-agent \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[<private-subnet>],securityGroups=[<agent-sg>]}" \
  --overrides '{"containerOverrides":[{"name":"signal-detection-agent","command":["python","-m","src","run-geopolitical-signal-pipeline"]}]}'
```

Step 6b is usually unnecessary: news-retrieval's `--callback-url` webhook fires
the same five stages the moment 6a finishes. Running it twice is harmless —
A–D only select rows the previous stage has not processed, and Stage E clears
and regroups its own two keys (verified: a re-run changed 0 of 57 rows).

---

## 7. Verify

```sql
-- Only the two keepers produced rows.
--
-- agent_classifications stores no source_type in metadata, so provenance is
-- read off the url plus metadata.uri (Event Registry's own article id).
--
-- IMPORTANT: metadata.uri is written by STAGE B, not at ingest. Rows that
-- skip Stage B therefore have no uri even though they came from Event
-- Registry. Stage A short-circuits any headline carrying a talk verb
-- (warns / urges / vows / calls for / may / could / expected to / slams /
-- condemns) straight to weak_signal without a model call, so there is
-- always a population of uri-less Event Registry rows. Measured on the
-- first clean staging run: 31 of 293, every one of them weak_signal and
-- every one matching a talk verb. They are correct, not leftovers.
--
-- So the third bucket is split: a row that skipped Stage B is expected,
-- and only a row that RAN Stage B yet has neither marker is a genuine
-- survivor of a retired source.
SELECT CASE
         WHEN url LIKE '%federalregister.gov%' THEN 'federal_register'
         WHEN jsonb_exists(metadata, 'uri')    THEN 'eventregistry'
         WHEN NOT jsonb_exists(metadata, 'stage_b_classified_at')
              THEN 'skipped Stage B (expected: talk-verb weak_signal)'
         ELSE 'OTHER — investigate'
       END AS src,
       count(*)
FROM agent_classifications WHERE source_type = 'geopolitical_signal'
GROUP BY 1 ORDER BY 2 DESC;

-- Funnel shape.
SELECT signal_detection, count(*) FROM agent_classifications
WHERE source_type = 'geopolitical_signal' GROUP BY 1;

-- Stage E ran: seeds and members both present.
SELECT
  count(*) FILTER (WHERE signal_detection = 'signal') AS signals,
  count(*) FILTER (WHERE jsonb_exists(metadata, 'story_development')) AS seeds,
  count(*) FILTER (WHERE jsonb_exists(metadata, 'in_story')) AS members
FROM agent_classifications WHERE source_type = 'geopolitical_signal';

-- No row hidden with no seed to land on. Must be 0.
SELECT count(*) FROM agent_classifications m
WHERE m.source_type = 'geopolitical_signal'
  AND jsonb_exists(m.metadata, 'in_story')
  AND NOT EXISTS (
    SELECT 1 FROM agent_classifications s
    WHERE s.id = (m.metadata->>'in_story')::int
      AND jsonb_exists(s.metadata, 'story_development'));
```

Pass conditions:

- [ ] `OTHER — investigate` is **0**. That bucket now means only one thing:
      a row that ran Stage B yet carries neither marker, i.e. a retired source
      survived step 4. The `skipped Stage B` bucket is expected and should be
      non-zero on a normal day — those are talk-verb headlines ("X warns…",
      "Y urges…") that Stage A resolves to weak_signal for free. Spot-check
      one if it looks large: every row in it must be `weak_signal`, since
      that is the only verdict Stage A emits on this path besides the
      Federal Register `signal` shortcut
- [ ] `signal` / `weak_signal` / `noise` all non-zero — all-noise means the
      Stage B prompt did not reach the image
- [ ] `seeds` > 0 if there is more than one signal on a related subject;
      `seeds = members = 0` is legitimate on a thin day, not a failure
- [ ] the orphan query returns **0**

In Python, that last predicate must be written
`jsonb_exists(metadata, 'in_story')`, never the bare `?` operator — psycopg2
scans the whole query string for `?` and `%` placeholders and will raise
`type "s" does not exist`. The `?` form above is SQL-console only.

---

## 8. Steady state

Nothing further to run. In place already:

| When | What | Rule |
|---|---|---|
| 02:00 UTC | fetch `geopolitical_news`, `--days-back 1` | `news_retrieval_geopolitical_news_daily` |
| on fetch completion | Stage A→B→C→D→E via webhook | `/webhooks/news-retrieval-run-completed` |
| 03:00 UTC | same five stages — fallback for a lost webhook | `..._geopolitical_signal_pipeline_daily` |
| 01:00 UTC | refresh the company/category cache Stage C reads | `..._geo_signal_cache_refresh_daily` |
| Sunday 04:00 UTC | expire `geopolitical_news` articles older than 7 days | `..._geopolitical_news_expire_weekly` |

The webhook is the primary trigger; 03:00 is the safety net, because delivery is
fire-and-forget with no retry on news-retrieval's side.

Worth knowing: `agent_classifications` has **no expiry for
`geopolitical_signal`** — only `news` is on a retention schedule. These rows
accumulate indefinitely. Not a problem at ~50–60 rows/day, but it is the reason
a cutover like this needs a deliberate delete rather than waiting for rows to
age out.

---

## Rollback

Steps 4 and 5 delete; they do not transform. To restore a retired source,
un-comment its entry in the aggregate `SOURCES` list in `src/seed.py`, redeploy,
and the next `trigger` re-seeds it automatically — `seed()` runs at the start of
every invocation.

Deleted classifications and articles are not recoverable except by re-fetching
and re-classifying, which is exactly what step 6 does — so the real cost of a
mistake here is one day's rerun, not lost data. The one genuinely irreversible
thing is the `--days-back 1` window: articles older than yesterday are not
re-fetchable, so anything deleted in step 5b from earlier days is gone for good.
That is intended here (the point is to discard the old source mix), but it is
the reason step 3 records the starting counts first.
