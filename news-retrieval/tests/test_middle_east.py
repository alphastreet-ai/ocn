"""Middle East market signal: the four new fetchers, the match filter, and
the url_scheme / query / require_body / max_pages options added to the
shared Event Registry and Federal Register fetchers. HTTP is faked; the
response shapes are the real ones recorded live on 2026-10-19."""
import json
from datetime import datetime, timedelta, timezone

import pipeline as pl


class _Resp:
    def __init__(self, payload=None, text: str | None = None, status_code: int = 200,
                 headers: dict | None = None) -> None:
        self._payload = payload
        self.text = text if text is not None else json.dumps(payload)
        self.content = self.text.encode()
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        pass

    def json(self):
        return self._payload


def _iso(hours_ago: float, tz: str = "+04:00") -> str:
    when = datetime.now(timezone.utc) - timedelta(hours=hours_ago)
    return when.astimezone(timezone(timedelta(hours=4))).isoformat()


# --- match filter ------------------------------------------------------------

def _source(terms):
    return {"config": {"match_terms": terms}}


def test_match_is_word_bounded_so_ai_does_not_match_dubai_or_said() -> None:
    s = _source(["AI", "e&", "data centre", "data centres"])
    assert pl._me_matches(s, "Abu Dhabi opens AI campus")
    assert not pl._me_matches(s, "Dubai said rents fell")
    assert pl._me_matches(s, "e& signs cloud deal")
    assert pl._me_matches(s, "Two new data centres in Riyadh")


def test_no_match_terms_keeps_everything() -> None:
    assert pl._me_matches({"config": {}}, "anything at all")


# --- SEC 13D/13G ---------------------------------------------------------------

_13G = """<edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13g">
<issuerCik>0001709048</issuerCik><issuerName>GLOBALFOUNDRIES Inc.</issuerName>
<issuerCusipNumber>G39387108</issuerCusipNumber>
<eventDateRequiresFilingThisStatement>06/30/2026</eventDateRequiresFilingThisStatement>
<reportingPersonName>Mubadala Investment Company PJSC</reportingPersonName>
<reportingPersonBeneficiallyOwnedAggregateNumberOfShares>399573756</reportingPersonBeneficiallyOwnedAggregateNumberOfShares>
<classPercent>72.82</classPercent>
<reportingPersonName>Mamoura Diversified Global Holding PJSC</reportingPersonName>
<classPercent>72.82</classPercent>
</edgarSubmission>"""

_13D = """<edgarSubmission><issuerCIK>0001412558</issuerCIK><issuerName>Evotec SE</issuerName>
<dateOfEvent>10/09/2026</dateOfEvent><reportingPersonName>Mubadala Investment Company PJSC</reportingPersonName>
<aggregateAmountOwned>0.00</aggregateAmountOwned><percentOfClass>0.0</percentOfClass>
<item4>Pursuant to the Investment Agreement the Reporting Persons sold all of their shares in the Issuer.</item4>
<transactionPurpose>Pursuant to the Investment Agreement the Reporting Persons sold all of their shares in the Issuer.</transactionPurpose>
<principalJob>A long list of officers that is not narrative and must not become the body text at all.</principalJob>
</edgarSubmission>"""


def test_parse_13g_reads_the_13g_spellings() -> None:
    p = pl._parse_sec_stake_xml(_13G)
    assert p["issuer_name"] == "GLOBALFOUNDRIES Inc."
    assert p["issuer_cusip"] == "G39387108"
    assert p["event_date"] == "06/30/2026"
    assert p["percent_of_class"] == 72.82
    assert p["shares_owned"] == 399573756
    assert p["reporting_persons"] == [
        "Mubadala Investment Company PJSC", "Mamoura Diversified Global Holding PJSC"]


def test_parse_13d_keeps_narrative_items_once_and_skips_other_long_fields() -> None:
    p = pl._parse_sec_stake_xml(_13D)
    assert p["issuer_cik"] == "0001412558"
    assert p["percent_of_class"] == 0.0
    # item4 and transactionPurpose carry the same text in real filings;
    # principalJob is long but not narrative.
    assert p["narrative"] == [
        "Pursuant to the Investment Agreement the Reporting Persons sold all of their shares in the Issuer."]


def test_sec_fund_stake_keeps_only_13d_13g_in_window(monkeypatch) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    old = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()
    submissions = {"filings": {"recent": {
        "form": ["SCHEDULE 13G/A", "13F-HR", "SCHEDULE 13D/A"],
        "filingDate": [today, today, old],
        "accessionNumber": ["0001-26-1", "0001-26-2", "0001-26-3"],
    }}}
    calls = []

    def fake_get(url, **_):
        calls.append(url)
        return _Resp(submissions) if "submissions" in url else _Resp(text=_13G)

    monkeypatch.setattr(pl.httpx, "get", fake_get)
    monkeypatch.setattr(pl.time, "sleep", lambda _: None)
    rows = pl._fetch_one_sec_fund_stake(
        {"config": {"funds": [{"cik": "1704268", "fund": "Mubadala"}]}}, 1)
    assert [r["url"] for r in rows] == ["sec-stake://1704268/0001-26-1"]
    row = rows[0]
    assert row["title"] == "Mubadala: SCHEDULE 13G/A - GLOBALFOUNDRIES Inc. (72.82% of class)"
    assert "399,573,756 shares, 72.82% of class" in row["body"]
    assert "CUSIP G39387108" in row["body"]
    m = row["metadata"]
    assert m["issuer_cik"] == "0001709048"
    assert m["percent_of_class"] == 72.82
    # Everything the synthetic url encodes is also its own field.
    assert (m["fund_cik"], m["accession_number"], m["filing_date"]) == ("1704268", "0001-26-1", today)
    assert m["issuer_cusip"] == "G39387108"
    assert m["reporting_persons"] == [
        "Mubadala Investment Company PJSC", "Mamoura Diversified Global Holding PJSC"]


# --- WAM -------------------------------------------------------------------------

def test_wam_pages_filters_before_body_fetch_and_drops_bodyless(monkeypatch) -> None:
    def item(slug, title, hours):
        return {"id": 1, "title": title, "subTitle": "", "urlSlug": slug,
                "shortCode": slug[:3], "articleDate": _iso(hours)}

    view = {"sections": [{"articlesResult": {
        "paging": {"hasNext": True, "pageNumber": 0, "pageSize": 2},
        "items": [item("a1-ai", "G42 switches on AI campus", 1),
                  item("a2-weather", "Fog expected tomorrow", 2)]}}]}
    page2 = {"paging": {"hasNext": True, "pageNumber": 1},
             "items": [item("a3-chips", "Nvidia chips arrive", 30),  # outside window
                       item("a4-helium", "Helium supply update", 40)]}
    bodies = {"a1-ai": {"body": "<p>ABU DHABI (WAM) -- G42 switched on...</p>",
                        "categories": [{"title": "Business"}]}}
    posts, fetched = [], []

    def fake_get(url, params=None, **_):
        if url.endswith("GetViewByUrl"):
            return _Resp(view)
        fetched.append(params["slug"])
        return _Resp(bodies.get(params["slug"]), text=json.dumps(bodies[params["slug"]])
                     if params["slug"] in bodies else "")

    def fake_post(url, json=None, **_):
        posts.append(json)
        return _Resp(page2)

    monkeypatch.setattr(pl.httpx, "get", fake_get)
    monkeypatch.setattr(pl.httpx, "post", fake_post)
    rows = pl._fetch_one_wam_wire({"config": {"match_terms": ["AI", "chips", "helium"]}}, 1)
    # Walks until the page's oldest item leaves the window, then stops.
    assert [p["pageNumber"] for p in posts] == [1]
    assert fetched == ["a1-ai"]  # weather filtered out, old items outside the window
    assert [r["url"] for r in rows] == ["https://www.wam.ae/en/article/a1-ai"]
    assert rows[0]["body"] == "ABU DHABI (WAM) -- G42 switched on..."
    assert rows[0]["metadata"] == {"source_type": "wam_wire", "source_category": "me_official_wire",
                                   "source_outlet_type": "state_press"}


# --- Substack (Middle East AI News) ------------------------------------------------

def test_substack_keeps_free_written_posts_in_window_with_api_body(monkeypatch) -> None:
    def post(slug, hours, type_="newsletter", audience="everyone"):
        when = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat().replace("+00:00", "Z")
        return {"slug": slug, "title": slug, "subtitle": f"dek {slug}", "post_date": when,
                "type": type_, "audience": audience,
                "canonical_url": f"https://www.middleeastainews.com/p/{slug}"}

    archive = [post("deal", 2), post("minute", 3, type_="podcast"),
               post("paid", 4, audience="only_paid"), post("old", 50)]
    fetched = []

    def fake_get(url, params=None, **_):
        if url.endswith("/api/v1/archive"):
            return _Resp(archive if params["offset"] == 0 else [])
        slug = url.rsplit("/", 1)[1]
        fetched.append(slug)
        return _Resp({"body_html": f"<p>#UAE - full article {slug}</p>"})

    monkeypatch.setattr(pl.httpx, "get", fake_get)
    monkeypatch.setattr(pl.time, "sleep", lambda _: None)
    rows = pl._fetch_one_substack_post(
        {"name": "Middle East AI News", "config": {"base_url": "https://www.middleeastainews.com"}}, 1)
    assert fetched == ["deal"]  # podcast, paid and out-of-window posts never fetched
    assert rows[0]["url"] == "https://www.middleeastainews.com/p/deal"
    assert rows[0]["body"] == "#UAE - full article deal"
    assert rows[0]["summary"] == "dek deal"
    assert rows[0]["metadata"]["source_type"] == "substack_post"


def test_substack_retries_429_instead_of_losing_the_post(monkeypatch) -> None:
    when = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    archive = [{"slug": "deal", "title": "deal", "post_date": when, "type": "newsletter",
                "audience": "everyone"}]
    calls, slept = [], []

    def fake_get(url, params=None, **_):
        if url.endswith("/api/v1/archive"):
            return _Resp(archive if params["offset"] == 0 else [])
        calls.append(url)
        if len(calls) < 3:
            return _Resp({}, status_code=429, headers={"Retry-After": "1"})
        return _Resp({"body_html": "<p>full article</p>"})

    monkeypatch.setattr(pl.httpx, "get", fake_get)
    monkeypatch.setattr(pl.time, "sleep", slept.append)
    rows = pl._fetch_one_substack_post({"config": {"base_url": "https://x.substack.com"}}, 1)
    assert len(calls) == 3 and [r["body"] for r in rows] == ["full article"]
    assert slept[:2] == [1.0, 1.0]  # honoured Retry-After


# --- BIS + DOJ --------------------------------------------------------------------

def _bis_page(teasers):
    data = {"props": {"pageProps": {"pressReleaseTeasers": teasers}}}
    return f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(data)}</script>'


def test_bis_keeps_tagged_country_and_fills_body_from_doj_by_title(monkeypatch) -> None:
    now = datetime.now(timezone.utc).timestamp()
    teasers = [
        {"title": "Department of Commerce Eases Export Controls for UAE", "path": "/press-release/uae",
         "publishedOn": {"timestamp": now - 3600}, "body": {"processed": "<p>" + "x " * 150 + "</p>"},
         "countryTag": [{"name": "United Arab Emirates"}]},
        {"title": "Emirati Firm Charged With Shipping GPUs to China", "path": "/press-release/doj",
         "publishedOn": {"timestamp": now - 7200}, "body": {"processed": "<p><a href='x'>DOJ</a></p>"},
         "externalLink": {"url": "https://www.justice.gov/opa/pr/old-slug-404s"}, "countryTag": []},
        {"title": "BIS Settles With Ohio Exporter", "path": "/press-release/ohio",
         "publishedOn": {"timestamp": now - 3600}, "body": {"processed": "<p>" + "y " * 150 + "</p>"},
         "countryTag": [{"name": "Russia"}]},
    ]
    doj = {"results": [
        {"title": "Emirati firm charged with shipping GPUs to China!", "date": str(int(now - 7000)),
         "url": "https://www.justice.gov/opa/pr/new-slug", "body": "<p>WASHINGTON - An Emirati firm...</p>"},
    ]}

    def fake_get(url, params=None, **_):
        return _Resp(text=_bis_page(teasers)) if "bis.gov" in url else _Resp(doj)

    monkeypatch.setattr(pl.httpx, "get", fake_get)
    rows = pl._fetch_one_bis_press(
        {"config": {"countries": ["United Arab Emirates"], "match_terms": ["Emirati", "UAE"]}}, 1)
    by_path = {r["url"].rsplit("/", 1)[1]: r for r in rows}
    assert set(by_path) == {"uae", "doj"}  # the Ohio/Russia release is not kept
    assert by_path["uae"]["metadata"]["body_source"] == "bis"
    assert by_path["uae"]["metadata"]["doj_url"] is None
    assert by_path["uae"]["metadata"]["country_tags"] == ["United Arab Emirates"]
    assert by_path["doj"]["metadata"]["body_source"] == "doj"
    assert by_path["doj"]["metadata"]["external_link"] == "https://www.justice.gov/opa/pr/old-slug-404s"
    assert by_path["doj"]["metadata"]["doj_url"] == "https://www.justice.gov/opa/pr/new-slug"
    assert by_path["doj"]["body"] == "WASHINGTON - An Emirati firm..."


# --- Event Registry options -----------------------------------------------------------

# Real prose: the shared fetcher drops a long body with too few common
# English words as scrambled text (_is_readable_prose), so a repeated
# placeholder would be dropped as corrupt rather than kept.
_PROSE = ("The tanker was hit by a mine in the strait and the crew was "
          "taken to the port by the navy for treatment. ") * 6


def _er_article(uri, body=_PROSE):
    return {"uri": uri, "url": f"https://news.example/{uri}", "title": f"t{uri}",
            "body": body, "dateTimePub": datetime.now(timezone.utc).isoformat(),
            "source": {"title": "Example"}}


def test_event_registry_keyword_source_is_unchanged(monkeypatch) -> None:
    sent = []
    monkeypatch.setattr(pl.httpx, "post", lambda url, json=None, **_: (
        sent.append(json), _Resp({"articles": {"results": [_er_article("1")], "totalResults": 1}}))[1])
    rows = pl._fetch_one_event_registry(
        {"url": "eventregistry:x", "config": {"keywords": ["sanctions", "tariffs"]}}, 1, "k")
    cond = sent[0]["query"]["$query"]["$and"][0]
    assert cond == {"$or": [{"keyword": "sanctions", "keywordLoc": "title"},
                            {"keyword": "tariffs", "keywordLoc": "title"}]}
    assert len(sent) == 1
    assert rows[0]["url"] == "https://news.example/1"
    assert "article_url" not in rows[0]["metadata"]


def test_event_registry_query_scheme_require_body_and_pages(monkeypatch) -> None:
    pages = {1: [_er_article("1"), _er_article("2", body=None)], 2: [_er_article("3")]}
    sent = []

    def fake_post(url, json=None, **_):
        sent.append(json)
        return _Resp({"articles": {"results": pages[json.get("articlesPage", 1)], "totalResults": 3}})

    monkeypatch.setattr(pl.httpx, "post", fake_post)
    query = {"conceptUri": "http://en.wikipedia.org/wiki/Strait_of_Hormuz"}
    rows = pl._fetch_one_event_registry({"url": "eventregistry:me_shipping", "config": {
        "query": query, "url_scheme": "er-me", "require_body": True, "max_pages": 2}}, 1, "k")
    assert sent[0]["query"]["$query"]["$and"][0] == query
    assert [s.get("articlesPage") for s in sent] == [None, 2]
    assert [r["url"] for r in rows] == ["er-me://1", "er-me://3"]  # uri 2 had no body
    assert rows[0]["metadata"]["article_url"] == "https://news.example/1"
    assert rows[0]["metadata"]["er_query"] == "eventregistry:me_shipping"


def test_partial_er_bodies_are_detected_but_short_complete_items_kept() -> None:
    def art(body, outlet="example.com", link="https://example.com/news/x"):
        return {"body": body, "metadata": {"source_uri": outlet, "article_url": link}}

    # Real shapes: a Bloomberg teaser, a video page, Reuters' first paragraph.
    assert pl._er_body_is_partial(art("The tanker, which loaded an LNG cargo, is signaling ..."))
    assert pl._er_body_is_partial(art("Oil markets open the week on edge.",
                                      link="https://www.reuters.com/video/watch/idRW1/"))
    assert pl._er_body_is_partial(art("BANGKOK, Oct 15 (Reuters) - The impact...", outlet="reuters.com"))
    # The real 310-character liveblog entry is complete and must stay.
    liveblog = ("Fire and rescue services have been dispatched after hazardous materials were "
                "detected by workers at the Intel factory in Kiryat Gat, where a deadly blast "
                "occurred last week. According to the services, rescue teams are on site to manage "
                "the incident, which is localized and poses no danger to nearby residents.")
    assert len(liveblog) >= pl._ER_MIN_FULL_BODY_CHARS
    assert not pl._er_body_is_partial(art(liveblog, outlet="timesofisrael.com"))
    # Under 300 characters: a video series' standing blurb, a "[...]" teaser.
    assert pl._er_body_is_partial(art("From tariff turmoil and stock shocks, the editor cuts through the noise."))
    assert pl._er_body_is_partial(art(liveblog + " Officials announced on Tuesday [...]"))


def test_federal_register_body_cap_is_configurable(monkeypatch) -> None:
    long_text = "x" * 33187
    monkeypatch.setattr(pl, "_fetch_federal_register_body_from_govinfo",
                        lambda doc, pub, max_chars=pl._FEDERAL_REGISTER_BODY_MAX_CHARS: long_text[:max_chars])
    assert len(pl._fetch_federal_register_body("u", "d", "2026-07-14")) == 20000
    assert len(pl._fetch_federal_register_body("u", "d", "2026-07-14", None)) == 33187


def test_also_reported_entry_uses_publisher_link_for_scheme_urls() -> None:
    # Title-similarity dedup records a discarded duplicate as
    # {outlet, title, url}; an er-me://{uri} row must name its publisher,
    # not "9509743911".
    article = {"url": "er-me://9509743911", "title": "Iran urges US to reopen Hormuz",
               "metadata": {"article_url": "https://www.reuters.com/world/x"}}
    assert pl._also_reported_entry(article) == {
        "outlet": "reuters.com", "title": "Iran urges US to reopen Hormuz",
        "url": "https://www.reuters.com/world/x"}
    plain = {"url": "https://www.bbc.com/news/y", "title": "t", "metadata": {}}
    assert pl._also_reported_entry(plain)["outlet"] == "bbc.com"


def test_middle_east_title_dedup_matches_geopolitical_and_spares_official_rows() -> None:
    d = "middle_east_market_signal"
    assert d in pl._TITLE_DEDUP_DOMAINS and d in pl._SAME_EVENT_LLM_DOMAINS
    assert pl._DOMAIN_TITLE_DEDUP_WINDOW_HOURS[d] == pl._DOMAIN_TITLE_DEDUP_WINDOW_HOURS["geopolitical_news"]
    assert pl._DOMAIN_TITLE_DEDUP_SIMILARITY_THRESHOLD[d] == pl._DOMAIN_TITLE_DEDUP_SIMILARITY_THRESHOLD["geopolitical_news"]
    assert pl._DOMAIN_TITLE_DEDUP_EXCLUDED_CATEGORIES[d] == {
        "me_official_wire", "me_us_policy", "me_fund_stake"}


def test_run_drops_bodyless_and_out_of_window_rows_for_middle_east(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    fetched = [
        {"url": "u-ok", "title": "a", "body": "text", "published": now - timedelta(hours=3), "metadata": {}},
        {"url": "u-nobody", "title": "b", "body": None, "published": now, "metadata": {}},
        # Event Registry selects on crawl date: a late crawl of a 6-day-old story.
        {"url": "u-old", "title": "c", "body": "text", "published": now - timedelta(days=6), "metadata": {}},
        {"url": "u-margin", "title": "d", "body": "text", "published": now - timedelta(hours=40), "metadata": {}},
    ]
    monkeypatch.setattr(pl, "load_sources", lambda *a: [{"source_type": "rss"}])
    monkeypatch.setattr(pl, "_fetch_articles", lambda *a, **k: [dict(x) for x in fetched])
    monkeypatch.setattr(pl, "get_already_stored_urls", lambda urls: set())
    monkeypatch.setattr(pl, "_dedup_by_title_similarity_for_domain", lambda arts, d: arts)
    urls = [a["url"] for a in pl.run("middle_east_market_signal", days_back=1)["articles"]]
    assert urls == ["u-ok", "u-margin"]  # 40h is inside 1 day + 1 day of margin
    # Other domains are untouched.
    urls = [a["url"] for a in pl.run("geopolitical_news", days_back=1)["articles"]]
    assert urls == ["u-ok", "u-nobody", "u-old", "u-margin"]


# --- Federal Register options ----------------------------------------------------------

def test_federal_register_terms_and_url_scheme(monkeypatch) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    doc = {"title": "Enhanced Favorable Treatment for the UAE", "document_number": "2026-14132",
           "html_url": "https://www.federalregister.gov/d/2026-14132", "publication_date": today,
           "effective_on": "2026-07-10", "agencies": [{"name": "Industry and Security Bureau"}],
           "raw_text_url": "https://example/raw.txt", "type": "Rule"}
    params_seen = []

    def fake_get(url, params=None, **_):
        params_seen.append(params)
        return _Resp({"results": [doc], "next_page_url": None})

    monkeypatch.setattr(pl.httpx, "get", fake_get)
    monkeypatch.setattr(pl, "_fetch_federal_register_body", lambda *a, **k: "rule text")
    rows = pl._fetch_one_federal_register({"url": "federal_register:me", "config": {
        "terms": '"United Arab Emirates" | Qatar', "url_scheme": "me-federal-register"}}, 1)
    assert params_seen[0]["conditions[term]"] == '"United Arab Emirates" | Qatar'
    assert rows[0]["url"] == "me-federal-register://2026-14132"
    assert rows[0]["metadata"]["html_url"] == doc["html_url"]
    assert rows[0]["metadata"]["effective_on"] == "2026-07-10"

    params_seen.clear()
    rows = pl._fetch_one_federal_register({"url": "federal_register:geo", "config": {}}, 1)
    assert "conditions[term]" not in params_seen[0]
    assert rows[0]["url"] == doc["html_url"]
