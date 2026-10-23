"""SEC EDGAR filing text fetch + results-section extraction + XBRL financial facts.

Filing metadata (ticker, form_type, accession_number, item_codes,
primary_doc_url, ...) is owned entirely by news-retrieval: its poller fetches
from EDGAR daily and stores in DynamoDB (`ocn-sec-filings`), read via
GET /market/sec-filings/{ticker} (see adapters/news_client.get_sec_filings).
signal-detection-agent does not call EDGAR's submissions API itself - only
the functions here remain, since fetching a filing's full body text (and,
for 10-K/10-Q, its structured XBRL facts) is not something news-retrieval
does (it stores metadata + a link only).
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_USER_AGENT = "OCN SignalDetection kajal.nain@opengrowth.com"
_MIN_INTERVAL = 0.15  # ~6-7 req/sec, under SEC's 10 req/sec guidance
_COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
_FILING_INDEX_URL = "https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{accession}-index.htm"

# The exhibit "type" (e.g. "EX-99.1") is NOT present in the machine-readable
# index.json for a filing - that file's directory.item[].type is only a
# MIME-icon hint ("text.gif", "compressed.gif", ...), confirmed empirically
# against a real NVDA 8-K. The exhibit type only appears in the human-readable
# {accession}-index.htm document table, one <tr> per document with 5 <td>
# cells (Seq, Description, Document-link, Type, Size). The 2nd <td> is NOT
# reliably the type - confirmed on real NVDA 2020/2021 filings where it holds
# a free-text description ("Q2FY21 PRESS RELEASE") instead, with the actual
# "EX-99.1" code only in the 4th <td> (a bug this session: the first version
# of this regex only checked the 2nd <td> and silently returned "no exhibit
# found" - i.e. wrongly downgraded to exhibit_fetch_status="partial" - on
# every one of those filings). Matching each <tr>'s href + ALL its <td> text
# (not a fixed cell index) and checking whether ANY of them contains "EX-99"
# is robust to which column carries the code on a given filer/era.
_INDEX_ROW_PATTERN = re.compile(
    r'<tr[^>]*>((?:\s*<td[^>]*>.*?</td>\s*)+)</tr>',
    re.I | re.S,
)
_INDEX_CELL_PATTERN = re.compile(r'<td[^>]*>(.*?)</td>', re.I | re.S)
_INDEX_HREF_PATTERN = re.compile(r'<a href="([^"]+)"', re.I)

# 8-K item codes whose substance (the actual figures/announcement) typically
# lives in an attached EX-99* exhibit, not the cover-page primary document -
# confirmed empirically this session: every 2.02 (earnings results) in real
# NVDA/FLY output was scored noise with a reason of the form "figures are in
# Exhibit 99.1 which is not included", because only the primary doc was ever
# fetched. 7.01 (Reg FD disclosure) and 8.01 (other events) commonly attach
# the same kind of press-release exhibit, so they're included too.
_EXHIBIT_FETCH_ITEM_CODES = {"2.02", "7.01", "8.01"}
# Tested empirically this session against 46 real 2.02/7.01/8.01 filings,
# comparing signal_detection agreement vs the original 20K cap: 13K cap ->
# 42/46 (91%) agree, avg |score delta| 0.011 among agreements - a tighter
# match than an 8K cap tried first (41/46, avg delta 0.019), while still
# cutting average filing text sent to the model by ~24% (18,899 -> 14,334
# chars), directly reducing the uncached per-call cost. The disagreements at
# both cap sizes turned out to be model run-to-run variance on data-scarce
# filings (confirmed on one case: a 13K-cap run scored lower than 20K despite
# its own text_len being LARGER than the 20K run's, i.e. not a truncation
# effect at all), not attributable to the cap itself.
_MAX_EXHIBIT_CHARS = 13_000  # combined cap across all EX-99* exhibits for one filing


def _rate_sleep(last_call: list[float]) -> None:
    elapsed = time.monotonic() - last_call[0]
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)
    last_call[0] = time.monotonic()


def fetch_filing_text(primary_doc_url: str, last_call: list[float], *, max_chars: int = 600_000) -> str:
    """Fetch a filing's primary document live and return plain text (HTML stripped).

    Never persisted by the caller - fetched fresh for one classification call.
    Returns "" on any failure.
    """
    if not primary_doc_url:
        return ""
    _rate_sleep(last_call)
    try:
        resp = httpx.get(primary_doc_url, headers={"User-Agent": _USER_AGENT}, timeout=30.0)
        resp.raise_for_status()
        html = resp.text
    except Exception as exc:
        logger.warning("[SEC_EDGAR] filing text fetch failed url=%s error=%s", primary_doc_url, exc)
        return ""

    import html as html_module

    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_module.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def _strip_html(html: str) -> str:
    import html as html_module

    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_module.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_filing_exhibits_text(
    cik: str, accession_number: str, item_codes: list[str], last_call: list[float],
    *, max_chars: int = _MAX_EXHIBIT_CHARS,
) -> tuple[str, str]:
    """For 8-Ks whose item codes indicate the substance sits in an attached
    exhibit (see _EXHIBIT_FETCH_ITEM_CODES), fetch every EX-99* exhibit's text
    and concatenate it. Returns (exhibit_text, quality) where quality is
    "full" (index fetched, every EX-99* document fetched successfully),
    "partial" (index fetched, at least one EX-99* document found but one or
    more failed to fetch), or "none" (index fetch failed, or no EX-99*
    document was listed at all - e.g. this filing has no exhibit).

    Matches on the EX-99 prefix rather than an exact "EX-99.1"/"EX-99.2"
    string, since some filers list the type as just "EX-99" with no suffix.

    Returns ("", "none") immediately if item_codes has no overlap with
    _EXHIBIT_FETCH_ITEM_CODES - callers should still check this themselves
    before calling, to avoid the index.json fetch entirely for filings that
    can never need it (most 8-K item codes, e.g. 5.02/5.07, are procedural
    and rarely carry a substantive exhibit).
    """
    if not cik or not accession_number:
        return "", "none"

    accession_nodash = accession_number.replace("-", "")
    cik_int = str(int(cik))
    _rate_sleep(last_call)
    try:
        resp = httpx.get(
            _FILING_INDEX_URL.format(cik_int=cik_int, accession_nodash=accession_nodash, accession=accession_number),
            headers={"User-Agent": _USER_AGENT},
            timeout=30.0,
        )
        resp.raise_for_status()
        index_html = resp.text
    except Exception as exc:
        logger.warning(
            "[SEC_EDGAR] filing index fetch failed cik=%s accession=%s error=%s",
            cik, accession_number, exc,
        )
        return "", "none"

    exhibit_docs: list[str] = []
    for row_html in _INDEX_ROW_PATTERN.findall(index_html):
        cells = _INDEX_CELL_PATTERN.findall(row_html)
        is_exhibit = any(cell.strip().upper().startswith("EX-99") for cell in cells)
        if not is_exhibit:
            continue
        href_match = _INDEX_HREF_PATTERN.search(row_html)
        if href_match:
            exhibit_docs.append(href_match.group(1))
    if not exhibit_docs:
        return "", "none"

    texts: list[str] = []
    any_failed = False
    for href in exhibit_docs:
        _rate_sleep(last_call)
        doc_url = href if href.startswith("http") else f"https://www.sec.gov{href}"
        try:
            resp = httpx.get(doc_url, headers={"User-Agent": _USER_AGENT}, timeout=30.0)
            resp.raise_for_status()
            texts.append(_strip_html(resp.text))
        except Exception as exc:
            any_failed = True
            logger.warning("[SEC_EDGAR] exhibit fetch failed url=%s error=%s", doc_url, exc)

    if not texts:
        return "", "none"
    combined = "\n\n".join(texts)[:max_chars]
    return combined, ("partial" if any_failed else "full")


def _fetch_filing_index(cik: str, accession_number: str, last_call: list[float]) -> str | None:
    """Fetch a filing's human-readable index page. Returns None on failure -
    shared by fetch_filing_exhibits_text (EX-99*) and
    fetch_annual_report_exhibit_text (EX-13) below.
    """
    if not cik or not accession_number:
        return None
    accession_nodash = accession_number.replace("-", "")
    cik_int = str(int(cik))
    _rate_sleep(last_call)
    try:
        resp = httpx.get(
            _FILING_INDEX_URL.format(cik_int=cik_int, accession_nodash=accession_nodash, accession=accession_number),
            headers={"User-Agent": _USER_AGENT},
            timeout=30.0,
        )
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        logger.warning(
            "[SEC_EDGAR] filing index fetch failed cik=%s accession=%s error=%s",
            cik, accession_number, exc,
        )
        return None


def fetch_annual_report_exhibit_text(
    cik: str, accession_number: str, last_call: list[float], *, max_chars: int = 300_000,
) -> str:
    """Some 10-K filers (confirmed empirically this session: IBM) file their
    primaryDocument as a thin cover-page/XBRL-cover shell with no real MD&A
    narrative at all - the actual Item 7 discussion is furnished as a
    separate "EX-13" (Annual Report) exhibit instead, a long-standing SEC
    convention distinct from the 2.02/7.01/8.01 EX-99* press-release exhibits
    fetch_filing_exhibits_text handles. Confirmed on a real IBM FY2025 10-K:
    the primaryDocument (ibm-20251231.htm, EDGAR's own metadata) mentioned
    "revenue" only 36 times, almost all inside XBRL tag URIs, not prose -
    the real ~600K-char narrative lived entirely in EX-13
    (ibm-20251231_d2.htm). Checked against 6 other major filers (AAPL, TSLA,
    GOOGL, MSFT, Berkshire) which all publish one combined primaryDocument
    with genuine MD&A content and no EX-13 at all - this is a filer-specific
    pattern, not the norm, so callers should only reach for this as a
    fallback (see extract_filing_sections' found=False signal), not fetch it
    unconditionally for every 10-K/10-Q.

    Returns "" if the index fetch fails, no EX-13 is listed (the common
    case), or the EX-13 document itself fails to fetch - callers should treat
    an empty return as "no annual report exhibit available", not an error.
    """
    index_html = _fetch_filing_index(cik, accession_number, last_call)
    if not index_html:
        return ""

    exhibit_href = None
    for row_html in _INDEX_ROW_PATTERN.findall(index_html):
        cells = _INDEX_CELL_PATTERN.findall(row_html)
        if any(cell.strip().upper().startswith("EX-13") for cell in cells):
            href_match = _INDEX_HREF_PATTERN.search(row_html)
            if href_match:
                exhibit_href = href_match.group(1)
                break
    if not exhibit_href:
        return ""

    # Confirmed empirically this session: the EX-13 row's link (unlike
    # fetch_filing_exhibits_text's EX-99* rows) points at EDGAR's inline
    # Inline XBRL viewer wrapper - "/ix?doc=/Archives/edgar/data/..." - a
    # JS-rendered page, not the document itself. The real path is the
    # doc= query value; strip the wrapper down to it, same content either
    # way when the wrapper isn't present.
    if exhibit_href.startswith("/ix?doc="):
        exhibit_href = exhibit_href[len("/ix?doc="):]

    _rate_sleep(last_call)
    doc_url = exhibit_href if exhibit_href.startswith("http") else f"https://www.sec.gov{exhibit_href}"
    try:
        resp = httpx.get(doc_url, headers={"User-Agent": _USER_AGENT}, timeout=30.0)
        resp.raise_for_status()
        return _strip_html(resp.text)[:max_chars]
    except Exception as exc:
        logger.warning("[SEC_EDGAR] EX-13 exhibit fetch failed url=%s error=%s", doc_url, exc)
        return ""


_MDNA_TITLE = re.compile(r"management[’'`]?s?\s+discussion\s+(?:and|&)\s+analysis|\bMD&A\b", re.I)
_NOT_MDNA_TITLE = re.compile(
    r"annual information form|management[’'`]?s?\s+responsibility for financial|report of independent"
    r"|consolidated (?:audited )?financial statements",
    re.I,
)
# The exhibit's own title sits in its first few hundred characters; a wider
# head also catches cross-references - every real MD&A exhibit checked
# (AEM, CCJ, DNN, HBM, KGC) mentions "Annual Information Form" within its
# first 3,000 characters.
_MDNA_EXHIBIT_HEAD_CHARS = 600
_MDNA_EXHIBIT_MAX_TRIED = 8


def fetch_mdna_exhibit_text(
    cik: str, accession_number: str, last_call: list[float], *, max_chars: int = 3_000_000,
) -> str:
    """40-F/20-F fallback: return the ONE EX-99* exhibit that is the MD&A,
    read whole, or "" if none is found.

    A 40-F's primary document is a cover shell; the annual report is split
    across EX-99* exhibits - typically an Annual Information Form, the
    financial statements and the MD&A, in a filer-specific order
    (confirmed on real 2025 40-Fs: AEM's MD&A is EX-99.3, KGC's EX-99.2,
    TECK's EX-99.3). fetch_filing_exhibits_text concatenates them all and
    cuts at max_chars, so a multi-MB AIF in EX-99.1 pushes the MD&A out of
    the cut. The index page's descriptions are just "EX-99.2", so each
    exhibit is identified by its own opening text instead: the first one
    whose head names the MD&A before any AIF / financial-statements title
    (those open with "Annual Information Form" or "Management's
    Responsibility for Financial Statements") is returned. Tries at most
    _MDNA_EXHIBIT_MAX_TRIED exhibits, in index order.
    """
    index_html = _fetch_filing_index(cik, accession_number, last_call)
    if not index_html:
        return ""

    hrefs: list[str] = []
    for row_html in _INDEX_ROW_PATTERN.findall(index_html):
        cells = _INDEX_CELL_PATTERN.findall(row_html)
        if not any(re.sub(r"<[^>]+>", "", cell).strip().upper().startswith("EX-99") for cell in cells):
            continue
        href_match = _INDEX_HREF_PATTERN.search(row_html)
        if href_match and not href_match.group(1).lower().endswith(".pdf"):
            hrefs.append(href_match.group(1).removeprefix("/ix?doc="))

    for href in hrefs[:_MDNA_EXHIBIT_MAX_TRIED]:
        _rate_sleep(last_call)
        doc_url = href if href.startswith("http") else f"https://www.sec.gov{href}"
        try:
            resp = httpx.get(doc_url, headers={"User-Agent": _USER_AGENT}, timeout=60.0)
            resp.raise_for_status()
        except Exception as exc:
            logger.warning("[SEC_EDGAR] MD&A exhibit fetch failed url=%s error=%s", doc_url, exc)
            continue
        text = _strip_html(resp.text)
        head = text[:_MDNA_EXHIBIT_HEAD_CHARS]
        mdna = _MDNA_TITLE.search(head)
        other = _NOT_MDNA_TITLE.search(head)
        if mdna and (not other or mdna.start() < other.start()):
            return text[:max_chars]
    return ""


_RESULTS_MARKERS_PRECISE = [
    "net revenues increased",
    "net revenues decreased",
    "total net revenues increased",
    "total net revenues decreased",
    "net revenue increased",
    "net revenue decreased",
    "total revenues increased",
    "total revenues decreased",
    "revenue increased",
    "revenue decreased",
    "revenues increased",
    "revenues decreased",
    "net loss increased",
    "net loss decreased",
    "net income increased",
    "net income decreased",
    "net sales increased",
    "net sales decreased",
    "sales increased",
    "sales decreased",
    "income increased",
    "income decreased",
]

_RESULTS_MARKERS_GENERIC = [
    re.compile(r"fiscal (?:year )?\d{4} compared to fiscal (?:year )?\d{4}", re.I),
    re.compile(r"(?:year|quarter) ended [a-z]+ \d{1,2},? \d{4},? compared to", re.I),
    re.compile(r"results of operations for (?:the )?(?:fiscal )?(?:year|quarter)", re.I),
    re.compile(r"increased \$[\d,.]+ (?:million|billion)", re.I),
    re.compile(r"decreased \$[\d,.]+ (?:million|billion)", re.I),
    re.compile(r"net loss of \$[\d,.]+ (?:million|billion)", re.I),
    re.compile(r"net income of \$[\d,.]+ (?:million|billion)", re.I),
    re.compile(r"were \$[\d,.]+ (?:million|billion) in \d{4}, compared to", re.I),
]

# Structural fallback, checked only when NEITHER of the above tiers matches -
# rather than continuing to enumerate exact filer phrasings one at a time
# (confirmed brittle this session: AAPL, CACI, and NVIDIA each needed their
# own hand-added phrase/pattern before this existed), these two patterns
# describe the SHAPE a revenue comparison takes regardless of exact wording:
# a financial term near both a dollar figure and a comparison signal
# (percent sign or a comparison word), in either order since filers write
# "$X billion, up N%" and "increased N% or $X billion" equally often.
_RESULTS_STRUCTURAL_PROSE = re.compile(
    r"(?:net )?(?:revenue|revenues|net sales|net income|net loss|gross margin|operating income)"
    r"(?:"
    r"[^.]{0,120}?\$[\d,.]+\s*(?:million|billion)[^.]{0,80}?(?:%|percent|increased?|decreased?|compared|grew|rose|declined|up |down )"
    r"|"
    r"[^.]{0,80}?(?:increased|decreased|grew|rose|declined|up |down )[^.]{0,80}?\$[\d,.]+\s*(?:million|billion)"
    r")",
    re.I,
)
# Tabular MD&A comparison layout ("Revenues $ X $ Y Z%") - three dollar-ish
# figures followed by a percent, with no prose comparison word nearby at all
# (confirmed on a real CACI 10-Q: the actual +10.1% YoY table sat outside the
# window every prose-based marker found elsewhere in the same document).
_RESULTS_STRUCTURAL_TABLE = re.compile(
    r"(?:net )?(?:revenue|revenues|net sales)s?\s*\$?\s*[\d,.]+\s*\$?\s*[\d,.]+\s*\$?\s*[\d,.]+\s*\d{1,3}(?:\.\d)?\s*%",
    re.I,
)

# Last-resort tier, checked only when all three tiers above find nothing:
# the period-comparison heading/sentence MD&A writes around its results
# ("Three Months Ended March 31, 2026 Compared to...", "Comparison of the
# Three Months Ended...", "revenues were $1,401,207 as compared to..."),
# with amounts in "$ in thousands" tables or plain dollars that the
# "$X million/billion" markers above never match. Measured on 59 extraction
# failures (2026-10-24): found the MD&A results discussion in 26 of the 33
# domestic 10-K/10-Q failures (APH, RPAY, TVTX, UAVS, NNE, LAC, ...).
# Anchored after the MD&A heading because 10-Q financial-statement notes
# come BEFORE the MD&A and use the same "three months ended ... compared to"
# phrasing - on BNO and NEM the first match in the document was in a note.
_RESULTS_MARKERS_PERIOD_COMPARISON = [
    re.compile(r"(?:three|six|nine|twelve) months ended [A-Za-z]+ \d{1,2},? \d{4},? (?:as )?compared (?:to|with)", re.I),
    re.compile(r"comparison of (?:the )?(?:three|six|nine|twelve) months ended", re.I),
    re.compile(r"(?:years?|fiscal years?) ended [A-Za-z]+ \d{1,2},? \d{4},? (?:as )?compared (?:to|with)", re.I),
    re.compile(r"(?:revenues?|net sales|sales|net loss|net income)\s+(?:was|were)\s+\$\s?[\d,.]+\s+(?:as )?compared (?:to|with)", re.I),
]
_MDNA_HEADING = re.compile(
    r"item\s*[257]\s*[.:—–-]?\s*(?:management[’'`]?s?\s+discussion|operating and financial review)", re.I,
)
# After that, still last-resort: a results heading inside the MD&A that is
# followed by real figures - the tabular layouts ("Operating Results Sales
# ($ in millions) 2025 % Change ...", MRK; "(in thousands, except
# percentages) Three months ended ...", RXRX). Requiring figures keeps it
# off the many prose cross-references ("see Results of Operations in our
# 10-K") - a filing whose MD&A is incorporated by reference (XNDU 20-F)
# correctly stays not-found instead of getting an unrelated window.
_RESULTS_HEADING = re.compile(r"results of operations|operating results", re.I)
_RESULTS_FIGURE = re.compile(r"\$\s?[\d,]+(?:\.\d+)?|\d+(?:\.\d+)?\s?%")
# "% of revenue" table row - "Revenue $ 511.0 100.0 % $ 404.6 100.0 %"
# (AEIS), "Revenue, net 468.3 100.0 %" (WOLF): two numbers before the
# percent, which _RESULTS_STRUCTURAL_TABLE (three numbers) never matches.
# [\s​] because some filers separate table cells with zero-width
# spaces (AEIS), which \s does not match.
_RESULTS_PCT_OF_REVENUE_ROW = re.compile(
    r"(?:net )?(?:revenues?|net sales)(?:,\s*net)?[\s​]*\$?[\s​]*[\d,]+(?:\.\d+)?[\s​]+100(?:\.0+)?\s?%",
    re.I,
)
_RESULTS_HEADING_MIN_FIGURES = 3
_RESULTS_HEADING_FIGURE_SPAN = 600
_MDNA_SEARCH_SPAN = 200_000

_RESULTS_WINDOW_BEFORE = 500
_RESULTS_WINDOW_AFTER = 15_000


def _find_results_window(text: str, *, last_resort: bool = False) -> tuple[int, int, bool]:
    """Locate the results-of-operations window's (start, end) offsets in text.
    Same marker logic as before extract_results_section was split into this
    helper + the note-based extraction below - see that function's docstring
    for why marker coverage (not position filtering) is what matters here.

    last_resort=True adds the two looser tiers after the structural one
    (period-comparison phrasing, then a results heading with figures). Off
    by default: the caller must first try the three tiers on every text it
    has (the whole document, exhibits) - on the 59 failures, running the
    loose tiers on the 600K-cut text first put PONY, TEM and a KKR 10-Q on
    risk-factor / macro text that the precise markers found correctly in
    the full document.
    """
    low = text.lower()

    precise_positions = [idx for marker in _RESULTS_MARKERS_PRECISE if (idx := low.find(marker)) != -1]
    if precise_positions:
        idx = min(precise_positions)
        start = max(0, idx - _RESULTS_WINDOW_BEFORE)
        return start, idx + _RESULTS_WINDOW_AFTER, True

    generic_positions = [m.start() for pattern in _RESULTS_MARKERS_GENERIC if (m := pattern.search(text))]
    if generic_positions:
        idx = min(generic_positions)
        start = max(0, idx - _RESULTS_WINDOW_BEFORE)
        return start, idx + _RESULTS_WINDOW_AFTER, True

    # Structural fallback - only reached when no exact phrase/pattern above
    # matched at all. Checks EVERY match of both structural patterns and
    # takes the earliest position, same "earliest wins" principle as the two
    # tiers above (not first-pattern-in-list-order), for the same reason:
    # a later-checked pattern can legitimately match earlier in the document.
    structural_positions = (
        [m.start() for m in _RESULTS_STRUCTURAL_PROSE.finditer(text)]
        + [m.start() for m in _RESULTS_STRUCTURAL_TABLE.finditer(text)]
    )
    if structural_positions:
        idx = min(structural_positions)
        start = max(0, idx - _RESULTS_WINDOW_BEFORE)
        return start, idx + _RESULTS_WINDOW_AFTER, True

    if not last_resort:
        return 0, _RESULTS_WINDOW_AFTER, False

    comparison_positions = sorted(
        m.start() for pattern in _RESULTS_MARKERS_PERIOD_COMPARISON for m in pattern.finditer(text)
    )
    if comparison_positions:
        # Earliest match after the LAST MD&A heading that has one after it -
        # the table of contents lists the heading first, so the last heading
        # with a match after it is the real section; no heading -> earliest.
        idx = comparison_positions[0]
        for heading in reversed(list(_MDNA_HEADING.finditer(text))):
            after = [p for p in comparison_positions if p > heading.start()]
            if after:
                idx = after[0]
                break
        start = max(0, idx - _RESULTS_WINDOW_BEFORE)
        return start, idx + _RESULTS_WINDOW_AFTER, True

    mdna_headings = list(_MDNA_HEADING.finditer(text))
    if mdna_headings:
        mdna_start = mdna_headings[-1].start()
        for heading in _RESULTS_HEADING.finditer(text, mdna_start, mdna_start + _MDNA_SEARCH_SPAN):
            span = text[heading.start():heading.start() + _RESULTS_HEADING_FIGURE_SPAN]
            if len(_RESULTS_FIGURE.findall(span)) >= _RESULTS_HEADING_MIN_FIGURES:
                start = max(0, heading.start() - _RESULTS_WINDOW_BEFORE)
                return start, heading.start() + _RESULTS_WINDOW_AFTER, True
        row = _RESULTS_PCT_OF_REVENUE_ROW.search(text, mdna_start, mdna_start + _MDNA_SEARCH_SPAN)
        if row:
            start = max(0, row.start() - _RESULTS_WINDOW_BEFORE)
            return start, row.start() + _RESULTS_WINDOW_AFTER, True

    return 0, _RESULTS_WINDOW_AFTER, False


def extract_results_section(text: str) -> tuple[str, bool]:
    """Extract the quantitative results-of-operations section from 10-K/10-Q text.

    Returns (section_text, found) - found=False means no marker matched and
    the fallback (first max_chars of the raw document) was used; callers
    should treat that case as lower-confidence, since the fallback is likely
    to be XBRL tag metadata or front-matter, not the real section.
    """
    start, end, found = _find_results_window(text)
    return text[start:end], found


# Financial-statement notes carrying qualitative risk content that a fixed
# results-of-operations window misses - confirmed empirically on a real AAPL
# 10-Q: the EU State Aid legal matter (a genuinely material item) lives in
# the "Income Taxes" note at char ~34K, while the results-of-operations
# window (found via revenue-comparison phrasing) sits at ~72K-87K - neither
# a wider nor an earlier-shifted single window covers both, since they are
# in different, non-adjacent notes. Every 10-K/10-Q numbers its financial
# statement notes "Note N - Title" per GAAP disclosure requirements; the
# title wording is standard even though the note NUMBER varies by filer
# (confirmed: AAPL's Contingencies note is Note 10, MSFT's is Note 14) - so
# matching on title text, not number, generalizes across filers.
_QUALITATIVE_NOTE_TITLES = [
    "income taxes",
    "commitments and contingencies",
    "contingencies",
    "legal proceedings",
    "debt",
]
_NOTE_HEADER_PATTERN = re.compile(r"Note\s+\d+\s*[—–-]\s*([A-Za-z][A-Za-z ,]+)")
_QUALITATIVE_NOTE_MAX_CHARS = 6_000  # per note - these are usually a few paragraphs, not tables


def _extract_qualitative_notes(text: str) -> str:
    """Find and return the content of qualitative-risk notes (Income Taxes,
    Contingencies, Debt, ...) by locating "Note N - Title" headers and
    slicing to the next header (or _QUALITATIVE_NOTE_MAX_CHARS, whichever is
    shorter) - a note's real extent, not a guessed fixed window.
    """
    headers = list(_NOTE_HEADER_PATTERN.finditer(text))
    sections: list[str] = []
    for i, match in enumerate(headers):
        title = match.group(1).strip().lower()
        if not any(keyword in title for keyword in _QUALITATIVE_NOTE_TITLES):
            continue
        start = match.start()
        next_start = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        end = min(start + _QUALITATIVE_NOTE_MAX_CHARS, next_start)
        sections.append(text[start:end])
    return "\n\n".join(sections)


def extract_filing_sections(text: str, *, last_resort: bool = False) -> tuple[str, bool]:
    """Extract both the quantitative results-of-operations window AND the
    qualitative risk notes (income taxes, contingencies/legal, debt) from
    10-Q/10-K text, concatenated - replaces sending the full ~100K-500K char
    document while still covering the content a single fixed window misses.

    Returns (combined_text, results_found) - results_found mirrors
    extract_results_section's found flag (whether the numbers narrative was
    located); qualitative notes are additive best-effort and don't affect it,
    since their absence just means this filing didn't discuss those topics,
    not that extraction failed. last_resort: see _find_results_window.
    """
    start, end, found = _find_results_window(text, last_resort=last_resort)
    results_section = text[start:end]
    qualitative = _extract_qualitative_notes(text)
    if not qualitative:
        return results_section, found
    return results_section + "\n\n--- Other notes (legal/tax/debt) ---\n\n" + qualitative, found


# ---------------------------------------------------------------------------
# XBRL structured financial facts (10-K/10-Q only - 8-K rarely carries them)
# ---------------------------------------------------------------------------

# Each logical field lists fallback tag names in priority order - companies
# tag the same concept differently depending on which XBRL taxonomy version
# they adopted (e.g. pre/post ASC 606 revenue recognition), so a single fixed
# tag name misses real data on some filers. Confirmed empirically: AAPL's
# actual revenue tag for this era is SalesRevenueNet, not Revenues.
_XBRL_FIELD_CONCEPTS: dict[str, list[str]] = {
    "revenue": ["RevenueFromContractWithCustomerExcludingAssessedTax", "Revenues", "SalesRevenueNet"],
    "net_income": ["NetIncomeLoss", "ProfitLoss"],
    "gross_profit": ["GrossProfit"],
    "operating_income": ["OperatingIncomeLoss"],
    "eps_basic": ["EarningsPerShareBasic"],
    "eps_diluted": ["EarningsPerShareDiluted"],
    "rd_expense": ["ResearchAndDevelopmentExpense"],
    "sga_expense": ["SellingGeneralAndAdministrativeExpense"],
    "dividends_per_share": ["CommonStockDividendsPerShareDeclared", "CommonStockDividendsPerShareCashPaid"],
    "dividends_paid": ["PaymentsOfDividends", "PaymentsOfDividendsCommonStock"],
    "stock_repurchased": ["PaymentsForRepurchaseOfCommonStock"],
    "repurchase_program_authorized": ["StockRepurchaseProgramAuthorizedAmount1", "StockRepurchaseProgramAuthorizedAmount"],
    "total_assets": ["Assets"],
    "total_liabilities": ["Liabilities"],
    "stockholders_equity": ["StockholdersEquity"],
    "cash_and_equivalents": ["CashAndCashEquivalentsAtCarryingValue"],
    "long_term_debt": ["LongTermDebtNoncurrent", "LongTermDebt"],
    "income_tax_expense": ["IncomeTaxExpenseBenefit"],
    "effective_tax_rate": ["EffectiveIncomeTaxRateContinuingOperations"],
}


def fetch_company_facts(cik: str, last_call: list[float]) -> dict:
    """Fetch the full XBRL companyfacts document for a CIK. Returns {} on failure.

    This is a large (multi-MB, multi-year) document - callers should extract
    what they need via extract_xbrl_facts_for_filing() and discard the rest,
    not re-fetch per filing if classifying several filings for the same
    ticker in one batch (see classify_filings_for_ticker's per-ticker caching
    pattern for company_overview - the same principle applies here).

    cik is zero-padded to 10 digits internally regardless of the caller's
    input format - confirmed empirically this session that EDGAR's
    companyfacts endpoint 404s on an unpadded CIK (e.g. CIK1860160.json for
    Firefly Aerospace) and only succeeds zero-padded (CIK0001860160.json).
    Some callers already store cik pre-padded (news-retrieval's
    _load_cik_map), some don't (raw DynamoDB-stored values observed this
    session) - padding here makes the function correct either way rather
    than relying on every caller to have done it already.
    """
    cik = str(cik).zfill(10)
    _rate_sleep(last_call)
    try:
        resp = httpx.get(
            _COMPANYFACTS_URL.format(cik=cik),
            headers={"User-Agent": _USER_AGENT},
            timeout=30.0,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("[SEC_EDGAR] companyfacts fetch failed cik=%s error=%s", cik, exc)
        return {}


def _period_days(entry: dict) -> int:
    """Duration in days of an XBRL fact's reporting period, or 0 if it's a
    point-in-time fact (balance sheet items have no 'start', only 'end').
    """
    if not entry.get("start") or not entry.get("end"):
        return 0
    from datetime import date
    start = date.fromisoformat(entry["start"])
    end = date.fromisoformat(entry["end"])
    return (end - start).days


# Target reporting-period length per form type, for disambiguating candidates
# that share the same accession + end date. Confirmed empirically THIS IS
# FORM-TYPE-DEPENDENT, not "shortest always wins": a 10-Q's own accession
# tags both the single quarter (~91 days) and year-to-date cumulative
# (~182-273 days) under the same end date - want the quarter, the shorter
# one. A 10-K's own accession ALSO tags both the full fiscal year (~365
# days) AND just Q4 alone (~91 days) under the SAME end date (confirmed on a
# real AbbVie 10-K: EarningsPerShareDiluted tagged both $6.45/year and
# $2.26/Q4-only under one accession + end=2021-12-31) - here the shorter
# period is the WRONG one; a 10-K's "current results" means the full year.
# Point-in-time facts (period_days=0, e.g. balance sheet items) have no
# competing candidate at the same end date regardless of form type, so the
# target is irrelevant for those - there is only ever one to pick.
_TARGET_PERIOD_DAYS = {"10-Q": 91, "10-K": 365}


def _find_prior_year_value(
    usgaap: dict, accession_number: str, period_of_report: str, target_days: int,
) -> float | None:
    """Same-period-prior-year revenue, tagged under the SAME accession as the
    current filing (every 10-Q/10-K states its comparison to the prior-year
    period, and XBRL tags both current and comparative figures together).

    Built because relying on text markers to find a filer's revenue
    comparison table is unreliable - confirmed on a real CACI 10-Q where the
    actual "Revenues $1,744,270 ... $1,583,980 ... 10.1%" comparison table
    sat outside the extracted text window entirely, causing both a cheap and
    an expensive model to independently get materiality wrong (one guessed
    "medium" without the real number, one correctly said "insufficient data"
    and defaulted low) - neither had the actual percentage. XBRL's own
    'frame' field (e.g. 'CY2022Q1') explicitly marks these prior-year
    comparative facts, but matching on year-over-year end-date arithmetic
    (not the frame string, whose format isn't fully reliable across filers)
    is more robust: take period_of_report's end date, subtract ~1 year, and
    look for a same-form-type-length entry ending on that date instead.
    """
    from datetime import date, timedelta
    if not period_of_report:
        return None
    try:
        current_end = date.fromisoformat(period_of_report)
    except ValueError:
        return None
    prior_end = current_end - timedelta(days=365)
    # Filings don't always land on the exact same calendar date a year prior.
    # Confirmed empirically this needs to be wider than a few days: AAPL's
    # fiscal year (52/53-week, ending the last Saturday of a month) put the
    # real prior-year comparative 6 days outside an initial +/-4 day window -
    # a leap year alone can shift this by a day, and 52/53-week fiscal
    # calendars can drift further. +/-10 days comfortably covers both
    # without being so wide it risks matching a different, non-comparative
    # period (the next-nearest real candidate is a full quarter away).
    prior_window = {(prior_end + timedelta(days=d)).isoformat() for d in range(-10, 11)}

    for concept in _XBRL_FIELD_CONCEPTS["revenue"]:
        concept_data = usgaap.get(concept)
        if not concept_data:
            continue
        candidates = [
            e for entries in (concept_data.get("units") or {}).values() for e in entries
            if e.get("accn") == accession_number and e.get("end") in prior_window
        ]
        if candidates:
            best = min(candidates, key=lambda e: abs(_period_days(e) - target_days))
            return best["val"]
    return None


def extract_xbrl_facts_for_filing(
    company_facts: dict, accession_number: str, period_of_report: str, form_type: str,
) -> dict[str, float | None]:
    """Filter a companyfacts document down to the exact figures reported under
    one specific filing (by accession_number - XBRL tags every fact with the
    accession of the filing that reported it, so this isolates the current
    filing's own tagged data from other filings' data in the same document).

    One accession_number is not enough by itself, though: a single filing's
    own accession tags MULTIPLE overlapping periods that share the same end
    date - confirmed on two real filings this session: AAPL's Q2 FY2017 10-Q
    tags both the single quarter ($52.9B revenue) and year-to-date six months
    ($131.2B) under one accession + end date; AbbVie's FY2021 10-K tags both
    the full year ($6.45 diluted EPS) and Q4-only ($2.26) under one accession
    + end date. period_of_report filters to entries whose 'end' matches it
    (eliminating prior-year comparatives), and among same-end-date
    candidates, the one whose period length is CLOSEST to this form type's
    expected reporting period (_TARGET_PERIOD_DAYS) wins - not a blanket
    "shortest wins" rule, which is correct for 10-Q but silently picks the
    wrong (Q4-only) figure for 10-K.

    Returns a flat dict of logical field name -> value (None if the filing
    didn't tag that concept under any of its fallback names, or under this
    exact period). Exact numbers from SEC's own structured data - not
    model-extracted, not summarized, so there is no hallucination risk from
    the model's own recall - but this period-selection step is itself a real
    correctness requirement, not a formality; confirmed getting it wrong
    silently produces a plausible-looking but wrong number (Q4 EPS quoted as
    the annual figure), not an obviously-broken one.
    """
    usgaap = (company_facts.get("facts") or {}).get("us-gaap") or {}
    result: dict[str, float | None] = {}
    target_days = _TARGET_PERIOD_DAYS.get(form_type, 365)

    for field, concept_names in _XBRL_FIELD_CONCEPTS.items():
        candidates: list[dict] = []
        for concept in concept_names:
            concept_data = usgaap.get(concept)
            if not concept_data:
                continue
            for entries in (concept_data.get("units") or {}).values():
                candidates.extend(
                    e for e in entries
                    if e.get("accn") == accession_number and e.get("end") == period_of_report
                )
            if candidates:
                break  # this concept name matched - don't also check lower-priority fallbacks

        if candidates:
            best = min(candidates, key=lambda e: abs(_period_days(e) - target_days))
            result[field] = best["val"]
        else:
            result[field] = None

    result["revenue_prior_year"] = _find_prior_year_value(
        usgaap, accession_number, period_of_report, target_days,
    )

    return result


_TRAILING_QUARTERS_COUNT = 8


def get_trailing_quarterly_revenue(
    company_facts: dict, period_of_report: str, form_type: str,
) -> dict[str, Any]:
    """Return up to the 8 most recent QUARTERLY revenue figures strictly
    before period_of_report, from the same companyfacts document already
    fetched for extract_xbrl_facts_for_filing() - no second API call, since
    companyfacts already carries every revenue concept's full multi-year
    history (confirmed empirically: the same us-gaap/RevenueFromContract...
    data this function reads is identical to what
    data.sec.gov/api/xbrl/companyconcept/.../Revenues.json would return for
    the same CIK).

    Built to let the model judge THIS quarter's growth against the company's
    OWN recent trend, not a fixed absolute-growth threshold - a fixed >10%
    band means a company that grows 60%+ every quarter (e.g. a high-growth
    filer) is permanently flagged "high" for being normal, while a company
    whose growth suddenly drops from 60% to 12% (a real deceleration) reads
    as unremarkable under the same fixed band despite the second case being
    the one that actually changed. Deviation from trailing average is what
    the model needs to tell "normal for this company" from "something broke".

    Only quarterly-length entries (~91 days, via _TARGET_PERIOD_DAYS) are
    returned, ALWAYS - even for a 10-K - since annual filings need the same
    quarter-over-quarter trend as 10-Qs to judge Q4 deviation; the 10-K's own
    full-year figure is not comparable to a list of quarters and is already
    available separately via extract_xbrl_facts_for_filing()'s 'revenue'
    field for that form type.

    Strictly BEFORE period_of_report (not <=) so the current filing's own
    period is never included as one of its own trailing quarters - only
    entries with a distinct, earlier end date fall in the "trailing" window.
    Deduplicated by end date (a quarter can be tagged under multiple later
    accessions as a comparative figure; only the value closest to a 91-day
    period is kept per end date, and if several accessions report an
    identical quarter, any one is representative since restatements are rare
    and out of scope here).

    Returns {"quarters": [{end, value, yoy_growth_pct}, ...], "trailing_average_yoy_growth_pct":
    float | None} - oldest quarter first, capped at the 8 most recent ("8" is
    deliberately quarters, not a trailing-year lookback, so it covers ~2
    fiscal years regardless of form type). yoy_growth_pct per quarter and the
    trailing average are computed HERE, not left for the model - confirmed
    empirically this session that gpt-4.1 gets this arithmetic wrong on real
    filings (substituting sequential quarter-over-quarter ratios for true
    year-over-year ratios), so the model should use these values verbatim
    rather than recompute them from the raw quarter list.
    """
    usgaap = (company_facts.get("facts") or {}).get("us-gaap") or {}
    if not period_of_report:
        return {"quarters": [], "trailing_average_yoy_growth_pct": None}
    try:
        from datetime import date
        cutoff = date.fromisoformat(period_of_report)
    except ValueError:
        return {"quarters": [], "trailing_average_yoy_growth_pct": None}

    by_end: dict[str, dict] = {}
    for concept in _XBRL_FIELD_CONCEPTS["revenue"]:
        concept_data = usgaap.get(concept)
        if not concept_data:
            continue
        for entries in (concept_data.get("units") or {}).values():
            for e in entries:
                end = e.get("end")
                if not end or not e.get("start"):
                    continue
                try:
                    if date.fromisoformat(end) >= cutoff:
                        continue
                except ValueError:
                    continue
                days = _period_days(e)
                if abs(days - 91) > 20:  # quarterly only, not YTD/annual entries sharing this concept
                    continue
                existing = by_end.get(end)
                if existing is None or abs(days - 91) < abs(existing["_days"] - 91):
                    by_end[end] = {"end": end, "value": e["val"], "_days": days}
        # Unlike extract_xbrl_facts_for_filing's single-period lookup, do NOT
        # break after the first concept with any match - confirmed on real
        # NVDA data this session: it tagged revenue as
        # RevenueFromContractWithCustomerExcludingAssessedTax only through
        # FY2022, then switched to Revenues for every quarter since. These
        # are sequential eras for the SAME filer, not interchangeable
        # fallbacks - stopping at the first match silently returns only the
        # oldest, pre-2022 quarters when the cutoff is recent. Merging all
        # concepts (by_end already dedupes by end date) covers a filer's
        # full history regardless of which era's tag name it used when.

    all_ordered = sorted(by_end.values(), key=lambda e: e["end"])
    trailing = all_ordered[-_TRAILING_QUARTERS_COUNT:]

    # Pre-compute each trailing quarter's own YoY growth here, in code, rather
    # than handing the model 8 raw {end, value} points and a prompt
    # instruction to "compute the average YoY growth across these trailing
    # quarters" itself. Confirmed empirically this session: gpt-4.1
    # systematically gets this arithmetic wrong on real NVDA/FLY filings -
    # it substitutes sequential quarter-over-quarter ratios between adjacent
    # list entries for true year-over-year ratios (same quarter, prior year),
    # producing a materially wrong trailing-average figure that then
    # propagates into a wrong signal_detection verdict. Sonnet gets this
    # right most of the time in the same test, but there is no reason to
    # leave EITHER model doing arithmetic on numbers this function already
    # has - the same principle already applied to extract_xbrl_facts_for_filing
    # (exact current-period figures, not model-extracted) extends naturally
    # to this trailing comparison.
    from datetime import timedelta

    by_end_all = {e["end"]: e["value"] for e in all_ordered}

    def _yoy_for(end: str, value: float) -> float | None:
        try:
            end_date = date.fromisoformat(end)
        except ValueError:
            return None
        prior_target = end_date - timedelta(days=365)
        prior_window = {(prior_target + timedelta(days=d)).isoformat() for d in range(-10, 11)}
        for candidate_end in prior_window:
            if candidate_end in by_end_all:
                prior_value = by_end_all[candidate_end]
                if prior_value:
                    return (value - prior_value) / prior_value
        return None

    result_quarters = []
    yoy_values = []
    for e in trailing:
        yoy = _yoy_for(e["end"], e["value"])
        entry = {"end": e["end"], "value": e["value"], "yoy_growth_pct": round(yoy, 4) if yoy is not None else None}
        result_quarters.append(entry)
        if yoy is not None:
            yoy_values.append(yoy)

    trailing_average_yoy_growth_pct = round(sum(yoy_values) / len(yoy_values), 4) if yoy_values else None

    return {
        "quarters": result_quarters,
        "trailing_average_yoy_growth_pct": trailing_average_yoy_growth_pct,
    }
