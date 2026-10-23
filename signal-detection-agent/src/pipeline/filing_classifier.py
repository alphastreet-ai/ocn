"""LLM-based SEC filing classifier - single pass, no category, no novelty.

Mirrors classify_with_model()/validate_classification() from classifier.py but
for the filing schema (ticker, form_type, item_codes, fetched filing text).
"""
from __future__ import annotations

import math
import time
from pathlib import Path
from typing import Any

from pipeline.classifier import ALLOWED_MATERIALITY, classify_with_model, extract_json_object, norm

ALLOWED_SIGNAL = {'signal', 'weak_signal', 'noise'}
ALLOWED_ENTITY_TYPES = {'company', 'person', 'regulator', 'other'}
ALLOWED_EXTRACTION_QUALITY = {'full', 'partial', 'failed'}
SIGNAL_SCORE_THRESHOLD = 0.60
WEAK_SIGNAL_SCORE_THRESHOLD = 0.40

REQUIRED_FIELDS = (
    'signal_detection', 'signal_score', 'signal_reason', 'materiality',
    'already_disclosed', 'extraction_quality',
    'entities',
)

EXTRACTION_FAILED_RESULT = {
    'signal_detection': 'noise',
    'signal_score': 0.05,
    'signal_reason': (
        'Automated extraction could not locate the results-of-operations section '
        'in this filing; classification was not attempted against unreliable text. '
        'Needs manual review.'
    ),
    'materiality': 'none',
    'already_disclosed': False,
    'extraction_quality': 'failed',
    'entities': [],
}


def expected_signal_detection_for_score(signal_score: float) -> str:
    if signal_score >= SIGNAL_SCORE_THRESHOLD:
        return 'signal'
    if signal_score > WEAK_SIGNAL_SCORE_THRESHOLD:
        return 'weak_signal'
    return 'noise'


def validate_filing_classification(
    payload: dict[str, Any],
    *,
    form_type: str = '',
) -> dict[str, Any]:
    missing = [field for field in REQUIRED_FIELDS if field not in payload]
    if missing:
        raise ValueError(f'Missing fields: {", ".join(missing)}')

    signal_detection = norm(str(payload.get('signal_detection', '')))
    raw_signal_score = payload.get('signal_score')
    signal_reason = norm(str(payload.get('signal_reason', '')))
    materiality = norm(str(payload.get('materiality', '')))
    already_disclosed = payload.get('already_disclosed')
    extraction_quality = norm(str(payload.get('extraction_quality', '')))
    entities = payload.get('entities', [])

    if signal_detection not in ALLOWED_SIGNAL:
        raise ValueError(f'Invalid signal_detection: {signal_detection}')
    try:
        signal_score = float(raw_signal_score)
    except (TypeError, ValueError):
        raise ValueError(f'Invalid signal_score: {raw_signal_score}') from None
    if not math.isfinite(signal_score) or signal_score < 0.0 or signal_score > 1.0:
        raise ValueError(f'signal_score must be between 0 and 1: {signal_score}')

    # 10-K/10-Q use the same noise/weak_signal/signal band logic as 8-K -
    # routing these form types to simulation regardless of score is the
    # caller's job (keyed on form_type, not signal_score); this validator
    # no longer special-cases them.
    expected = expected_signal_detection_for_score(signal_score)
    if signal_detection != expected:
        raise ValueError(
            f'signal_detection must match score bands '
            f'noise<= {WEAK_SIGNAL_SCORE_THRESHOLD:.2f}, '
            f'weak_signal< {SIGNAL_SCORE_THRESHOLD:.2f}, '
            f'signal>= {SIGNAL_SCORE_THRESHOLD:.2f}: '
            f'{signal_detection} vs {signal_score}'
        )
    if not signal_reason:
        raise ValueError('signal_reason must be non-empty')
    if materiality not in ALLOWED_MATERIALITY:
        raise ValueError(f'Invalid materiality: {materiality}')
    if signal_detection == 'noise' and materiality != 'none':
        # Confirmed empirically this session: gpt-4.1 (not sonnet) sometimes
        # correctly judges a filing "noise" (score/reason both state the
        # event is routine/in-line) but still writes a non-"none" materiality
        # like "low", apparently conflating the absolute size of the
        # underlying dollar figures with whether THIS filing deviates from
        # baseline - three separate targeted prompt rewordings (including one
        # naming this exact failure case) did not stop it. The model's own
        # signal_detection judgment and reasoning are trustworthy here; only
        # the materiality token is wrong, and always wrong in this one
        # direction (never the reverse - weak_signal/signal paired with
        # "none" has not been observed). Normalizing here rather than
        # rejecting-and-retrying avoids failing the whole filing over a
        # single self-contradictory field the model has already told us how
        # to fix in its own text.
        materiality = 'none'
    if signal_detection in {'weak_signal', 'signal'} and materiality == 'none':
        raise ValueError('materiality must not be "none" when signal_detection is "weak_signal" or "signal"')

    if not isinstance(already_disclosed, bool):
        raise ValueError(f'already_disclosed must be a boolean: {already_disclosed!r}')
    if extraction_quality not in ALLOWED_EXTRACTION_QUALITY:
        raise ValueError(f'Invalid extraction_quality: {extraction_quality}')
    if extraction_quality == 'failed' and signal_detection != 'noise':
        raise ValueError('signal_detection must be "noise" when extraction_quality is "failed"')

    if not isinstance(entities, list):
        raise ValueError('entities must be a list')

    cleaned_entities: list[dict[str, str]] = []
    for item in entities:
        if not isinstance(item, dict):
            raise ValueError('entities must be a list of objects with name and type')
        name = norm(str(item.get('name', '')))
        entity_type = norm(str(item.get('type', '')))
        if not name:
            raise ValueError('entity name must be non-empty')
        if entity_type not in ALLOWED_ENTITY_TYPES:
            entity_type = 'other'
        cleaned_entities.append({'name': name, 'type': entity_type})

    return {
        'signal_detection': signal_detection,
        'signal_score': round(signal_score, 4),
        'signal_reason': signal_reason,
        'materiality': materiality,
        'already_disclosed': already_disclosed,
        'extraction_quality': extraction_quality,
        'entities': cleaned_entities,
    }


def build_filing_user_prompt(
    filing: dict[str, Any],
    filing_text: str,
    *,
    company_overview: dict[str, Any] | None = None,
    xbrl_facts: dict[str, Any] | None = None,
    exhibit_fetch_status: str = 'not_applicable',
    trailing_quarterly_revenue: dict[str, Any] | None = None,
) -> str:
    metadata = {
        'ticker': filing.get('ticker', ''),
        'cik': filing.get('cik', ''),
        'form_type': filing.get('form_type', ''),
        'item_codes': filing.get('item_codes', []),
        'filed_at': filing.get('filed_at', ''),
        'accepted_at': filing.get('accepted_at', ''),
        'period_of_report': filing.get('period_of_report', ''),
        'filer_category': filing.get('filer_category', ''),
        'accession_number': filing.get('accession_number', ''),
        'exhibit_fetch_status': exhibit_fetch_status,
    }
    reference = company_overview or {}
    company_reference_data = {
        'annual_revenue': reference.get('revenue_ttm'),
        'market_capitalization': reference.get('market_cap'),
        'shares_outstanding': reference.get('shares_outstanding'),
    }

    import json
    prompt = 'Classify this SEC filing.\n\nFiling metadata:\n' + json.dumps(metadata, ensure_ascii=False, indent=2)
    prompt += '\n\nCompany reference data (use as denominator for materiality; null means unavailable - judge qualitatively and say so):\n'
    prompt += json.dumps(company_reference_data, ensure_ascii=False, indent=2)
    if xbrl_facts:
        prompt += (
            '\n\nExact reported financial figures for THIS filing (from SEC\'s own '
            'structured XBRL data, not extracted from the text below - use these '
            'values verbatim for any number you report; null means this filing did '
            'not tag that concept, not zero):\n'
        )
        prompt += json.dumps(xbrl_facts, ensure_ascii=False, indent=2)
    if trailing_quarterly_revenue and trailing_quarterly_revenue.get('quarters'):
        this_quarter_yoy = None
        if xbrl_facts:
            revenue = xbrl_facts.get('revenue')
            revenue_prior_year = xbrl_facts.get('revenue_prior_year')
            if revenue is not None and revenue_prior_year:
                this_quarter_yoy = round((revenue - revenue_prior_year) / revenue_prior_year, 4)
        prompt += (
            '\n\nTrailing quarterly revenue for this company, oldest first, from SEC\'s own '
            'structured XBRL data (up to 8 quarters strictly before this filing\'s '
            'period_of_report). yoy_growth_pct per quarter and trailing_average_yoy_growth_pct '
            'below are ALREADY COMPUTED from SEC\'s own structured data - use them verbatim, '
            'do NOT recompute or re-derive YoY growth yourself from the raw values. this_quarter_yoy_growth_pct '
            f'for the filing you are classifying now is {this_quarter_yoy if this_quarter_yoy is not None else "unavailable"}. '
            'Band materiality on the DIFFERENCE between this_quarter_yoy_growth_pct and '
            'trailing_average_yoy_growth_pct, not on the raw growth percentage alone. A company '
            'that consistently grows 60%+ every quarter is not "high" materiality for growing '
            '60% again; a company whose growth suddenly drops from a 60% trailing average to 12% '
            'is a real deceleration worth flagging even though 12% alone looks modest:\n'
        )
        prompt += json.dumps(trailing_quarterly_revenue, ensure_ascii=False, indent=2)
    prompt += '\n\nFiling text:\n' + (filing_text or '(no text available)')
    prompt += '\n\nReturn strict JSON only.'
    return prompt


def classify_filing(
    filing: dict[str, Any],
    filing_text: str,
    *,
    system_prompt: str,
    models: list[str],
    api_key: str,
    base_url: str,
    timeout: int,
    max_attempts: int,
    extraction_found: bool = True,
    cache_system_prompt: bool = True,
    company_overview: dict[str, Any] | None = None,
    xbrl_facts: dict[str, Any] | None = None,
    exhibit_fetch_status: str = 'not_applicable',
    trailing_quarterly_revenue: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """extraction_found=False (10-K/10-Q only, set by the caller from
    extract_results_section()'s second return value) skips the LLM call
    entirely and returns a fixed noise/needs-review result - classifying
    against unreliable fallback text (XBRL tag noise, front-matter) produces
    a confident-looking but ungrounded result, which is worse than flagging
    it honestly for manual review. No LLM call happened, so no real
    classification was made; this fallback means "unclassifiable", not
    "classified as unimportant".

    company_overview is the dict returned by adapters.news_client.get_company_overview()
    (market_cap, revenue_ttm, shares_outstanding, etc). Fetch once per ticker
    and pass the same dict into every filing classified for that ticker in a
    batch - these figures don't change filing-to-filing, no need to re-fetch
    per filing. None is a valid value (overview unavailable); the prompt is
    designed to judge qualitatively and say so, not fail, when this is absent.

    xbrl_facts (10-K/10-Q only) is adapters.sec_edgar.extract_xbrl_facts_for_filing()'s
    output - exact reported figures (revenue, EPS, dividends, ...) for this
    specific filing, from SEC's own structured data. Confirmed empirically
    this session: without this, the model can hallucinate plausible-looking
    but wrong figures from its training data (e.g. quoting a later fiscal
    year's dividend/buyback numbers for an earlier filing) rather than
    grounding strictly in the provided text. Passing exact figures explicitly
    removes the need for the model to recall or re-derive them from memory.

    exhibit_fetch_status ("full"/"partial"/"not_applicable", set by the caller
    from adapters.sec_edgar.fetch_filing_exhibits_text()'s second return value
    for 8-Ks with item codes 2.02/7.01/8.01) tells the model definitively
    whether the EX-99* exhibit appended to filing_text was actually fetched -
    without this, the model can only guess from context whether "no figures
    visible" means the exhibit is genuinely empty of them or the fetch failed,
    which the prompt otherwise has no way to distinguish.

    trailing_quarterly_revenue (10-K/10-Q only) is
    adapters.sec_edgar.get_trailing_quarterly_revenue()'s output - up to 8
    prior quarters of this company's own reported revenue, each with its own
    yoy_growth_pct already computed, plus a trailing_average_yoy_growth_pct.
    Lets the model judge THIS quarter's growth against the company's own
    recent trend instead of a fixed absolute-growth threshold - a fixed >10%
    band flags a consistently high-growth filer as permanently "high" for
    being normal, while missing the case where growth suddenly decelerates
    from a high baseline to something that still looks fine in isolation.
    The YoY arithmetic is done here, not left to the model - confirmed
    empirically this session that gpt-4.1 gets it wrong on real filings
    (substituting sequential quarter-over-quarter ratios for true
    year-over-year ratios), producing a wrong trailing average that then
    propagates into a wrong signal_detection verdict.

    cache_system_prompt=True (default) marks the system prompt with an
    ephemeral cache breakpoint - unlike the news classifier, this prompt has
    no per-article dynamic example injection, so the ENTIRE system prompt is
    identical and cacheable on every single call, not just a static prefix.
    """
    if not extraction_found:
        return {
            **EXTRACTION_FAILED_RESULT,
            'ticker': filing.get('ticker', ''),
            'form_type': filing.get('form_type', ''),
            'item_codes': filing.get('item_codes', []),
            'accession_number': filing.get('accession_number', ''),
            'filed_at': filing.get('filed_at', ''),
            'primary_doc_url': filing.get('primary_doc_url', ''),
        }

    form_type = filing.get('form_type', '')

    def _validator(payload: dict[str, Any]) -> dict[str, Any]:
        return validate_filing_classification(payload, form_type=form_type)

    user_prompt = build_filing_user_prompt(
        filing, filing_text,
        company_overview=company_overview,
        xbrl_facts=xbrl_facts,
        exhibit_fetch_status=exhibit_fetch_status,
        trailing_quarterly_revenue=trailing_quarterly_revenue,
    )
    errors: list[str] = []
    for model in models:
        for attempt in range(1, max_attempts + 1):
            try:
                result = classify_with_model(
                    system_prompt, user_prompt, model, api_key, base_url, timeout,
                    validator=_validator,
                    max_tokens=800,
                    cache_system_prompt=cache_system_prompt,
                    stage='sec_filing',
                    article_id=filing.get('accession_number'),
                )
                result['ticker'] = filing.get('ticker', '')
                result['form_type'] = filing.get('form_type', '')
                result['item_codes'] = filing.get('item_codes', [])
                result['accession_number'] = filing.get('accession_number', '')
                result['filed_at'] = filing.get('filed_at', '')
                result['primary_doc_url'] = filing.get('primary_doc_url', '')
                return result
            except Exception as exc:
                errors.append(f'{model} attempt {attempt}: {exc}')
                time.sleep(0.25)
                continue
    raise RuntimeError('Filing classification failed: ' + ' | '.join(errors))


async def classify_filings_for_ticker(
    ticker: str,
    filings_with_text: list[tuple[dict[str, Any], str, bool]],
    *,
    system_prompt: str,
    models: list[str],
    api_key: str,
    base_url: str,
    timeout: int,
    max_attempts: int,
    cache_system_prompt: bool = True,
    get_company_overview_fn=None,
) -> list[dict[str, Any]]:
    """Classify every filing for one ticker, fetching company_overview ONCE
    and reusing it across all of them - revenue/market_cap/shares_outstanding
    don't change filing-to-filing within a batch, so there is no reason to
    re-fetch per filing. This is the fix for the gap where classify_filing()
    correctly ACCEPTS company_overview but nothing previously called it more
    than once per ticker in a real batch.

    filings_with_text: list of (filing_dict, filing_text_or_section, extraction_found)
    tuples - callers have already done the fetch_filing_text()/
    extract_results_section() step per filing before calling this.

    get_company_overview_fn: injected for testability (defaults to
    adapters.news_client.get_company_overview if not provided) - avoids a
    hard import-time dependency on the adapter for callers that want to pass
    a stub/mock.
    """
    if get_company_overview_fn is None:
        from adapters.news_client import get_company_overview as get_company_overview_fn

    overview = await get_company_overview_fn(ticker)

    results = []
    for filing, filing_text, extraction_found in filings_with_text:
        result = classify_filing(
            filing, filing_text,
            system_prompt=system_prompt,
            models=models,
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            max_attempts=max_attempts,
            extraction_found=extraction_found,
            cache_system_prompt=cache_system_prompt,
            company_overview=overview,
        )
        results.append(result)
    return results


SEC_FILING_PROMPT_PATH = Path(__file__).parent.parent.parent / 'prompts' / 'sec_filing_classifier_v1.txt'
SEC_FILING_SUMMARY_PROMPT_PATH = Path(__file__).parent.parent.parent / 'prompts' / 'sec_filing_summarizer_v1.txt'


def load_sec_filing_prompt() -> str:
    text = SEC_FILING_PROMPT_PATH.read_text(encoding='utf-8')
    if not text.strip():
        raise ValueError(f'Prompt file is empty: {SEC_FILING_PROMPT_PATH}')
    return text


def load_sec_filing_summary_prompt() -> str:
    text = SEC_FILING_SUMMARY_PROMPT_PATH.read_text(encoding='utf-8')
    if not text.strip():
        raise ValueError(f'Prompt file is empty: {SEC_FILING_SUMMARY_PROMPT_PATH}')
    return text


ALLOWED_GUIDANCE_DIRECTION = {'raised', 'cut', 'maintained', 'initiated', 'withdrawn', 'not_provided'}
SUMMARY_REQUIRED_FIELDS = (
    'extraction_quality', 'headline', 'guidance', 'stated_figures',
    'positives', 'negatives', 'outlook', 'disclosure_flags', 'citations',
)


def validate_filing_summary(payload: dict[str, Any]) -> dict[str, Any]:
    missing = [field for field in SUMMARY_REQUIRED_FIELDS if field not in payload]
    if missing:
        raise ValueError(f'Missing fields: {", ".join(missing)}')

    extraction_quality = norm(str(payload.get('extraction_quality', '')))
    if extraction_quality not in ALLOWED_EXTRACTION_QUALITY:
        raise ValueError(f'Invalid extraction_quality: {extraction_quality}')

    headline = norm(str(payload.get('headline', '')))
    if not headline and extraction_quality == 'failed':
        # Confirmed empirically this session (a real JNJ 8-K whose fetched
        # primary document was XBRL cover-page metadata only, same failure
        # shape as the IBM 10-K EX-13 case, but no equivalent fallback
        # exhibit exists for most 8-K item codes): the model correctly
        # flags extraction_quality="failed" but understandably has nothing
        # to write a real headline from. Filling a placeholder here, rather
        # than raising and losing the row entirely, mirrors
        # EXTRACTION_FAILED_RESULT's reasoning in classify_filing() - "needs
        # manual review" is a valid, honest answer when the text itself was
        # unusable, and stage 2 already has its own extraction_quality
        # check for this exact case (classify_filing's extraction_found
        # parameter) so nothing downstream is misled into treating this as
        # a confident real summary.
        headline = 'Filing text could not be extracted; automated summarization was not attempted.'

    guidance = payload.get('guidance')
    if not isinstance(guidance, dict):
        raise ValueError('guidance must be an object')
    guidance_direction = norm(str(guidance.get('direction', '')))
    if guidance_direction not in ALLOWED_GUIDANCE_DIRECTION:
        raise ValueError(f'Invalid guidance.direction: {guidance_direction}')
    cleaned_guidance = {
        'direction': guidance_direction,
        'metric': guidance.get('metric'),
        'new_range_stated': guidance.get('new_range_stated'),
        'prior_range_stated': guidance.get('prior_range_stated'),
    }

    stated_figures = payload.get('stated_figures', [])
    if not isinstance(stated_figures, list):
        raise ValueError('stated_figures must be a list')
    cleaned_figures: list[dict[str, Any]] = []
    for item in stated_figures:
        if not isinstance(item, dict) or not norm(str(item.get('label', ''))) or not norm(str(item.get('value', ''))):
            raise ValueError('stated_figures items must have non-empty label and value')
        cleaned_figures.append({
            'label': norm(str(item['label'])),
            'value': norm(str(item['value'])),
            'comparison_stated': item.get('comparison_stated'),
        })

    positives = payload.get('positives', [])
    negatives = payload.get('negatives', [])
    if not isinstance(positives, list) or not isinstance(negatives, list):
        raise ValueError('positives and negatives must be lists')
    cleaned_positives = [norm(str(p)) for p in positives if norm(str(p))][:5]
    cleaned_negatives = [norm(str(n)) for n in negatives if norm(str(n))][:5]

    outlook = norm(str(payload.get('outlook', '')))
    if not outlook:
        if extraction_quality == 'failed':
            outlook = 'none stated'
        else:
            raise ValueError('outlook must be non-empty (use "none stated" when absent)')

    disclosure_flags = payload.get('disclosure_flags')
    if not isinstance(disclosure_flags, dict):
        raise ValueError('disclosure_flags must be an object')
    cleaned_flags: dict[str, bool] = {}
    for key in ('references_prior_disclosure', 'going_concern', 'material_weakness', 'segment_reclassification'):
        value = disclosure_flags.get(key)
        if not isinstance(value, bool):
            raise ValueError(f'disclosure_flags.{key} must be a boolean')
        cleaned_flags[key] = value

    citations = payload.get('citations', [])
    if not isinstance(citations, list):
        raise ValueError('citations must be a list')
    cleaned_citations: list[dict[str, str]] = []
    for item in citations:
        if not isinstance(item, dict) or not norm(str(item.get('claim', ''))) or not norm(str(item.get('source_snippet', ''))):
            raise ValueError('citations items must have non-empty claim and source_snippet')
        cleaned_citations.append({
            'claim': norm(str(item['claim'])),
            'source_snippet': norm(str(item['source_snippet'])),
        })

    return {
        'extraction_quality': extraction_quality,
        'headline': headline,
        'guidance': cleaned_guidance,
        'stated_figures': cleaned_figures,
        'positives': cleaned_positives,
        'negatives': cleaned_negatives,
        'outlook': outlook,
        'disclosure_flags': cleaned_flags,
        'citations': cleaned_citations,
    }


def build_filing_summary_user_prompt(filing: dict[str, Any], filing_text: str) -> str:
    metadata = {
        'ticker': filing.get('ticker', ''),
        'form_type': filing.get('form_type', ''),
        'item_codes': filing.get('item_codes', []),
        'filed_at': filing.get('filed_at', ''),
        'period_of_report': filing.get('period_of_report', ''),
    }

    import json
    prompt = 'Filing metadata:\n' + json.dumps(metadata, ensure_ascii=False, indent=2)
    prompt += '\n\nFiling text:\n' + (filing_text or '(no text available)')
    prompt += '\n\nReturn strict JSON only.'
    return prompt


def summarize_filing(
    filing: dict[str, Any],
    filing_text: str,
    *,
    system_prompt: str,
    models: list[str],
    api_key: str,
    base_url: str,
    timeout: int,
    max_attempts: int,
    cache_system_prompt: bool = True,
) -> dict[str, Any]:
    """Stage 1 of the two-stage filing pipeline: extract a structured, cited
    summary of the filing's PROSE content only (no financial-statement-table
    figures - those come from XBRL separately and are merged in by the
    caller, never by this LLM call, to avoid restating a number the filer's
    own structured data already gives exactly). Returns the summary dict
    validated by validate_filing_summary().

    This never sees xbrl_facts/trailing_quarterly_revenue/company_overview -
    those bypass the summarizer entirely and are passed directly into
    classify_filing()'s stage 2 prompt by the caller, unchanged.
    """
    user_prompt = build_filing_summary_user_prompt(filing, filing_text)
    # 1800 was too tight for content-dense filings (confirmed empirically
    # this session: two real DV 8.01 filings - a ~$2.15B M&A announcement
    # and its follow-up merger agreement - both hit finish_reason=length
    # and failed JSON parsing at 1800, succeeded cleanly at 3000 using only
    # ~1500 of the 3000 tokens available). 10-Q/10-K need more headroom
    # still - their MD&A section packs many more line-item revenue/expense
    # comparisons than a typical 8-K exhibit (confirmed on a real TSLA
    # 10-Q: truncated at 3000, completed cleanly at 4500 using ~2700).
    #
    # Even 4500 was not a safe ceiling: a live 6,461-filing production run
    # showed 54 filings still hitting finish_reason=length exactly at their
    # cap (3000 or 4500) - i.e. ~0.8% of all filings, not a rare fluke. A
    # real CRWV 10-K in that set needed 4,216 tokens (extract_filing_sections
    # returned 82K chars for it, vs. ~27-30K for the filings the original
    # 4500 figure was tuned against) - some filers' MD&A sections are simply
    # much longer than the samples used to pick any single fixed cap.
    # Rather than guess a new fixed ceiling that could still be wrong for
    # the next outlier, escalate max_tokens on each retry instead of reusing
    # the same value twice (which just fails the same way twice, as
    # confirmed in that production run - every one of the 54 failed
    # IDENTICALLY on both attempts). Base caps unchanged (3000 8-K / 4500
    # 10-K/10-Q, both already cover the large majority of real filings);
    # a length-truncated retry gets 1.5x more headroom each time.
    base_max_tokens = 4500 if filing.get('form_type') in ('10-K', '10-Q') else 3000
    errors: list[str] = []
    for model in models:
        for attempt in range(1, max_attempts + 1):
            attempt_max_tokens = int(base_max_tokens * (1.5 ** (attempt - 1)))
            try:
                return classify_with_model(
                    system_prompt, user_prompt, model, api_key, base_url, timeout,
                    validator=validate_filing_summary,
                    max_tokens=attempt_max_tokens,
                    cache_system_prompt=cache_system_prompt,
                    stage='sec_filing_summary',
                    article_id=filing.get('accession_number'),
                )
            except Exception as exc:
                errors.append(f'{model} attempt {attempt} (max_tokens={attempt_max_tokens}): {exc}')
                time.sleep(0.25)
                continue
    raise RuntimeError('Filing summarization failed: ' + ' | '.join(errors))


def render_summary_as_filing_text(summary: dict[str, Any]) -> str:
    """Flatten a validated filing summary back into a text block for
    classify_filing()'s existing filing_text-shaped prompt slot - stage 2's
    prompt/validation logic is unchanged, it just reads a summary instead of
    raw fetched text.
    """
    import json
    return json.dumps(summary, ensure_ascii=False, indent=2)


def classify_filing_two_stage(
    filing: dict[str, Any],
    filing_text: str,
    *,
    summary_system_prompt: str,
    classify_system_prompt: str,
    models: list[str],
    api_key: str,
    base_url: str,
    timeout: int,
    max_attempts: int,
    extraction_found: bool = True,
    cache_system_prompt: bool = True,
    company_overview: dict[str, Any] | None = None,
    xbrl_facts: dict[str, Any] | None = None,
    exhibit_fetch_status: str = 'not_applicable',
    trailing_quarterly_revenue: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Two-stage filing pipeline: summarize_filing() (stage 1, narrative only)
    -> classify_filing() (stage 2) fed the summary instead of raw filing_text.
    xbrl_facts/trailing_quarterly_revenue/company_overview keep bypassing the
    summarizer entirely, exactly as they already bypass stage 2's own prose
    handling - only filing_text itself is replaced with the summary.

    Stage 1 is skipped (same as classify_filing()'s own extraction_found=False
    short-circuit) when extraction_found is False - there is no reliable text
    to summarize, so summarizing it would produce a confident-looking but
    ungrounded result, same reasoning as classify_filing()'s existing check.

    STAGE 2 IS TEMPORARILY DISABLED (commented out below) while stage 1 is
    being validated on its own - every filing gets a placeholder
    classification and only the real stage 1 summary in filing_summary.
    Uncomment the classify_filing() call and its return to re-enable.

    The placeholder's signal_detection is 'waiting' (not judged yet), never
    'noise' (judged and found ordinary) - same meaning 'waiting' has for
    geopolitical_signal and china_market_signal. It bypasses
    validate_filing_classification (ALLOWED_SIGNAL is stage 2's output set);
    the DB CHECK already allows 'waiting'. The extraction_found=False path
    above still returns classify_filing()'s real 'noise' result.
    """
    if not extraction_found:
        return classify_filing(
            filing, filing_text,
            system_prompt=classify_system_prompt,
            models=models,
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            max_attempts=max_attempts,
            extraction_found=False,
            cache_system_prompt=cache_system_prompt,
            company_overview=company_overview,
            xbrl_facts=xbrl_facts,
            exhibit_fetch_status=exhibit_fetch_status,
            trailing_quarterly_revenue=trailing_quarterly_revenue,
        )

    summary = summarize_filing(
        filing, filing_text,
        system_prompt=summary_system_prompt,
        models=models,
        api_key=api_key,
        base_url=base_url,
        timeout=timeout,
        max_attempts=max_attempts,
        cache_system_prompt=cache_system_prompt,
    )

    # --- STAGE 2 (commented out for now - stage 1 only) ---
    # summary_text = render_summary_as_filing_text(summary)
    # result = classify_filing(
    #     filing, summary_text,
    #     system_prompt=classify_system_prompt,
    #     models=models,
    #     api_key=api_key,
    #     base_url=base_url,
    #     timeout=timeout,
    #     max_attempts=max_attempts,
    #     extraction_found=True,
    #     cache_system_prompt=cache_system_prompt,
    #     company_overview=company_overview,
    #     xbrl_facts=xbrl_facts,
    #     exhibit_fetch_status=exhibit_fetch_status,
    #     trailing_quarterly_revenue=trailing_quarterly_revenue,
    # )
    # result['filing_summary'] = summary
    # return result

    return {
        'signal_detection': 'waiting',
        'signal_score': 0.0,
        'signal_reason': 'Stage 2 classification is disabled - this row holds only the stage 1 summary, not a real classification.',
        'materiality': 'none',
        'already_disclosed': False,
        'extraction_quality': summary.get('extraction_quality', 'full'),
        'entities': [],
        'ticker': filing.get('ticker', ''),
        'form_type': filing.get('form_type', ''),
        'item_codes': filing.get('item_codes', []),
        'accession_number': filing.get('accession_number', ''),
        'filed_at': filing.get('filed_at', ''),
        'primary_doc_url': filing.get('primary_doc_url', ''),
        'filing_summary': summary,
    }
