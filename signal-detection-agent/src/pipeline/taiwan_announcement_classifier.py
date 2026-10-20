"""Rating + English summary for Taiwan material announcements
(taiwan_market_signal, source_category mops_material).

Why a model, and where not. A price backtest over ~4,300 historical
announcements (2019-2026, our 20 companies) showed the clause a filing is
made under often cannot decide whether it matters: the catch-all clauses,
clause 11 (a routine bond issue vs a merger) and clause 17 (a meeting
notice vs a merger vote on the agenda) all depend on what the text says.
Those go to one SEC_FILING_MODEL call that rates the filing and writes the
summary. Clauses whose filings moved the price no more than
an ordinary day get a fixed NOISE and no model call.

The model answers with the same names as the stored columns:
signal_detection (signal / weak_signal / noise) and signal_reason (one or
two sentences naming the facts in the filing that decided it).

Stored in EXISTING fields only (no new metadata keys). Companies named in the
filing go to the existing entities_json / entity_names_normalized columns, like
the SEC classifier's - the filing company from our ticker list, the others from
the model, each only if its name appears in the filing.

Stored (metadata, besides the signal_detection / signal_reason columns):
  translated_title    literal English translation of the subject line
                      (written by the translation step)
  translated_summary  the model's summary + key facts, checked against the
                      filing - every filing, fixed-NOISE ones included
  translated_body     literal translation of the whole filing, ONLY when no
                      summary passed the checks (written by the translation
                      step), so a row carries one or the other, never both
The HIGH/WEAK/NOISE label and its reason travel in
announcement_materiality_signal / _reason in memory and are popped before
insert - the columns hold them.

Guardrails, in order:
  1. Taiwan-calendar dates are converted here, deterministically, before
     the model sees the text - the model never converts a date.
  2. The filing is passed as delimited data; the prompt tells the model
     to treat anything inside it as text, not instructions.
  3. JSON mode, temperature 0, a fixed signal_detection set, and a
     signal_reason that must pass the same checks as the summary.
  4. Every number, Chinese name and company-style English name in the
     output must appear in the filing; month names and figure-free
     quantity words ("doubled") are refused. A refused answer gets up to
     TWO repair attempts, each told which rule it broke and checked again
     in full. If it still fails, the summary is discarded and the row gets
     the literal translation; the label and reason are kept if they pass
     the checks on their own. Only when they fail too is the row stored
     as weak_signal with a reason saying so, so an unverified answer is
     never shown and a possibly material filing is never hidden as noise.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.request import Request, urlopen

from pypinyin import Style, pinyin
from pypinyin.constants import PINYIN_DICT

import config
from pipeline.taiwan_listed_companies import TAIWAN_LISTED_COMPANIES

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Taiwan (ROC / 民國) calendar -> Western, before anything else reads it.
# Years 100-130 only (2011-2041): a bare 3-digit number outside that range is
# far more likely an amount than a year. Seven-digit forms (1151014) are left
# alone - inside free text they cannot be told apart from an amount.
# ---------------------------------------------------------------------------
_Y = r"(1[0-3]\d)"
_ROC_RULES: list[tuple[re.Pattern, Any]] = []


def _ymd(m: re.Match) -> str:
    y, mo, d = int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3))
    if 1 <= mo <= 12 and 1 <= d <= 31:
        return f"{y}-{mo:02d}-{d:02d}"
    return m.group(0)


def _ym(m: re.Match) -> str:
    y, mo = int(m.group(1)) + 1911, int(m.group(2))
    return f"{y}-{mo:02d}" if 1 <= mo <= 12 else m.group(0)


_ROC_RULES = [
    # 民國115年06月24日 / 115年 6 月 24 日 (whitespace from collapsed line breaks allowed)
    (re.compile(r"(?:民國\s*)?(?<!\d)" + _Y + r"\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"), _ymd),
    # 115/10/14 and 115.10.14
    (re.compile(r"(?<![\d/.])" + _Y + r"[/.](\d{1,2})[/.](\d{1,2})(?![\d/.])"), _ymd),
    # 115年7月 (no day)
    (re.compile(r"(?:民國\s*)?(?<!\d)" + _Y + r"\s*年\s*(\d{1,2})\s*月"), _ym),
    # 115年度, 115年第3季, 民國115年 -> 2026年度 / 2026年第3季 / 2026年
    (re.compile(r"(?:民國\s*)?(?<!\d)" + _Y + r"\s*年"), lambda m: f"{int(m.group(1)) + 1911}年"),
    # Western years written the Chinese way (TSMC: "2025年4月營收報告") -> 2025-04.
    # Left as is, the model wrote "April 2025" and the answer was refused.
    (re.compile(r"(?<!\d)((?:19|20)\d\d)\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"),
     lambda m: _ymd_western(m)),
    (re.compile(r"(?<!\d)((?:19|20)\d\d)\s*年\s*(\d{1,2})\s*月(?!\s*\d{1,2}\s*日)"),
     lambda m: f"{m.group(1)}-{int(m.group(2)):02d}" if 1 <= int(m.group(2)) <= 12 else m.group(0)),
]


def _ymd_western(m: re.Match) -> str:
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return f"{y}-{mo:02d}-{d:02d}" if 1 <= mo <= 12 and 1 <= d <= 31 else m.group(0)


# Years written in Chinese numerals, digit by digit ("一一四年" = 114 = 2025).
# Seen live in shareholder-meeting subjects; left unconverted, the model
# guessed the Western year (wrongly) and the answer had to be refused.
_CN_DIGIT = {"〇": "0", "○": "0", "零": "0", "一": "1", "二": "2", "三": "3", "四": "4",
             "五": "5", "六": "6", "七": "7", "八": "8", "九": "9"}
_CN_YEAR_RE = re.compile("(?:民國\\s*)?([一][〇○零一二三][〇○零一二三四五六七八九])\\s*年")


# Month and day in Chinese numerals after a digit year ("2019年六月二十一日",
# seen in a Unimicron filing): the model wrote "2019-06-21" and the digits
# 6 and 21 were nowhere in the filing, so a correct date was refused.
_CN_MD_RE = re.compile(r"((?:19|20)\d\d|1[0-3]\d)\s*年\s*([一二三四五六七八九十]{1,3})\s*月"
                       r"(?:\s*([一二三四五六七八九十]{1,3})\s*日)?")


_CN_QUARTER_RE = re.compile(r"第([一二三四])季")


def _cn_small(text: str) -> int:
    if "十" not in text:
        return int("".join(_CN_DIGIT[c] for c in text))
    tens, _, ones = text.partition("十")
    return (int(_CN_DIGIT[tens]) if tens else 1) * 10 + (int(_CN_DIGIT[ones]) if ones else 0)


def _cn_month_day(m: re.Match) -> str:
    try:
        mo = _cn_small(m.group(2))
        d = _cn_small(m.group(3)) if m.group(3) else None
    except (KeyError, ValueError):
        return m.group(0)
    if not 1 <= mo <= 12 or (d is not None and not 1 <= d <= 31):
        return m.group(0)
    return f"{m.group(1)}年{mo}月" + (f"{d}日" if d is not None else "")


def _cn_year(m: re.Match) -> str:
    roc = int("".join(_CN_DIGIT[c] for c in m.group(1)))
    return f"{roc + 1911}年" if 100 <= roc <= 139 else m.group(0)


# Full-width digits and separators (１１１／０３／０１) appear in some filings;
# made ASCII first so the date rules below and the number check both see them.
_FULLWIDTH = str.maketrans("０１２３４５６７８９／", "0123456789/")  # digits only - Chinese punctuation stays


# Amounts written in Chinese numerals (新台幣壹佰貳拾壹億伍仟萬元). The model
# turned these into digits that appear nowhere in the filing, so a correct
# figure was refused; converting them first lets the number check see them.
_CN_NUM = {"零": 0, "〇": 0, "一": 1, "壹": 1, "二": 2, "貳": 2, "兩": 2, "三": 3, "參": 3,
           "四": 4, "肆": 4, "五": 5, "伍": 5, "六": 6, "陸": 6, "七": 7, "柒": 7,
           "八": 8, "捌": 8, "九": 9, "玖": 9}
_CN_SMALL = {"十": 10, "拾": 10, "百": 100, "佰": 100, "千": 1000, "仟": 1000}
_CN_BIG = {"萬": 10_000, "億": 100_000_000}
_CN_AMOUNT_RE = re.compile(
    "([零〇一壹二貳兩三參四肆五伍六陸七柒八捌九玖十拾百佰千仟萬億]{2,})(?=(?:元|股|美元|美金|日圓|人民幣))")


def _cn_to_int(text: str) -> int | None:
    if not any(c in _CN_SMALL or c in _CN_BIG for c in text):
        return None  # digit-by-digit forms (years) are handled elsewhere
    total, section, digit = 0, 0, 0
    for c in text:
        if c in _CN_NUM:
            digit = _CN_NUM[c]
        elif c in _CN_SMALL:
            section += (digit or 1) * _CN_SMALL[c]; digit = 0
        elif c in _CN_BIG:
            total += (section + digit) * _CN_BIG[c]; section, digit = 0, 0
    return total + section + digit


_DIGITS_BEFORE_RE = re.compile(r"[\d.,]\s*$")


def normalize_amounts(text: str) -> str:
    def repl(m: re.Match) -> str:
        # After Arabic digits the characters are a UNIT, not a number:
        # "441佰萬元" is 441 million, and converting "佰萬" alone turned it
        # into "4411000000元" - a ten-times error the model then copied
        # (seen on Delta and TSMC filings).
        if _DIGITS_BEFORE_RE.search(text[max(0, m.start() - 3):m.start()]):
            return m.group(0)
        v = _cn_to_int(m.group(1))
        return str(v) if v else m.group(0)
    return _CN_AMOUNT_RE.sub(repl, text)


def normalize_roc_dates(text: str | None) -> str:
    """Rewrite every Taiwan-calendar date in `text` as a Western date (and
    Chinese-numeral amounts as digits)."""
    out = _CN_YEAR_RE.sub(_cn_year, (text or "").translate(_FULLWIDTH))
    out = _CN_MD_RE.sub(_cn_month_day, out)
    # 第一季 -> 第1季: the model writes "Q1", and a "1" that appears nowhere in
    # the filing was refused (Unimicron's "2026第一季 富邦企業日").
    out = _CN_QUARTER_RE.sub(lambda m: f"第{_CN_DIGIT[m.group(1)]}季", out)
    # 矽品精密工業(股)公司 is the standard short form of 股份有限公司; the model
    # writes the full form, which the filing text then did not contain.
    out = re.sub(r"[（(]股[)）]公司", "股份有限公司", out)
    # Line breaks inside a Chinese name survive as spaces ("昆山鼎昌鑫電子
    # 科技有限公司"); Chinese never puts a space between two characters.
    out = re.sub(r"(?<=[㐀-鿿豈-﫿])\s+(?=[㐀-鿿豈-﫿])", "", out)
    out = normalize_amounts(out)
    for pattern, repl in _ROC_RULES:
        out = pattern.sub(repl, out)
    return out


# ---------------------------------------------------------------------------
# 2. Fixed NOISE - no model call.
# From the backtest (solo filings, 95% interval vs the 10% ordinary-day
# big-move rate): investor conference 11% (8-14%), shareholder-meeting
# resolutions 8%, loans to others 6%, guarantees 15% (3-32%), capital
# reduction / par value 17% (4-34%). Clause 31 splits in two: results
# approved 38% big moves, the notice of the meeting DATE 14% - so only the
# date notice is fixed NOISE. Same clause numbers on TWSE and TPEx (the two
# lists only diverge after clause 46).
# ---------------------------------------------------------------------------
_FIXED_NOISE_CLAUSES = frozenset({12, 18, 22, 23, 36})
_MEETING_DATE_NOTICE_RE = re.compile(r"預計召開|召開日期|召集")
_ON_BEHALF_OF_SUBSIDIARY_RE = re.compile(r"代.{0,6}子公司")
_DIVIDEND_TIMETABLE_RE = re.compile(r"基準日|除息|除權")
# A per-share dividend recalculated because the share count changed (bond
# conversions, buybacks, employee shares) - the total is unchanged. All 58
# such clause-14 subjects in 2019-2026 give a share-count cause; the model
# rated one weak_signal and all five reviewers said noise (2026-10-19).
_FIRM_CHANGE_RE = re.compile(r"(?:更換|變更)會計師事務所")
_AUDIT_FIRM_RE = re.compile(r"事務所名稱[^:：]{0,10}[:：](.{2,14}?事務所)")
_ROLE_FIELD_RE = re.compile(r"人員變動別（[^）]*）[:：](.{1,30}?)(?=\d{1,2}\.)")
_ROUTINE_ROLE_RE = re.compile(r"(?:代理)?發言人|公司治理主管|(?:內部)?稽核主管|會計主管(?:\(代理財務主管\)|（代理財務主管）)?|"
                              r"資訊安全長|資安長|資訊安全主管|訴訟及非訟代理人")
_DIVIDEND_ADJUST_RE = re.compile(r"調整.{0,12}(?:配息率|配股率|配股配息率|每股現金股利金額)|(?:配息率|配股率)之調整")
# A supplement or correction of an earlier filing: the news was the first
# announcement, so the rating is capped at WEAK. The prompt said so and the
# model still rated a 補充公告 of a NT$6.95bn land deal signal, twice.
_SUPPLEMENT_RE = re.compile(r"補充|更正|補正")
# A disclosure the exchange required after unusual trading (注意交易資訊,
# 異常): it repeats figures already published, so it is capped at WEAK. The
# prompt says so; the model still rated one signal in the 160-card rerun.
_UNUSUAL_TRADING_RE = re.compile(r"注意交易資訊|異常|有價證券於集中交易市場達")
# Money into an investment vehicle (a venture-capital or investment company)
# is not a new operating business: capped at WEAK. The prompt excludes it;
# the model still rated Wistron's new venture-capital subsidiary signal.
_INVESTMENT_VEHICLE_RE = re.compile(r"創業投資|投資(?:股份)?有限公司|Venture Capital|Investment Co", re.I)


def _clause_number(code: str | None) -> int | None:
    m = re.search(r"\d+", code or "")
    return int(m.group()) if m else None


# Dividend decisions (clause 14) on the standard form: cash per share in
# fields (1)-(3), stock per share in (5)-(7), par value in field 6. Two
# outcomes are read from the figures by rule, because the model ignored the
# prompt on both (reviewer, 2026-10-19): no dividend at all is NOISE, and a
# stock dividend of half the par value or more - Wiwynn's NT$20 per NT$10
# share is two new shares for every one held - is HIGH.
_DIV_FIELD_RE = re.compile(r"[（(]([1-8])[)）]([^：:（(]|[（(][^)）]{1,8}[)）]){2,40}?[：:]\s*([\d,]+(?:\.\d+)?)")
_PAR_RE = re.compile(r"每股面額[^：:]*[：:]\s*新(?:台|臺)幣\s*([\d.]+)")
_NO_DIVIDEND_RE = re.compile(r"不(?:分配|發放|配發)[^。]{0,20}股利")


def dividend_figures(body: str) -> dict[str, float] | None:
    """{cash, stock, par} per share from a clause-14 dividend form, or None
    when the form fields are not there."""
    fields = {int(n): float(v.replace(",", "")) for n, _, v in _DIV_FIELD_RE.findall(body or "")}
    if not {1, 5} <= set(fields):
        return None
    par = _PAR_RE.search(body or "")
    return {"cash": sum(fields.get(i, 0.0) for i in (1, 2, 3)),
            "stock": sum(fields.get(i, 0.0) for i in (5, 6, 7)),
            "par": float(par.group(1)) if par else 10.0}


def _fmt(x: float) -> str:
    return f"{x:,.2f}".rstrip("0").rstrip(".")


def large_stock_dividend_reason(meta: dict[str, Any], body: str) -> str | None:
    """The signal_reason for a stock dividend of at least half the par value,
    or None. Built from the form's own figures."""
    if _clause_number(meta.get("disclosure_clause_code")) != 14:
        return None
    d = dividend_figures(body)
    if not d or d["par"] <= 0 or d["stock"] < d["par"] / 2:
        return None
    ratio = d["stock"] / d["par"]
    cash = f" plus a cash dividend of NT${_fmt(d['cash'])} per share" if d["cash"] else ""
    return (f"A stock dividend of NT${_fmt(d['stock'])} per NT${_fmt(d['par'])}-par share{cash}: "
            f"{_fmt(ratio)} new share{'s' if ratio != 1 else ''} for every share held, which changes "
            f"the share count, not just the payout.")


# A board election form lists the old and the new directors. In a scheduled
# election at the end of the term (任期屆滿) the form's "a third or more of
# the directors changed" box (屬三分之一以上董事發生變動) reads 是 whoever is
# elected, and the model took it as turnover: Lotes 2019 re-elected all 9 and
# Delta 2024 replaced 1 of 12, both rated signal; all five reviewers said
# noise (2026-10-19). The seats that really change are counted from the lists.
_SEAT_TITLE_RE = re.compile(r"(?:副董事長|董事長|獨立董事|法人董事|自然人董事|董事|監察人)\s*[：:]")
_SEAT_NAME_RE = re.compile(r"(?:[㐀-鿿]{2,4}|[A-Za-z][A-Za-z .\-]{1,28}|[㐀-鿿]{2,4}/[^/]{1,12})")


def _form_field(body: str, label: str) -> str | None:
    m = re.search(r"\d{1,2}\.\s*" + label + r"[^:：]{0,20}[:：](.*?)(?=\s\d{1,2}\.\D|$)", body)
    return m.group(1) if m else None


def board_seat_changes(body: str) -> tuple[list[str], int] | None:
    """(new directors not on the old list, seats elected) for a term-end
    board election, or None when the form cannot be read cleanly - then
    nothing is counted and the model judges alone."""
    if "任期屆滿" not in (body or ""):
        return None
    old, new = _form_field(body, "舊任者職稱"), _form_field(body, "新任者職稱")
    if not old or not new:
        return None
    names = []
    for entry in _SEAT_TITLE_RE.split(new)[1:]:
        name = entry.split("、")[0]
        if "代表人" in name:
            name = name.split("代表人", 1)[1]
        names.append(re.split(r"先生|女士|[(（]", name)[0].strip("：: "))
    # Whole boards only, every entry a plain name: other layouts ran
    # biographies into the names and gave nonsense counts.
    if len(names) < 3 or not all(_SEAT_NAME_RE.fullmatch(n) for n in names):
        return None
    return [n for n in names if n.split("/")[0] not in old], len(names)


def board_unchanged_reason(meta: dict[str, Any], body: str) -> str | None:
    """The signal_reason for a term-end election that re-elected everyone."""
    if _clause_number(meta.get("disclosure_clause_code")) != 6:
        return None
    seats = board_seat_changes(body)
    if not seats or seats[0]:
        return None
    return (f"A scheduled board election at the end of the term in which all {seats[1]} directors "
            f"and supervisors were re-elected: the board is unchanged.")


def fixed_noise_reason(meta: dict[str, Any], subject: str, body: str = "") -> str | None:
    """The fixed-NOISE reason for this filing, or None if it needs the model."""
    n = _clause_number(meta.get("disclosure_clause_code"))
    if n == 14:
        d = dividend_figures(body)
        if (d and d["cash"] == 0 and d["stock"] == 0) or _NO_DIVIDEND_RE.search(body or ""):
            return _FIXED_NOISE_REASON["no_dividend"]
    if n in _FIXED_NOISE_CLAUSES:
        return _FIXED_NOISE_REASON[n]
    # A dividend decided by a subsidiary (clause 14 filed "代...子公司") is
    # paid to the parent - money moving inside the group. The prompt said so
    # and the model still rated 2 of 3 such filings weak_signal; all four
    # reviewers said noise.
    if n == 14 and _ON_BEHALF_OF_SUBSIDIARY_RE.search(subject or ""):
        return _FIXED_NOISE_REASON["subsidiary_dividend"]
    # The ex-dividend / record-date timetable of a dividend decided earlier.
    # In 2019-2026 history every clause-14 subject naming 基準日, 除息 or 除權
    # (185 filings) is this timetable; the decision itself is a separate
    # filing. All four reviewers said noise; the model said weak_signal on 3 of 3.
    if n == 14 and _DIVIDEND_TIMETABLE_RE.search(subject or ""):
        return _FIXED_NOISE_REASON["dividend_timetable"]
    if n == 14 and _DIVIDEND_ADJUST_RE.search(re.sub(r"\s+", "", subject or "")):
        return _FIXED_NOISE_REASON["dividend_adjustment"]
    # Officer-change form (clause 8) naming only routine posts - spokesperson,
    # governance, internal audit, accounting, information security (about 100
    # of 2019-2026's 200). The prompt lists them as noise; Elite Material's
    # accounting head (also acting finance head) was rated weak, five of
    # five reviewers said noise (2026-10-19). CFO, CEO, COO ... stay with the model.
    # Auditor change (clause 7) inside the same firm - partner rotation,
    # noise by the prompt; Unimicron's was rated weak in 1 of 2 runs. A change
    # of audit FIRM (two firms named, or 更換會計師事務所) stays with the model.
    if n == 7 and not _FIRM_CHANGE_RE.search(re.sub(r"\s+", "", subject or "")) \
            and len(set(_AUDIT_FIRM_RE.findall(re.sub(r"\s+", "", body or "")))) == 1:
        return _FIXED_NOISE_REASON["audit_partner"]
    if n == 8 and (roles := _ROLE_FIELD_RE.search(re.sub(r"\s+", "", body or ""))):
        if all(_ROUTINE_ROLE_RE.fullmatch(p) for p in re.split(r"[、及暨,，]", roles.group(1)) if p):
            return _FIXED_NOISE_REASON["routine_officer"]
    if n == 31 and _MEETING_DATE_NOTICE_RE.search(subject or ""):
        return _FIXED_NOISE_REASON[31]
    return None


# ---------------------------------------------------------------------------
# 3. The model call.
# ---------------------------------------------------------------------------
# Model answer (column vocabulary) -> this pipeline's HIGH/WEAK/NOISE.
_SIGNAL_DETECTION = {"signal": "HIGH", "weak_signal": "WEAK", "noise": "NOISE"}
_DETECTION_OF = {v: k for k, v in _SIGNAL_DETECTION.items()}
# Counted without the bracketed Chinese after names: two "English (原文)"
# names put a two-sentence reason at 486 characters against the old 400.
_REASON_MIN, _REASON_MAX = 20, 500


def _reason_length_ok(reason: str) -> bool:
    return _REASON_MIN <= len(_BRACKETED_RE.sub("", reason).strip()) <= _REASON_MAX

# signal_reason for rows the model never sees, or whose answer was refused.
_FIXED_NOISE_REASON = {
    12: "A notice that the company will hold or attend an investor conference. Such notices give "
        "no new business information and have not moved the share price more than an ordinary "
        "day (2019-2026).",
    18: "A report of resolutions passed at a shareholders' meeting. Such reports have not moved "
        "the share price more than an ordinary day (2019-2026).",
    22: "A notice of a guarantee the company gave for another company's borrowing. Such notices "
        "have not moved the share price more than an ordinary day (2019-2026).",
    23: "A notice of funds the company lent to another company. Such notices have not moved the "
        "share price more than an ordinary day (2019-2026).",
    36: "A procedural step in a capital reduction or a change of share par value. Such notices have "
        "not moved the share price more than an ordinary day (2019-2026).",
    31: "A notice of the date of the board meeting that will approve the financial report - the "
        "results themselves come later. Such notices have not moved the share price more than an "
        "ordinary day (2019-2026).",
    "no_dividend": "The board decided to pay no dividend for this period. Nothing is paid out, so "
                   "there is no new information on shareholder returns beyond the decision itself.",
    "dividend_timetable": "The ex-dividend or record-date timetable of a dividend whose amount "
                          "was decided earlier. The dates are administrative; the amount was "
                          "news when it was first decided.",
    "audit_partner": "The audit firm rotated the partners who sign the company's accounts; the "
                     "audit firm itself is unchanged.",
    "routine_officer": "A change of a routine officer post - spokesperson, corporate-governance, "
                       "internal-audit, accounting or information-security head. These posts do "
                       "not run the business.",
    "dividend_adjustment": "The per-share dividend was recalculated because the number of shares "
                           "changed; the total amount decided earlier is unchanged.",
    "subsidiary_dividend": "A dividend decided by a subsidiary, paid to its parent company. The "
                           "money stays inside the group, so nothing changes for the company's "
                           "outside shareholders.",
}
_FALLBACK_REASON = {
    "model_failed": "This filing could not be assessed automatically, so it is shown as possibly "
                    "relevant rather than hidden. The text below is a direct translation of the "
                    "filing.",
    "model_output_rejected": "The automatic summary of this filing could not be checked against "
                             "the filing itself, so it was discarded. The filing is shown as "
                             "possibly relevant, with a direct translation below.",
}
_SUMMARY_MAX = 1500
_KEY_FACTS_MAX = 8
_ENTITIES_MAX = 10


def _system_prompt() -> str:
    return config.TAIWAN_ANNOUNCEMENT_PROMPT.read_text(encoding="utf-8")


def _user_message(meta: dict[str, Any], subject: str, body: str) -> str:
    return (
        "<filing>\n"
        f"Company: {meta.get('translated_company_name') or ''} "
        f"({meta.get('company_name') or ''}), code {meta.get('ticker') or ''}, "
        f"{meta.get('exchange') or ''}\n"
        f"Clause: {meta.get('disclosure_clause_code') or ''} - "
        f"{meta.get('clause_label') or 'meaning not known'}\n"
        f"Filed: {meta.get('statement_date') or ''}\n"
        f"Subject (主旨): {subject}\n"
        f"Text (說明): {body}\n"
        + "".join(f"Known English name - use exactly: {zh} = {en}\n" for zh, en in known_names_in(subject + body).items())
        + _seat_line(meta, body)
        + "</filing>"
    )


def _seat_line(meta: dict[str, Any], body: str) -> str:
    seats = board_seat_changes(body) if _clause_number(meta.get("disclosure_clause_code")) == 6 else None
    if not seats:
        return ""
    who = f": {', '.join(seats[0])}" if seats[0] else ""
    return (f"Counted from the form's old and new lists: {len(seats[0])} of {seats[1]} seats go to a "
            f"person not on the old board{who}. In a term-end election the form's 屬三分之一以上董事"
            f"發生變動 box says 是 whoever is elected - use this count, not the box. Fewer than a "
            f"third changed is noise, or weak_signal if the founder, the chairman or the CEO leaves "
            f"the board.\n")


# Spend of the current run. The instructions (~4,600 tokens, ~94% of every
# call's input) are the same for every filing and come first, so the provider
# caches them automatically - measured: 3,968 of 4,594 input tokens cached,
# $0.0041 instead of $0.0071 per call. Keep anything that varies per filing
# OUT of the system prompt, or that cache is lost.
_usage_lock = threading.Lock()
_usage = {"calls": 0, "prompt": 0, "cached": 0, "completion": 0, "cost": 0.0}


def _record_usage(u: dict[str, Any]) -> None:
    with _usage_lock:
        _usage["calls"] += 1
        _usage["prompt"] += u.get("prompt_tokens") or 0
        _usage["cached"] += (u.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
        _usage["completion"] += u.get("completion_tokens") or 0
        _usage["cost"] += float(u.get("cost") or 0)


def _call_model(messages: list[dict[str, str]]) -> str | None:
    """One chat completion against the OpenAI-compatible endpoint, raw
    HTTP like the rest of this service. None on any failure."""
    payload = {
        "model": config.SEC_FILING_MODEL,
        "temperature": 0,
        "max_tokens": 1500,
        "response_format": {"type": "json_object"},
        "messages": messages,
        # OpenRouter returns each call's cost; summed per run in the log.
        "usage": {"include": True},
    }
    try:
        req = Request(
            f"{config.OPENAI_BASE_URL}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Authorization": f"Bearer {config.OPENAI_API_KEY}",
                     "Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(req, timeout=config.OPENAI_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        _record_usage(data.get("usage") or {})
        return (data.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()
    except Exception as exc:
        logger.warning("[TAIWAN_ANNOUNCEMENT] model call failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# 4. Output checks.
# ---------------------------------------------------------------------------
_NUMBER_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")
_MONTH_NAME_RE = re.compile(
    r"\b(jan(uary)?|feb(ruary)?|mar(ch)?|apr(il)?|may|june?|july?|aug(ust)?|"
    r"sep(t|tember)?|oct(ober)?|nov(ember)?|dec(ember)?)\b\.?\s*\d", re.I)
_MONTHS = {m: i for i, m in enumerate(
    ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), 1)}
_M = r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?"
_ISO_REWRITES = [
    # "January to August 2022" / "January-August 2022" -> "2022-01 to 2022-08"
    (re.compile(r"\b" + _M + r"\s*(?:to|through|and|-|–)\s*" + _M + r",?\s+(\d{4})\b", re.I),
     lambda m: f"{m.group(3)}-{_MONTHS[m.group(1)[:3].lower()]:02d} to "
               f"{m.group(3)}-{_MONTHS[m.group(2)[:3].lower()]:02d}"),
    (re.compile(r"\b" + _M + r"\s+(\d{1,2}),?\s+(\d{4})\b", re.I),
     lambda m: f"{m.group(3)}-{_MONTHS[m.group(1)[:3].lower()]:02d}-{int(m.group(2)):02d}"),
    (re.compile(r"\b(\d{1,2})\s+" + _M + r",?\s+(\d{4})\b", re.I),
     lambda m: f"{m.group(3)}-{_MONTHS[m.group(2)[:3].lower()]:02d}-{int(m.group(1)):02d}"),
    (re.compile(r"\b" + _M + r",?\s+(\d{4})\b", re.I),
     lambda m: f"{m.group(2)}-{_MONTHS[m.group(1)[:3].lower()]:02d}"),
]


def iso_dates(text: str) -> str:
    """"April 2025" -> "2025-04", "October 14, 2026" -> "2026-10-14". The
    model wrote revenue months this way on every attempt (TSMC, BizLink); the
    date was right, only the format broke the rule. Rewritten here, it then
    goes through the same number check as any other date. A month name with
    no year cannot be rewritten and is still refused."""
    for pattern, repl in _ISO_REWRITES:
        text = pattern.sub(repl, text)
    return text


_VAGUE_QUANTITY_RE = re.compile(r"\b(doubl\w*|tripl\w*|quadrupl\w*|dozens?|halved|significant(ly)?)\b", re.I)
# One nesting level: "(勤昆科技(昆山)有限公司)" is one bracketed name.
_BRACKETED_RE = re.compile(r"[（(](?:[^()（）]|[（(][^()（）]*[)）])*[)）]")
# Predictions and generalisations: the prompt forbids them, but the live model
# still wrote "potential operational and financial implications" and "does not
# usually affect company operations" - neither is in any filing. Refused here
# so the repair attempt is told exactly which phrase to drop.
_SPECULATION_RE = re.compile(
    r"\b(potential(ly)?|could|might|likely|unlikely|usually|typically|generally|"
    r"expected to|is set to|may (affect|impact|lead|result|cause|influence|improve|weaken))\b", re.I)
_CJK_RUN_RE = re.compile(r"[㐀-鿿豈-﫿]{2,}")
_ENTITY_RE = re.compile(
    # Spaces, not \s: key facts are joined by newlines, and "...Germany GmbH\n
    # Holding after transaction" read as one name (2026-10-19).
    r"((?:[A-Z][\w&.'\-]*[ \t]+){0,6}[A-Z][\w&.'\-]*,?[ \t]+"
    r"(?:Co\.,?\s*Ltd\.?|Co\.|Ltd\.?|Inc\.?|Corp\.?|Corporation|Limited|LLC|GmbH|"
    r"Sdn\.?\s*Bhd\.?|B\.V\.|AG|S\.A\.|PLC|Pte\.?\s*Ltd\.?|"
    # English company-name endings: catches a Chinese-only name the model
    # romanised ("Taiwan Fulei Electronics") instead of keeping it as filed.
    r"Electronics|Technology|Technologies|Industrial|Industries|Industry|Holdings?|"
    r"Group|Bank|Securities|Semiconductors?|Precision|Investments?|Development))"
    # Whole word only: "Corp" inside "Corporate Day" made "Q1 Fubon Corp" a
    # company name and cost a retry. Nor the start of an event title: "Q4
    # Investment Forum", "Regional Investment Conference".
    # Nor a regulation's title: "Trial Measures ... of Overseas Securities
    # Offering and Listing" (CSRC filings, 2026-10-19).
    r"(?![A-Za-z])(?!\s+(?:Forum|Conference|Summit|Day|Seminar|Symposium|Expo|Week|Meeting|Roadshow|"
    r"Offering|Issuance|Listing|Act|Law|Regulations?|Measures|Rules|"
    # ... nor a place or a government office: "Ruifang Industrial Zones,
    # Industrial Development Bureau" in a meeting address.
    r"Zones?|Parks?|Areas?|Districts?|Estates?|Development|Bureau|Service)\b)")


# English words (no digits) directly followed by a bracketed Chinese name.
_GLOSS_RE = re.compile(
    r"((?:[A-Za-z&][A-Za-z&.,'\-]*(?:\s+\([A-Za-z ]+\))?\s+){0,11}[A-Za-z][A-Za-z&.,'\-]*\.?(?:\s+\([A-Za-z ]+\))?)"
    # The original may start in Latin letters: "LEONI Group (LEONI集團)".
    r"\s*[（(]((?:[A-Za-z0-9&.\-]+\s?)?[㐀-鿿豈-﫿](?:[^()（）]|[（(][^()（）]{0,20}[)）]){0,80})[)）]")
_NAME_LEAD_WORDS = frozenset({"the", "this", "that", "its", "their", "a", "an", "our", "each", "both"})


def _norm_num(token: str) -> str:
    # "02" in a YYYY-MM-DD date is the filing's "2"; compare without padding.
    digits = token.replace(",", "").rstrip(".")
    whole, dot, frac = digits.partition(".")
    return (whole.lstrip("0") or "0") + dot + frac


_SCALE = {"": 1, "仟": 1e3, "千": 1e3, "萬": 1e4, "百萬": 1e6, "佰萬": 1e6, "仟萬": 1e7, "千萬": 1e7, "億": 1e8,
          "百億": 1e10, "佰億": 1e10, "仟億": 1e11, "千億": 1e11, "兆": 1e12,
          "thousand": 1e3, "million": 1e6, "billion": 1e9, "trillion": 1e12}
_SOURCE_AMOUNT_RE = re.compile(r"(\d[\d,]*(?:\.\d+)?)\s*(百萬|佰萬|仟萬|千萬|百億|佰億|仟億|千億|億|萬|仟|千)?")
_OUTPUT_AMOUNT_RE = re.compile(r"(\d[\d,]*(?:\.\d+)?)\s*(百萬|佰萬|仟萬|千萬|百億|佰億|仟億|千億|億|萬|仟|千|thousand|million|billion|trillion)?", re.I)


_UNIT_WORDS = {"thousand": ("仟元", "千元"), "million": ("百萬", "佰萬"), "billion": ("十億",),
               "仟": ("仟",), "千": ("千",), "萬": ("萬",), "百萬": ("百萬",), "佰萬": ("佰萬",), "仟萬": ("仟萬",), "千萬": ("千萬",),
               "百億": ("百億",), "佰億": ("佰億",), "仟億": ("仟億",), "千億": ("千億",), "億": ("億",)}
# Standard English names that contain digits the filing writes in Chinese.
_STANDARD_TERMS = {"COVID-19": ("新冠", "COVID", "武漢肺炎", "冠狀病毒", "嚴重特殊傳染性肺炎"),
                   # 每仟股 / 每千股 is "per thousand shares" - written "per 1,000 shares".
                   "1,000 shares": ("每仟股", "每千股"), "1000 shares": ("每仟股", "每千股")}
# Hedged wording the filing itself uses - the speculation check exists to stop
# the model ADDING a prediction, not to stop it repeating the filing's own
# (a convertible-bond form's "對股權可能稀釋情形" refused "potential dilution").
_FILING_HEDGES = {r"potential(ly)? dilut\w*": ("可能稀釋",),
                  r"expected benefits?": ("預計可能產生效益", "預計效益"),
                  # Rule 6b tells the model to KEEP "expected" / "planned"; refusing
                  # it when the filing says 預計 contradicted the prompt.
                  r"\b(?:is |are |was |were )?expected(?: to)?\b": ("預計", "預定", "預估")}
# Amounts written in two or more parts: 3,495億6,700萬元, 1兆1,888億2,100萬元.
# ... and 1兆 460億9千萬元 (TSMC) = 1,046.09 billion.
_COMPOUND_AMOUNT_RE = re.compile(r"(?:\d[\d,]*(?:\.\d+)?\s*(?:仟萬|千萬|兆|億|萬)\s*){2,}")
_COMPOUND_PART_RE = re.compile(r"(\d[\d,]*(?:\.\d+)?)\s*(仟萬|千萬|兆|億|萬)")


_HEADER_UNIT_RE = re.compile(r"(?:[（(]\s*(?:新台幣|NT\$)?|單位\s*[:：]\s*(?:新台幣)?)\s*"
                             r"(1,?000,?000|1,?000|仟|千|百萬|佰萬)\s*元")
_HEADER_SCALE = {"1000000": 1e6, "1,000,000": 1e6, "1000": 1e3, "1,000": 1e3, "仟": 1e3, "千": 1e3,
                 "百萬": 1e6, "佰萬": 1e6}


def _value(number: str, unit: str) -> float:
    return round(float(number.replace(",", "").rstrip(".") or 0) * _SCALE[(unit or "").lower()], 2)


def _squash(text: str) -> str:
    # Spaces, full stops and commas ignored: "Foxconn Singapore Pte Ltd." is
    # the filing's "Foxconn Singapore Pte. Ltd.".
    return re.sub(r"[\s.,]+", "", text).lower()


# --- Romanised words in an English name must come from its own Chinese ----
# The model glossed 台光電子材料(黃石) as "Suzhou Elite Material (Huangshi)"
# and 元富證券 as "Yuanta Securities" (元大 - another firm) (2026-10-19). A
# word of the English that is a romanisation (two or more pinyin
# syllables) must be spelled by consecutive characters of the bracketed
# Chinese, in any reading (重慶 = Chongqing) or Wade-Giles (台北 = Taipei).
# Works for any company - no list of names or places.
def _toneless(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn").replace("ü", "v").replace("'", "").lower()


# Syllables with a vowel only: the dictionary also holds interjections (嗯
# ng, 呣 m, 哼 hng) that made "Main", "Chief" and "Daily" parse as pinyin.
_PINYIN_SYLLABLES = frozenset(s for v in PINYIN_DICT.values() for s in map(_toneless, v.split(","))
                              if s.isalpha() and re.search(r"[aeiouv]", s))
# English place names that are not romanisations of their characters: the
# exonym is accepted when the original has the characters (新北 = New Taipei).
_ENGLISH_PLACE_WORDS = {"china": ("中國", "中华", "中華", "華"), "asia": ("亞",), "india": ("印度",),
                        "taipei": ("北",)}
# Words added as part of an official English name without a Chinese
# counterpart ("PricewaterhouseCoopers Taiwan" = 資誠).
_FREE_WORDS = frozenset({"taiwan"})
# A person's name: 2-4 characters, no company or place ending. Taiwanese
# spellings (Lee 李, Chang 張) are not pinyin, so people are not checked.
_PERSON_NAME_RE = re.compile(r"[㐀-鿿]{2,4}")


def _romanised(word: str) -> bool:
    """Two or more pinyin syllables: "Suzhou", "Yuanta". Some English words
    parse too (e-li-te); words the filing itself writes are exempt."""
    w, n = word.lower(), len(word)
    best = [None] * (n + 1)
    best[0] = 0
    for i in range(n):
        if best[i] is None:
            continue
        for j in range(i + 1, min(n, i + 6) + 1):
            if w[i:j] in _PINYIN_SYLLABLES and (best[j] is None or best[j] < best[i] + 1):
                best[j] = best[i] + 1
    return best[n] is not None and best[n] >= 2


def _spelled_by(word: str, zh: str) -> bool:
    readings = [{_toneless(r) for style in (Style.NORMAL, Style.WADEGILES)
                 for r in pinyin(ch, style=style, heteronym=True)[0]} for ch in zh]
    w = word.lower()
    for start in range(len(zh)):
        ends = {0}
        for k in range(start, len(zh)):
            ends = {e + len(r) for e in ends for r in readings[k] if w.startswith(r, e)}
            if len(w) in ends:
                return True
            if not ends:
                break
    return False


def romanisation_not_in_original(english: str, zh: str, source_text: str) -> str | None:
    """The first romanised word of `english` that its Chinese original `zh`
    does not spell, or None."""
    if _PERSON_NAME_RE.fullmatch(zh) and not re.search(r"公司|集團|銀行|基金會|大學|醫院|市|縣|區|路|廠", zh):
        return None
    for word in re.findall(r"[A-Za-z]+", english):
        lower = word.lower()
        if (not word[0].isupper() or lower in _FREE_WORDS or not _romanised(word)
                or re.search(rf"(?<![A-Za-z]){word}(?![A-Za-z])", source_text, re.I)):
            continue
        if any(z in zh for z in _ENGLISH_PLACE_WORDS.get(lower, ())):
            continue
        if not _spelled_by(word, zh):
            return word
    return None


# --- ... nor a listed company's brand that is not this company -------------
# "Darfon (Chongqing)" for 達豐(重慶) - Darfon is 達方 (TWSE 8163). Brands from
# every TWSE / TPEx company's official English short name (generated table,
# all ~2,000 listed companies - nothing per company to maintain here). Only
# coined brands: one company's, five letters or more, not a romanisation
# (those are checked above).
def _brand_table() -> dict[str, tuple[str, ...]]:
    # A word anywhere in two or more companies' English names is an ordinary
    # word ("Group", "Taiwan", "Global"), not one company's brand.
    uses: dict[str, int] = {}
    for *_, en in TAIWAN_LISTED_COMPANIES:
        for w in {w.lower() for w in re.findall(r"[A-Za-z]+", en)}:
            uses[w] = uses.get(w, 0) + 1
    brands: dict[str, tuple[str, ...]] = {}
    for _code, full, short, en in TAIWAN_LISTED_COMPANIES:
        words = re.findall(r"[A-Za-z]+", en)
        # Not a romanisation of the company's own name either ("Shihlin" =
        # 士林, a district other companies share) - only coined brands.
        if (words and uses[w := words[0].lower()] == 1 and len(w) >= 5 and not _romanised(w)
                and not _spelled_by(w, full)):
            # Short name and full-name stem: 士電 / 士林電機廠 are one company.
            brands[w] = (re.sub(r"-?KY$|\*", "", short)[:2], re.sub(r"(?:股份)?有限公司$", "", full))
    return brands


_LISTED_BRANDS = _brand_table()
_LISTED_FULL_NAMES = frozenset(full for _c, full, _s, _e in TAIWAN_LISTED_COMPANIES)


def other_listed_brand(english: str, zh: str, source_text: str) -> str | None:
    """A word of `english` that is the brand of a DIFFERENT listed company
    whose name starts like this one, or None. A look-alike shares the first
    character (達方 Darfon / 達豐, 元大 / 元富, 南亞 / 南茂); an ordinary English
    word that is also some company's brand ("Advanced", "Daily") does not.
    Not checked when the original is itself a listed company (its English
    may be its group's brand - 啟碁 = Wistron NeWeb)."""
    if zh in _LISTED_FULL_NAMES:
        return None
    first = next((c for c in re.sub(r"^(?:台灣|臺灣|財團法人)", "", zh) if "㐀" <= c <= "鿿"), "")
    for word in re.findall(r"[A-Za-z]+", english):
        cores = _LISTED_BRANDS.get(word.lower())
        if (cores and cores[0][:1] == first and not any(c in zh for c in cores)
                and not re.search(rf"(?<![A-Za-z]){word}(?![A-Za-z])", source_text, re.I)):
            return word
    return None


def invented_content(output_text: str, source_text: str) -> str | None:
    """Why `output_text` states something the filing does not, or None.

    `source_text` is the date-normalised filing plus the company's names,
    code and clause label - everything the model was given."""
    if _MONTH_NAME_RE.search(output_text):
        return "month name instead of YYYY-MM-DD"
    for pattern, markers in _FILING_HEDGES.items():
        if any(m in source_text for m in markers):
            output_text = re.sub(pattern, "", output_text, flags=re.I)
    if m := _VAGUE_QUANTITY_RE.search(output_text):
        return f"quantity word without a figure: {m.group(0)!r}"
    if m := _SPECULATION_RE.search(output_text):
        return f"prediction or generalisation the filing does not state: {m.group(0)!r}"
    for term, markers in _STANDARD_TERMS.items():
        if any(m in source_text for m in markers):
            output_text = output_text.replace(term, "")
    source_numbers = {_norm_num(t) for t in _NUMBER_RE.findall(source_text)}
    source_values = {_value(n, u) for n, u in _SOURCE_AMOUNT_RE.findall(source_text)}
    source_values |= {round(sum(_value(n, u) for n, u in _COMPOUND_PART_RE.findall(c)), 2)
                      for c in _COMPOUND_AMOUNT_RE.findall(source_text)}
    # A table's unit header scales the figures under it: "營業收入(1000000元)
    # 958" is 958,000,000, and the model wrote it so (Auras, 2026-10-19).
    for scale in {_HEADER_SCALE.get(u, 0) for u in _HEADER_UNIT_RE.findall(source_text)} - {0}:
        source_values |= {_value(n, "") * scale for n in _NUMBER_RE.findall(source_text)}
    for number, unit in _OUTPUT_AMOUNT_RE.findall(output_text):
        # With a scale word the VALUE must match: "600億" filed and "NT$600
        # billion" written share the digits 600 but are ten times apart (seen
        # live on a TSMC bond filing). Without one, the digits or the value.
        if unit:
            # Value match, or the same digits where the filing states that unit
            # elsewhere - results tables put it in a header (單位：新台幣仟元),
            # not next to each figure. "600 billion" for 600億 still fails:
            # the filing's unit is 億, not 十億.
            same_unit = any(w in source_text for w in _UNIT_WORDS.get(unit.lower(), ()))
            if _value(number, unit) not in source_values and not (
                    same_unit and _norm_num(number) in source_numbers):
                return f"amount not in the filing: {number} {unit}"
        elif _norm_num(number) not in source_numbers and _value(number, "") not in source_values:
            return f"number not in the filing: {number!r}"
    squashed_source = _squash(source_text)
    for run in _CJK_RUN_RE.findall(output_text):
        if run not in source_text:
            return f"Chinese text not in the filing: {run!r}"
    # Everything is in English: Chinese only as the original name in
    # brackets after its English translation. Prompted, and still ignored
    # (a subsidiary written in Chinese only, 2026-10-18).
    # Every instance, not just the first: told about one, the model fixed
    # that one and the next failed the retry.
    if bare := list(dict.fromkeys(_CJK_RUN_RE.findall(_BRACKETED_RE.sub("", output_text)))):
        return f"Chinese written without an English translation: {', '.join(map(repr, bare))}"
    # An English translation written right before the filing's own Chinese
    # name - "Suzhou Qunce Technology Co., Ltd. (蘇州群策科技股份有限公司)" - is
    # a gloss, not a name taken from nowhere: the Chinese (checked above)
    # anchors it. The same English may then be repeated on its own.
    glossed: list[str] = []
    for m in _GLOSS_RE.finditer(output_text):
        if m.group(2) in source_text:
            # The English must not add a place its own original lacks:
            # "Suzhou Elite Material (Huangshi) (台光電子材料(黃石)有限公司)"
            # (2026-10-19).
            words = m.group(1).split()
            while words and any(w[:1].islower() for w in words):
                words = words[1:]          # the name only: its capitalised tail
            name = " ".join(words)
            if word := romanisation_not_in_original(name, m.group(2), source_text):
                return f"romanised word not in the original: {word!r} for {m.group(2)!r}"
            if word := other_listed_brand(name, m.group(2), source_text):
                return f"another company's name: {word!r} is a different listed company, not {m.group(2)!r}"
            glossed.append(_squash(m.group(1)))
    name_text = _GLOSS_RE.sub(lambda m: m.group(2) if m.group(2) in source_text else m.group(0), output_text)
    # A match can run across a full stop ("... Delta Electronics. Delta
    # Electronics ...") - each sentence's part is checked on its own.
    for name in (part for found in _ENTITY_RE.findall(name_text) for part in re.split(r"\.\s+", found)):
        words = name.split()
        while words and words[0].strip(",").lower() in _NAME_LEAD_WORDS:
            words = words[1:]
        if len(words) < 2:
            continue  # "The Group", "Its Subsidiary" - ordinary words, not a name
        squashed = _squash(" ".join(words))
        if (squashed not in squashed_source and not any(squashed in g for g in glossed)
                and not any(squashed in k for k in _known_english_in(source_text))):
            return f"company name not in the filing: {' '.join(words)!r}"
    return None


# A form's blank fields are not facts: Quanta's no-dividend form gave four
# key facts reading "0" (2026-10-18). Dropped, never refused - the summary
# states the zero once when it is the news.
_BLANK_FACT_RE = re.compile(
    r"(?:NT\$|US\$|USD|TWD)?\s*0+(?:\.0+)?\s*(?:%|shares?|元|thousand|million|billion)?|"
    r"none|nil|n/?a|not applicable|not stated|not disclosed|-+|—|無|不適用", re.I)


# Verified English names of group companies that file often under their
# Chinese name only (2026-10-19). The model guessed these - one filing's
# 乾坤科技 came back as "Chilisin", "Chicony" and "Chyng Hong" in one run
# (reviewer); it is Delta's subsidiary Cyntec. Given to the model when the
# Chinese name is in the filing, and enforced on its answer and on the title
# translation. Add a name only with a source that pairs the two.
KNOWN_NAMES = {
    "乾坤科技股份有限公司": "Cyntec Co., Ltd.",                          # deltaww.com (Delta subsidiary)
    "矽品精密工業股份有限公司": "Siliconware Precision Industries Co., Ltd.",  # ASE subsidiary (SPIL)
    "日月光半導體製造股份有限公司": "Advanced Semiconductor Engineering, Inc.",  # ASE subsidiary (ASE Inc.)
    "富士康工業互聯網股份有限公司": "Foxconn Industrial Internet Co., Ltd.",     # Hon Hai subsidiary, SSE 601138
    # Written "Yuanta Securities" (元大 - another firm) on a Delta filing (2026-10-19).
    "元富證券股份有限公司": "MasterLink Securities Corporation",               # LEI 254900KCX3I87YXG6L48
    # MOPS writes her rare final character as its parts in brackets; the
    # model wrote "Li Hong". wiwynn.com/about-wiwynn/leadership: Emily Hong, Chair.
    "洪麗(宓冉)": "Emily Hong",
}


def _known_english_in(source_text: str) -> set[str]:
    """Squashed verified English names whose Chinese the filing contains -
    the model was told to use them, so they are anchored like the filing's
    own words ("Siliconware Precision Industries Co., Ltd." for 矽品)."""
    return {_squash(en) for en in known_names_in(source_text).values()}


def known_names_in(text: str) -> dict[str, str]:
    return {zh: en for zh, en in KNOWN_NAMES.items() if zh in (text or "")}


def apply_known_names(text: str) -> str:
    """"Chilisin Electronics Corp. (乾坤科技股份有限公司)" -> "Cyntec Co., Ltd.
    (乾坤科技股份有限公司)": the English before a known Chinese name is replaced
    by its verified form."""
    if not isinstance(text, str) or not any(zh in text for zh in KNOWN_NAMES):
        return text

    def repl(m: re.Match) -> str:
        en = KNOWN_NAMES.get(m.group(2))
        if not en:
            return m.group(0)
        # The match's English part can start with ordinary words ("on behalf
        # of subsidiary Chilisin ..."): keep everything up to the last word
        # that starts in lower case, replace only the name after it.
        words = m.group(1).split(" ")
        cut = max((i + 1 for i, w in enumerate(words) if w[:1].islower()), default=0)
        keep = " ".join(words[:cut])
        return f"{keep} {en} ({m.group(2)})" if keep else f"{en} ({m.group(2)})"
    return _GLOSS_RE.sub(repl, text)


# A share issue form's 發行總金額 is the total at PAR value (shares x NT$10);
# the money raised is shares x the issue price. The model kept presenting the
# par total as "raising NT$50,000,000" for a NT$1.15bn BizLink issue.
_PAR_TOTAL_RE = re.compile(r"發行總(?:金)?額[^：:]{0,10}[：:]\s*新(?:台|臺)幣\s*([\d,]+)\s*元")
_PAR_EACH_RE = re.compile(r"每股面額[^：:]{0,6}[：:]\s*新(?:台|臺)幣\s*([\d.]+)\s*元")
_ISSUE_PRICE_RE = re.compile(r"發行價格[^：:]{0,6}[：:][^0-9]{0,12}([\d,.]+)\s*元")
_RAISE_WORDS_RE = re.compile(r"rais|proceeds|total amount|amount raised|to raise|funds of", re.I)


def par_total_misread(output_text: str, source_text: str) -> str | None:
    total, par, price = (_PAR_TOTAL_RE.search(source_text), _PAR_EACH_RE.search(source_text),
                         _ISSUE_PRICE_RE.search(source_text))
    if not (total and par and price):
        return None
    try:
        if float(price.group(1).replace(",", "")) <= float(par.group(1)):
            return None
    except ValueError:
        return None
    digits = total.group(1)
    for m in re.finditer(re.escape(digits), output_text):
        if _RAISE_WORDS_RE.search(output_text[max(0, m.start() - 60):m.end() + 20]):
            return (f"par value total as money raised: NT${digits} is the total at NT${par.group(1)} par "
                    f"value, not the money raised")
    return None


# A results filing (clause 31) whose form states the figures must have a
# summary that states them: two Auras cards said "includes figures for
# revenue, profit..." and gave none (reviewer, 2026-10-19).
_RESULT_FIGURE_RE = re.compile(r"(?:營業收入|基本每股盈餘|歸屬於母公司業主淨利)[^:：]{0,30}[:：]\s*(-?[\d,]+(?:\.\d+)?)")


def results_without_figures(clause: int | None, summary: str, source_text: str) -> str | None:
    if clause != 31:
        return None
    figures = {f.replace(",", "") for f in _RESULT_FIGURE_RE.findall(source_text) if f.strip("-0,.")}
    if not figures:
        return None
    if any(f in summary.replace(",", "") for f in figures):
        return None
    return "results summary without figures: state the revenue, net profit attributable to the parent and EPS the filing gives"


def _drop_latin_bracket(name: Any, source_text: str) -> Any:
    """"Green I-PARK CORP. (Green I-PARK CORP.)" or "Wistron InfoComm (Vietnam)
    Co., Ltd. (WVN)": an English-only bracket after an entity name the filing
    itself contains is dropped, not refused - the model repeats or
    abbreviates the name it was told to gloss. Only removes."""
    if not isinstance(name, str):
        return name
    m = re.fullmatch(r"(.+?)\s*[（(]([^()（）]*)[)）]", name.strip())
    if m and not _CJK_RUN_RE.search(m.group(2)) and _squash(m.group(1)) in _squash(source_text):
        return m.group(1).strip()
    return name


def _drop_invented_glosses(text: str, source_text: str) -> str:
    """Remove a bracketed Chinese name the model added after an English name
    the filing itself writes in English: "J.P. Morgan (摩根大通)" where the
    filing says "J.P. Morgan" becomes "J.P. Morgan". Only removes - what
    stays was already in the filing."""
    squashed = _squash(source_text)

    def repl(m: re.Match) -> str:
        if m.group(2) in source_text:
            return m.group(0)
        words = m.group(1).split()
        if any(_squash(" ".join(words[i:])) in squashed for i in range(len(words)) if len(words) - i <= 6):
            return m.group(1)
        return m.group(0)
    return _GLOSS_RE.sub(repl, text)


_NO_BOARD_DATE_RE = re.compile(r"董事會通過日期[^:：]{0,12}[:：]\s*不適用")
_DECISION_DATE_LABEL_RE = re.compile(r"(?:decision|approv\w*|board|resolution)\b.*\bdate", re.I)


def _drop_repeated_bracket(text: str) -> str:
    """"CHENBRO (MALAYSIA) SDN. BHD. (CHENBRO (MALAYSIA) SDN. BHD.)": a bracket
    that only repeats the words right before it is removed (2026-10-19 - the
    model glossed an English name with itself). Only removes."""
    i = 0
    while (start := text.find("(", i)) != -1:
        depth, end = 0, start
        for end in range(start, len(text)):
            depth += {"(": 1, ")": -1}.get(text[end], 0)
            if depth == 0:
                break
        inner = text[start + 1:end].strip()
        before = text[:start].rstrip()
        if depth == 0 and len(inner) >= 3 and before.endswith(inner):
            text = before + text[end + 1:]
            i = len(before)
        else:
            i = start + 1
    return text


_ISO_DATE_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")
_QUARTER_END = {"03-31", "06-30", "09-30", "12-31"}


def invented_date(output_text: str, source_text: str) -> str | None:
    """A YYYY-MM-DD whose month and day the filing does not give.

    The number check passes "2023-01-20" when 2023, 01 and 20 each appear
    somewhere. Allowed: the date as filed; a yearless 1/20 or 1月20日 given
    a year the filing states (all four such dates in the 160 reviewed
    filings were the fact-date year); a quarter end for a quarter's report."""
    filed = {(int(y), int(m), int(d)) for y, m, d in re.findall(r"(\d{4})-(\d{1,2})-(\d{1,2})", source_text)}
    years = {y for y, _, _ in filed} | {int(y) for y in re.findall(r"(?<!\d)(\d{4})(?:年|-\d)", source_text)}
    yearless = {(int(m), int(d)) for m, d in re.findall(r"(?<![\d/-])(\d{1,2})\s*(?:/|月)\s*(\d{1,2})(?:日|(?![\d/]))", source_text)}
    for m in _ISO_DATE_RE.finditer(output_text):
        y, mo, d = int(m[1]), int(m[2]), int(m[3])
        if (y, mo, d) in filed or ((mo, d) in yearless and y in years):
            continue
        if f"{m[2]}-{m[3]}" in _QUARTER_END and y in years and "季" in source_text:
            continue
        return f"date not in the filing: {m.group(0)}"
    return None


def parse_and_check(raw: str | None, source_text: str, clause: int | None = None) -> tuple[dict[str, Any] | None, str]:
    """Validated model answer, or (None, why it was refused)."""
    if not raw:
        return None, "model_failed"
    try:
        out = json.loads(raw)
    except ValueError:
        return None, "model_output_rejected: not JSON"
    if not isinstance(out, dict):
        return None, "model_output_rejected: not an object"
    detection, reason, summary = out.get("signal_detection"), out.get("signal_reason"), out.get("summary")
    facts = out.get("key_facts") or []
    drop = lambda t: (apply_known_names(_drop_repeated_bracket(_drop_invented_glosses(t, source_text)))
                      if isinstance(t, str) else t)
    reason, summary = drop(reason), drop(summary)
    if isinstance(facts, list):
        facts = [{**f, "value": drop(f.get("value"))} if isinstance(f, dict) else f for f in facts]
    if isinstance(out.get("entities"), list):
        out["entities"] = [{**e, "name": _drop_latin_bracket(drop(e.get("name")), source_text)}
                           if isinstance(e, dict) else e for e in out["entities"]]
    if isinstance(facts, list):
        # Tidy, never add: a number given as a number becomes text (it is
        # checked like any other), a fact with no label or value is dropped,
        # and only the first _KEY_FACTS_MAX are kept. Refusing the whole
        # answer for these lost 4 of 240 summaries.
        facts = [{"label": f["label"].strip(), "value": str(f["value"]).strip()} for f in facts
                 if isinstance(f, dict) and isinstance(f.get("label"), str) and f["label"].strip()
                 and isinstance(f.get("value"), (str, int, float)) and str(f["value"]).strip()
                 and not _BLANK_FACT_RE.fullmatch(str(f["value"]).strip())
                 # "Decision date: 2021-05-28" where the form says
                 # 董事會通過日期: 不適用 (Elite Material, 2026-10-19).
                 and not (_NO_BOARD_DATE_RE.search(source_text) and _DECISION_DATE_LABEL_RE.search(f["label"]))
                 ][:_KEY_FACTS_MAX]
    reason = iso_dates(reason) if isinstance(reason, str) else reason
    summary = iso_dates(summary) if isinstance(summary, str) else summary
    if isinstance(facts, list):
        # Labels too: "December 2022 consolidated net revenue" (2026-10-19).
        facts = [{"label": iso_dates(f["label"]), "value": iso_dates(f["value"])}
                 if isinstance(f, dict) and isinstance(f.get("value"), str) and isinstance(f.get("label"), str)
                 else f for f in facts]
    if detection not in _SIGNAL_DETECTION:
        return None, f"model_output_rejected: signal_detection {detection!r}"
    if not isinstance(reason, str) or not _reason_length_ok(reason):
        return None, "model_output_rejected: signal_reason"
    if not isinstance(summary, str) or not summary.strip() or len(summary) > _SUMMARY_MAX:
        return None, "model_output_rejected: summary"
    if (not isinstance(facts, list) or len(facts) > _KEY_FACTS_MAX
            or not all(isinstance(f, dict) and isinstance(f.get("label"), str)
                       and isinstance(f.get("value"), str) for f in facts)):
        return None, "model_output_rejected: key_facts"
    entities = out.get("entities") or []
    if isinstance(entities, list):
        # Exchanges, regulators, courts are dropped as entities anyway - never
        # refuse a whole answer over how one of them was named.
        entities = [e for e in entities if not (isinstance(e, dict) and isinstance(e.get("name"), str)
                                                and _NOT_A_PARTY_RE.search(e["name"]))]
    if (not isinstance(entities, list) or len(entities) > _ENTITIES_MAX
            or not all(isinstance(e, dict) and isinstance(e.get("name"), str) and e["name"].strip()
                       and e.get("type") == "company" for e in entities)):
        return None, "model_output_rejected: entities"
    squashed_source = _squash(source_text)
    for e in entities:
        # As filed, or "English (原文)" whose Chinese is the filing's own.
        gloss = _GLOSS_RE.fullmatch(e["name"].strip())
        if (_squash(e["name"]) not in squashed_source and not (gloss and gloss.group(2) in source_text)
                and _squash(e["name"]) not in _known_english_in(source_text)):
            return None, f"model_output_rejected: entity not in the filing: {e['name']!r}"
    # Entity names included: "Jiaming Investment Co., Ltd. (嘉明投資股份有限公司)"
    # glossed only in the entities list anchors the same English in a key
    # fact (Lotes 2019, refused twice - 2026-10-19).
    everything = "\n".join([reason, summary] + [f"{f['label']}: {f['value']}" for f in facts]
                           + [e["name"] for e in entities if _GLOSS_RE.fullmatch(e["name"].strip())])
    why = (invented_content(everything, source_text) or invented_date(everything, source_text)
           or par_total_misread(everything, source_text)
           or results_without_figures(clause, summary, source_text))
    if why:
        return None, f"model_output_rejected: {why}"
    return {"signal_detection": detection, "signal_reason": reason.strip(),
            "summary": summary.strip(), "key_facts": facts,
            "entities": [{"name": e["name"].strip(), "type": "company"} for e in entities]}, "ok"


def label_only(raw: str | None, source_text: str) -> dict[str, str] | None:
    """The label and reason from an answer whose summary was refused, if
    those two pass every check on their own. A results release lost its
    signal because a key fact failed the number check (4 of 80 fresh
    filings); the label never depended on that fact."""
    try:
        out = json.loads(raw or "")
    except ValueError:
        return None
    if not isinstance(out, dict):
        return None
    detection, reason = out.get("signal_detection"), out.get("signal_reason")
    if detection not in _SIGNAL_DETECTION or not isinstance(reason, str):
        return None
    reason = iso_dates(reason)
    if not _reason_length_ok(reason) or invented_content(reason, source_text):
        return None
    return {"signal_detection": detection, "signal_reason": reason.strip()}


# Clauses whose filings have no counterparty: people changes (6 board, 7
# auditor, 8 managers), shareholder meetings (17, 18), investor conferences
# (12), results (31) and internal-control statements (29). The model was told
# to return no entities for these but still listed directors' holding
# companies and employers on a board-election filing - so for these clauses
# the filing company is the only entity, by rule. Same numbers on TWSE/TPEx.
_NO_PARTY_CLAUSES = frozenset({6, 7, 8, 12, 17, 18, 29, 31})
# Exchanges, regulators, courts and prosecutors are where an event happens,
# not parties to it. The prompt excludes them; the model still listed the
# Hong Kong exchange and the CSRC - dropped by rule.
_NOT_A_PARTY_RE = re.compile(
    r"交易所|櫃檯買賣中心|證監會|證券監督管理委員會|金融監督管理委員會|金管會|法院|檢察署|"
    r"Stock Exchange|Securities and Futures Commission|Regulatory Commission|Court", re.I)
# The subsidiary a filing is made for ("代子公司XXX公告") is named in the
# subject, so it is taken from there by rule: the model left it out on about
# 15 of 80 reviewed filings. Several subsidiaries joined by 、 are left to
# the model - splitting them reliably needs the full names.
_FILED_FOR_RE = re.compile(
    r"代(?:重要|大陸|海外|孫)?子公司[\s:：]*(.+?)(?:\s*[\(（]簡稱.*?[\)）])?"
    r"\s*(?:公告|之|董事會|股東會|決議|取得|處分|$)")


def _entity_key(name: str) -> str:
    """The same company written "蘇州群策科技股份有限公司" or "Suzhou Qunce
    Technology Co., Ltd. (蘇州群策科技股份有限公司)" is one entity."""
    gloss = _GLOSS_RE.fullmatch(name.strip())
    return _squash(gloss.group(2) if gloss else name)


def filed_for_subsidiary(subject: str | None) -> str | None:
    m = _FILED_FOR_RE.search(re.sub(r"\s+", " ", subject or "").strip())
    name = m.group(1).strip(" -:：、") if m else ""
    # "代重要子公司公告武漢肺炎影響之說明" names no subsidiary - the text after
    # 子公司 is the subject itself.
    if len(name) < 2 or "、" in name or name.startswith("公告"):
        return None
    return name
# Trading halted or resumed by the exchange pending material information:
# all four reviewers flagged a halt labelled noise as a filing nobody should
# miss, so it is signal by rule. Clause 40 on both TWSE and TPEx.
_ALWAYS_SIGNAL_CLAUSES = frozenset({40})


# Extra help for the refusal the model most often repeats on its retry:
# 49億 written as "49 billion" (it is 4.9 billion), twice on one filing.
_REPAIR_HINTS = {
    "amount not in the filing": (
        "億 is a hundred million (49億 = 4.9 billion, never 49 billion); 萬 is ten "
        "thousand; 仟元 is thousand; 百萬 and 佰萬 are million. If unsure, copy the "
        "figure with the filing's own Chinese unit (NT$49億). "),
    "results summary without figures": (
        "Write the revenue, net profit attributable to the parent and basic EPS with the exact figures "
        "and units the filing gives, e.g. revenue of NT$11,653,759 thousand. "),
    "par value total as money raised": (
        "Give the share count and the issue price per share as the filing states them, and call the "
        "發行總金額 figure the total at par value. Do not call it the amount raised. "),
    "quantity word without a figure": (
        "Do not judge size or importance with words like \"significant\"; state the "
        "figure the filing gives, or say nothing about size. "),
    "Chinese written without an English translation": (
        "Write the English translation first and the filing's exact Chinese name in brackets "
        "right after it: Suzhou Qunce Technology Co., Ltd. (蘇州群策科技股份有限公司). "),
    "month name instead of YYYY-MM-DD": (
        "Write dates as YYYY-MM-DD or YYYY-MM. Where the filing gives a date with no year "
        "(1/20, 2月10日), copy it exactly as the filing writes it - never as a month name. "),
    "place name not in the original": (
        "Translate the bracketed Chinese name as it stands: add no city or region that its "
        "characters do not contain. "),
    "another company's name": (
        "That English name belongs to a different company. Write the bracketed Chinese name "
        "in pinyin with its parts translated (環誠科技有限公司 = Huancheng Technology Co., Ltd.). "),
    "date not in the filing": (
        "Use only dates the filing gives. A date the filing writes without a year "
        "(12/19) may take the year of the filing's own dates; never make up a day or month. "),
}


def _rating(meta: dict[str, Any], subject: str, detection: str, body: str = "") -> str:
    """The stored HIGH/WEAK/NOISE: the model's answer with the prompt rules
    the model did not follow reliably applied on top. Each was measured on
    the 2019-2026 history before it was added (2026-10-19)."""
    rating = _SIGNAL_DETECTION[detection]
    clause = _clause_number(meta.get("disclosure_clause_code"))
    flat = re.sub(r"\s+", "", subject or "")
    if clause in _ALWAYS_SIGNAL_CLAUSES:
        return "HIGH"
    if rating == "HIGH" and (_SUPPLEMENT_RE.search(subject or "") or _UNUSUAL_TRADING_RE.search(subject or "")
                             or _INVESTMENT_VEHICLE_RE.search(subject or "")):
        return "WEAK"
    if rating == "NOISE" and overseas_injection(meta, subject):
        return "WEAK"
    # A change of finance head (財務主管 = CFO) is weak_signal (prompt rule);
    # BizLink's was rated noise as "no CFO change" (2026-10-19). An accounting
    # head only ACTING as finance head (會計主管(代理財務主管)) is routine.
    if rating == "NOISE" and clause == 8 and (roles := _ROLE_FIELD_RE.search(re.sub(r"\s+", "", body or ""))) \
            and re.search(r"(?<!代理)財務主管|財務長|財務總監", roles.group(1)):
        return "WEAK"
    # Board and top-management changes (clause 6) are signal only when a
    # third or more of the board really changes; the founder, chairman or
    # CEO leaving is weak_signal (review 2026-10-19: "a product choice for an
    # AI-economy feed"). The model said signal for Terry Gou in 4 of 4 runs.
    if rating == "HIGH" and clause == 6 and not _third_of_board_changed(body):
        return "WEAK"
    # The company's own equipment / plant-works form is weak_signal: KYEC's
    # NT$1.84bn was signal in 1 of 4 runs, Unimicron's NT$595m works noise
    # in 1 of 2. Only the plain form subject - an "AI park ... investment
    # plan" naming a site stays with the model.
    if clause == 20 and _OWN_EQUIPMENT_FORM_RE.fullmatch(flat):
        return "WEAK"
    # A lease of land or buildings with anyone outside the group - a related
    # party such as WNC for Wiwynn included - is weak_signal (148 lease
    # filings: 90 outside, 43 related party, 15 inside the group). The model
    # called both Wiwynn/WNC leases "inside the group"; five of five
    # reviewers said weak.
    if clause == 20 and _LEASE_RE.search(flat) and not _GROUP_COUNTERPARTY_RE.search(_counterparty(body)):
        return "WEAK"
    # A press report of an acquisition the company does not deny ("we keep
    # evaluating targets and do not comment on rumours"): BizLink/Interplex,
    # noise in 2 of 3 runs; four of five reviewers said weak. Forecast
    # disclaimers (nearly all clarifications) stay with the model.
    if rating == "NOISE" and "澄清" in flat and _ma_rumour_not_denied(body):
        return "WEAK"
    # A cyber attack that reached the company's or a subsidiary's systems is
    # at least weak_signal (prompt rule); Unimicron's subsidiary ransomware
    # case was rated noise "because it is at a subsidiary" (2026-10-19).
    # A website-only DDoS stays with the model (noise by the prompt).
    if rating == "NOISE" and _CYBER_SUBJECT_RE.search(flat) and not _WEBSITE_ONLY_RE.search(re.sub(r"\s+", "", body or "")):
        return "WEAK"
    # Stoppage, resumption and epidemic-impact notices are weak_signal unless
    # a material effect is stated (prompt rule). Unimicron's COVID supplement
    # flipped weak/noise across 6 runs. Meeting postponements and online
    # investor conferences are not stoppages.
    if rating == "NOISE" and _STOPPAGE_SUBJECT_RE.search(flat) and not _NOT_A_STOPPAGE_RE.search(flat):
        return "WEAK"
    # A bond issue with no named investor for working capital, debt or
    # material purchases is weak_signal (prompt rule); Auras' NT$1.5bn CB was
    # rated signal, five of five reviewers said weak.
    if rating == "HIGH" and clause == 11 and _BOND_ISSUE_RE.search(flat) and "私募" not in flat \
            and _ROUTINE_BOND_USE_RE.search(_form_value(body, "募得價款之用途")) \
            and "產能" not in _form_value(body, "募得價款之用途"):
        return "WEAK"
    return rating


_OWN_EQUIPMENT_FORM_RE = re.compile(r"(?:公告)?本公司(?:取得|訂購|購置|購買|採購)(?:機器|生產|廠務)?(?:設備|工程)(?:之)?(?:相關資料|公告)")
_CYBER_SUBJECT_RE = re.compile(r"網路攻擊|資安事件|駭客|資料外洩|網路安全事件")
_WEBSITE_ONLY_RE = re.compile(r"DDoS|DDOS|阻斷服務")
_STOPPAGE_SUBJECT_RE = re.compile(r"停工|復工|疫情影響|肺炎影響|肺炎相關|疫情對|肺炎對|肺炎疫情")
_NOT_A_STOPPAGE_RE = re.compile(r"股東會|股東常會|法說會")
_LEASE_RE = re.compile(r"使用權資產|租賃|承租")
_GROUP_COUNTERPARTY_RE = re.compile(r"子公司|母公司")
_BOND_ISSUE_RE = re.compile(r"發行.{0,12}公司債")
_ROUTINE_BOND_USE_RE = re.compile(r"營運資金|償還|購料")


def _form_value(body: str, label: str) -> str:
    """The value of a numbered form field, by its label."""
    m = re.search(label + r"[^:：]{0,60}[:：]\s*(.{0,120}?)(?=\s?\d{1,2}\.\D|$)", re.sub(r"\s+", "", body or ""))
    return m.group(1) if m else ""


def _counterparty(body: str) -> str:
    return _form_value(body, "與公司之關係") or _form_value(body, "交易相對人")


def _third_of_board_changed(body: str) -> bool:
    """The form's 屬三分之一以上董事發生變動 answer - except in a term-end
    election, where it reads 是 whoever is elected and the seats are counted."""
    if (seats := board_seat_changes(body)) is not None:
        return len(seats[0]) * 3 >= seats[1]
    return _form_value(body, "屬三分之一以上董事發生變動").startswith("是")


def _ma_rumour_not_denied(body: str) -> bool:
    report, response = _form_value(body, "報導內容"), _form_value(body, "因應措施")
    # A named deal of any kind - not 合併營收 (consolidated revenue).
    return bool(re.search(r"收購|併購|購併|入股|合資|策略聯盟|合併案|(?:接獲|取得|拿下).{0,6}訂單|(?:取得|出售).{0,8}股權",
                          report)) and not re.search(
        r"否認|並非|不實|無此|未曾|非屬|並無|沒有|子虛", response)


# A capital injection into a subsidiary outside Taiwan is weak_signal
# whatever purpose it states (prompt rule). The model still rated 4 of 4 in
# set 5 noise - one of them stating it funds a new plant - and all five
# reviewers said weak (2026-10-19). Taiwan subsidiaries are named in Chinese;
# a Latin-script name in the subject is a subsidiary abroad. 100 clause-20
# subjects in 2019-2026 match (TSMC Arizona, Wiwynn Mexico, Delta India, ...).
# Overseas holding vehicles count too (UniWonderful Holding, US$43m: four
# of five reviewers said weak); Taiwan holding companies (named in Chinese)
# never match.
# Not 擴增資本支出 (capex) and not reserves or earnings turned into capital
# (公積/盈餘轉增資 - no money moves). A subsidiary abroad is named in Latin
# script, or in Chinese with its place in brackets (環榮電子(惠州)有限公司), or
# by region (對泰國子公司增資, 對大陸子公司增資).
_INJECTION_RE = re.compile(r"(?<!擴)(?<!公積轉)(?<!盈餘轉)增資|增加投資|投資子公司")
_LATIN_NAME_RE = re.compile(r"[A-Za-z]{2,}")
_ABROAD_RE = re.compile(r"[（(][㐀-鿿]{2,4}[)）](?:股份)?有限公司|大陸|中國|越南|泰國|印度|墨西哥|美國|北美|馬來西亞|日本|"
                        r"德國|歐洲|菲律賓|印尼|海外|境外|新加坡|捷克|波蘭|匈牙利|斯洛伐克|荷蘭|英國|加拿大")
_HOLDING_NAME_RE = re.compile(r"投資(?:控股)?(?:股份)?有限公司|控股股份有限公司")


def overseas_injection(meta: dict[str, Any], subject: str) -> bool:
    subject = re.sub(r"\s+", "", subject or "")
    return (_clause_number(meta.get("disclosure_clause_code")) == 20 and bool(_INJECTION_RE.search(subject))
            and bool(_LATIN_NAME_RE.search(subject) or _ABROAD_RE.search(subject))
            and not _HOLDING_NAME_RE.search(subject))


# A filing the model did not answer at all (outage, timeout) is tried once
# more at the end of the run, after this pause; if it still gets no answer it
# is stored with the literal translation, never dropped.
_OUTAGE_PAUSE_SECS = 60


# Two retries after the first answer: with one, ~1 in 80 summaries was still
# refused; each retry is told exactly which check failed.
_REPAIR_ATTEMPTS = 2


def _format_body(summary: str, facts: list[dict[str, str]]) -> str:
    if not facts:
        return summary
    lines = "\n".join(f"- {f['label']}: {f['value']}" for f in facts)
    return f"{summary}\n\nKey facts:\n{lines}"


# ---------------------------------------------------------------------------
# 5. Entry point.
# ---------------------------------------------------------------------------
def classify_announcements(articles: list[dict[str, Any]]) -> None:
    """Rate every mops_material article in place and write its English
    summary into metadata.translated_summary.

    Every filing gets the model call, fixed-NOISE ones included: their
    label and reason stay set by rule (the model's label is ignored), but
    the summary is the model's, so every row carries one. The title
    (translated_title) is the literal translation step's job, and so is
    translated_body - the whole filing translated literally - which that
    step writes only when no summary passed the checks.

    The checks compare the answer with the filing's own text only (subject
    line and body) plus the company's names - never with the clause meaning
    or other context the model was shown - so the summary states what the
    filing says and nothing else."""
    pending: list[tuple[dict, dict, str, str, str | None]] = []
    counts: dict[str, int] = {}
    # Model labels _rating changed - the path counts below carry the model's
    # own label, so without this the "stored:" totals would not reconcile.
    overridden: list[str] = []
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "mops_material":
            continue
        subject = normalize_roc_dates(a.get("summary"))
        body = normalize_roc_dates(a.get("body"))
        # The literal translation that runs after this (title, and the body
        # when the summary is refused) reads these same fields - give it the
        # converted text too, or it renders "115/06/08" as is. Neither field
        # is stored by this service.
        a["summary"], a["body"] = subject, body
        # The filing company comes from our own ticker list, never from the
        # model. Stored in entities_json / entity_names_normalized (columns,
        # not metadata), the same place the SEC classifier puts them.
        filer = meta.get("translated_company_name")
        a["_entities"] = [{"name": filer, "type": "company"}] if filer else []
        if sub := filed_for_subsidiary(subject):
            a["_entities"].append({"name": sub, "type": "company"})
        fixed = fixed_noise_reason(meta, subject, body)
        if fixed:
            meta["announcement_materiality_signal"] = "NOISE"
            meta["announcement_materiality_reason"] = fixed
        pending.append((a, meta, subject, body, fixed))

    if not pending:
        return
    system = _system_prompt()

    def _one(item: tuple[dict, dict, str, str, str | None], first_pass: bool = True) -> str:
        a, meta, subject, body, fixed = item
        user = _user_message(meta, subject, body)
        source = "\n".join(str(x) for x in (
            subject, body, meta.get("translated_company_name") or "",
            meta.get("company_name") or "", meta.get("ticker") or ""))
        messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
        raw = _call_model(messages)
        answers = [raw]
        out, status = parse_and_check(raw, source, _clause_number(meta.get("disclosure_clause_code")))
        for _ in range(_REPAIR_ATTEMPTS):
            if out is not None or not answers[-1] or not status.startswith("model_output_rejected"):
                break
            # Told exactly which rule it broke; re-checked in full - a
            # repaired answer earns no trust the first one lacked.
            why = status.split(": ", 1)[-1]
            # Every retry costs a call - logged with its cause so checker
            # gaps (too strict) can be told apart from real model mistakes.
            logger.info("[TAIWAN_ANNOUNCEMENT] %s %s retry: %s",
                        meta.get("ticker"), meta.get("statement_date"), why)
            messages += [
                {"role": "assistant", "content": answers[-1]},
                {"role": "user", "content": (
                    f"Your answer was refused: {why}. {_REPAIR_HINTS.get(why.split(':')[0], '')}"
                    "Return the corrected JSON object only. Fix that problem everywhere it "
                    "occurs - reason, summary, key facts and entities - and change nothing "
                    "else; every rule in the instructions still applies.")},
            ]
            answers.append(_call_model(messages))
            out, status = parse_and_check(answers[-1], source, _clause_number(meta.get("disclosure_clause_code")))
            if out is not None:
                status = "repaired"

        if out is None and status == "model_failed" and first_pass:
            # The model never answered (outage, timeout) - tried again at the
            # end of this run. Not left for a later run: the scheduled run
            # reads only today's fetches, so a filing deferred from the last
            # run of the day would never be seen again.
            return "model_unavailable"
        if out is not None:
            meta["translated_summary"] = _format_body(out["summary"], out["key_facts"])
            if _clause_number(meta.get("disclosure_clause_code")) not in _NO_PARTY_CLAUSES:
                _add_entities(a, out["entities"])
        else:
            logger.warning("[TAIWAN_ANNOUNCEMENT] %s %s: %s",
                           meta.get("ticker"), meta.get("statement_date"), status)
        if fixed:
            return "fixed_noise" if out is not None else "fixed_noise_no_summary"
        if (stock := large_stock_dividend_reason(meta, body)) is not None:
            meta["announcement_materiality_signal"] = "HIGH"
            meta["announcement_materiality_reason"] = stock
            return "stock_dividend_rule"
        if (same_board := board_unchanged_reason(meta, body)) is not None:
            meta["announcement_materiality_signal"] = "NOISE"
            meta["announcement_materiality_reason"] = same_board
            return "board_unchanged_rule"
        if out is not None:
            meta["announcement_materiality_signal"] = _rating(meta, subject, out["signal_detection"], body)
            meta["announcement_materiality_reason"] = out["signal_reason"]
            label = out["signal_detection"]
            if meta["announcement_materiality_signal"] != _SIGNAL_DETECTION[label]:
                overridden.append(label)
            return f"model_{label}_repaired" if status == "repaired" else f"model_{label}"
        # The summary is discarded (translated_body gets the literal
        # translation), but a label and reason that pass the checks on
        # their own still stand - whether the filing is worth reading does
        # not depend on whether one key fact could be verified.
        kept = next((k for k in map(lambda r: label_only(r, source), reversed(answers)) if k), None)
        if kept:
            meta["announcement_materiality_signal"] = _rating(meta, subject, kept["signal_detection"], body)
            meta["announcement_materiality_reason"] = kept["signal_reason"]
            if meta["announcement_materiality_signal"] != _SIGNAL_DETECTION[kept["signal_detection"]]:
                overridden.append(kept["signal_detection"])
            return f"model_{kept['signal_detection']}_label_only"
        meta["announcement_materiality_signal"] = "WEAK"
        meta["announcement_materiality_reason"] = _FALLBACK_REASON[status.split(":")[0]]
        return "fallback"

    with ThreadPoolExecutor(max_workers=config.TAIWAN_CLASSIFY_CONCURRENCY) as ex:
        results = list(ex.map(_one, pending))
    unavailable = [item for item, r in zip(pending, results) if r == "model_unavailable"]
    if unavailable:
        logger.warning("[TAIWAN_ANNOUNCEMENT] model did not answer for %d filing(s); retrying in %ds",
                       len(unavailable), _OUTAGE_PAUSE_SECS)
        time.sleep(_OUTAGE_PAUSE_SECS)
        with ThreadPoolExecutor(max_workers=config.TAIWAN_CLASSIFY_CONCURRENCY) as ex:
            retried = list(ex.map(lambda item: _one(item, first_pass=False), unavailable))
        results = [r for r in results if r != "model_unavailable"] + retried
    for result in results:
        counts[result] = counts.get(result, 0) + 1
    logger.info("[TAIWAN_ANNOUNCEMENT] %s", " ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    # What is stored (signal_detection column), from the final rating itself -
    # the one line here that always matches the database.
    stored = {label: 0 for label in _SIGNAL_DETECTION}
    for _, meta, *_ in pending:
        stored[_DETECTION_OF[meta["announcement_materiality_signal"]]] += 1
    logger.info("[TAIWAN_ANNOUNCEMENT] stored: %s rule_overrides=%d",
                " ".join(f"{k}={v}" for k, v in stored.items()), len(overridden))
    with _usage_lock:
        u = dict(_usage)
        _usage.update(calls=0, prompt=0, cached=0, completion=0, cost=0.0)
    logger.info("[TAIWAN_ANNOUNCEMENT] spend: %d filings, %d calls, %d input tokens (%.0f%% cached), "
                "%d output tokens, $%.4f", len(pending), u["calls"], u["prompt"],
                100 * u["cached"] / u["prompt"] if u["prompt"] else 0, u["completion"], u["cost"])


def _add_entities(a: dict[str, Any], found: list[dict[str, str]]) -> None:
    for e in found:
        if _NOT_A_PARTY_RE.search(e["name"]):
            continue
        # The subsidiary from the subject is Chinese only; the model's
        # "English (原文)" for the same company replaces it.
        same = next((i for i, x in enumerate(a["_entities"]) if _entity_key(x["name"]) == _entity_key(e["name"])), None)
        if same is None:
            a["_entities"].append(e)
        elif same > 0 and len(e["name"]) > len(a["_entities"][same]["name"]):
            a["_entities"][same] = e
