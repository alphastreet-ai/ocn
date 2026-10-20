"""Taiwan material announcements: date conversion, fixed NOISE, and the
checks that keep an unverified model answer from being stored."""
from __future__ import annotations

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared', 'src'))

import pipeline.taiwan_announcement_classifier as tac  # noqa: E402
from pipeline.taiwan_announcement_classifier import (  # noqa: E402
    classify_announcements,
    filed_for_subsidiary,
    fixed_noise_reason,
    invented_content,
    normalize_roc_dates,
    parse_and_check,
)


# --- 1. Taiwan calendar -> Western (all six formats seen in the live feed) ---

@pytest.mark.parametrize("raw,expected", [
    ("1.董事會決議日期:115/10/14", "1.董事會決議日期:2026-10-14"),
    ("民國115年06月24日", "2026-06-24"),
    ("115年05月29日股東常會", "2026-05-29股東常會"),
    ("112年 9月12日金管證發字", "2023-09-12金管證發字"),       # line break collapsed to a space
    ("公告期間：115年7月22日至115年10月21日", "公告期間：2026-07-22至2026-10-21"),
    ("115年第3季合併財務報告", "2026年第3季合併財務報告"),
    ("115年度財務報告", "2026年度財務報告"),
    ("董事會決議召開一一四年股東常會", "董事會決議召開2025年股東常會"),   # Chinese-numeral year
    ("１１１／０３／０１", "2022-03-01"),                                      # full-width digits
    ("自115年7月起", "自2026-07起"),
])
def test_roc_dates_are_converted(raw, expected) -> None:
    assert normalize_roc_dates(raw) == expected


@pytest.mark.parametrize("raw", [
    "交易總金額：新台幣 301,200,000 元",   # amounts untouched
    "每股新台幣1151014元",                 # 7-digit number is not treated as a date
    "第115條",                              # no 年
])
def test_non_dates_are_left_alone(raw) -> None:
    assert normalize_roc_dates(raw) == raw


# --- 2. Fixed NOISE (no model call) ---

@pytest.mark.parametrize("clause,subject,expected", [
    ("第12款", "本公司受邀參加法人說明會", "A notice that the company will hold or attend an investor conference"),
    ("第23款", "資金貸與", "A notice of funds the company lent"),
    ("第31款", "公告本公司2026年第一季財務報告董事會召開日期", "A notice of the date of the board meeting"),
    ("第31款", "公告本公司2026年第1季合併財務報告業經董事會通過", None),   # results -> model
    ("第20款", "取得機器設備", None),
    ("第51款", "其他", None),
])
def test_fixed_noise_rule(clause, subject, expected) -> None:
    got = fixed_noise_reason({"disclosure_clause_code": clause}, subject)
    assert got == expected if expected is None else got.startswith(expected)


# --- 3. Output checks ---

SOURCE = ("Subject (主旨): 公告本公司取得營業用不動產\n"
          "Text (說明): 交易日期:2026-10-14 交易總金額：新台幣 301,200,000 元 "
          "交易相對人：禾融工程有限公司；非關係人 Western Digital (Malaysia) Sdn. Bhd.")


@pytest.mark.parametrize("text", [
    "The company bought a building for NT$301,200,000 on 2026-10-14.",
    "Effective 2026-10-14; first payment in 2026-10.",
    "The counterparty is Herong Engineering Co., Ltd. (禾融工程有限公司), a non-related party.",
    "Seller: Western Digital (Malaysia) Sdn. Bhd.",
])
def test_grounded_text_passes(text) -> None:
    assert invented_content(text, SOURCE) is None


@pytest.mark.parametrize("text,why", [
    ("The deal is worth NT$301 million.", "amount not in the filing"),            # rounded (filing: 301,200,000)
    ("Signed on October 14, 2026.", "month name"),
    ("The price doubled versus last year.", "quantity word"),
    ("The seller is 鴻海精密工業.", "Chinese text not in the filing"),
    ("Bought from Hon Hai Precision Industry Co., Ltd.", "company name not in the filing"),
    ("A fire with potential financial implications.", "prediction or generalisation"),
    ("Such changes do not usually affect operations.", "prediction or generalisation"),
    ("This could lead to higher sales.", "prediction or generalisation"),
])
def test_invented_text_is_refused(text, why) -> None:
    assert why in (invented_content(text, SOURCE) or "")


def _answer(**over) -> str:
    base = {"signal_detection": "signal",
            "signal_reason": "The company bought an operating property from a non-related party.",
            "summary": "The company bought a building for NT$301,200,000 on 2026-10-14.",
            "key_facts": [{"label": "Amount", "value": "NT$301,200,000"}]}
    base.update(over)
    return json.dumps(base, ensure_ascii=False)


@pytest.mark.parametrize("raw,status", [
    (None, "model_failed"),
    ("not json", "model_output_rejected: not JSON"),
    (_answer(signal_detection="HIGH"), "model_output_rejected: signal_detection"),
    (_answer(signal_reason="ok"), "model_output_rejected: signal_reason"),
    (_answer(signal_reason="The deal is the company's largest since NT$500,000,000 in the prior year."),
     "model_output_rejected: number not in the filing"),
    (_answer(summary=""), "model_output_rejected: summary"),
    (_answer(key_facts="Amount: NT$301,200,000"), "model_output_rejected: key_facts"),
    (_answer(key_facts=[{"label": "Amount", "value": "NT$300,000,000"}]),
     "model_output_rejected: number not in the filing"),
])
def test_bad_answers_are_refused(raw, status) -> None:
    out, why = parse_and_check(raw, SOURCE)
    assert out is None and why.startswith(status)


def test_good_answer_is_accepted() -> None:
    out, why = parse_and_check(_answer(), SOURCE)
    assert why == "ok" and out["signal_detection"] == "signal"


# --- 4. End to end with a stubbed model ---

def _material(clause: str, subject: str, body: str) -> dict:
    return {"summary": subject, "body": body,
            "metadata": {"source_category": "mops_material", "ticker": "3231",
                         "exchange": "TWSE", "disclosure_clause_code": clause,
                         "statement_date": "2026-10-15"}}


def test_end_to_end_model_path_fixed_noise_and_fallback(monkeypatch) -> None:
    calls: list[str] = []
    bad_reason = "The company bought an operating property on October 14."
    noise_summary = _answer(signal_detection="signal", summary="The company will attend an investor conference on 2026-11-02.",
                            key_facts=[{"label": "Date", "value": "2026-11-02"}])

    def model(messages):
        user = messages[1]["content"]
        calls.append(user)
        if "法人說明會" in user:
            return noise_summary
        if "REFUSE" in user:
            return _answer(signal_reason=bad_reason)              # refused every time
        return _answer()

    monkeypatch.setattr(tac, "_call_model", model)
    body = "交易日期:115/10/14 交易總金額：新台幣 301,200,000 元"
    good = _material("第20款", "公告本公司取得營業用不動產", body)
    noise = _material("第12款", "本公司受邀參加法人說明會", "日期:115/11/02")
    refused = _material("第20款", "公告本公司取得營業用不動產REFUSE", body)
    classify_announcements([good, noise, refused])

    assert len(calls) == 1 + 1 + 3                                 # fixed NOISE gets a summary call; 2 repairs
    assert all("115/10/14" not in c for c in calls)                # model saw converted dates
    g = good["metadata"]
    assert g["announcement_materiality_signal"] == "HIGH"
    assert g["announcement_materiality_reason"].startswith("The company bought an operating property")
    assert g["translated_summary"].startswith("The company bought a building")
    assert "Key facts:\n- Amount: NT$301,200,000" in g["translated_summary"]
    assert "translated_title" not in g and "translated_body" not in g   # the translation step's job
    n = noise["metadata"]
    assert n["announcement_materiality_signal"] == "NOISE"         # label by rule, model's "signal" ignored
    assert n["announcement_materiality_reason"].startswith("A notice that the company will hold or attend an investor conference")
    assert n["translated_summary"].startswith("The company will attend an investor conference")
    r = refused["metadata"]
    assert r["announcement_materiality_signal"] == "WEAK"
    assert r["announcement_materiality_reason"].startswith("The automatic summary of this filing could not be checked")
    assert "translated_summary" not in r                           # translated_body comes from the literal translation


def test_repair_attempts_fix_a_refused_answer(monkeypatch) -> None:
    sent: list[list[dict]] = []
    answers = iter([_answer(summary="Bought on October 14."), _answer(summary="Bought on October 14."), _answer()])
    monkeypatch.setattr(tac, "_call_model", lambda messages: sent.append(list(messages)) or next(answers))
    a = _material("第20款", "公告本公司取得營業用不動產", "交易日期:115/10/14 交易總金額：新台幣 301,200,000 元")
    classify_announcements([a])
    assert len(sent) == 3                                  # second repair succeeded
    assert "month name" in sent[1][-1]["content"]          # told exactly what to fix
    assert a["metadata"]["announcement_materiality_signal"] == "HIGH"
    assert a["metadata"]["translated_summary"].startswith("The company bought a building")


def test_refused_summary_keeps_a_label_and_reason_that_pass_on_their_own(monkeypatch) -> None:
    # A results release fell to weak_signal because one key fact failed the
    # number check - the label never depended on that fact.
    bad = _answer(key_facts=[{"label": "EPS", "value": "NT$70.17"}])
    monkeypatch.setattr(tac, "_call_model", lambda messages: bad)
    a = _material("第20款", "公告本公司取得營業用不動產", "交易日期:115/10/14 交易總金額：新台幣 301,200,000 元")
    classify_announcements([a])
    m = a["metadata"]
    assert m["announcement_materiality_signal"] == "HIGH"
    assert m["announcement_materiality_reason"].startswith("The company bought an operating property")
    assert "translated_summary" not in m                   # unverified summary never stored


def test_supplement_is_capped_at_weak_and_subsidiary_dividend_is_noise(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(tac, "_call_model", lambda messages: calls.append(1) or _answer())
    body = "交易日期:115/10/14 交易總金額：新台幣 301,200,000 元"
    supplement = _material("第20款", "(補充公告)本公司董事會決議通過擬取得國內不動產", body)
    dividend = _material("第14款", "代重要子公司Goldex Holding Limited公告決議變更股利分派", body)
    own_dividend = _material("第14款", "董事會決議股利發放事宜", body)
    classify_announcements([supplement, dividend, own_dividend])
    assert len(calls) == 3                                           # every filing gets a summary call
    assert supplement["metadata"]["announcement_materiality_signal"] == "WEAK"
    assert dividend["metadata"]["announcement_materiality_signal"] == "NOISE"
    assert dividend["metadata"]["announcement_materiality_reason"].startswith("A dividend decided by a subsidiary")
    assert own_dividend["metadata"]["announcement_materiality_signal"] == "HIGH"


def test_repair_message_explains_chinese_units(monkeypatch) -> None:
    sent: list[list[dict]] = []
    answers = iter([_answer(summary="The budget is NT$49 billion."), _answer()])
    monkeypatch.setattr(tac, "_call_model", lambda messages: sent.append(list(messages)) or next(answers))
    classify_announcements([_material("第15款", "公告本公司資本預算", "資本預算新台幣49億元 新台幣 301,200,000 元 115/10/14")])
    assert "49億 = 4.9 billion" in sent[1][-1]["content"]


@pytest.mark.parametrize("raw,expected", [
    ("台積公司2025年4月營收報告", "台積公司2025-04營收報告"),
    ("於2024年3月5日完成", "於2024-03-05完成"),
])
def test_western_years_written_the_chinese_way_are_converted(raw, expected) -> None:
    assert normalize_roc_dates(raw) == expected


@pytest.mark.parametrize("filing,output,ok", [
    ("合併營收約為新台幣3,495億6,700萬元", "revenue of about NT$349,567 million", True),   # TSMC, two-part amount
    ("合併營收約為新台幣3,495億6,700萬元", "revenue of about NT$349,568 million", False),
    ("有關武漢肺炎疫情影響之補充說明", "an update on COVID-19", True),
    ("對股權可能稀釋情形:另行公告", "the potential dilution will be announced later", True),
    ("發行總面額上限為新臺幣四十億元整", "this could potentially dilute shareholders", False),
])
def test_review_driven_checks_round_3(filing, output, ok) -> None:
    assert (invented_content(output, filing) is None) is ok


@pytest.mark.parametrize("subject,expected", [
    ("本公司代子公司Delta Electronics (Thailand) Public\nCompany Limited公告現金股利除息基準日",
     "Delta Electronics (Thailand) Public Company Limited"),
    ("代重要子公司中山台光電子材料有限公司公告盈餘分配案", "中山台光電子材料有限公司"),
    ("代子公司緯創資通(昆山)有限公司(簡稱WAKS)公告處分土地使用權", "緯創資通(昆山)有限公司"),
    ("代子公司翔耀電子(深圳)、康聯精密機電(深圳)有限公司公告取得使用權資產", None),
    ("公告本公司董事會決議股利分派", None),
    ("代重要子公司公告武漢肺炎影響之說明", None),                  # no subsidiary named
])
def test_filed_for_subsidiary_comes_from_the_subject(subject, expected) -> None:
    assert filed_for_subsidiary(subject) == expected


def test_dividend_timetable_is_fixed_noise() -> None:
    meta = {"disclosure_clause_code": "第14款"}
    assert fixed_noise_reason(meta, "公告本公司訂定除息基準日").startswith("The ex-dividend or record-date timetable")
    assert fixed_noise_reason(meta, "台積公司訂定除息交易日為9月11日") is not None
    assert fixed_noise_reason(meta, "公告本公司董事會決議股利分派") is None   # the decision itself


def test_unit_after_digits_is_not_turned_into_a_number() -> None:
    # "441佰萬元" is 441 million; converting "佰萬" alone gave "4411000000元".
    assert normalize_roc_dates("新台幣441佰萬元") == "新台幣441佰萬元"
    assert normalize_roc_dates("美金10.7 佰萬元") == "美金10.7 佰萬元"
    assert invented_content("NT$441 million", "新台幣441佰萬元") is None
    assert invented_content("NT$4,411 million", "新台幣441佰萬元") is not None


# --- entities ------------------------------------------------------------------

def test_entities_must_appear_in_the_filing() -> None:
    out, why = parse_and_check(_answer(entities=[{"name": "禾融工程有限公司", "type": "company"}]), SOURCE)
    assert why == "ok" and out["entities"] == [{"name": "禾融工程有限公司", "type": "company"}]
    out, why = parse_and_check(_answer(entities=[{"name": "Apple Inc.", "type": "company"}]), SOURCE)
    assert out is None and why.startswith("model_output_rejected: entity not in the filing")
    out, why = parse_and_check(_answer(entities=[{"name": "禾融工程有限公司", "type": "person"}]), SOURCE)
    assert out is None and why == "model_output_rejected: entities"


def test_filer_is_always_an_entity_and_model_adds_the_others(monkeypatch) -> None:
    monkeypatch.setattr(tac, "_call_model", lambda messages: _answer(
        entities=[{"name": "禾融工程有限公司", "type": "company"}]))
    body = "交易日期:115/10/14 交易總金額：新台幣 301,200,000 元 交易相對人：禾融工程有限公司"
    deal = _material("第20款", "公告本公司取得營業用不動產", body)
    deal["metadata"]["translated_company_name"] = "Wistron"
    conference = _material("第12款", "本公司受邀參加法人說明會", "日期:115/11/02")
    conference["metadata"]["translated_company_name"] = "Wistron"
    classify_announcements([deal, conference])
    assert deal["_entities"] == [{"name": "Wistron", "type": "company"},
                                 {"name": "禾融工程有限公司", "type": "company"}]
    assert conference["_entities"] == [{"name": "Wistron", "type": "company"}]


def test_board_change_keeps_only_the_filer_even_if_the_model_lists_others(monkeypatch) -> None:
    monkeypatch.setattr(tac, "_call_model", lambda messages: _answer(
        entities=[{"name": "禾融工程有限公司", "type": "company"}]))
    board = _material("第6款", "公告本公司董事變動達三分之一",
                      "交易日期:115/10/14 交易總金額：新台幣 301,200,000 元 禾融工程有限公司")
    board["metadata"]["translated_company_name"] = "Aspeed Technology"
    classify_announcements([board])
    assert board["_entities"] == [{"name": "Aspeed Technology", "type": "company"}]


# --- amounts in another scale / in Chinese numerals ------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("新台幣壹佰貳拾壹億伍仟萬元", "新台幣12150000000元"),
    ("美金拾億元", "美金1000000000元"),
    ("一一四年股東常會", "2025年股東常會"),          # year form untouched by the amount rule
])
def test_chinese_numeral_amounts_become_digits(raw, expected) -> None:
    assert normalize_roc_dates(raw) == expected


@pytest.mark.parametrize("filing,output,ok", [
    ("預算上限泰銖60.6億元", "a budget of THB 6.06 billion", True),
    ("投資上限5億美元", "up to USD 500 million", True),
    ("面額美金20萬元", "face value USD 200,000", True),
    ("營收新台幣592,640,123仟元", "revenue of NT$592,640 million", False),   # rounded
    ("營收新台幣592,640,123仟元", "revenue of NT$592,640,123 thousand", True),
])
def test_amounts_are_checked_by_value(filing, output, ok) -> None:
    assert (invented_content(output, filing) is None) is ok


# --- fixes from the four-reviewer review --------------------------------------

@pytest.mark.parametrize("filing,output,why", [
    ("募集新台幣600億元公司債", "a bond issue of NT$600 billion", "amount not in the filing"),   # live TSMC error
    ("募集新台幣600億元公司債", "a bond issue of NT$60 billion", None),
    ("交易相對人：台灣福雷電子", "The counterparty is Taiwan Fulei Electronics.", "company name not in the filing"),
    ("交易相對人：台灣福雷電子", "The Group bought shares from Taiwan Fulei Electronics (台灣福雷電子).", None),
])
def test_review_driven_checks(filing, output, why) -> None:
    got = invented_content(output, filing)
    assert (got is None) if why is None else (why in (got or ""))


def test_trading_halt_is_always_signal(monkeypatch) -> None:
    monkeypatch.setattr(tac, "_call_model", lambda messages: _answer(
        signal_detection="noise", signal_reason="The company announced that trading in its shares is halted."))
    halt = _material("第40款", "本公司因有重大訊息待公布暫停交易", "交易日期:115/10/14 交易總金額：新台幣 301,200,000 元")
    classify_announcements([halt])
    assert halt["metadata"]["announcement_materiality_signal"] == "HIGH"


@pytest.mark.parametrize("filing,output,ok", [
    ("單位：新台幣仟元 營業收入 33,783,411", "operating revenue of 33,783,411 thousand", True),   # unit in table header
    ("單位：新台幣百萬元 營收 3,083", "revenue of NT$3,083 million", True),
    ("募集新台幣600億元公司債", "a bond issue of NT$600 billion", False),                        # still refused
    ("因新冠肺炎疫情停工", "operations stopped due to COVID-19 measures", True),
    ("因疫情停工", "operations stopped due to COVID-19 measures", False),                      # filing never says COVID
])
def test_header_units_and_standard_terms(filing, output, ok) -> None:
    assert (invented_content(output, filing) is None) is ok


def test_month_name_with_a_year_is_rewritten_then_checked() -> None:
    # TSMC / BizLink: "April 2025" written on every attempt; the date was right.
    out, why = parse_and_check(_answer(summary="The company bought a building for NT$301,200,000 on October 14, 2026."), SOURCE)
    assert why == "ok" and "on 2026-10-14" in out["summary"]
    out, why = parse_and_check(_answer(summary="The company bought a building on October 15, 2026."), SOURCE)
    assert out is None and "number not in the filing" in why          # rewritten, then still checked


@pytest.mark.parametrize("filing,output,ok", [
    ("發放現金股利共計美金7仟萬元", "a total cash dividend of USD 70 million", True),      # 仟萬 = ten million
    ("現金股利：每仟股配發新台幣2,600元", "NT$2,600 per 1,000 shares", True),
    ("17.預定完成日程: 預計2025-05底前完成", "The merger is expected to be completed by 2025-05.", True),
    ("交易金額新台幣100元", "Sales are expected to rise.", False),                    # no 預計 in the filing
])
def test_review_driven_checks_round_4(filing, output, ok) -> None:
    assert (invented_content(output, filing) is None) is ok


def test_chinese_numeral_month_day_and_month_ranges() -> None:
    assert normalize_roc_dates("於2019年六月二十一日股東常會") == "於2019-06-21股東常會"
    assert normalize_roc_dates("民國一一四年十二月三十一日") == "2025-12-31"
    out, why = parse_and_check(_answer(summary="The company bought a building for NT$301,200,000 between "
                                               "October and October 2026."), SOURCE)
    assert why == "ok" and "2026-10 to 2026-10" in out["summary"]


def test_english_name_is_allowed_only_next_to_the_filings_own_chinese_name() -> None:
    src = "本公司之子公司蘇州群策科技股份有限公司遞交香港聯合交易所有限公司主板上市申請文件"
    ok = ("Its subsidiary Suzhou Qunce Technology Co., Ltd. (蘇州群策科技股份有限公司) applied to the "
          "Main Board of The Stock Exchange of Hong Kong Limited (香港聯合交易所有限公司主板). "
          "Suzhou Qunce Technology Co., Ltd. filed on its own.")
    assert invented_content(ok, src) is None
    assert "company name not in the filing" in invented_content("Suzhou Qunce Technology Co., Ltd. applied.", src)
    assert "Chinese text not in the filing" in invented_content("Suzhou Qunce Technology Co., Ltd. (蘇州某某公司)", src)


def test_chinese_outside_brackets_is_refused_and_exchanges_are_not_parties(monkeypatch) -> None:
    src = "本公司之子公司蘇州群策科技股份有限公司遞交香港聯合交易所有限公司主板上市申請文件"
    assert "without an English translation" in invented_content("Its subsidiary 蘇州群策科技股份有限公司 applied.", src)
    monkeypatch.setattr(tac, "_call_model", lambda messages: _answer(entities=[
        {"name": "禾融工程有限公司", "type": "company"}, {"name": "香港聯合交易所有限公司主板", "type": "company"}]))
    a = _material("第20款", "公告本公司取得營業用不動產",
                  "交易日期:115/10/14 交易總金額：新台幣 301,200,000 元 禾融工程有限公司 香港聯合交易所有限公司主板")
    classify_announcements([a])
    assert [e["name"] for e in a["_entities"]] == ["禾融工程有限公司"]


def test_entity_in_english_with_the_filed_chinese_replaces_the_subject_one(monkeypatch) -> None:
    gloss = "Taiwan Fulei Electronics Co., Ltd. (台灣福雷電子股份有限公司)"
    monkeypatch.setattr(tac, "_call_model", lambda messages: _answer(entities=[{"name": gloss, "type": "company"}]))
    a = _material("第20款", "代子公司台灣福雷電子股份有限公司公告取得機器設備",
                  "交易日期:115/10/14 交易總金額：新台幣 301,200,000 元 台灣福雷電子股份有限公司")
    a["metadata"]["translated_company_name"] = "ASE Technology"
    classify_announcements([a])
    assert [e["name"] for e in a["_entities"]] == ["ASE Technology", gloss]
    out, why = parse_and_check(_answer(entities=[{"name": "Apple Inc. (蘋果公司)", "type": "company"}]), SOURCE)
    assert out is None and "entity not in the filing" in why          # Chinese part not in the filing


def test_translation_step_writes_title_and_body_only_when_no_summary(monkeypatch) -> None:
    import pipeline.taiwan_signal_classifier as tsc
    monkeypatch.setattr(tsc, "_translate_one", lambda text, *a, **k: f"EN:{text}")
    with_summary = _material("第20款", "主旨A", "說明A")
    with_summary["metadata"].update(source_category="mops_material", translated_summary="checked summary")
    without = _material("第20款", "主旨B", "說明B")
    tsc.translate_taiwan_articles([with_summary, without], model="m", api_key="k", base_url="u", timeout=1)
    assert with_summary["metadata"]["translated_title"] == "EN:主旨A"
    assert "translated_body" not in with_summary["metadata"]        # the summary covers the body
    assert without["metadata"]["translated_title"] == "EN:主旨B"
    assert without["metadata"]["translated_body"] == "EN:說明B"


def test_model_outage_is_retried_at_the_end_of_the_run_then_stored(monkeypatch) -> None:
    import pipeline.taiwan_signal_classifier as tsc
    monkeypatch.setattr(tac, "_OUTAGE_PAUSE_SECS", 0)
    monkeypatch.setattr(tsc, "_translate_one", lambda text, *a, **k: f"EN:{text}")
    calls = {"n": 0}

    def flaky(messages):                       # down for the first answer of each filing
        calls["n"] += 1
        return None if calls["n"] <= 2 else _answer()
    monkeypatch.setattr(tac, "_call_model", flaky)
    recovers = _material("第20款", "公告本公司取得營業用不動產", "交易總金額：新台幣 301,200,000 元 115/10/14")
    recovers["metadata"]["ticker"] = "1111"
    other = _material("第20款", "公告本公司取得營業用不動產", "交易總金額：新台幣 301,200,000 元 115/10/14")
    classify_announcements([recovers, other])
    for a in (recovers, other):
        assert a["metadata"]["translated_summary"].startswith("The company bought a building")

    monkeypatch.setattr(tac, "_call_model", lambda messages: None)          # down for the whole run
    down = _material("第20款", "公告本公司取得營業用不動產", "交易總金額：新台幣 301,200,000 元")
    down.update(url="twse-material://3231/2026-10-15/1", published="2026-10-15")
    results = tsc.classify_taiwan_signal_batch([down], model="m")
    assert [r["article"] for r in results] == [down]                # stored, never dropped
    m = down["metadata"]
    assert m["translated_body"].startswith("EN:") and "translated_summary" not in m
    assert "announcement_materiality_signal" not in m              # popped - lives in the columns
    assert results[0]["result"]["signal"] == "WEAK"


def test_key_facts_are_tidied_never_added_and_nested_brackets_are_one_name() -> None:
    out, why = parse_and_check(_answer(key_facts=[{"label": "Amount"}, {"label": "Total", "value": 301200000},
                                                  {"label": "", "value": "x"}]), SOURCE)
    assert why == "ok" and out["key_facts"] == [{"label": "Total", "value": "301200000"}]
    src = "交易相對人：勤昆科技(昆山)有限公司 翔耀電子（深圳）有限公司"
    assert invented_content("Kun Kun Technology (Kunshan) Co., Ltd. (勤昆科技(昆山)有限公司) and "
                            "Xiangyao Electronics (Shenzhen) Co., Ltd. (翔耀電子（深圳）有限公司).", src) is None
    why = invented_content("Sold to 勤昆科技 and 翔耀電子.", src)
    assert "'勤昆科技'" in why and "'翔耀電子'" in why                  # every instance listed


def test_company_suffix_must_be_a_whole_word() -> None:
    src = normalize_roc_dates("本公司受邀參加富邦證券舉辦之2026第一季 富邦企業日")
    assert src.endswith("2026第1季富邦企業日")
    assert invented_content("It will attend the 2026 Q1 Fubon Corporate Day.", src) is None
    assert "company name not in the filing" in invented_content("Hosted by Fubon Corp.", src)


def test_round_5_first_answer_fixes() -> None:
    # Chinese the model added to a name the filing writes in English is dropped, not refused
    src = "本公司受邀參加J.P. Morgan舉辦之Taiwan CEO-CFO conference"
    out, why = parse_and_check(_answer(summary="It will attend a J.P. Morgan (摩根大通) conference.", key_facts=[],
                                       entities=[{"name": "J.P. Morgan (摩根大通)", "type": "company"}],
                                       signal_reason="An investor conference notice from the company."), src)
    assert why == "ok" and "摩根大通" not in out["summary"] and out["entities"][0]["name"] == "J.P. Morgan"
    # ... but invented Chinese after an English name NOT in the filing is still refused
    assert parse_and_check(_answer(summary="Sold to Apple (蘋果).", key_facts=[], signal_reason="A sale to another company."), src)[0] is None
    # 1兆 460億9千萬元 = 1,046.09 billion; 1,046.9 billion is wrong
    t = "合併營收約新台幣1兆 460億9千萬元"
    assert invented_content("revenue of NT$1,046.09 billion", t) is None
    assert invented_content("revenue of NT$1,046.9 billion", t) is not None
    # (股)公司 is expanded; "&" and multi-word brackets are part of a name
    assert normalize_roc_dates("代子公司矽品精密工業(股)公司公告") == "代子公司矽品精密工業股份有限公司公告"
    assert tac._GLOSS_RE.fullmatch("Tongtai Machine & Tool Co., Ltd. (東台精機股份有限公司)")
    assert tac._GLOSS_RE.fullmatch("Shangce Fund Partnership (Limited Partnership) (上海尚策)")


def test_event_titles_and_line_breaks_in_chinese_names() -> None:
    src = normalize_roc_dates("本公司受邀參加元大證券舉辦之第四季投資論壇 子公司昆山鼎昌鑫電子 科技有限公司")
    assert "昆山鼎昌鑫電子科技有限公司" in src
    assert invented_content("It attends the Yuanta Securities (元大證券) Q4 Investment Forum.", src) is None
    assert invented_content("Kunshan Dingchangxin Electronics Technology Co., Ltd. (昆山鼎昌鑫電子科技有限公司)", src) is None
    assert "company name not in the filing" in invented_content("Hosted by Yuanta Securities.", src)


def test_blank_form_fields_are_not_key_facts() -> None:
    blanks = [{"label": "Cash dividend per share", "value": "0"}, {"label": "Total cash dividend", "value": "NT$0"},
              {"label": "Stock dividend (shares)", "value": 0}, {"label": "Insurance compensation", "value": "None"},
              {"label": "Other", "value": "不適用"}, {"label": "Amount", "value": "NT$301,200,000"}]
    out, why = parse_and_check(_answer(key_facts=blanks), SOURCE)
    assert why == "ok" and out["key_facts"] == [{"label": "Amount", "value": "NT$301,200,000"}]


# --- review 2026-10-19: rules the model ignored, now in code ------------------

_DIV = ("4. 股東配發內容： (1)盈餘分配之現金股利(元/股)：{cash} (2)法定盈餘公積發放之現金(元/股)：0 "
        "(3)資本公積發放之現金(元/股)：0 (4)股東配發之現金(股利)總金額(元)：1,000 "
        "(5)盈餘轉增資配股(元/股)：{stock} (6)法定盈餘公積轉增資配股(元/股)：0 "
        "(7)資本公積轉增資配股(元/股)：0 (8)股東配股總股數(股)：0 5. 其他 6. 普通股每股面額欄位：新台幣10.0000元")


def test_no_dividend_is_noise_and_large_stock_dividend_is_signal(monkeypatch) -> None:
    calls: list[int] = []
    monkeypatch.setattr(tac, "_call_model", lambda messages: calls.append(1) or _answer(signal_detection="weak_signal"))
    none = _material("第14款", "董事會決議股利分派", _DIV.format(cash="0", stock="0"))
    plain = _material("第14款", "董事會決議不分配股利", "2.發放股利種類及金額:董事會決議不分配114年度股利。")
    stock = _material("第14款", "董事會決議股利分派", _DIV.format(cash="145.00000000", stock="20.00000000"))
    small = _material("第14款", "董事會決議股利分派", _DIV.format(cash="5", stock="1"))
    classify_announcements([none, plain, stock, small])
    assert none["metadata"]["announcement_materiality_signal"] == "NOISE"
    assert plain["metadata"]["announcement_materiality_signal"] == "NOISE"
    assert stock["metadata"]["announcement_materiality_signal"] == "HIGH"
    assert "2 new shares for every share held" in stock["metadata"]["announcement_materiality_reason"]
    assert small["metadata"]["announcement_materiality_signal"] == "WEAK"            # left to the model


def test_known_names_replace_guessed_english() -> None:
    assert tac.apply_known_names("on behalf of subsidiary Chilisin Electronics Corp. (乾坤科技股份有限公司) regarding") == \
        "on behalf of subsidiary Cyntec Co., Ltd. (乾坤科技股份有限公司) regarding"
    assert "Known English name - use exactly: 乾坤科技股份有限公司 = Cyntec Co., Ltd." in \
        tac._user_message({}, "代子公司乾坤科技股份有限公司公告", "說明")


def test_par_value_total_is_not_money_raised() -> None:
    src = "5.發行總金額及股數:新台幣50,000,000元及發行5,000,000股 8.每股面額:新台幣10元 9.發行價格:每股新台幣230元整"
    assert "par value" in tac.par_total_misread("issuing 5,000,000 shares at NT$230, raising NT$50,000,000", src)
    assert tac.par_total_misread("5,000,000 shares at NT$230 each; NT$50,000,000 at par value", src) is None


def test_unusual_trading_disclosure_is_capped_at_weak() -> None:
    meta = {"disclosure_clause_code": "第51款"}
    assert tac._rating(meta, "本公司有價證券近期股價異常，故公告相關訊息", "signal") == "WEAK"
    assert tac._rating(meta, "公告本公司董事會通過財務報告", "signal") == "HIGH"


def test_investment_vehicle_is_capped_at_weak() -> None:
    meta = {"disclosure_clause_code": "第20款"}
    assert tac._rating(meta, "董事會決議新設子公司緯創創業投資股份有限公司", "signal") == "WEAK"
    assert tac._rating(meta, "公告本公司取得亞灣超算股份有限公司股份", "signal") == "HIGH"


def test_results_summary_must_state_the_filed_figures() -> None:
    src = "4.1月1日累計至本期止營業收入(仟元):11653759 10.1月1日累計至本期止基本每股盈餘(損失) (元):16.05"
    assert "without figures" in tac.results_without_figures(31, "The board approved results including revenue and EPS.", src)
    assert tac.results_without_figures(31, "Revenue was NT$11,653,759 thousand and EPS NT$16.05.", src) is None
    assert tac.results_without_figures(20, "No figures here.", src) is None          # only results filings


def test_name_check_does_not_read_across_lines_or_regulation_titles() -> None:
    src = "交易相對人：BizLink Special Cables Germany GmbH 持股比例：100% 「境內企業境外發行證券和上市管理試行辦法」"
    assert tac.invented_content("Counterparty: BizLink Special Cables Germany GmbH\nHolding after transaction: 100%", src) is None
    assert tac.invented_content("under the Trial Measures of Overseas Securities Offering and Listing", src) is None
    assert "company name" in tac.invented_content("bought from Overseas Securities Ltd.", src)


def test_gloss_may_start_with_latin_letters() -> None:
    assert tac._GLOSS_RE.fullmatch("LEONI Group (LEONI集團)").groups() == ("LEONI Group", "LEONI集團")
    assert tac.invented_content("bought a unit of LEONI Group (LEONI集團)", "德商LEONI集團旗下工業應用事業群") is None


def test_bracket_repeating_the_name_is_dropped() -> None:
    assert tac._drop_repeated_bracket("into CHENBRO (MALAYSIA) SDN. BHD. (CHENBRO (MALAYSIA) SDN. BHD.), the") \
        == "into CHENBRO (MALAYSIA) SDN. BHD., the"
    kept = "Wistron InfoComm (Vietnam) Co., Ltd. (WVN) and Delta (台達電)"
    assert tac._drop_repeated_bracket(kept) == kept


def test_date_must_be_one_the_filing_gives() -> None:
    src = "1.事實發生日:2022-04-02 5.發生緣由:自4月2日起至4月6日暫時停工 12/19董事會"
    assert tac.invented_date("halted from 2022-04-02 to 2022-04-06; board on 2022-12-19", src) is None
    assert "2022-03-05" in tac.invented_date("signed on 2022-03-05", src)
    assert tac.invented_date("for the quarter ended 2019-03-31", "2019年第1季合併財務報表 2019-05-06") is None


def test_table_unit_header_scales_its_figures() -> None:
    src = "營業收入(1000000元) 958 626 53% 稅前純益(1000000元) 129 33"
    assert tac.invented_content("Revenue was NT$958,000,000, against 626 million a year earlier.", src) is None
    assert tac.invented_content("Revenue was NT$958 million.", src) is None
    assert "958000" in tac.invented_content("Revenue was NT$958,000.", src).replace(",", "")


_LOTES_2019 = ("3.舊任者職稱、姓名及簡歷: 董事長：嘉明投資(股)代表人朱德祥、嘉澤端子工業(股)董事長董事：嘉明投資(股)代表人何德佑、總經理"
               "董事：蔡明叡、業務處副總董事：金昌民、所長 4.新任者職稱、姓名及簡歷: 董事：嘉明投資(股)代表人朱德祥、董事長"
               "董事：嘉明投資(股)代表人何德佑、總經理董事：金昌民、所長董事：蔡明叡、業務處副總 5.異動情形: 任期屆滿 "
               "13.屬三分之一以上董事發生變動（請輸入是或否）:是")


def test_board_seat_changes_counted_from_the_lists() -> None:
    assert tac.board_seat_changes(_LOTES_2019) == ([], 4)
    delta = _LOTES_2019.replace("4.新任者職稱、姓名及簡歷: 董事：嘉明投資(股)代表人朱德祥", "4.新任者職稱、姓名及簡歷: 董事：徐秀蘭女士")
    assert tac.board_seat_changes(delta) == (["徐秀蘭"], 4)
    assert tac.board_seat_changes(_LOTES_2019.replace("任期屆滿", "辭職")) is None   # not a term-end election


def test_unchanged_board_is_noise_and_the_count_reaches_the_model() -> None:
    meta = {"disclosure_clause_code": "第6款"}
    assert "all 4 directors" in tac.board_unchanged_reason(meta, _LOTES_2019)
    assert tac.board_unchanged_reason({"disclosure_clause_code": "第8款"}, _LOTES_2019) is None
    assert "0 of 4 seats" in tac._user_message(meta, "全面改選董事", _LOTES_2019)


def test_overseas_subsidiary_injection_is_at_least_weak() -> None:
    meta = {"disclosure_clause_code": "第20款"}
    assert tac._rating(meta, "董事會決議擬增資DELTA ELECTRONICS INDIA MANUFACTURING PRIVATE LIMITED", "noise") == "WEAK"
    assert tac._rating(meta, "公告本公司投資子公司UniWonderful Holding Limited之相關資料", "noise") == "WEAK"   # overseas holding too
    assert tac._rating(meta, "董事會決議對緯創數技投資控股股份有限公司增資", "noise") == "NOISE"          # Taiwan holding
    assert tac._rating(meta, "董事會決議對子公司緯成資產管理(股)公司增資", "noise") == "NOISE"            # Taiwan subsidiary
    assert tac._rating({"disclosure_clause_code": "第11款"}, "增資Wiwynn Mexico", "noise") == "NOISE"


def test_dividend_rate_adjustment_is_fixed_noise() -> None:
    meta = {"disclosure_clause_code": "第14款"}
    assert tac.fixed_noise_reason(meta, "公告本公司調整現金股利配息率") == tac._FIXED_NOISE_REASON["dividend_adjustment"]
    assert tac.fixed_noise_reason(meta, "公告本公司調整 114年股票及現金股利配股配息率") is not None


def test_english_name_adds_no_place() -> None:
    src = "台光電子材料(黃石)有限公司 達豐(重慶)電腦有限公司"
    assert "Suzhou" in tac.invented_content("Suzhou Elite Material (Huangshi) Co., Ltd. (台光電子材料(黃石)有限公司)", src)
    assert tac.invented_content("Taipei Branch (台北分公司)", "台北分公司") is None          # Wade-Giles reading
    assert tac.invented_content("Dafeng (Chongqing) Computer Co., Ltd. (達豐(重慶)電腦有限公司)", src) is None
    assert tac.invented_content("plants in Suzhou and at Dafeng (Chongqing) Computer Co., Ltd. (達豐(重慶)電腦有限公司)",
                                src + " 蘇州") is None      # the place is in the sentence, not the name


def test_own_equipment_form_is_capped_at_weak() -> None:
    meta = {"disclosure_clause_code": "第20款"}
    assert tac._rating(meta, "公告本公司取得廠務設備之相關資料", "signal") == "WEAK"
    assert tac._rating(meta, "公告本公司新竹縣國際AI智慧園區擬進行建築物改良及取得設備投資案", "signal") == "HIGH"


def test_another_companys_name_is_refused() -> None:
    src = "元富證券股份有限公司 達豐(重慶)電腦有限公司 啟碁科技股份有限公司"
    # A romanisation the Chinese does not spell (元富 is yuan-fu, not Yuanta).
    assert "Yuanta" in tac.invented_content("Yuanta Securities Co., Ltd. (元富證券股份有限公司)", src)
    # Another listed company's brand (Darfon is 達方, TWSE 8163).
    assert "Darfon" in tac.invented_content("Darfon (Chongqing) Computer Co., Ltd. (達豐(重慶)電腦有限公司)", src)
    assert tac.invented_content("Dafeng (Chongqing) Computer Co., Ltd. (達豐(重慶)電腦有限公司)", src) is None
    # The original is itself listed: its English may carry its group's brand.
    assert tac.invented_content("Wistron NeWeb Corporation (啟碁科技股份有限公司)", src) is None


def test_founder_or_ceo_change_is_weak_unless_a_third_of_the_board_changes() -> None:
    meta = {"disclosure_clause_code": "第6款"}
    resign = "1.發生變動日期:2023-09-01 13.屬三分之一以上董事發生變動（請輸入是或否）:否 14.其他應敘明事項:無"
    assert tac._rating(meta, "公告本公司董事辭職", "signal", resign) == "WEAK"
    assert tac._rating(meta, "公告本公司董事異動", "signal", resign.replace(":否", ":是")) == "HIGH"
    assert tac._rating(meta, "全面改選董事", "signal", _LOTES_2019) == "WEAK"   # box says 是, nobody changed


def test_lease_outside_the_group_is_weak() -> None:
    meta = {"disclosure_clause_code": "第20款"}
    related = "4.交易相對人及其與公司之關係（交易相對人如屬自然人，且非公司之關係人者，得免揭露其姓名）: 交易相對人：啟碁科技股份有限公司與公司之關係：關係人 5.交易相對人為關係人者"
    assert tac._rating(meta, "本公司向關係人啟碁科技股份有限公司取得不動產使用權資產", "noise", related) == "WEAK"
    assert tac._rating(meta, "本公司向關係人啟碁科技股份有限公司取得不動產使用權資產", "signal", related) == "WEAK"
    inside = related.replace("關係人 5.", "子公司 5.")
    assert tac._rating(meta, "本公司向子公司承租廠房", "noise", inside) == "NOISE"


def test_acquisition_rumour_not_denied_is_weak() -> None:
    meta = {"disclosure_clause_code": "第51款"}
    body = ("5.傳播媒體名稱:經濟日報 6.報導內容:貿聯據傳考慮收購黑石旗下Interplex ICT部門 7.發生緣由:不適用 "
            "8.因應措施: 本公司持續評估合適併購標的，對市場就特定標的之謠言與臆測，本公司不予評論。 9.其他應敘明事項:無")
    assert tac._rating(meta, "澄清媒體報導", "noise", body) == "WEAK"
    forecast = body.replace("貿聯據傳考慮收購黑石旗下Interplex ICT部門", "預估今年合併營收成長三成")
    assert tac._rating(meta, "澄清媒體報導", "noise", forecast) == "NOISE"
    denied = body.replace("本公司持續評估合適併購標的，對市場就特定標的之謠言與臆測，本公司不予評論。", "報導內容並非事實。")
    assert tac._rating(meta, "澄清媒體報導", "noise", denied) == "NOISE"


def test_working_capital_bond_issue_is_capped_at_weak() -> None:
    meta = {"disclosure_clause_code": "第11款"}
    body = "4.發行總額:發行總面額為新台幣15億元 10.募得價款之用途及運用計畫:充實營運資金 11.承銷方式:公開承銷"
    assert tac._rating(meta, "公告董事會決議通過發行國內第五次無擔保轉換公司債", "signal", body) == "WEAK"
    assert tac._rating(meta, "公告董事會決議通過發行國內第五次無擔保轉換公司債", "signal",
                       body.replace("充實營運資金", "支應產能擴充")) == "HIGH"


def test_routine_officer_change_is_fixed_noise() -> None:
    meta = {"disclosure_clause_code": "第8款"}
    form = "1.人員變動別（請輸入發言人、代理發言人、重要營運主管(如:執行長、營運長、 行銷長及策略長等)、財務主管、會計主管）:{} 2.發生變動日期:2024-02-29"
    assert tac.fixed_noise_reason(meta, "會計主管異動", form.format("會計主管(代理財務主管)")) == tac._FIXED_NOISE_REASON["routine_officer"]
    assert tac.fixed_noise_reason(meta, "發言人異動", form.format("發言人、代理發言人")) is not None
    assert tac.fixed_noise_reason(meta, "財務主管異動", form.format("財務主管、會計主管")) is None   # CFO: the model decides


def test_decision_date_dropped_when_the_form_has_none() -> None:
    raw = json.dumps({"signal_detection": "weak_signal", "signal_reason": "The Huangshi capacity budget rose by RMB 138,700,000 on 2021-05-28.",
                      "summary": "The Huangshi capacity budget rose by RMB 138,700,000.",
                      "key_facts": [{"label": "Decision date", "value": "2021-05-28"}, {"label": "Increase", "value": "RMB 138,700,000"}],
                      "entities": []})
    src = "1.事實發生日:2021-05-28 增加人民幣138,700,000元 18.董事會通過日期:不適用"
    out, status = tac.parse_and_check(raw, src)
    assert status == "ok" and [f["label"] for f in out["key_facts"]] == ["Increase"]


def test_cyber_cfo_and_stoppage_floors() -> None:
    assert tac._rating({"disclosure_clause_code": "第26款"}, "代子公司聯能科技(深圳)有限公司公告發生網路資安事件", "noise",
                       "5.發生緣由:遭受勒索病毒攻擊") == "WEAK"
    assert tac._rating({"disclosure_clause_code": "第26款"}, "本公司網路資安事件說明", "noise",
                       "5.發生緣由:官網遭受DDoS攻擊，無資料外洩") == "NOISE"
    form = "1.人員變動別（請輸入發言人、財務主管、會計主管）:{} 2.發生變動日期:2020-01-12"
    assert tac._rating({"disclosure_clause_code": "第8款"}, "財務及會計主管異動", "noise", form.format("財務主管、會計主管")) == "WEAK"
    assert tac._rating({"disclosure_clause_code": "第51款"}, "有關武漢肺炎疫情影響之補充說明。", "noise") == "WEAK"
    assert tac._rating({"disclosure_clause_code": "第17款"}, "董事會決議變更110年股東常會召開日期(因應疫情)", "noise") == "NOISE"


def test_entity_gloss_anchors_the_same_english_in_key_facts() -> None:
    raw = json.dumps({"signal_detection": "noise", "signal_reason": "Directors waived their subscription rights to new shares for personal reasons.",
                      "summary": "Directors waived subscriptions.",
                      "key_facts": [{"label": "Chairman (Jiaming Investment Co., Ltd.) shares waived", "value": "903,917 shares"}],
                      "entities": [{"name": "Jiaming Investment Co., Ltd. (嘉明投資股份有限公司)", "type": "company"}]})
    assert tac.parse_and_check(raw, "董事長：嘉明投資股份有限公司，放棄認購股數903,917股")[1] == "ok"


def test_audit_partner_rotation_is_fixed_noise() -> None:
    meta = {"disclosure_clause_code": "第7款"}
    one = "1.董事會通過日期:2023-07-25 2.舊任會計師事務所名稱:勤業眾信聯合會計師事務所 3.舊任簽證會計師姓名:A、B 5.新任會計師事務所名稱:勤業眾信聯合會計師事務所"
    assert tac.fixed_noise_reason(meta, "公告本公司因會計師事務所內部組織重新調整變更簽證會計師", one) == tac._FIXED_NOISE_REASON["audit_partner"]
    two = one.replace("5.新任會計師事務所名稱:勤業眾信", "5.新任會計師事務所名稱:安永")
    assert tac.fixed_noise_reason(meta, "公告本公司董事會決議更換會計師事務所及簽證會計師", two) is None


def test_known_english_name_is_anchored_without_its_chinese() -> None:
    raw = json.dumps({"signal_detection": "weak_signal", "signal_reason": "Siliconware Precision Industries Co., Ltd. approved a capital plan for its plant.",
                      "summary": "Siliconware Precision Industries Co., Ltd. approved a capital plan.", "key_facts": [],
                      "entities": [{"name": "Siliconware Precision Industries Co., Ltd.", "type": "company"}]})
    assert tac.parse_and_check(raw, "代子公司矽品精密工業股份有限公司公告其董事會通過重要決議")[1] == "ok"
