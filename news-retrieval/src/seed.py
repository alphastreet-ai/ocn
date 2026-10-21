"""Seed the database with initial domain and source data.

Run directly to populate a fresh database:

    python seed.py

The script is idempotent - rows that already exist (matched by slug
or URL) are silently skipped.
"""
import json
import logging
from typing import Any

from db import get_db, init_db, transaction

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------------------

RUN_STATUSES: list[str] = ["running", "completed", "failed"]

FREQUENCIES: list[dict[str, Any]] = [
    {"name": "daily",   "min_days_back": 1},
    {"name": "weekly",  "min_days_back": 7},
    {"name": "monthly", "min_days_back": 30},
]

DOMAINS: list[dict[str, Any]] = [
    {
        "name": "AI News",
        "slug": "ai_news",
        "description": (
            "Covers AI models, hardware, semiconductors, data centre"
            " infrastructure, energy, robotics, enterprise AI,"
            " security, policy, funding, and applied science."
        ),
    },
    {
        "name": "Smart Money",
        "slug": "smart_money",
        "description": (
            "Covers agentic payments, stablecoins, digital assets,"
            " embedded finance, cross-border settlement, AI fraud"
            " detection, and enterprise treasury automation."
        ),
    },
    {
        "name": "Adverse Media",
        "slug": "adverse_media",
        "description": (
            "Covers corporate misconduct, fraud, short-seller research,"
            " regulatory actions, and investigative journalism targeting"
            " companies in the AI economy universe."
        ),
    },
    {
        "name": "VC Commentary",
        "slug": "vc_commentary",
        "description": (
            "Covers venture capital theses, startup investing, AI economy"
            " market dynamics, and operator insights from leading VCs and"
            " founders."
        ),
    },
    {
        "name": "Company News",
        "slug": "company_news",
        "description": (
            "Company-specific news for entities in the AI economy universe,"
            " fetched via Alpha Vantage News & Sentiments API by ticker symbol."
        ),
    },
    {
        "name": "Geopolitical News",
        "slug": "geopolitical_news",
        "description": (
            "Covers geopolitical developments via U.S. federal actions:"
            " sanctions, export controls, tariffs, executive orders, and"
            " presidential documents affecting foreign policy and trade."
        ),
    },
    {
        "name": "Taiwan Market Signal",
        "slug": "taiwan_market_signal",
        "description": (
            "Monthly revenue and material announcements for Taiwan"
            " AI-supply-chain companies, fetched via the TWSE and TPEx"
            " OpenData APIs."
        ),
    },
    {
        "name": "Korea Market Signal",
        "slug": "korea_market_signal",
        "description": (
            "Regulator filings, Korean-language trade press, and"
            " semiconductor export statistics for Korea AI-supply-chain"
            " companies, fetched via DART (Financial Supervisory"
            " Service), GDELT/RSS, and a Korea Customs Service"
            " press-release scrape (see KOREA_CUSTOMS_EXPORT_SOURCE"
            " below - the data.go.kr structured API is blocked behind"
            " Korean-phone-verified signup, so this is a no-login"
            " fallback)."
        ),
    },
    {
        "name": "Japan Market Signal",
        "slug": "japan_market_signal",
        "description": (
            "Earnings-forecast revisions, disclosures, ownership filings,"
            " buybacks, industry billings, and capex news for Japan"
            " AI-supply-chain equipment and materials companies, fetched"
            " via IRBANK (free forecast/buyback history mirror), Kabutan"
            " (free TDnet disclosure mirror), and EDINET (Financial"
            " Services Agency's official filing API). TDnet's own paid"
            " API (~JPY 50,000+/mo) and J-Quants' forecast-revision-reason"
            " add-on were both confirmed live and rejected as unnecessary"
            " given these free substitutes - see news-retrieval/CLAUDE.md."
        ),
    },
    {
        "name": "China Market Signal",
        "slug": "china_market_signal",
        "description": (
            "Chinese government policy actions (export licensing,"
            " antitrust, security reviews), mainland and Hong Kong company"
            " filings, and Chinese-language trade press for the companies"
            " that compete with or supply the US AI-economy universe."
            " Unlike the other three market domains, a positive signal"
            " here is usually a NEGATIVE read for the US names attached"
            " to it - direction is carried downstream by"
            " signal-detection-agent, not here. Customs (GACC) and the"
            " statistics bureau (NBS) were both confirmed unreachable"
            " (JS-cookie anti-bot and a network-level block respectively)"
            " - see news-retrieval/CLAUDE.md."
        ),
    },
    {
        "name": "Middle East Market Signal",
        "slug": "middle_east_market_signal",
        "description": (
            "Gulf AI buying and investment, US decisions on Gulf chip"
            " sales, and the war, shipping, helium and energy disruptions"
            " that reach US AI and chip stocks - fetched from the UAE and"
            " Saudi state news agencies' own APIs, BIS press releases (with"
            " DOJ case text), the Federal Register, Gulf funds' SEC 13D/13G"
            " filings, Middle East AI News, and newsapi.ai. Every source is"
            " an API or structured JSON, and every article carries a body -"
            " see news-retrieval/CLAUDE.md."
        ),
    },
]

# P0 core universe (spec Section 14.1), hardcoded pending research-universe
# integration for Taiwan. Ticker codes are as listed on their exchange —
# 公司代號/SecuritiesCompanyCode in the TWSE/TPEx API responses.
# NOTE: 3324 (Auras Technology) is TPEx, not TWSE as some reference material
# states — verified live against both revenue dumps on 2026-08-19.
# native_name is the Chinese company name used for GDELT queries (native-
# language press generally refers to companies by this name, not the English
# one) — sourced from spec Section 14.1.
# customers: verified customers only, as {name, relationship} - copied onto every
# Taiwan article's metadata.customers (pipeline._taiwan_customers, read from
# here at fetch time, never from the seeded sources.config, so an edit takes
# effect without a DB update). A customer is listed ONLY when a primary source
# names it: the company's own disclosure (annual report 年報, MOPS filing,
# press release), the customer's own disclosure (its SEC 10-K, Apple's
# official supplier list), or an official MOPS announcement naming the
# counterparty. Researched 2026-10-19. Not listed: analyst and press claims
# however often repeated (Meta/AWS/Google for the server makers), partners and
# investors (Amazon's 2026 placement in Gold Circuit, Sony-TSMC, Chenbro-NVIDIA
# MGX), and customers a company reports only as codes (客戶A, 甲客戶) - no name
# is guessed. [] means checked and none verified. Apple entries come from
# Apple's fiscal-2023 list (published 2024-04), the latest Apple has issued.
TAIWAN_TICKER_UNIVERSE: list[dict[str, Any]] = [
    {"ticker": "2330", "company": "TSMC", "native_name": "台積電", "exchange": "TWSE", "customers": [
        {"name": "Apple", "relationship": "Customer: lists the company on its official supplier list (fiscal 2023, published 2024-04)"},
        {"name": "NVIDIA", "relationship": "Customer: buys its semiconductor wafers from the company as its foundry"},
        {"name": "AMD", "relationship": "Customer: buys wafers for its HPC, FPGA and adaptive SoC products"},
        {"name": "Broadcom", "relationship": "Customer: outsources part of its front-end wafer manufacturing to the company"},
        {"name": "Qualcomm", "relationship": "Customer: uses the company as a primary foundry for its integrated circuits"},
        {"name": "Intel", "relationship": "Customer: buys key tiles for its products from the company as a foundry"},
        {"name": "Analog Devices", "relationship": "Customer: sources part of its wafers from the company"},
        {"name": "Lattice Semiconductor", "relationship": "Customer: has its 16nm FPGA products developed and manufactured by the company"},
        {"name": "Astera Labs", "relationship": "Customer: has all of its ICs fabricated by the company"},
        {"name": "Credo", "relationship": "Customer: bought all of its wafers from the company in fiscal 2026"},
        {"name": "Silicon Labs", "relationship": "Customer: buys most of its wafers from the company as a primary foundry"},
        {"name": "Cirrus Logic", "relationship": "Customer: has most of its wafers manufactured by the company"},
        {"name": "SiTime", "relationship": "Customer: uses the company as a primary foundry for its analog circuits"},
        {"name": "Allegro MicroSystems", "relationship": "Customer: uses the company as one of its primary fabs"},
        {"name": "Navitas Semiconductor", "relationship": "Customer: buys its GaN wafers from the company, which it reports has announced it will stop GaN production"},
        {"name": "Impinj", "relationship": "Customer: buys its endpoint IC wafers from the company (supplier since 2003)"},
        {"name": "MaxLinear", "relationship": "Customer: uses the company as a key foundry"},
        {"name": "indie Semiconductor", "relationship": "Customer: uses the company as a primary foundry"},
        {"name": "Ambiq Micro", "relationship": "Customer: buys wafers for its SoC products (180nm to 12nm)"},
    ]},
    {"ticker": "2317", "company": "Hon Hai / Foxconn", "native_name": "鴻海", "exchange": "TWSE", "customers": [
        {"name": "Apple", "relationship": "Customer: lists the company on its official supplier list (fiscal 2023, published 2024-04)"},
        {"name": "NVIDIA", "relationship": "Customer: uses the company as a contract manufacturer to assemble, test and package its final products"},
        {"name": "Arista Networks", "relationship": "Customer: uses the company as a primary contract manufacturer"},
        {"name": "NETGEAR", "relationship": "Customer: uses the company as a primary manufacturer of its products"},
    ]},
    {"ticker": "2382", "company": "Quanta", "native_name": "廣達", "exchange": "TWSE", "customers": [
        {"name": "Apple", "relationship": "Customer: lists the company on its official supplier list (fiscal 2023, published 2024-04)"},
        {"name": "Extreme Networks", "relationship": "Customer: uses the company to manufacture its products"},
    ]},
    {"ticker": "3231", "company": "Wistron", "native_name": "緯創", "exchange": "TWSE", "customers": [
        {"name": "NVIDIA", "relationship": "Customer: uses the company as a contract manufacturer to assemble, test and package its final products"},
        {"name": "Apple", "relationship": "Customer: lists the company on its official supplier list (fiscal 2023, published 2024-04)"},
    ]},
    {"ticker": "6669", "company": "Wiwynn", "native_name": "緯穎", "exchange": "TWSE", "customers": [
        {"name": "Microsoft", "relationship": "Customer: buys cloud server components and system integration from the company (Microsoft supplier award release, 2019-05)"},
    ]},
    {"ticker": "2356", "company": "Inventec", "native_name": "英業達", "exchange": "TWSE", "customers": [
        {"name": "HP", "relationship": "Customer: buys HP-brand notebooks built by the company (sales contract since 1998)"},
        {"name": "Hewlett Packard Enterprise", "relationship": "Customer: buys HP-brand servers built by the company (sales contract since 2000)"},
        {"name": "Inseego", "relationship": "Customer: uses the company's subsidiary Inventec Appliances as a primary contract manufacturer"},
        {"name": "Damon", "relationship": "Customer: buys components for its electric motorcycles"},
    ]},
    {"ticker": "3017", "company": "Asia Vital Components", "native_name": "奇鋐", "exchange": "TWSE", "customers": []},
    {"ticker": "3324", "company": "Auras Technology", "native_name": "雙鴻", "exchange": "TPEx", "customers": []},
    {"ticker": "2308", "company": "Delta Electronics", "native_name": "台達電", "exchange": "TWSE", "customers": [
        {"name": "Apple", "relationship": "Customer: lists the company on its official supplier list (fiscal 2023, published 2024-04)"},
        {"name": "EVgo", "relationship": "Customer: buys EV chargers under a supply agreement"},
        {"name": "NETGEAR", "relationship": "Customer: uses the company as a primary manufacturer of its products"},
        {"name": "Hushan Industrial Co., Ltd. (虎山實業股份有限公司)", "relationship": "Customer: contracted the company to install 989.48 kWp of solar at its Ruifang plant"},
    ]},
    {"ticker": "3665", "company": "BizLink", "native_name": "貿聯-KY", "exchange": "TWSE", "customers": [
        {"name": "Credo", "relationship": "Customer: has its active electrical cables (AEC) manufactured by the company"},
        {"name": "Siemens Healthineers", "relationship": "Customer: buys wiring harnesses, assemblies and cables (for over 30 years)"},
        {"name": "Lam Research", "relationship": "Customer: gave the company its 2026 Supplier Excellence Award"},
    ]},
    {"ticker": "3533", "company": "Lotes", "native_name": "嘉澤", "exchange": "TWSE", "customers": []},
    {"ticker": "2059", "company": "King Slide", "native_name": "川湖", "exchange": "TWSE", "customers": []},
    {"ticker": "8210", "company": "Chenbro", "native_name": "勤誠", "exchange": "TWSE", "customers": []},
    {"ticker": "2368", "company": "Gold Circuit Electronics", "native_name": "金像電", "exchange": "TWSE", "customers": []},
    {"ticker": "2383", "company": "Elite Material", "native_name": "台光電", "exchange": "TWSE", "customers": [
        {"name": "Insulectro", "relationship": "Customer (distributor): buys and resells the company's copper clad laminates"},
    ]},
    {"ticker": "3037", "company": "Unimicron", "native_name": "欣興", "exchange": "TWSE", "customers": [
        {"name": "Apple", "relationship": "Customer: lists the company on its official supplier list (fiscal 2023, published 2024-04)"},
    ]},
    {"ticker": "3711", "company": "ASE Technology", "native_name": "日月光投控", "exchange": "TWSE", "customers": [
        {"name": "Apple", "relationship": "Customer: lists the company on its official supplier list (fiscal 2023, published 2024-04)"},
        {"name": "AMD", "relationship": "Customer: buys assembly, test, mark and packaging from the company's subsidiary SPIL"},
        {"name": "Qualcomm", "relationship": "Customer: uses the company as a primary assembly and test supplier"},
        {"name": "Broadcom", "relationship": "Customer: outsources assembly and test to the company"},
        {"name": "Cirrus Logic", "relationship": "Customer: uses the company as a primary assembly and test house"},
        {"name": "Lattice Semiconductor", "relationship": "Customer: uses the company as its primary assembly and test supplier"},
        {"name": "Astera Labs", "relationship": "Customer: has its ICs assembled, packaged and tested by the company"},
        {"name": "Credo", "relationship": "Customer: has its ICs packaged by the company"},
        {"name": "SiTime", "relationship": "Customer: buys packaging, assembly and testing from the company"},
        {"name": "Ambarella", "relationship": "Customer: has chips it buys as tested die assembled by the company"},
        {"name": "MaxLinear", "relationship": "Customer: buys assembly, packaging and test from the company"},
        {"name": "Silicon Motion", "relationship": "Customer: uses the company as a primary assembly and test subcontractor"},
        {"name": "Sequans", "relationship": "Customer: buys chipset assembly and testing from the company"},
        {"name": "Himax", "relationship": "Customer: buys assembly and testing from the company"},
        {"name": "Valens Semiconductor", "relationship": "Customer: buys assembly and test from the company"},
        {"name": "GCT Semiconductor", "relationship": "Customer: buys assembly and testing from the company"},
        {"name": "GSI Technology", "relationship": "Customer: has most of its wafers packaged by the company"},
        {"name": "Ambiq Micro", "relationship": "Customer: buys packaging and testing from the company"},
        {"name": "Techpoint", "relationship": "Customer: buys packaging and testing from the company"},
    ]},
    {"ticker": "2449", "company": "King Yuan Electronics", "native_name": "京元電子", "exchange": "TWSE", "customers": [
        {"name": "NVIDIA", "relationship": "Customer: prepaid for US test capacity the company reserves for it (MOPS filing 2026-10-13)"},
        {"name": "AMD", "relationship": "Customer: buys assembly, test, mark and packaging from the company"},
        {"name": "Credo", "relationship": "Customer: has its ICs tested by the company"},
        {"name": "Lattice Semiconductor", "relationship": "Customer: uses the company as a second source for wafer sort testing"},
        {"name": "Ambarella", "relationship": "Customer: has final testing of its products done by the company"},
        {"name": "Himax", "relationship": "Customer: buys assembly and testing from the company"},
        {"name": "Silicon Motion", "relationship": "Customer: uses the company as a primary assembly and test subcontractor"},
        {"name": "Ambiq Micro", "relationship": "Customer: buys packaging and testing from the company"},
    ]},
    {"ticker": "3661", "company": "Alchip", "native_name": "世芯-KY", "exchange": "TWSE", "customers": []},
    {"ticker": "5274", "company": "Aspeed Technology", "native_name": "信驊", "exchange": "TPEx", "customers": []},
]


# Korea Signals spec, Section 3. stock_code is KRX's 6-character listing
# code, stored as a string everywhere — DART and KRX both return it as a
# zero-padded string (e.g. SK Hynix is "000660"), and coercing it to an int
# anywhere in the pipeline silently drops the leading zeros and produces a
# code that matches no company ("660" instead of "000660").
#
# group_prefix + exclude_terms exist because Korean business groups (재벌)
# share a name across many unrelated listed entities — e.g. multiple
# "Samsung"-branded companies exist that are not Samsung Electronics. The
# news-matching step (GDELT/RSS company-name filter) needs these to avoid
# attributing an unrelated group company's news to the tracked ticker.
#
# corp_code (DART's own 8-digit company identifier, distinct from
# stock_code) is intentionally omitted here — it is resolved at fetch time
# from DART's corpCode.xml bulk mapping (see src/dart_corp_code.py), not
# hardcoded, since DART's own filing-list API takes corp_code, not
# stock_code, and a wrong hardcoded value would fail silently (empty
# result, not an error).
KOREA_TICKER_UNIVERSE: list[dict[str, Any]] = [
    # Tier 1
    {"ticker": "000660", "company": "SK Hynix", "korean_name": "SK하이닉스", "group_prefix": "SK", "exclude_terms": []},
    {"ticker": "005930", "company": "Samsung Electronics", "korean_name": "삼성전자", "group_prefix": "Samsung", "exclude_terms": ["Samsung C&T", "Samsung Life", "Samsung SDI", "Samsung Fire"]},
    # Tier 2 — memory stacking chain
    {"ticker": "042700", "company": "Hanmi Semiconductor", "korean_name": "한미반도체", "group_prefix": "Hanmi", "exclude_terms": []},
    # Hanwha Semitech (한화세미텍) has NO public ticker - verified live
    # against four independent Korean financial sources (alphasquare,
    # Hankyung, FnGuide, Thinkpool) on 2026-09-22. It is a wholly-owned,
    # unlisted subsidiary of Hanwha Industrial Solutions, renamed from
    # 한화정밀기계 (Hanwha Precision Machinery) to 한화세미텍 in Feb 2025.
    # Ticker 272210 belongs to a DIFFERENT, unrelated company - 한화시스템
    # (Hanwha System, defense electronics/radar), not this one. This is
    # exactly the mistake the source spec's "check the code" warning was
    # flagging. Since it has no stock_code, DART's corp_code lookup and the
    # customs/filing fetchers below (which are keyed by listed ticker) do
    # not apply - this entry is a placeholder for the news-only fetchers
    # (GDELT/RSS), which can still query it by name.
    {"ticker": None, "company": "Hanwha Semitech", "korean_name": "한화세미텍", "group_prefix": "Hanwha", "exclude_terms": ["Hanwha Solutions", "Hanwha Aerospace", "Hanwha Life", "Hanwha Ocean", "Hanwha System"], "unlisted": True},
    {"ticker": "089030", "company": "Techwing", "korean_name": "테크윙", "group_prefix": "Techwing", "exclude_terms": []},
    {"ticker": "058470", "company": "Leeno Industrial", "korean_name": "리노공업", "group_prefix": "Leeno", "exclude_terms": []},
    {"ticker": "067310", "company": "Hana Micron", "korean_name": "하나마이크론", "group_prefix": "Hana", "exclude_terms": ["Hana Financial", "Hana Bank", "Hana Tour"]},
    {"ticker": "222800", "company": "Simmtech", "korean_name": "심텍", "group_prefix": "Simmtech", "exclude_terms": []},
    {"ticker": "007660", "company": "Isu Petasys", "korean_name": "이수페타시스", "group_prefix": "Isu", "exclude_terms": []},
    {"ticker": "353200", "company": "Daeduck Electronics", "korean_name": "대덕전자", "group_prefix": "Daeduck", "exclude_terms": ["Daeduck GDS"]},
    # Tier 3 — equipment, materials, chip making and power
    {"ticker": "402340", "company": "SK Square", "korean_name": "SK스퀘어", "group_prefix": "SK", "exclude_terms": []},
    {"ticker": "000990", "company": "DB HiTek", "korean_name": "DB하이텍", "group_prefix": "DB", "exclude_terms": ["DB Insurance", "DB Financial"]},
    {"ticker": "009150", "company": "Samsung Electro-Mechanics", "korean_name": "삼성전기", "group_prefix": "Samsung", "exclude_terms": ["Samsung C&T", "Samsung Life", "Samsung SDI", "Samsung Fire"]},
    {"ticker": "011070", "company": "LG Innotek", "korean_name": "LG이노텍", "group_prefix": "LG", "exclude_terms": ["LG Electronics", "LG Chem", "LG Display", "LG Energy Solution", "LG Uplus"]},
    {"ticker": "036930", "company": "Jusung Engineering", "korean_name": "주성엔지니어링", "group_prefix": "Jusung", "exclude_terms": []},
    {"ticker": "240810", "company": "Wonik IPS", "korean_name": "원익IPS", "group_prefix": "Wonik", "exclude_terms": ["Wonik Materials", "Wonik Holdings"]},
    {"ticker": "319660", "company": "PSK", "korean_name": "피에스케이", "group_prefix": "PSK", "exclude_terms": []},
    {"ticker": "357780", "company": "Soulbrain", "korean_name": "솔브레인", "group_prefix": "Soulbrain", "exclude_terms": []},
    {"ticker": "267260", "company": "HD Hyundai Electric", "korean_name": "HD현대일렉트릭", "group_prefix": "HD Hyundai", "exclude_terms": ["HD Hyundai Heavy Industries", "HD Hyundai Marine"]},
    {"ticker": "010120", "company": "LS Electric", "korean_name": "LS일렉트릭", "group_prefix": "LS", "exclude_terms": ["LS Cable", "LS Materials"]},
]


NEWSAPI_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "ai_news",
        "url": "newsapi:top-headlines",
        "name": "NewsAPI",
        "source_type": "newsapi",
        "frequency_name": "daily",
        "description": "Top headlines from technology, science, and business categories.",
        "config": {
            "endpoint": "top-headlines",
            "categories": ["technology", "science", "business"],
            "language": "en",
        },
    },
]

GOOGLE_NEWS_SOURCE: dict[str, Any] = {
    "domain_slug": "ai_news",
    "url": "google_news",
    "name": "Google News",
    "source_type": "google_news",
    "frequency_name": "daily",
    "description": "Google News search across AI topics via SerpAPI.",
    "config": {
        "queries": [
            "artificial intelligence",
            "AI chips GPU semiconductors",
            "AI regulation policy",
            "AI data training datasets",
            "AI stocks rally",
            "semiconductor stocks tariffs",
            "AI stocks market correction",
            "AI stocks regulation policy",
        ],
    },
}

SUBSTACK_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "ai_news",
        "url": "https://importai.substack.com/feed",
        "name": "Import AI",
        "frequency_name": "weekly",
        "description": "Weekly analysis of cutting-edge AI research.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://simonw.substack.com/feed",
        "name": "Simon Willison's Newsletter",
        "frequency_name": "weekly",
        "description": "Hands-on AI/devtools/open-source analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://decision.substack.com/feed",
        "name": "Decision Intelligence",
        "frequency_name": "weekly",
        "description": "Cassie Kozyrkov on AI, decision science, and management.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://artificialintelligencemadesimple.substack.com/feed",
        "name": "Artificial Intelligence Made Simple",
        "frequency_name": "weekly",
        "description": "Technical, social and economic angles on AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://berkeleyrdi.substack.com/feed",
        "name": "Agentic AI Weekly",
        "frequency_name": "weekly",
        "description": "AI agents and research updates.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://artificialintelligenceact.substack.com/feed",
        "name": "EU AI Act Newsletter",
        "frequency_name": "weekly",
        "description": "EU AI Act and regulation.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aiguide.substack.com/feed",
        "name": "AI: A Guide for Thinking Humans",
        "frequency_name": "weekly",
        "description": "Melanie Mitchell on AI for thoughtful readers.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aiagentssimplified.substack.com/feed",
        "name": "AI Agents Simplified",
        "frequency_name": "weekly",
        "description": "Practical explainers on AI agents.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://diamantai.substack.com/feed",
        "name": "DiamantAI",
        "frequency_name": "weekly",
        "description": "Agentic AI and engineering resources.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://wrk3.substack.com/feed",
        "name": "Work3 - The Future of Work",
        "frequency_name": "weekly",
        "description": "AI and work transformation.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thezvi.substack.com/feed",
        "name": "Don't Worry About the Vase",
        "frequency_name": "weekly",
        "description": "Zvi on AI discourse and developments.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://addyo.substack.com/feed",
        "name": "Elevate",
        "frequency_name": "weekly",
        "description": "Addy Osmani on AI and software development.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://jamwithai.substack.com/feed",
        "name": "Jam with AI",
        "frequency_name": "weekly",
        "description": "AI tools and workflows.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://substack.jurgenappelo.com/feed",
        "name": "The Maverick Mapmaker",
        "frequency_name": "weekly",
        "description": "AI and management/future of work.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://cosmosinstitute.substack.com/feed",
        "name": "Cosmos Institute",
        "frequency_name": "weekly",
        "description": "AI and ideas.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thenuancedperspective.substack.com/feed",
        "name": "The Nuanced Perspective",
        "frequency_name": "weekly",
        "description": "Nuanced AI analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aixeducation.substack.com/feed",
        "name": "AI x Education",
        "frequency_name": "weekly",
        "description": "AI and education.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://nickpotkalitsky.substack.com/feed",
        "name": "Educating AI",
        "frequency_name": "weekly",
        "description": "Teaching and education with AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://designingwithai.substack.com/feed",
        "name": "Designing with AI",
        "frequency_name": "weekly",
        "description": "Design workflows with AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://epochai.substack.com/feed",
        "name": "Epoch AI",
        "frequency_name": "weekly",
        "description": "Benchmarks, scaling and research data.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://theaileadershipedge.substack.com/feed",
        "name": "The AI Leadership Edge",
        "frequency_name": "weekly",
        "description": "Leadership and AI transformation.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thedigitalleader.substack.com/feed",
        "name": "The Digital Leader",
        "frequency_name": "weekly",
        "description": "Digital strategy and AI transformation.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aifrontiersmedia.substack.com/feed",
        "name": "AI Frontiers",
        "frequency_name": "weekly",
        "description": "Frontier AI coverage.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://theagentarchitect.substack.com/feed",
        "name": "The AI Agent Architect",
        "frequency_name": "weekly",
        "description": "Architecting AI agents.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://oliverpatel.substack.com/feed",
        "name": "Enterprise AI Governance",
        "frequency_name": "weekly",
        "description": "Governance in enterprise AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://futureworklife.substack.com/feed",
        "name": "Ollie on Work",
        "frequency_name": "weekly",
        "description": "Work, management, AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aimaker.substack.com/feed",
        "name": "The AI Maker",
        "frequency_name": "weekly",
        "description": "Maker-focused AI building and workflows.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://amistrongeryet.substack.com/feed",
        "name": "Second Thoughts",
        "frequency_name": "weekly",
        "description": "Thoughtful AI and product commentary.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://frontierai.substack.com/feed",
        "name": "The AI Frontier",
        "frequency_name": "weekly",
        "description": "Frontier AI systems and economics.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://themediabrain.substack.com/feed",
        "name": "Peter Csathy's the brAIn",
        "frequency_name": "weekly",
        "description": "AI and media intelligence.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aiblewmymind.substack.com/feed",
        "name": "AI blew my mind",
        "frequency_name": "weekly",
        "description": "Accessible AI insights.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://stevenadler.substack.com/feed",
        "name": "Clear-Eyed AI",
        "frequency_name": "weekly",
        "description": "Grounded AI policy and safety commentary.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://msukhareva.substack.com/feed",
        "name": "AI Realist",
        "frequency_name": "weekly",
        "description": "Pragmatic view of AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thestrategystack.substack.com/feed",
        "name": "The Strategy Stack",
        "frequency_name": "weekly",
        "description": "Strategy thinking with AI lens.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://theaimonitor.substack.com/feed",
        "name": "The AI Monitor",
        "frequency_name": "weekly",
        "description": "Monitoring AI developments.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aiagenteconomy.substack.com/feed",
        "name": "The AI Agent Economy",
        "frequency_name": "weekly",
        "description": "Agent economy and startups.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aicentral.substack.com/feed",
        "name": "AI Central",
        "frequency_name": "weekly",
        "description": "General AI coverage.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://unhypedai.substack.com/feed",
        "name": "Unhyped AI",
        "frequency_name": "weekly",
        "description": "Low-hype AI analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://theaiphilosopher.substack.com/feed",
        "name": "The AI Philosopher",
        "frequency_name": "weekly",
        "description": "Philosophical takes on AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thehumanplaybook.substack.com/feed",
        "name": "The Human Playbook",
        "frequency_name": "weekly",
        "description": "Human skills in AI era.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://honestai.substack.com/feed",
        "name": "Honest AI",
        "frequency_name": "weekly",
        "description": "Honest takes on AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://kaystoner.substack.com/feed",
        "name": "What Good Is AI?",
        "frequency_name": "weekly",
        "description": "Critical practical commentary on AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://rafebrena.substack.com/feed",
        "name": "The Skeptic AI Enthusiast",
        "frequency_name": "weekly",
        "description": "Skeptical but interested AI coverage.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://utopiainbeta.substack.com/feed",
        "name": "Utopia in Beta",
        "frequency_name": "weekly",
        "description": "AI and future concepts.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://engineeringharmony.substack.com/feed",
        "name": "Engineering Harmony",
        "frequency_name": "weekly",
        "description": "AI and engineering thinking.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aihumanity.substack.com/feed",
        "name": "AI for Humanity",
        "frequency_name": "weekly",
        "description": "AI for social good and society.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://diamantinoalmeida.substack.com/feed",
        "name": "Leadership as a Verb",
        "frequency_name": "weekly",
        "description": "Leadership and AI transition.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://futurebraining.substack.com/feed",
        "name": "Futurebraining",
        "frequency_name": "weekly",
        "description": "Future-oriented AI commentary.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://haridutt.substack.com/feed",
        "name": "The Turing Pilgrim",
        "frequency_name": "weekly",
        "description": "AI and broader reflection.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thepromptisthenovel.substack.com/feed",
        "name": "The Prompt Is the Novel",
        "frequency_name": "weekly",
        "description": "Prompting and generative writing.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aicloudweekly.substack.com/feed",
        "name": "AI & Cloud Weekly",
        "frequency_name": "weekly",
        "description": "AI and cloud weekly roundup.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aigovernancelead.substack.com/feed",
        "name": "AI Governance Ethics and Leadership",
        "frequency_name": "weekly",
        "description": "Governance and leadership.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://pascalbornet.substack.com/feed",
        "name": "IRREPLACEABLE with AI",
        "frequency_name": "weekly",
        "description": "Human + AI transformation.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://solrashidi.substack.com/feed",
        "name": "The Sol of AI",
        "frequency_name": "weekly",
        "description": "Pragmatic AI for technologists and non-technologists.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://ruben.substack.com/feed",
        "name": "How to AI",
        "frequency_name": "weekly",
        "description": "Step-by-step AI workflows.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://mitgenerationai.substack.com/feed",
        "name": "Generation AI",
        "frequency_name": "weekly",
        "description": "MIT Working Group on Generative AI and the future of work.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aiedusimplified.substack.com/feed",
        "name": "AI + Education = Simplified",
        "frequency_name": "weekly",
        "description": "AI and education for teachers.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aiandhowweteach.substack.com/feed",
        "name": "AI & How We Teach",
        "frequency_name": "weekly",
        "description": "AI-aware teaching and literacy.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://wonderingaboutai.substack.com/feed",
        "name": "Wondering About AI",
        "frequency_name": "weekly",
        "description": "Leadership and AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://taoofai.substack.com/feed",
        "name": "Tao of AI",
        "frequency_name": "weekly",
        "description": "AI strategy and philosophy.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://nextplayso.substack.com/feed",
        "name": "next play",
        "frequency_name": "weekly",
        "description": "Curated opportunities and deep tech content.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://metatrends.substack.com/feed",
        "name": "Metatrends",
        "frequency_name": "weekly",
        "description": "Future-facing technology trends.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://linas.substack.com/feed",
        "name": "Linas's Newsletter",
        "frequency_name": "weekly",
        "description": "Finance x AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thechipletter.substack.com/feed",
        "name": "The Chip Letter",
        "frequency_name": "weekly",
        "description": (
            "Computer history and architecture with semiconductor"
            " relevance."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://marklapedus.substack.com/feed",
        "name": "Semiecosystem",
        "frequency_name": "weekly",
        "description": "Semiconductor ecosystem commentary.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://metacircuits.substack.com/feed",
        "name": "The Circuit",
        "frequency_name": "weekly",
        "description": "Helping business leaders become better AI operators.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://theunchartedalgorithm.substack.com/feed",
        "name": "The Uncharted Algorithm",
        "frequency_name": "weekly",
        "description": "AI, enterprise, culture, future of work.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://interestingengineering.substack.com/feed",
        "name": "Interesting Engineering++",
        "frequency_name": "weekly",
        "description": "Engineering/tech commentary with AI relevance.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://dataintelligenceplatform.substack.com/feed",
        "name": "Data Intelligence Platform",
        "frequency_name": "weekly",
        "description": "Data platform and infrastructure recommendations.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://cloudsecurityguy.substack.com/feed",
        "name": "The Cloud Security Guy",
        "frequency_name": "weekly",
        "description": "Cloud security with AI chip/infrastructure links.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://connectingai.substack.com/feed",
        "name": "ConnectingAI",
        "frequency_name": "weekly",
        "description": "Hardware enables the future of AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://anastasiintech.substack.com/feed",
        "name": "Deep in Tech Newsletter",
        "frequency_name": "weekly",
        "description": "AI data centers, optics, power systems.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://iamfabian.substack.com/feed",
        "name": "Fabian's Substack",
        "frequency_name": "weekly",
        "description": (
            "Product and technology deep dives in optics and AI infra."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://gadallon.substack.com/feed",
        "name": "Gadallon",
        "frequency_name": "weekly",
        "description": "AI infrastructure boom and supply chain.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://axautikgroupllc.substack.com/feed",
        "name": "Patrick Kennedy's Axautik Group",
        "frequency_name": "weekly",
        "description": "Server and hardware analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thefiddler.substack.com/feed",
        "name": "Fiddler on the Proof",
        "frequency_name": "weekly",
        "description": "Science/math with AI adjacency.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://theneurotechnewsletter.substack.com/feed",
        "name": "The Neurotech Newsletter",
        "frequency_name": "weekly",
        "description": "Neurotechnology with AI relevance.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://mariannanakos.substack.com/feed",
        "name": "A2S | Automate to Scale",
        "frequency_name": "weekly",
        "description": "Automation and AI integration for scaling.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://harrybloom.substack.com/feed",
        "name": "The Financial Philosopher",
        "frequency_name": "weekly",
        "description": "Financial philosophy with tech lenses.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://khanfk.substack.com/feed",
        "name": "Technicity",
        "frequency_name": "weekly",
        "description": "Technology, business and AI intersections.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://oneusefulthing.substack.com/feed",
        "name": "One Useful Thing",
        "frequency_name": "weekly",
        "description": "Ethan Mollick on practical AI applications and research.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://understandingai.substack.com/feed",
        "name": "Understanding AI",
        "frequency_name": "weekly",
        "description": "Timothy Lee on AI research and policy analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://magazine.sebastianraschka.com/feed",
        "name": "Ahead of AI",
        "frequency_name": "weekly",
        "description": "Sebastian Raschka on ML research and engineering.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://newsletter.safe.ai/feed",
        "name": "AI Safety Newsletter",
        "frequency_name": "weekly",
        "description": "Center for AI Safety - AI alignment and safety research.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://theaiopportunities.com/feed",
        "name": "The AI Opportunity",
        "frequency_name": "weekly",
        "description": "AI business opportunities and market analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://brief.montrealethics.ai/feed",
        "name": "The AI Ethics Brief",
        "frequency_name": "weekly",
        "description": "Montreal AI Ethics Institute - AI ethics and governance.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://blog.ai-futures.org/feed",
        "name": "AI Futures Project",
        "frequency_name": "weekly",
        "description": "Long-range AI futures and scenario planning.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://whytryai.com/feed",
        "name": "Why Try AI",
        "frequency_name": "weekly",
        "description": "Practical AI tool introductions and guides.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aiadopters.club/feed",
        "name": "AI Adopters Club",
        "frequency_name": "weekly",
        "description": "AI adoption strategies for organisations.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aidisruption.ai/feed",
        "name": "AI Disruption",
        "frequency_name": "weekly",
        "description": "AI strategy and disruption for business leaders.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://workfutures.io/feed",
        "name": "workfutures.io",
        "frequency_name": "weekly",
        "description": "Future of work and AI transformation.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://connectedideasproject.com/feed",
        "name": "The Connected Ideas Project",
        "frequency_name": "weekly",
        "description": "Technology and AI ideas synthesis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://2ndorderthinkers.com/feed",
        "name": "2nd Order Thinkers",
        "frequency_name": "weekly",
        "description": "Second-order effects of AI on management and society.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://newsletter.forethought.org/feed",
        "name": "ForeWord",
        "frequency_name": "weekly",
        "description": "Forethought Institute - AI research and policy.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://academy.shiftbase.info/feed",
        "name": "Shift*Academy",
        "frequency_name": "weekly",
        "description": "Organisational change and AI leadership.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://phiand.ai/feed",
        "name": "Phi / AI",
        "frequency_name": "weekly",
        "description": "AI philosophy, ideas, and cultural commentary.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://effthealgorithm.com/feed",
        "name": "Eff the Algorithm",
        "frequency_name": "weekly",
        "description": "Critical perspectives on algorithms and AI in society.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://pseudorandombits.io/feed",
        "name": "Pseudorandom Bits",
        "frequency_name": "weekly",
        "description": "AI and computing commentary.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://newsletter.aipolicybulletin.org/feed",
        "name": "AI Policy Bulletin Newsletter",
        "frequency_name": "weekly",
        "description": "AI policy tracking and regulatory updates.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aitidbits.ai/feed",
        "name": "AI Tidbits",
        "frequency_name": "weekly",
        "description": "Curated AI news and tidbits.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://brennanmcdonald.com/feed",
        "name": "Brennan McDonald\'s Newsletter",
        "frequency_name": "weekly",
        "description": "AI and software engineering commentary.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://humanandmachine.com/feed",
        "name": "Human and Machine",
        "frequency_name": "weekly",
        "description": "Humans and AI working together - practical perspectives.",
    },

    {
        "domain_slug": "ai_news",
        "url": "https://readings.ramisayar.com/feed",
        "name": "Rami\'s Readings",
        "frequency_name": "weekly",
        "description": "Curated AI and technology readings.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://writing.antonleicht.me/feed",
        "name": "Threading the Needle",
        "frequency_name": "weekly",
        "description": "AI and engineering.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aiweekender.substack.com/feed",
        "name": "AI Weekender",
        "frequency_name": "weekly",
        "description": "Weekly AI news digest.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://tylerfolkman.substack.com/feed",
        "name": "The AI Architect",
        "frequency_name": "weekly",
        "description": "AI architecture and coding tools.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://airabbithole.substack.com/feed",
        "name": "The AI Rabbit Hole",
        "frequency_name": "weekly",
        "description": "Deep dives into AI topics and experiments.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://shmulc.substack.com/feed",
        "name": "AI Superhero",
        "frequency_name": "weekly",
        "description": "AI tools and productivity tips.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://the-ai-corner.com/feed",
        "name": "The AI Corner",
        "frequency_name": "weekly",
        "description": "AI tools, workflows, and use cases.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://creatoreconomy.so/feed",
        "name": "Behind the Craft",
        "frequency_name": "weekly",
        "description": "AI tools for creators and the creator economy.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thevccorner.com/feed",
        "name": "The VC Corner",
        "frequency_name": "weekly",
        "description": "Venture capital and AI startup landscape.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://cfooffice.io/feed",
        "name": "AI CFO Office",
        "frequency_name": "weekly",
        "description": "AI in finance and CFO office automation.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://newsletter.mkt1.co/feed",
        "name": "MKT1 Newsletter",
        "frequency_name": "weekly",
        "description": "AI in startup marketing and growth.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://sebastianbarros.substack.com/feed",
        "name": "Sebastian Barros Newsletter",
        "frequency_name": "weekly",
        "description": "Cloud, AI, and connectivity trends.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.globaldatacenterhub.com/feed",
        "name": "Global Data Center Hub",
        "frequency_name": "weekly",
        "description": "Data centre infrastructure and AI compute.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://datacenterfervor.substack.com/feed",
        "name": "Data Center Fervor",
        "frequency_name": "weekly",
        "description": "Data centre industry news and analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://dirttodata.substack.com/feed",
        "name": "Dirt to Data",
        "frequency_name": "weekly",
        "description": "Data centre development, land, and power strategy.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://sharongoldman.substack.com/feed",
        "name": "AI Side Notes with Sharon Goldman",
        "frequency_name": "weekly",
        "description": "Fortune reporter on AI industry notes and analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://techsoda.substack.com/feed",
        "name": "TechSoda",
        "frequency_name": "weekly",
        "description": "Semiconductors, optical, and AI infrastructure.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.technostatecraft.com/feed",
        "name": "Techno-Statecraft",
        "frequency_name": "weekly",
        "description": "Technology and statecraft - geopolitics of AI and semis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://semifundamental.substack.com/feed",
        "name": "Semi Fundamental",
        "frequency_name": "weekly",
        "description": "AI and semiconductor supply chain fundamentals.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://chipinsights.net/feed",
        "name": "Chip Insights",
        "frequency_name": "weekly",
        "description": "Semiconductor industry insights and analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.22ndcenturyfrontier.com/feed",
        "name": "22nd Century Frontier",
        "frequency_name": "weekly",
        "description": "Frontier technology and deep tech investing.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.augmentedswe.com/feed",
        "name": "The AI-Augmented Engineer",
        "frequency_name": "weekly",
        "description": "AI tools for software engineers.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://businessengineer.ai/feed",
        "name": "The Business Engineer",
        "frequency_name": "weekly",
        "description": "Business strategy and engineering with AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://scsp222.substack.com/feed",
        "name": "Special Competitive Studies Project",
        "frequency_name": "weekly",
        "description": "US technology competitiveness and AI strategy.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://tarotcapital.substack.com/feed",
        "name": "Tarot Capital",
        "frequency_name": "weekly",
        "description": "Incentive-driven technology investing.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://arushisharmafrank.substack.com/feed",
        "name": "Teach What I Know",
        "frequency_name": "weekly",
        "description": "Energy markets, grid policy, and AI infrastructure power.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://cozora.substack.com/feed",
        "name": "Cozora",
        "frequency_name": "weekly",
        "description": "AI creator tools and workshops.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thoughtforms.substack.com/feed",
        "name": "Thoughtforms",
        "frequency_name": "weekly",
        "description": "Technology, business, design, and culture futures.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://theslowai.substack.com/feed",
        "name": "Slow AI",
        "frequency_name": "weekly",
        "description": "Critical AI literacy and thoughtful AI engagement.",
    },

    {
        "domain_slug": "ai_news",
        "url": "https://mackcollier.substack.com/feed",
        "name": "Backstage Pass",
        "frequency_name": "weekly",
        "description": "AI in social media and community strategy.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://thehumanco.substack.com/feed",
        "name": "The Human Stack",
        "frequency_name": "weekly",
        "description": "AI, future of work, and HR/L&D leadership.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://fafi25.substack.com/feed",
        "name": "Lights On",
        "frequency_name": "weekly",
        "description": "AI business and productivity.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aiinuse.substack.com/feed",
        "name": "Operating",
        "frequency_name": "weekly",
        "description": "AI in operational business contexts.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://3foldoutcomes.substack.com/feed",
        "name": "3Fold Outcomes",
        "frequency_name": "weekly",
        "description": "M&A, organisational design, and AI.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://vickyho.substack.com/feed",
        "name": "VK 科技閱讀時間",
        "frequency_name": "weekly",
        "description": "Chinese-language technology and AI reading curation.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://purposefulai.substack.com/feed",
        "name": "Purposeful AI",
        "frequency_name": "weekly",
        "description": "AI in higher education and nonprofits.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://aiinfrastructure.substack.com/feed",
        "name": "AI Infrastructure Digest",
        "frequency_name": "weekly",
        "description": "AI infrastructure news and analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://inferenceatscale.substack.com/feed",
        "name": "Inference at Scale",
        "frequency_name": "weekly",
        "description": "AI inference infrastructure and deployment.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://llmengineer.substack.com/feed",
        "name": "LLM Engineer Weekly",
        "frequency_name": "weekly",
        "description": "LLM engineering tools and techniques.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.promptengineering.org/feed",
        "name": "Prompt Engineering Daily",
        "frequency_name": "weekly",
        "description": "Prompt engineering techniques and research.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
        "name": "CNBC Finance",
        "frequency_name": "daily",
        "description": "CNBC finance and markets coverage, including AI-stock moves and market corrections.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.ft.com/markets?format=rss",
        "name": "FT Markets",
        "frequency_name": "daily",
        "description": "Financial Times markets coverage, including AI-stock moves and market corrections.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://feeds.npr.org/1006/rss.xml",
        "name": "NPR Business",
        "frequency_name": "daily",
        "description": "NPR business coverage, including AI-stock moves and market corrections.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.economist.com/finance-and-economics/rss.xml",
        "name": "The Economist Finance",
        "frequency_name": "daily",
        "description": "The Economist finance and economics coverage, including AI-market analysis.",
    },
    {
        "domain_slug": "ai_news",
        "url": "https://feeds.bloomberg.com/markets/news.rss",
        "name": "Bloomberg Markets",
        "frequency_name": "daily",
        "description": "Bloomberg markets coverage, including AI-stock moves and market corrections.",
    },
]

# Company-specific news via Alpha Vantage News & Sentiments API.
# Tickers are sourced from the AI economy universe.
# Fetcher (source_type: "alpha_vantage") implemented in pipeline.py.
COMPANY_NEWS_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "company_news",
        "url": "alpha_vantage:company_news",
        "name": "Alpha Vantage Company News",
        "source_type": "alpha_vantage",
        "frequency_name": "daily",
        "description": (
            "Company-specific news and sentiment for AI economy universe"
            " entities, fetched by ticker symbol via Alpha Vantage."
        ),
        # Tickers are now managed via get_tracked_ticker_universe() in pipeline.py,
        # fetched live from research-universe. This config is kept empty — the
        # source row is required for domain registration.
        "config": {},
    },
]

# Federal Register documents (sanctions, export controls, tariffs, executive
# orders) — free public REST API, no key required.
# Fetcher (source_type: "federal_register") implemented in pipeline.py.
FEDERAL_REGISTER_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "geopolitical_news",
        "url": "federal_register:geopolitical",
        "name": "Federal Register",
        "source_type": "federal_register",
        "frequency_name": "daily",
        "description": (
            "U.S. Federal Register documents: sanctions actions,"
            " export controls, tariffs, executive orders, and"
            " presidential documents."
        ),
        "config": {
            "agencies": [
                "treasury-department",
                "foreign-assets-control-office",
                "industry-and-security-bureau",
                "trade-representative-office-of-united-states",
                "executive-office-of-the-president",
            ],
            "type": ["PRESDOCU", "RULE", "PRORULE", "NOTICE"],
        },
    },
]

# TWSE / TPEx OpenData monthly revenue (t187ap05_L / mopsfin_t187ap05_O).
# Both are full-dump, keyless JSON endpoints with no ticker query param -
# the fetcher pulls the whole dataset and filters to TAIWAN_TICKER_UNIVERSE.
# Fetchers (source_type: "twse_revenue" / "tpex_revenue") in pipeline.py.
TAIWAN_REVENUE_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "taiwan_market_signal",
        "url": "twse_revenue:monthly",
        "name": "TWSE Monthly Revenue",
        "source_type": "twse_revenue",
        "frequency_name": "daily",
        "description": (
            "TWSE OpenAPI monthly revenue (t187ap05_L) for TWSE-listed"
            " tickers in the Taiwan universe, with MoM/YoY deltas."
        ),
        "config": {
            "tickers": [
                t["ticker"] for t in TAIWAN_TICKER_UNIVERSE
                if t["exchange"] == "TWSE"
            ],
            # Known-correct English company names, keyed by ticker - used
            # by the fetcher to set metadata.translated_company_name
            # directly, rather than asking an LLM to translate the raw
            # Chinese name (confirmed live: this produced serious errors,
            # e.g. 2383 Elite Material mistranslated as "Taiwan
            # Semiconductor Manufacturing Company", 8210 Chenbro translated
            # literally as "Diligence and sincerity"). We already have the
            # ground truth here; no reason to let a model guess it.
            "ticker_names": {
                t["ticker"]: t["company"] for t in TAIWAN_TICKER_UNIVERSE
                if t["exchange"] == "TWSE"
            },
        },
    },
    {
        "domain_slug": "taiwan_market_signal",
        "url": "tpex_revenue:monthly",
        "name": "TPEx Monthly Revenue",
        "source_type": "tpex_revenue",
        "frequency_name": "daily",
        "description": (
            "TPEx OpenAPI monthly revenue (mopsfin_t187ap05_O) for"
            " TPEx/OTC-listed tickers in the Taiwan universe, with"
            " MoM/YoY deltas."
        ),
        "config": {
            "tickers": [
                t["ticker"] for t in TAIWAN_TICKER_UNIVERSE
                if t["exchange"] == "TPEx"
            ],
            "ticker_names": {
                t["ticker"]: t["company"] for t in TAIWAN_TICKER_UNIVERSE
                if t["exchange"] == "TPEx"
            },
        },
    },
]

# TWSE / TPEx OpenData material announcements (t187ap04_L / mopsfin_t187ap04_O).
# Same full-dump-then-filter shape as revenue above. TPEx's feed uses a
# different keyset (Date/SecuritiesCompanyCode/CompanyName vs TWSE's
# 出表日期/公司代號/公司名稱) - normalized to a common shape in the fetcher.
# Fetchers (source_type: "twse_material" / "tpex_material") in pipeline.py.
TAIWAN_MATERIAL_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "taiwan_market_signal",
        "url": "twse_material:announcements",
        "name": "TWSE Material Announcements",
        "source_type": "twse_material",
        "frequency_name": "daily",
        "description": (
            "TWSE OpenAPI material announcements (t187ap04_L, 重大訊息)"
            " for TWSE-listed tickers in the Taiwan universe."
        ),
        "config": {
            "tickers": [
                t["ticker"] for t in TAIWAN_TICKER_UNIVERSE
                if t["exchange"] == "TWSE"
            ],
            "ticker_names": {
                t["ticker"]: t["company"] for t in TAIWAN_TICKER_UNIVERSE
                if t["exchange"] == "TWSE"
            },
        },
    },
    {
        "domain_slug": "taiwan_market_signal",
        "url": "tpex_material:announcements",
        "name": "TPEx Material Announcements",
        "source_type": "tpex_material",
        "frequency_name": "daily",
        "description": (
            "TPEx OpenAPI material announcements (mopsfin_t187ap04_O)"
            " for TPEx/OTC-listed tickers in the Taiwan universe."
        ),
        "config": {
            "tickers": [
                t["ticker"] for t in TAIWAN_TICKER_UNIVERSE
                if t["exchange"] == "TPEx"
            ],
            "ticker_names": {
                t["ticker"]: t["company"] for t in TAIWAN_TICKER_UNIVERSE
                if t["exchange"] == "TPEx"
            },
        },
    },
]

# GDELT DOC 2.0 API scoped to Taiwan-language press for the Taiwan universe,
# reusing the existing gdelt fetcher (source_type: "gdelt", implemented in
# pipeline.py — same _fetch_gdelt/_fetch_one_gdelt code path already used by
# geopolitical_news, just with different queries).
#
# Verified live that "<name> sourcelang:chinese sourcecountry:TW" returns
# genuine Taiwan financial press (SETN, DigiTimes) with real signal (revenue
# prints, order news), NOT pre-translated to English (GDELT's "language"
# field on these results is "Chinese" and titles are in Chinese — contrary
# to the original spec's claim of "already translated"; a translation step
# is required downstream, not optional).
#
# GDELT rejects any query keyword of 2 CJK characters or fewer with a
# distinct ("keyword too short") response — confirmed live, HTTP 200 with a
# non-JSON plain-text body, which is NOT the same failure as a 429 and is
# not currently retried/handled specially by the fetcher (it would parse-
# error and log a generic warning every poll). 11 of the 20 tickers have
# exactly 2-character native names (鴻海, 廣達, 緯創, 緯穎, 奇鋐, 雙鴻, 嘉澤,
# 川湖, 勤誠, 欣興, 信驊). Two attempts to pad these to 3+ characters with a
# generic suffix were tested live and BOTH failed to surface results even
# for Hon Hai (2317), a company confirmed to have heavy Chinese-language
# coverage: "鴻海集團" (+Group) and "鴻海精密" (+Precision, its own formal
# name) both returned a clean empty response, not an error — meaning
# suffix-padding is unreliable per-company and would silently look
# identical to "no news" if shipped. Bare ticker-code queries were also
# tested and ruled out the same way (2330/TSMC returned empty despite
# known heavy coverage).
#
# Given no verified-working padding scheme, native-name querying is
# dropped for these 11 tickers; only the English-name query is used for
# them (already confirmed working and ≥3 chars for all 20 companies). The
# other 9 tickers with 3+ character native names keep both queries.
_GDELT_MIN_QUERY_CHARS = 3

TAIWAN_GDELT_SOURCE: dict[str, Any] = {
    "domain_slug": "taiwan_market_signal",
    "url": "gdelt:taiwan_market_signal",
    "name": "GDELT DOC API (Taiwan)",
    "source_type": "gdelt",
    "frequency_name": "daily",
    "description": (
        "Taiwan-language financial press coverage via GDELT DOC 2.0,"
        " scoped to sourcelang:chinese sourcecountry:TW, queried per"
        " ticker by native Chinese name (when >=3 characters) and"
        " English name."
    ),
    "config": {
        "queries": [
            q
            for t in TAIWAN_TICKER_UNIVERSE
            for q in (
                [f"{t['native_name']} sourcelang:chinese sourcecountry:TW"]
                if len(t["native_name"]) >= _GDELT_MIN_QUERY_CHARS
                else []
            ) + [f"{t['company']} sourcelang:chinese sourcecountry:TW"]
        ],
        # Maps each query string back to its ticker, since GDELT queries by
        # company name (there is no ticker query param) and the fetcher
        # otherwise has no way to know which company a result belongs to.
        # Used for the title-similarity dedup, which is scoped per-ticker.
        "query_ticker": {
            q: t["ticker"]
            for t in TAIWAN_TICKER_UNIVERSE
            for q in (
                ([f"{t['native_name']} sourcelang:chinese sourcecountry:TW"]
                 if len(t["native_name"]) >= _GDELT_MIN_QUERY_CHARS else [])
                + [f"{t['company']} sourcelang:chinese sourcecountry:TW"]
            )
        },
        # Maps each query string to the ticker's English name, regardless of
        # whether the query itself used the native or English name - lets
        # the company-name-in-title filter accept an English-language
        # headline even for a native-name query (confirmed live: GDELT's
        # sourcelang:chinese scope still returns some English-language
        # articles, e.g. Taipei Times/Focus Taiwan syndication, which were
        # being dropped even though they genuinely name the company).
        "query_english_name": {
            q: t["company"]
            for t in TAIWAN_TICKER_UNIVERSE
            for q in (
                ([f"{t['native_name']} sourcelang:chinese sourcecountry:TW"]
                 if len(t["native_name"]) >= _GDELT_MIN_QUERY_CHARS else [])
                + [f"{t['company']} sourcelang:chinese sourcecountry:TW"]
            )
        },
    },
}

# DART (Financial Supervisory Service) filing list + document body fetch,
# scoped to the 19 listed companies in KOREA_TICKER_UNIVERSE (Hanwha
# Semitech is excluded - confirmed unlisted, no stock_code, no DART
# corp_code exists for it under either its current or former name).
# Fetcher (source_type: "dart_filing") implemented in pipeline.py.
#
# Covers Korea Signals spec Section 4 S2 (supply contract), S3 (capacity
# commitment), S4 (preliminary earnings), S5 (guidance disclosure), and S6
# (rumour adjudication - confirmed live 2026-09-22 that DART carries both
# the exchange's demand and the company's answer as distinct filings; see
# the _DART_TARGET_REPORT_PATTERNS comment in pipeline.py for the precision
# caveat on "hours between the two"). Does NOT cover S1 (Korea Customs,
# separate source).
KOREA_DART_SOURCE: dict[str, Any] = {
    "domain_slug": "korea_market_signal",
    "url": "dart_filing:korea_market_signal",
    "name": "DART Filings (Korea)",
    "source_type": "dart_filing",
    "frequency_name": "daily",
    "description": (
        "Exchange-mandated disclosures (supply contracts, capacity"
        " investment, preliminary earnings, guidance, rumour adjudication)"
        " for Korea AI-supply-chain companies, via DART's list.json +"
        " document.xml, filtered to report_nm patterns matching those"
        " signal types."
    ),
    "config": {
        "companies": [
            {"ticker": t["ticker"], "company": t["company"]}
            for t in KOREA_TICKER_UNIVERSE
            if t["ticker"] is not None
        ],
    },
}

# KOREA_CUSTOMS_SOURCE_STATUS: covers Korea Signals spec Section 4 S1
# (export surprise), the doc's headline signal.
#
# THE ORIGINAL BLOCKER (still true, kept for context): the clean
# structured REST API for this data is data.go.kr, dataset 15157908
# ("관세청_수출 주요품목별 10일 단위 잠정치 통계") - endpoint, params
# (serviceKey/strtYymm/endYymm), and response fields confirmed live
# 2026-09-22. Getting a serviceKey requires a personal data.go.kr account,
# and that signup's "정보입력" step has a REQUIRED 휴대전화번호 (mobile
# phone) field wired to Korea's national identity-verification system
# (NICE/KCB) - it validates the phone's carrier subscription is tied to a
# real Korean identity credential (for a foreigner, an Alien Registration
# Card). No foreigner-specific signup track exists. A non-resident
# without an ARC-linked Korean phone number cannot complete this signup.
# KOSIS is residency-gated the same way; KITA gates the same data behind
# paid Korean corporate trade-registration membership (harder, not
# easier); tradedata.go.kr's own API layer routes back through the same
# data.go.kr account system.
#
# RESOLVED 2026-09-23 via a 4th option not in the original list: Korea
# Customs Service's own public press-release board
# (customs.go.kr/kcs/na/ntt/..., bbsId=1362, mi=2891) publishes the same
# underlying figure with NO login required at all - confirmed live. This
# is what KOREA_CUSTOMS_EXPORT_SOURCE below scrapes (fetcher:
# source_type "kr_customs_export" in pipeline.py).
#
# Confirmed live 2026-09-23: this board has no RSS feed, and post IDs
# (nttSn) are a global auto-increment across ~6,600+ unrelated posts, not
# usable as a date pattern - the fetcher instead uses the board's own
# title-search (a POST, not GET query params - a GET-param guess 404'd)
# to isolate the recurring "수출입 현황" report family and take the
# newest match.
#
# What the source actually gives us, confirmed live against real 10-day
# and 20-day releases: a dollar figure plus a superlative record/streak
# claim (e.g. "반도체(341억 달러) 수출 동기간 역대최대" - semiconductor
# exports of $34.1bn, a period record) - NOT a year-over-year percentage
# and NOT a share-of-total-exports percentage (checked every mention of
# "반도체" on a real page and searched for "차지"/share-of, found neither
# at 10-day/20-day granularity; a %-growth figure does appear on the
# separate monthly "확정치"/confirmed release, ~15th-18th of the
# following month, a 4th release type this board also carries).
#
# This means Section 5.1's "change against the same period one year
# earlier" and "spread over the last 12 equivalent periods" cannot come
# from the source's own text at 10-day granularity - this pipeline must
# compute both itself from its own stored history of these dollar
# figures, exactly matching the doc's own build note that "this rule
# needs two years of stored history before it means anything." That
# history is not backfilled, and S1 is now PERMANENTLY OUT OF SCOPE
# (decided 2026-09-23) rather than pending - Bank of Korea ECOS was the
# agreed backfill source, but its real signup flow was confirmed live to
# require the same Korean mobile-carrier identity verification (PASS,
# run by NICE) that already blocked data.go.kr - the earlier research
# calling ECOS "no Korean phone required" was wrong. No Korean contact is
# available to register on our behalf, and a paid vendor route (e.g.
# CEIC) was considered and explicitly not pursued. classify_export_
# surprise() is not implemented anywhere in signal-detection-agent as a
# result - see that service's korea_signal_classifier.py module docstring
# for the full record. This source (KOREA_CUSTOMS_EXPORT_SOURCE) stays
# seeded regardless - it still fetches the newest release's raw figure
# for the record and for the cross-check note below, even with no
# classifier ever consuming it for S1's own arithmetic rule.
#
# Also agreed but not yet built: cross-checking this scrape against
# same-day Korean wire coverage (Yonhap/Hankyung/ETNews all report this
# figure within minutes of release) as a disagreement detector - if the
# scraped number and the wire coverage disagree, that is a signal the
# parser broke, not a signal to silently publish. This would reuse the
# Korean news layer already being built for S7, not a new source type.
KOREA_CUSTOMS_EXPORT_SOURCE: dict[str, Any] = {
    "domain_slug": "korea_market_signal",
    "url": "kr_customs_export:korea_market_signal",
    "name": "Korea Customs Export Press Release",
    "source_type": "kr_customs_export",
    "frequency_name": "daily",
    "description": (
        "Semiconductor export figures scraped from Korea Customs"
        " Service's public press-release board (no login required) -"
        " fallback for the data.go.kr structured API, which is blocked"
        " behind Korean-phone-verified account signup. Publishes the"
        " newest 10-day/20-day/monthly release's dollar figure; YoY% is"
        " computed downstream from this pipeline's own stored history,"
        " not read from the source."
    ),
}

# Korean-language trade/business press RSS feeds - plain rss source_type
# (no config needed beyond the feed url itself), same as SUBSTACK_SOURCES.
# Covers Korea Signals spec Section 4 S7 (qualification news), upstream of
# the three-filter + LLM classification pipeline that lives in
# signal-detection-agent (see that service's CLAUDE.md) - this service's
# job is fetch/dedup only.
#
# Only THE ELEC (below) has a dedicated semiconductor/materials-equipment
# section feed - ZDNet Korea, BusinessPost, and DigitalDaily do not;
# semiconductor coverage appears as one recurring topic within their
# general business/industry feeds, not an isolated stream. This is a known
# tradeoff, not an oversight - broader feeds mean more volume for
# signal-detection-agent's classification filters to work through.
# (ETNews was also tried and dropped - see the comment further down where
# it used to sit, kept as a record of why.)
KOREA_RSS_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "korea_market_signal",
        "url": "https://www.thelec.kr/rss/S1N2.xml",
        "name": "THE ELEC - Semiconductors",
        "frequency_name": "daily",
        "description": (
            "디일렉 (THE ELEC), Korean specialist electronics-component"
            " trade press - semiconductors section. Closest Korean"
            " equivalent to DIGITIMES; founded specifically to cover"
            " Korea's component/equipment industry, which general/economic"
            " media under-covers."
        ),
    },
    {
        "domain_slug": "korea_market_signal",
        "url": "https://www.thelec.kr/rss/S1N3.xml",
        "name": "THE ELEC - Materials & Equipment",
        "frequency_name": "daily",
        "description": (
            "디일렉 (THE ELEC) materials & equipment section - directly"
            " relevant to the Tier 2 memory-stacking-chain companies in"
            " KOREA_TICKER_UNIVERSE."
        ),
    },
    # ETNews (전자신문) was tried and DROPPED - both candidate section feeds
    # (06061/장비/equipment, 06062/부품/components; ETNews has no dedicated
    # semiconductor section at all, confirmed against the site's own RSS
    # hub taxonomy) worked cleanly when first live-tested 2026-09-22, but a
    # second check hours later returned the EXACT SAME 50 entries, same
    # timestamps, all dated 2026-06-25 - a ~3-month-old snapshot, not a
    # live feed. Confirmed stuck, not a transient delay: two independent
    # fetches produced identical stale output rather than any forward
    # movement. Decision 2026-09-23: drop rather than keep flagged, since a
    # frozen "daily" source would silently stop contributing without any
    # error to notice - not worth the false confidence of a source entry
    # that looks live in seed.py but isn't. Revisit by re-testing
    # http://rss.etnews.com/06061.xml directly if ETNews coverage is
    # wanted again later.
    {
        "domain_slug": "korea_market_signal",
        "url": "https://feeds.feedburner.com/zdkorea",
        "name": "ZDNet Korea",
        "frequency_name": "daily",
        "description": (
            "지디넷코리아 (ZDNet Korea), general Korean tech/IT news -"
            " broader audience than THE ELEC, general feed (no"
            " dedicated semiconductor section)."
        ),
    },
    {
        "domain_slug": "korea_market_signal",
        "url": "https://www.businesspost.co.kr/rss/Article_3.xml",
        "name": "BusinessPost - Corporate & Industry",
        "frequency_name": "daily",
        "description": (
            "비즈니스포스트 (BusinessPost), Korean business news -"
            " Corporate & Industry (기업과산업) section, general feed"
            " with tech/industry crossover (no dedicated semiconductor"
            " section)."
        ),
    },
    {
        "domain_slug": "korea_market_signal",
        "url": "https://www.ddaily.co.kr/rss.xml",
        "name": "DigitalDaily",
        "frequency_name": "daily",
        "description": (
            "디지털데일리 (DigitalDaily), Korean IT/tech trade press -"
            " general feed (no dedicated semiconductor section)."
        ),
    },
    # Yonhap's only confirmed-working feed (en.yna.co.kr/RSS/news.xml) is
    # its general English-language wire, not a business/economy section -
    # no section-specific feed URL exists (checked economy.xml,
    # business.xml, industry.xml, markets.xml, all 404; no category field
    # or URL-encoded section code found on individual entries either, so
    # there's no structured field to filter on). Live-tested 2026-09-23: a
    # title-keyword regex against a real 102-entry sample matched 15
    # (16%) with zero false positives - correctly excluded all Asian
    # Games/politics/court content while catching every genuinely
    # business/economy-relevant headline (Seoul stocks, BOK, growth
    # forecast, consumer sentiment, Naver labor action, etc.). Applied via
    # config.title_filter (see _parse_feed in pipeline.py) rather than
    # dropping the source outright the way ETNews was, since the content
    # that DOES pass is real, fresh (today's timestamps), and on-target -
    # unlike ETNews, this isn't a dead/stale source, just an unfiltered
    # one without this regex.
    {
        "domain_slug": "korea_market_signal",
        "url": "https://en.yna.co.kr/RSS/news.xml",
        "name": "Yonhap News (business/economy filtered)",
        "frequency_name": "daily",
        "description": (
            "연합뉴스 (Yonhap News Agency), English-language wire -"
            " general/mixed feed (no business-specific section feed"
            " exists), filtered post-fetch to business/economy/markets"
            " headlines via config.title_filter since there is no"
            " structured category field to filter on instead."
        ),
        "config": {
            "title_filter": (
                r"\b(stocks?|won\b|\bBOK\b|econom|GDP|growth forecast|"
                r"chip(s|maker)?|semiconductor|exports?|imports?|"
                r"trade (deficit|surplus|balance|talks|war)|"
                r"market (woes|slowdown|data)|inflation|"
                r"consumer (sentiment|price)|investment|manufactur|"
                r"Samsung|SK Hynix|\bLG\b|Hyundai|Naver|Kakao|earnings|"
                r"profit|revenue|billion|trillion|fiscal|monetary policy|"
                r"\bADB\b|\bIMF\b|insurance|loan|delinquency|debt|"
                r"power plant|acquisition|stake\b|\bFTC\b|antitrust|"
                r"banknotes)"
            ),
        },
    },
]

# GDELT DOC 2.0 API scoped to Korean-language press for the Korea universe,
# reusing the existing gdelt fetcher (source_type: "gdelt", same
# _fetch_gdelt/_fetch_one_gdelt code path as geopolitical_news and
# TAIWAN_GDELT_SOURCE) - just different queries.
#
# Confirmed live 2026-09-22: "sourcelang:korean sourcecountry:KS" (KS, not
# KR - GDELT's sourcecountry operator uses FIPS 10-4 codes, not ISO 3166;
# KR is not a valid FIPS code) returns genuine Korean-language, Korea-
# domiciled results including real trade press (etnews.com).
#
# Short-native-name rejection (same GDELT quirk documented in
# TAIWAN_GDELT_SOURCE above, for CJK-script queries under ~3 characters)
# was NOT independently re-confirmed for Korean/Hangul specifically - live
# testing was blocked by GDELT's own rate limit (429, "limit requests to
# one every 5 seconds") persisting well past that stated window across
# multiple spaced-out retries on 2026-09-22, so this could not be verified
# before this was written. Applying the same defensive threshold Taiwan
# already uses is low-cost even if unnecessary here (worst case: one extra,
# redundant English-name query for the one affected company), so it is
# applied rather than assumed unneeded. Of the 19 listed companies, only
# Simmtech's Korean name (심텍, 2 characters) falls under the threshold -
# a much smaller exposure than Taiwan's 11/20, since almost all Korean
# company names in this universe are naturally 3+ characters.
_KOREA_GDELT_MIN_QUERY_CHARS = 3

KOREA_GDELT_SOURCE: dict[str, Any] = {
    "domain_slug": "korea_market_signal",
    "url": "gdelt:korea_market_signal",
    "name": "GDELT DOC API (Korea)",
    "source_type": "gdelt",
    "frequency_name": "daily",
    "description": (
        "Korean-language press coverage via GDELT DOC 2.0, scoped to"
        " sourcelang:korean sourcecountry:KS, queried per company by"
        " Korean name (when >=3 characters) and English name."
    ),
    "config": {
        "queries": [
            q
            for t in KOREA_TICKER_UNIVERSE
            for q in (
                [f"{t['korean_name']} sourcelang:korean sourcecountry:KS"]
                if len(t["korean_name"]) >= _KOREA_GDELT_MIN_QUERY_CHARS
                else []
            ) + [f"{t['company']} sourcelang:korean sourcecountry:KS"]
        ],
        # Maps each query string back to its ticker (or, for the one
        # unlisted company, its name) - GDELT queries by company name, not
        # ticker, so the fetcher otherwise has no way to attribute a result.
        # Used for the title-similarity dedup, scoped per-company.
        "query_ticker": {
            q: (t["ticker"] or t["company"])
            for t in KOREA_TICKER_UNIVERSE
            for q in (
                ([f"{t['korean_name']} sourcelang:korean sourcecountry:KS"]
                 if len(t["korean_name"]) >= _KOREA_GDELT_MIN_QUERY_CHARS else [])
                + [f"{t['company']} sourcelang:korean sourcecountry:KS"]
            )
        },
        "query_english_name": {
            q: t["company"]
            for t in KOREA_TICKER_UNIVERSE
            for q in (
                ([f"{t['korean_name']} sourcelang:korean sourcecountry:KS"]
                 if len(t["korean_name"]) >= _KOREA_GDELT_MIN_QUERY_CHARS else [])
                + [f"{t['company']} sourcelang:korean sourcecountry:KS"]
            )
        },
    },
}

# GDELT DOC 2.0 API scoped to ENGLISH-language coverage of the same Korea
# universe, for the English-coverage check (Korea Signals spec Section 7
# Step 5: "run the same search in English and record whether anything was
# found, and when"; displayed per-item in Section 6.4 as "English press:
# None found at time of retrieval"). This service's job is only to fetch
# and store this data - the actual comparison (does an English article
# exist, and if so was it published before/after the Korean one) is
# signal-detection-agent's job, computed from these rows plus
# KOREA_GDELT_SOURCE's rows, not done here.
#
# Deliberately does NOT share ticker keys with KOREA_GDELT_SOURCE's
# query_ticker, even though both ultimately concern the same companies -
# _fetch_gdelt's title-similarity dedup (pipeline.py) is scoped per ticker
# key across ALL merged GDELT sources in one run, and a same-ticker-key
# collision here would let the dedup logic silently drop a genuine English
# article as a "near-duplicate" of the Korean one, defeating the entire
# point of fetching both. Ticker keys are suffixed "-en" so the two
# languages' articles are dedup-scoped separately while both still get
# stored, with metadata.ticker letting a downstream reader join them back
# to the same company by stripping the suffix.
KOREA_GDELT_ENGLISH_SOURCE: dict[str, Any] = {
    "domain_slug": "korea_market_signal",
    "url": "gdelt:korea_market_signal_english",
    "name": "GDELT DOC API (Korea - English coverage check)",
    "source_type": "gdelt",
    "frequency_name": "daily",
    "description": (
        "English-language press coverage of the same Korea AI-supply-chain"
        " companies via GDELT DOC 2.0, scoped to sourcelang:english."
        " Exists solely to test whether/when English coverage of a fact"
        " exists, for comparison against the Korean-language sources -"
        " not a primary signal source itself."
    ),
    "config": {
        "queries": [
            f"{t['company']} sourcelang:english"
            for t in KOREA_TICKER_UNIVERSE
        ],
        "query_ticker": {
            f"{t['company']} sourcelang:english": f"{t['ticker'] or t['company']}-en"
            for t in KOREA_TICKER_UNIVERSE
        },
        "query_english_name": {
            f"{t['company']} sourcelang:english": t["company"]
            for t in KOREA_TICKER_UNIVERSE
        },
    },
}

# ---------------------------------------------------------------------------
# Japan market signal
# ---------------------------------------------------------------------------
#
# 20 companies from the Japan Signals spec Section 1 - narrow, near-monopoly
# steps in chipmaking (wafers, patterning chemicals, mask inspection, wafer
# dicing/grinding, test, substrates) plus the wider AI-universe read-through
# set (passives, memory, analogue, grid/data-centre power, and the SoftBank
# holding-company exposure).
#
# code is TEXT throughout, never cast to int - Kioxia's code (285A) contains
# a letter. fiscal_year_end (MM-DD) is static per-company reference data used
# downstream by signal-detection-agent's progress-vs-forecast math (almost
# every company here ends its fiscal year 03-31, not 12-31 - see spec
# Section 2.4). native_name is the Japanese company name, used the same way
# Taiwan's native_name is - for native-language press queries, not yet wired
# to a source below (see press_jp, still pending).
#
# CONFIRMED DELISTED 2026-09-28: Shinko Electric (6967) - the spec's own
# note flagged an unconfirmed take-private process; resolved by fetching
# EDINET's official code list (disclosure2dl.edinet-fsa.go.jp/.../
# codelist/Edinetcode.zip - see edinet_filing's config below) and checking
# it directly: Shinko Electric's own entry (EDINET code E01957) now shows
# "非上場" (unlisted) with a BLANK 証券コード (securities code) field, where
# every other company in this universe has a populated 5-digit code.
# Removed from the universe on that basis: a delisted company files
# nothing new, so every fetcher keyed on its ticker returns empty and
# every habit built from its history describes a company that no longer
# trades. Its past filings were real, but they describe a market the
# tracked universe no longer covers.
# aliases: other written forms real Japanese press articles use INSTEAD
# OF the full native_name - confirmed as a real, measured gap
# 2026-09-28 by comparing full-native_name matches against short-form
# matches over the same real MONOist article sample: Resonac Holdings
# (レゾナック・ホールディングス) matched 0 articles by full name vs. 5 by
# its short form (レゾナック); Renesas Electronics
# (ルネサスエレクトロニクス) matched 0 vs. 7; Shin-Etsu Chemical
# (信越化学工業) matched 8 vs. 9. Only present where it's genuinely
# DIFFERENT from native_name - most company names here are already short
# enough that press articles use the same string either way (e.g.
# Advantest's アドバンテスト is never abbreviated further in practice).
# Named `aliases` to match research-universe, which owns this data now
# and holds a list per company because Korea needs several; these rows
# remain as the fallback used when that service cannot be reached.
# Used by monoist_capex/press_jp's company-name matching (matches against
# native_name OR any alias) - not by any source
# that queries by native_name as an exact API parameter (Taiwan/Korea's
# GDELT sources), where using an abbreviation could return TOO MANY
# unrelated results instead of too few.
# Customers are NOT held here. A "customers" field carrying bare
# major-customer names was removed 2026-10-02: it was never used by any
# fetch or matching logic in this service, and the classification side
# now holds the same relationships with each company's own disclosed
# share of sales (signal-detection-agent's
# pipeline/japan_company_profile.py). Customer data belongs with the
# classifier that reads it, not duplicated into the fetcher's universe
# where it would drift out of sync unnoticed.
JAPAN_TICKER_UNIVERSE: list[dict[str, str]] = [
    {"code": "6857", "company": "Advantest", "native_name": "アドバンテスト", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "8035", "company": "Tokyo Electron", "native_name": "東京エレクトロン", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "6146", "company": "Disco", "native_name": "ディスコ", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "6920", "company": "Lasertec", "native_name": "レーザーテック", "exchange": "TSE", "fiscal_year_end": "06-30"},
    {"code": "5803", "company": "Fujikura", "native_name": "フジクラ", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "4063", "company": "Shin-Etsu Chemical", "native_name": "信越化学工業", "aliases": ["信越化学"], "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "3436", "company": "SUMCO", "native_name": "SUMCO", "exchange": "TSE", "fiscal_year_end": "12-31"},
    {"code": "4062", "company": "Ibiden", "native_name": "イビデン", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "7735", "company": "Screen Holdings", "native_name": "SCREENホールディングス", "aliases": ["SCREEN"], "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "6525", "company": "Kokusai Electric", "native_name": "KOKUSAI ELECTRIC", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "4186", "company": "Tokyo Ohka Kogyo", "native_name": "東京応化工業", "aliases": ["東京応化"], "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "4004", "company": "Resonac Holdings", "native_name": "レゾナック・ホールディングス", "aliases": ["レゾナック"], "exchange": "TSE", "fiscal_year_end": "12-31"},
    {"code": "6315", "company": "Towa", "native_name": "TOWA", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "6981", "company": "Murata Manufacturing", "native_name": "村田製作所", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "6762", "company": "TDK", "native_name": "TDK", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "285A", "company": "Kioxia", "native_name": "キオクシア", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "6723", "company": "Renesas Electronics", "native_name": "ルネサスエレクトロニクス", "aliases": ["ルネサス"], "exchange": "TSE", "fiscal_year_end": "12-31"},
    {"code": "6501", "company": "Hitachi", "native_name": "日立製作所", "exchange": "TSE", "fiscal_year_end": "03-31"},
    {"code": "9984", "company": "SoftBank Group", "native_name": "ソフトバンクグループ", "exchange": "TSE", "fiscal_year_end": "03-31"},
]

# irbank_financials: earnings-forecast revision history via IRBANK
# (irbank.net), a free Japanese financial-data aggregator - NOT the official
# J-Quants API (confirmed live 2026-09-27: J-Quants' free tier caps history
# at 2 years with a 12-week data delay, and even a paid tier only returns
# revision figures, not the reason/context text; IRBANK substitutes for both
# gaps at no cost).
#
# Two-step fetch, confirmed live against a real company (Advantest, 6857):
#   1. GET irbank.net/{code}/tdnet - a single page per company holding its
#      full disclosure history (confirmed live 13+ years deep for
#      Advantest), each entry shaped roughly:
#        <dt>{date}</dt><dd><a href="/{code}/{doc_id}">{title}</a><br>
#        <a href="/news/{news_id}">{irbank auto-generated summary}</a></dd>
#      Filter titles matching "業績予想.*修正" (e.g. "通期連結業績予想の修正
#      に関するお知らせ") to find revision notices among the full feed.
#   2. For each new match, GET irbank.net/news/{news_id} - confirmed live to
#      contain BOTH a plain-language summary sentence (old figure -> new
#      figure -> %change -> YoY%) AND a structured old/new table
#      ("業績予想の修正（連結）" section, <dl class="gdl"> rows labelled
#      予（前）[previous forecast] vs 予 [new forecast]) for revenue,
#      operating profit, and net income. This is real structured data, not
#      prose-only.
#   No company-stated "reason" text is present anywhere on IRBANK (only
#   IRBANK's own auto-generated summary sentence) - a real, accepted gap
#   versus the original spec's assumption of a quoted reason field.
#   No revision-sequence number exists either - counting/ordering revisions
#   within a fiscal year is deferred to signal-detection-agent, computed
#   from this source's own dated rows rather than fabricated here.
#
# Dedup: synthetic url "irbank-financials://{code}/{news_id}" - news_id is
# globally unique per IRBANK news item, so the DB's existing global
# uq_articles_url index does all dedup work, same convention as
# dart-filing://, twse-revenue://, etc. above. No per-source dedup logic
# needed.
#
# Fetcher (source_type: "irbank_financials") implemented in pipeline.py.
JAPAN_IRBANK_FINANCIALS_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "irbank_financials:japan_market_signal",
    "name": "IRBANK Forecast Revisions (Japan)",
    "source_type": "irbank_financials",
    "frequency_name": "daily",
    "description": (
        "Earnings-forecast revision notices for Japan AI-supply-chain"
        " companies, via IRBANK's per-company TDnet disclosure mirror"
        " (free) - old-vs-new revenue/operating-profit/net-income figures"
        " AND the company's own stated reason, both extracted from the"
        " filing's own source PDF (see pipeline.py's _fetch_irbank_"
        "filing_pdf) - not from IRBANK's own HTML, which carries neither."
    ),
    "config": {
        # Resolved live from research-universe at fetch time
        # (get_tracked_company_universe) rather than seeded here.
        # `sources` seeds ON CONFLICT DO NOTHING, so a company
        # added or corrected in code never reached a database that
        # had already been seeded - an alias added for Resonac and
        # Renesas silently kept matching 62 articles instead of 74
        # until the stored config was updated by hand. The source
        # row is still required for domain registration.
    },
}

# irbank_buyback: buyback-program history via IRBANK's per-company
# /{code}/buyback page (free, no API key) - the PRIMARY source for buyback
# figures in this pipeline. Confirmed live against Advantest (6857):
# genuinely clean, already-structured data (grouped by board-resolution
# program/year, one dated monthly status-report entry each with share-count
# change, cumulative yen amount, and cumulative % of the program's
# authorized upper limit) - unlike edinet_buyback_status's docType 220/230
# filing, which has NO discrete numeric field at all for the same data
# (see that source's own comment above). Not days_back-scoped - confirmed
# live this page is small (a handful of programs, a few entries each per
# company), so walking the whole thing per run and relying on dedup is
# simpler than a client-side cutoff.
#
# Fetcher (source_type: "irbank_buyback") implemented in pipeline.py.
JAPAN_IRBANK_BUYBACK_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "irbank_buyback:japan_market_signal",
    "name": "IRBANK Buyback Status (Japan)",
    "source_type": "irbank_buyback",
    "frequency_name": "daily",
    "description": (
        "Share-buyback program history for Japan AI-supply-chain"
        " companies, via IRBANK's free per-company /buyback page - clean"
        " structured data (date, share-count change, cumulative amount,"
        " cumulative % of program limit) for each board-resolution"
        " program. The primary source for buyback figures in this"
        " pipeline; edinet_buyback_status corroborates same-day filings"
        " but carries no comparable structured numbers."
    ),
    "config": {
        # Resolved live from research-universe at fetch time
        # (get_tracked_company_universe) rather than seeded here.
        # `sources` seeds ON CONFLICT DO NOTHING, so a company
        # added or corrected in code never reached a database that
        # had already been seeded - an alias added for Resonac and
        # Renesas silently kept matching 62 articles instead of 74
        # until the stored config was updated by hand. The source
        # row is still required for domain registration.
    },
}

# irbank_company_reference: each company's latest total assets AND
# shares-outstanding estimate, via IRBANK's free per-company /{code}/bs
# ("financial condition") page plus its main /{code} page. Confirmed live
# 2026-09-29 against all 20 universe companies (0 failures on the
# total-assets half) - a clean per-fiscal-period history table with 総資産
# (total assets) as its own labelled column; only the single NEWEST row
# is kept per run (a slow-changing reference fact, not a news feed - see
# pipeline.py's own comment on _fetch_one_irbank_company_reference for why
# no history is needed here, unlike irbank_financials). Shares outstanding
# is derived (market cap / previous-close price, both on the main page -
# no direct share-count field exists anywhere on IRBANK's pages checked),
# so it is allowed to be independently None on a row without dropping the
# whole row - total assets alone still serves J5 even if that derivation
# fails for one company.
#
# Exists to serve J5 (capacity commitment) and J6 (ownership/capital
# policy) in signal-detection-agent's japan_signal_classifier.py: the
# spec's own Section 6.5 rule needs total assets as the denominator for
# "investment >= 10% of total assets" (J5), and shares outstanding as the
# denominator for "buyback >= 5% of shares outstanding" (J6).
# monoist_capex (jp_capex) carries J5's investment yen figure and
# irbank_buyback carries J6's buyback share-count figure; this source
# carries the other half of both fractions. Folded into one source_type
# rather than two - see pipeline.py's own top-of-section comment for why.
#
# Fetcher (source_type: "irbank_company_reference") implemented in
# pipeline.py.
JAPAN_IRBANK_COMPANY_REFERENCE_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "irbank_company_reference:japan_market_signal",
    "name": "IRBANK Company Reference (Japan)",
    "source_type": "irbank_company_reference",
    "frequency_name": "monthly",
    "description": (
        "Latest total-assets and shares-outstanding figures for Japan"
        " AI-supply-chain companies, via IRBANK's free per-company /bs"
        " and main pages - the denominators J5's investment-vs-total-"
        "assets rule and J6's buyback-vs-shares-outstanding rule need."
        " Reference data, not a news feed - only the newest known"
        " fiscal-period figure is kept per run."
    ),
    "config": {
        # Resolved live from research-universe at fetch time
        # (get_tracked_company_universe) rather than seeded here.
        # `sources` seeds ON CONFLICT DO NOTHING, so a company
        # added or corrected in code never reached a database that
        # had already been seeded - an alias added for Resonac and
        # Renesas silently kept matching 62 articles instead of 74
        # until the stored config was updated by hand. The source
        # row is still required for domain registration.
    },
}

# kabutan_tdnet_mirror: near-real-time TDnet disclosure mirror via Kabutan
# (kabutan.jp/disclosures/), free, no API key. Confirmed live 2026-09-28:
# a global (not per-ticker) feed, newest-first, paginated (?page=N, ~16
# rows/page) - NOT the per-company page IRBANK uses. A single Japan trading
# day spans roughly 30 pages of this feed across ALL companies (confirmed
# live: page 30 was still 2026-09-28 before rolling into 2026-09-26, the
# prior trading day - 09-27 was a Sunday). This is DELIBERATELY daily-only,
# not backfilled - IRBANK already covers 5-year forecast-revision history;
# Kabutan's value here is same-day corroboration and disclosure TYPES
# IRBANK's own /tdnet page mixes in without a clean category filter
# (confirmed live category params exist: kubun=kgh 決算/results, j 自己株式
# 取得/buyback, e エクイティ/equity, t 追加・訂正/corrections, s その他/
# other - not yet used here, since the daily page-walk already returns
# every category and client-side filtering to JAPAN_TICKER_UNIVERSE is
# cheap regardless of category).
#
# Table structure (class="stock_table"), confirmed live: code, company
# name, market segment (東証Ｇ/Ｐ etc, sometimes blank for some listings -
# confirmed live, e.g. 171A), category, title + PDF link
# (kabutan.jp/disclosures/pdf/{date}/{doc_id}/ - the same underlying TDnet
# PDF IRBANK also mirrors, giving a cross-check opportunity later), and an
# ISO 8601 timestamp with timezone in a <time datetime="..."> attribute -
# used directly rather than parsing the co-located display text
# ("26/09/28 17:53"), which is redundant and more failure-prone to parse.
#
# Dedup: synthetic url "kabutan-tdnet://{code}/{doc_id}" (doc_id parsed out
# of the PDF url's own path) - global uq_articles_url index handles dedup,
# same convention as every other Japan source_type.
#
# Fetcher (source_type: "kabutan_tdnet_mirror") implemented in pipeline.py.
JAPAN_KABUTAN_TDNET_MIRROR_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "kabutan_tdnet_mirror:japan_market_signal",
    "name": "Kabutan TDnet Disclosure Mirror (Japan)",
    "source_type": "kabutan_tdnet_mirror",
    "frequency_name": "daily",
    "description": (
        "Same-day TDnet disclosure mirror for Japan AI-supply-chain"
        " companies via Kabutan's free, global disclosure feed"
        " (kabutan.jp/disclosures/) - walked page-by-page until the date"
        " rolls over to the prior trading day, filtered client-side to"
        " JAPAN_TICKER_UNIVERSE. Daily-only by design, not backfilled -"
        " irbank_financials already owns 5-year forecast-revision history;"
        " this source corroborates same-day disclosures and covers types"
        " (buybacks, equity, corrections) outside that source's scope."
    ),
    "config": {
        "codes": [t["code"] for t in JAPAN_TICKER_UNIVERSE],
    },
}

# kabutan_buyback: buyback ANNOUNCEMENT trigger via the same Kabutan
# disclosure feed as kabutan_tdnet_mirror, scoped server-side to kubun=j
# (自社株取得/buyback) - confirmed live this category param works exactly
# as documented (every returned row was 自社株取得, none of any other
# category) and spans far fewer pages per day than the unfiltered feed
# (confirmed live: one page covered 3 trading days of buyback-only
# filings, vs. ~30 pages/day unfiltered). Shares one function
# (_fetch_kabutan_disclosures in pipeline.py) with kabutan_tdnet_mirror via
# the kubun parameter, not a near-duplicate fetcher.
#
# This is the ANNOUNCEMENT-trigger role in the buyback signal design (a
# company just filed something buyback-related), distinct from
# irbank_buyback's ONGOING status role (the actual cumulative
# shares/amount/% figures) - cross-checking the two lets a downstream
# classifier confirm a new Kabutan buyback disclosure lines up with a
# corresponding new entry on IRBANK's own buyback page, same idea as
# EDINET/IRBANK corroboration above.
#
# Fetcher (source_type: "kabutan_buyback") implemented in pipeline.py.
JAPAN_KABUTAN_BUYBACK_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "kabutan_buyback:japan_market_signal",
    "name": "Kabutan Buyback Disclosures (Japan)",
    "source_type": "kabutan_buyback",
    "frequency_name": "daily",
    "description": (
        "Same-day buyback-related disclosure announcements for Japan"
        " AI-supply-chain companies, via Kabutan's disclosure feed scoped"
        " server-side to kubun=j (自社株取得). The announcement-trigger"
        " signal (a buyback-related notice was just filed), distinct from"
        " irbank_buyback's ongoing structured status figures."
    ),
    "config": {
        "codes": [t["code"] for t in JAPAN_TICKER_UNIVERSE],
    },
}

# edinet_filing: 5% large-shareholding filings (docTypeCode 350) via
# EDINET's official REST API (api.edinet-fsa.go.jp) - free, but requires a
# registered Subscription-Key (EDINET_API_KEY, already in .env), confirmed
# live 2026-09-27/28: v2 returns a hard 401 with no key at all, unlike
# IRBANK/Kabutan above.
#
# EDINET's documents.json metadata NEVER carries the actual percentage held
# (confirmed live: secCode is null on every 350-type row, and no ratio
# field exists in the list endpoint at all) - the real value is only
# inside the filing's own CSV export (type=5), fetched per-filing. Element
# ID confirmed live: "jplvh_cor:HoldingRatioOfShareCertificatesEtc".
#
# issuerEdinetCode (the company whose shares were bought, NOT edinetCode -
# the filer who bought them) is the join key back to our ticker universe -
# but EDINET's own API gives no ticker<->issuerEdinetCode mapping
# (secCode null on this doctype, no lookup endpoint). Resolved instead via
# EDINET's own bulk code-list download (disclosure2dl.edinet-fsa.go.jp/.../
# codelist/Edinetcode.zip, Shift-JIS CSV, 証券コード=securities code field)
# - see edinet_code.py's resolve_edinet_codes(), same process-lifetime-
# cache pattern as dart_corp_code.py's resolve_corp_codes(). This code list
# is also what CONFIRMED Shinko Electric (6967) as delisted - see
# JAPAN_TICKER_UNIVERSE's own comment above.
#
# Fetcher (source_type: "edinet_filing") implemented in pipeline.py.
JAPAN_EDINET_FILING_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "edinet_filing:japan_market_signal",
    "name": "EDINET Large Shareholding Filings (Japan)",
    "source_type": "edinet_filing",
    "frequency_name": "daily",
    "description": (
        "5%-large-shareholding filings (docTypeCode 350) for Japan"
        " AI-supply-chain companies, via EDINET's official documents.json"
        " list + per-filing CSV export (for the holding-ratio percentage,"
        " never present in the list metadata itself). Requires"
        " EDINET_API_KEY."
    ),
    "config": {
        "codes": [t["code"] for t in JAPAN_TICKER_UNIVERSE],
    },
}

# edinet_buyback_status: 自己株券買付状況報告書 (report on status of share
# repurchase, docTypeCode 220/230) via the SAME EDINET fetch infrastructure
# as edinet_filing above (see pipeline.py's _fetch_edinet_by_mode) - a
# config variant, not a separate near-duplicate fetcher, mirroring how
# Taiwan's TWSE/TPEx material-announcement fetcher shares one function via
# a key_map rather than being two functions.
#
# UNLIKE edinet_filing, this doctype's list-metadata carries secCode/
# edinetCode DIRECTLY (issuerEdinetCode is null - a company reporting its
# OWN buyback has no separate "issuer" field, confirmed live against a
# real filing, Godo Steel 2026-09-15), and its CSV export has NO discrete
# numeric field for shares/amount/progress at all - every figure lives
# inside a single free-text XBRL element (confirmed live:
# AcquisitionsByResolutionOfBoardOfDirectorsMeetingTextBlock, a long
# unstructured Japanese paragraph with dates, share counts, and yen amounts
# run together). Per explicit decision: store the filing's existence and
# that raw text block AS-IS, do not attempt to parse it into numeric
# fields - IRBANK's own /{code}/buyback page (source_type irbank_buyback,
# not yet built) already has this as genuinely clean structured fields and
# is the intended primary source for buyback NUMBERS; this source's role
# is same-day EDINET corroboration only.
#
# Fetcher (source_type: "edinet_buyback_status") implemented in pipeline.py.
JAPAN_EDINET_BUYBACK_STATUS_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "edinet_buyback_status:japan_market_signal",
    "name": "EDINET Buyback Status Reports (Japan)",
    "source_type": "edinet_buyback_status",
    "frequency_name": "daily",
    "description": (
        "Share-repurchase status reports (docTypeCode 220/230) for Japan"
        " AI-supply-chain companies, via EDINET's official documents.json"
        " list. The filing's raw acquisition text block is stored as-is"
        " (not parsed into numeric fields - no discrete field exists for"
        " this doctype); irbank_buyback is the primary source for"
        " structured buyback figures. Requires EDINET_API_KEY."
    ),
    "config": {
        "codes": [t["code"] for t in JAPAN_TICKER_UNIVERSE],
    },
}

# edinet_extraordinary_report: 臨時報告書 (extraordinary report, docTypeCode
# 180/190) via the SAME EDINET fetch infrastructure as edinet_filing/
# edinet_buyback_status above (_fetch_edinet_by_mode, mode="extraordinary")
# - a config variant, not a separate fetcher. The design spec's Step 3
# explicitly asks for "5% shareholding filings AND extraordinary reports"
# - this doctype was missed in the initial build (only 350/220/230 were
# implemented) and added after a direct gap check against the spec.
#
# Confirmed live 2026-09-28 via a real filing (Shin-Etsu Chemical, 4063, a
# stock-option/warrant issuance to directors and employees) - joined via
# edinetCode DIRECTLY (issuerEdinetCode was null, same as buyback status -
# a company reporting its OWN extraordinary event has no separate "issuer"
# field). "Extraordinary report" is a broad, varied category - confirmed
# live it can mean anything from a stock-option grant to M&A to a
# disaster disclosure, distinguished by documents.json's own
# currentReportReason legal-clause code (already present in list
# metadata, no extra fetch needed for that). The filing's free-text reason
# (jpcrp-esr_cor:ReasonForFilingTextBlock) is stored as-is, same
# no-numeric-parsing decision as edinet_buyback_status, since this doctype
# varies too much for one consistent numeric extraction to generalize.
#
# Fetcher (source_type: "edinet_extraordinary_report") implemented in
# pipeline.py.
JAPAN_EDINET_EXTRAORDINARY_REPORT_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "edinet_extraordinary_report:japan_market_signal",
    "name": "EDINET Extraordinary Reports (Japan)",
    "source_type": "edinet_extraordinary_report",
    "frequency_name": "daily",
    "description": (
        "Extraordinary reports (docTypeCode 180/190) for Japan"
        " AI-supply-chain companies, via EDINET's official documents.json"
        " list - a broad, varied category (M&A, executive/capital"
        " changes, disasters, stock-option grants, etc.), distinguished by"
        " the filing's own currentReportReason legal-clause code. The"
        " filing's free-text reason is stored as-is, not parsed into"
        " numeric fields (this doctype varies too much for one consistent"
        " extraction). Requires EDINET_API_KEY."
    ),
    "config": {
        "codes": [t["code"] for t in JAPAN_TICKER_UNIVERSE],
    },
}

# seaj_billings: monthly Japan-based semiconductor equipment billings via
# SEAJ's free English press-release PDF (seaj.or.jp/english/statistics) -
# free, no API key, and UNLIKE every other Japan source_type, no
# per-company scoping at all (this is a single industry-wide figure).
#
# Confirmed live 2026-09-28: the free release is a 3-MONTH MOVING AVERAGE,
# never a true single-month figure - SEAJ's own PDF states this explicitly,
# and this is confirmed (via earlier research) to be the ceiling of what
# ANY free source publishes for this data, including SEMI's own WWSEMS
# report and every third-party aggregator checked - there is no free path
# to the underlying true monthly number anywhere. Section 6.4's
# classification rule for this signal is designed around this limitation
# (compare the 3-month-average series against its own 12-month average,
# use SEAJ's own published YoY% directly) rather than assuming a true
# single-month figure exists - not implemented here, since classification
# is signal-detection-agent's job.
#
# Fetcher (source_type: "seaj_billings") implemented in pipeline.py.
JAPAN_SEAJ_BILLINGS_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "seaj_billings:japan_market_signal",
    "name": "SEAJ Equipment Billings (Japan)",
    "source_type": "seaj_billings",
    "frequency_name": "monthly",
    "description": (
        "Monthly Japan-based semiconductor equipment billings (3-month"
        " moving average, not a true single-month figure - no free source"
        " publishes that), via SEAJ's free English press-release PDF."
        " Industry-wide, not scoped to any single company."
    ),
    "config": {},
}

# monoist_capex: capacity/investment news via MONOist's free 工場ニュース
# (Factory News) series listing (monoist.itmedia.co.jp/mn/series/1464/) -
# free, no API key, no per-company query capability at all (unlike
# GDELT). Confirmed live 2026-09-28: a single page load already returns
# ~950 real articles spanning several months, filtered CLIENT-SIDE by
# native_name substring match, same approach as Taiwan's GDELT
# title-filter - 74 of those ~950 real articles matched our 20-company
# universe on this same test, with genuinely relevant capex signal (e.g.
# Resonac's 15bn-yen HDD-media investment cited as generative-AI-demand
# driven, Shin-Etsu's 530bn-yen US PVC expansion, Fujikura's 45bn-yen
# next-gen optical fiber plant).
#
# This is prose, not structured filing data (unlike every other Japan
# source_type) - amounts/company/plant details live in free text, not
# discrete fields; extraction of those specifics into structured signal
# fields is signal-detection-agent's job, not news-retrieval's - this
# fetcher stores title + description as fetched, same fetch/store-only
# boundary every other source_type in this domain follows.
#
# Fetcher (source_type: "monoist_capex") implemented in pipeline.py.
JAPAN_MONOIST_CAPEX_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "monoist_capex:japan_market_signal",
    "name": "MONOist Factory News (Japan)",
    "source_type": "monoist_capex",
    "frequency_name": "daily",
    "description": (
        "Capacity/investment news for Japan AI-supply-chain companies, via"
        " MONOist's free 工場ニュース (Factory News) series - a general"
        " manufacturing-news feed filtered client-side by company"
        " native_name, not a per-company query (no such capability exists"
        " for this source)."
    ),
    "config": {
        # Resolved live from research-universe at fetch time
        # (get_tracked_company_universe) rather than seeded here -
        # see the note on irbank_financials above for why a seeded
        # copy cannot be kept correct.
    },
}

# press_jp: free Nikkei-equivalent wire coverage. Nikkei itself stays
# paywalled by design (per this domain's spec - headline+timestamp only
# would still mean scraping a paywalled site's teaser, not attempted).
# Free substitutes were checked live 2026-09-28. Two sub-sources are
# usable, merged into one source_type:
#   - Reuters Japan: blocked outright by an active DataDome anti-bot
#     challenge on every path tried (article pages, RSS guesses all 401) -
#     no path around this without a headless browser, out of scope.
#   - Kyodo News English: reachable, but confirmed to have NO business/
#     economy/tech category at all in its site nav (only Japan/World/
#     Sports/Arts/Feature/Travel-Tourism/Sumo/Asian Games/Podcast).
#   - Jiji Press: a free, unauthenticated economy-category listing
#     (jiji.com/jc/list?g=eco), confirmed live to return ~50 real
#     headline+timestamp entries per page load. BROAD general-economy
#     feed, not semiconductor-specific - a real, accepted limitation
#     (confirmed live it can return zero matches against
#     JAPAN_TICKER_UNIVERSE on a given page load, same legitimate
#     zero-match behavior as kabutan_buyback).
#   - newswitch.jp (Nikkan Kogyo Shimbun's free site): a dedicated 半導体
#     (semiconductor) keyword-tag page (newswitch.jp/keyword/detail/573) -
#     genuinely more targeted than Jiji, semiconductor-specific by
#     construction. Confirmed live to find real matches Jiji's broad feed
#     does not (e.g. Resonac's 12-inch SiC substrate development). No
#     timestamp on the listing itself - a two-step fetch (list, then each
#     matched article's own page for its date) is needed, same
#     "cheap discovery, expensive fetch only for matches" shape as
#     irbank_financials.
#
# Both sub-sources are still an accepted, real gap versus Nikkei's own
# scoop lead time - this domain's own design notes are explicit that no
# free source reproduces that specificity.
#
# Headline + timestamp only, body always None - this is a deliberate
# copyright/scope boundary (same treatment as Nikkei itself), not a
# technical limitation of what could be scraped.
#
# Fetcher (source_type: "press_jp") implemented in pipeline.py
# (_fetch_press_jp_jiji + _fetch_press_jp_newswitch, merged by
# _fetch_press_jp).
JAPAN_PRESS_JP_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "press_jp:japan_market_signal",
    "name": "Japan Press Coverage - Jiji Press + Newswitch (Japan)",
    "source_type": "press_jp",
    "frequency_name": "daily",
    "description": (
        "Free Nikkei-equivalent wire coverage for Japan AI-supply-chain"
        " companies, via Jiji Press's free economy-category listing"
        " (broad, general-economy) and newswitch.jp's free semiconductor"
        " keyword-tag page (targeted, semiconductor-specific) -"
        " headline+timestamp only, no article body. Reuters Japan and"
        " Kyodo News English were checked and confirmed unusable"
        " (Reuters: anti-bot blocked; Kyodo: no business/economy category"
        " exists)."
    ),
    "config": {
        # Resolved live from research-universe at fetch time
        # (get_tracked_company_universe) rather than seeded here -
        # see the note on irbank_financials above for why a seeded
        # copy cannot be kept correct.
    },
}

# press_jp_english_check: the English-coverage mirror the design spec's own
# Step 5 explicitly asks for ("mirror every search in English recording
# whether anything was found and when" - "the English mirror makes the
# timing claim testable rather than assumed"). Independent per-company
# search, not a per-article cross-reference (same shape as Korea's
# KOREA_GDELT_ENGLISH_SOURCE) - run once per company at fetch time,
# separate from any specific press_jp/monoist_capex article already found.
#
# DuckDuckGo (via the free `ddgs` package, no API key) - the same provider
# signal-detection-agent's web_search.py already uses for a DIFFERENT job
# (classification-time entity context for already-scored signals); this
# source_type is a distinct, earlier-in-the-pipeline use of the same free
# tool, not a duplication of that job. Confirmed live 2026-09-28 real,
# relevant results for these company names (e.g. a real Reuters article on
# Advantest's forecast revision, dated 2025-07-29).
#
# This service's job is only to fetch and store what DuckDuckGo returns -
# the actual timing comparison (was there English coverage, and was it
# before/after the Japanese-language disclosure) is signal-detection-
# agent's job, computed from these rows plus press_jp/monoist_capex/
# irbank_financials's rows, not done here.
#
# Fetcher (source_type: "press_jp_english_check") implemented in
# pipeline.py.
# A bare `company` name is used as the DuckDuckGo News query for every
# ticker except these two - confirmed live (2026-09-28) that "Disco" and
# "Towa" collide with common English words/names (nightclub/music culture
# and unrelated brand usage respectively), returning 0/5 and 0/5 relevant
# results even within a tight 1-day window. Appending "Corporation
# semiconductor" was confirmed live to fix this (Disco: 0/5 -> 5/5 relevant;
# Towa: 0/5 -> 4/5, the remaining miss being an unrelated company, "Tower
# Semiconductor", not generic noise) without degrading results for an
# already-clean name (Advantest: still 5/5 relevant with the same suffix
# appended) - so the suffix is applied only to the two confirmed-colliding
# tickers, not universally.
_JAPAN_ENGLISH_CHECK_SEARCH_QUERY_OVERRIDES: dict[str, str] = {
    "6146": "Disco Corporation semiconductor",
    "6315": "Towa Corporation semiconductor",
}

JAPAN_PRESS_JP_ENGLISH_CHECK_SOURCE: dict[str, Any] = {
    "domain_slug": "japan_market_signal",
    "url": "press_jp_english_check:japan_market_signal",
    "name": "Japan Press English-Coverage Check (DuckDuckGo)",
    "source_type": "press_jp_english_check",
    "frequency_name": "daily",
    "description": (
        "English-language coverage check for Japan AI-supply-chain"
        " companies, via DuckDuckGo News search (free, no API key) -"
        " headline+timestamp only, no article body. Exists solely to test"
        " whether/when English coverage exists, for comparison against"
        " press_jp/monoist_capex/irbank_financials's Japanese-language"
        " sources - not a primary signal source itself."
    ),
    "config": {
        # Resolved live from research-universe at fetch time
        # (get_tracked_company_universe) rather than seeded here -
        # see the note on irbank_financials above for why a seeded
        # copy cannot be kept correct.
    },
}

# ===========================================================================
# China Market Signal
# ===========================================================================
#
# Hardcoded here, NOT read from research-universe, unlike Japan. That is a
# deliberate interim choice: research-universe currently files these
# companies under two different `country` values ("China" for Hygon,
# Loongson and Inspur; "China/Hong Kong" for Naura, SMIC, Cambricon and the
# rest) with no rule distinguishing them, and Alibaba and Tencent are filed
# as "United States" with ADR tickers. `get_tracked_company_universe()`
# matches `country` exactly, so reading live today would return an arbitrary
# subset. Migrating is a separate decision about what that column means -
# see news-retrieval/CLAUDE.md.
#
# `company` is the exact `company_name` already stored in
# research-universe for the fifteen that exist there (hence "Amec", not
# "AMEC"; "Lenovo", not "Lenovo Group"; "Inspur Electronic Information",
# not "Inspur Information"), so that a later migration needs no name
# reconciliation. Five - ACM Research Shanghai, Piotech, Hwatsing, China
# Northern Rare Earth, JL MAG - are not in research-universe at all.
#
# `native_name` is the exchange's OWN registered short name (`zwjc` in
# cninfo's szse_stock.json), confirmed live for all 16 mainland codes on
# 2026-10-05 - not the full legal name. This is the same finding Japan made
# the hard way (Resonac's legal レゾナック・ホールディングス matched zero real
# articles vs. 5 for レゾナック): Chinese press writes 中芯国际, never
# 中芯国际集成电路制造. `aliases` carries the other written forms.
#
# `code` is TEXT throughout - 002371, 000977, 000725 and 0700 all lose
# their leading zeros as integers, the same reason Taiwan and Japan store
# theirs as text.
#
# Exchange-internal identifiers are NOT stored here. cninfo's `org_id`
# and HKEX's `stockId` are both required to query their APIs at all (a
# bare code returns zero rows / an empty 200 rather than an error), and
# both were hardcoded here until 2026-10-06. They are now resolved per
# run by `china_code.py` - cninfo from its own bulk new/data/
# szse_stock.json, HKEX from its autocomplete - the same pattern Japan
# (edinet_code.py), Korea (dart_corp_code.py) and the US (SEC
# company_tickers.json) already use.
#
# This matters as the universe grows: adding a company here needs only
# its `code` (and `hk_code` if it is HK-listed). Both identifiers are
# looked up automatically, verified live for codes never hardcoded -
# Xiaomi 01810 -> 190371, JD 09618 -> 1000042149. A hand-copied id
# would otherwise be one transcription error away from a source that
# silently returns nothing.
#
# `exchange` was removed at the same time: it was never read. cninfo's
# column is derived from the code prefix (_cninfo_column: 0/3 -> szse,
# else sse), and nothing else consulted the field.
CHINA_TICKER_UNIVERSE: list[dict[str, Any]] = [
    # --- Equipment: the substitution front line ---------------------------
    {"code": "002371", "company": "Naura Technology", "native_name": "北方华创",
     "aliases": ["北方华创科技集团", "NAURA"], "fiscal_year_end": "12-31"},
    {"code": "688012", "company": "Amec", "native_name": "中微公司",
     "aliases": ["中微半导体设备", "AMEC"], "fiscal_year_end": "12-31"},
    {"code": "688082", "company": "ACM Research Shanghai",
     "native_name": "盛美上海", "aliases": ["盛美半导体"],
     "fiscal_year_end": "12-31"},
    {"code": "688072", "company": "Piotech", "native_name": "拓荆科技",
     "aliases": ["拓荆"], "fiscal_year_end": "12-31"},
    {"code": "688120", "company": "Hwatsing Technology",
     "native_name": "华海清科", "aliases": [], "fiscal_year_end": "12-31"},
    # --- Foundry and memory ----------------------------------------------
    # SMIC and Hua Hong are dual-listed; the mainland code is primary here
    # because cninfo answers to it. Their HK codes (0981, 1347) are what
    # hkex_filing queries - carried as `hk_code`, not a separate row.
    {"code": "688981", "company": "SMIC", "native_name": "中芯国际",
     "aliases": ["中芯国际集成电路制造", "中芯"], "hk_code": "00981", "fiscal_year_end": "12-31"},
    {"code": "688347", "company": "Hua Hong Semiconductor",
     "native_name": "华虹宏力", "aliases": ["华虹半导体", "华虹公司", "华虹"],
     "hk_code": "01347",
     "fiscal_year_end": "12-31"},
    {"code": "603986", "company": "GigaDevice", "native_name": "兆易创新",
     "aliases": ["兆易"], "fiscal_year_end": "12-31"},
    # --- Domestic accelerators: the Nvidia substitution read --------------
    {"code": "688256", "company": "Cambricon", "native_name": "寒武纪",
     "aliases": ["中科寒武纪科技", "寒武纪科技"], "fiscal_year_end": "12-31"},
    {"code": "688041", "company": "Hygon Information Technology",
     "native_name": "海光信息", "aliases": ["海光信息技术", "海光"],
     "fiscal_year_end": "12-31"},
    {"code": "688047", "company": "Loongson Technology",
     "native_name": "龙芯中科", "aliases": ["龙芯中科技术", "龙芯"],
     "fiscal_year_end": "12-31"},
    # --- Servers, systems and components ----------------------------------
    {"code": "000977", "company": "Inspur Electronic Information",
     "native_name": "浪潮信息", "aliases": ["浪潮电子信息产业"],
     "exclude_terms": ["浪潮软件", "浪潮数字"], "fiscal_year_end": "12-31"},
    {"code": "002475", "company": "Luxshare Precision",
     "native_name": "立讯精密", "aliases": ["立讯精密工业", "立讯"],
     "fiscal_year_end": "12-31"},
    # BOE's registered short name carries its share-class suffix ("京东方A");
    # press writes 京东方 without it, so both are needed.
    {"code": "000725", "company": "BOE Technology", "native_name": "京东方A",
     "aliases": ["京东方", "京东方科技集团", "BOE"], "fiscal_year_end": "12-31"},
    # --- Materials and the retaliation lever -------------------------------
    {"code": "600111", "company": "China Northern Rare Earth",
     "native_name": "北方稀土", "aliases": ["中国北方稀土"],
     "exclude_terms": ["包钢股份"], "fiscal_year_end": "12-31"},
    {"code": "300748", "company": "JL MAG Rare-Earth",
     "native_name": "金力永磁", "aliases": ["金力永磁科技"], "fiscal_year_end": "12-31"},
    # --- Hong Kong-listed only (no mainland code, no cninfo org_id) --------
    {"code": "00992", "company": "Lenovo", "native_name": "联想集团",
     "aliases": ["联想", "Lenovo"], "hk_code": "00992", "fiscal_year_end": "03-31"},
    {"code": "09988", "company": "Alibaba Group", "native_name": "阿里巴巴",
     "aliases": ["阿里巴巴集团", "阿里", "阿里云"], "hk_code": "09988", "fiscal_year_end": "03-31"},
    {"code": "00700", "company": "Tencent", "native_name": "腾讯控股",
     "aliases": ["腾讯", "腾讯云"], "hk_code": "00700", "fiscal_year_end": "12-31"},
    {"code": "09888", "company": "Baidu", "native_name": "百度",
     "aliases": ["百度智能云"], "hk_code": "09888", "fiscal_year_end": "12-31"},
]

# Every China source row carries `source_outlet_type` in its config, and
# every article it produces carries the same value in metadata. Three
# values, and the distinction is load-bearing rather than decorative: a
# state outlet is authoritative for WHAT POLICY IS but is not independent
# confirmation of a commercial fact, and conflating the two would let a
# policy-signalling article count as corroboration of a company claim.
# Set at fetch time because it is a property of the source, which the
# classifier cannot recover from the article text.
#   official       - a ministry/regulator publishing its own action
#   state_press    - state news agency relaying policy
#   commercial_press - a commercial outlet reporting

# China policy sources. Four separate ministry/regulator listing pages,
# each its own source_type because each publishes a different KIND of
# action and the downstream classifier rules differ per kind - not
# because the HTML differs (it does, but that is handled in one parser).
#
# Confirmed live 2026-10-05 against all four: the announcement list IS in
# the initial HTML for MOFCOM, SAMR and CAC (no JS execution needed), and
# article bodies render as plain text. MIIT is the exception - its listing
# page returns a 2,048-byte shell with ZERO anchors and builds the list
# client-side, so its listing is fetched from the same CMS's article pages
# rather than the index. MOFCOM's English site (english.mofcom.gov.cn)
# refuses a bare request and needs full browser headers; the Chinese site
# does not, which is why the Chinese original is the primary fetch and the
# English version is a later cross-check rather than the source of record.
CHINA_POLICY_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "china_market_signal",
        "url": "mofcom_policy:china_market_signal",
        "name": "MOFCOM Policy Announcements (China)",
        "source_type": "mofcom_policy",
        "frequency_name": "daily",
        "description": (
            "Ministry of Commerce policy releases and announcements"
            " (公告) - export licensing changes, trade countermeasures,"
            " anti-dumping rulings and control-list revisions. The single"
            " highest-impact China source: these move US prices within"
            " the hour and nothing else in this programme covers the"
            " Chinese side of export control. Listing confirmed present"
            " in the initial HTML; article bodies render as plain text."
        ),
        "config": {
            "source_outlet_type": "official",
            "issuing_body": "MOFCOM",
            "listing_urls": [
                "http://www.mofcom.gov.cn/zcfb/index.html",
                "http://www.mofcom.gov.cn/zcfb/dwmygl/index.html",
                "http://www.mofcom.gov.cn/zcfb/gpmy/index.html",
            ],
        },
    },
    {
        "domain_slug": "china_market_signal",
        "url": "miit_policy:china_market_signal",
        "name": "MIIT Policy Documents (China)",
        "source_type": "miit_policy",
        "frequency_name": "daily",
        "description": (
            "Ministry of Industry and Information Technology policy"
            " documents and industry news - industrial policy, standards,"
            " and catalogues of encouraged/restricted technologies. Its"
            " listing page is a client-side-rendered shell (confirmed"
            " live: 2,048 bytes, zero anchors), so the CMS article paths"
            " are walked directly instead of the index."
        ),
        "config": {
            "source_outlet_type": "official",
            "issuing_body": "MIIT",
            # Same shape as SAMR: the section pages are client-side
            # shells (/zwgk/zcwj/wjfb/ returns ~2KB with zero anchors)
            # and the home page is the only server-rendered listing.
            # Note MIIT writes art_<hash>.htm, not .html - the one
            # character that made this source return nothing until the
            # link pattern was loosened.
            "listing_urls": [
                "https://www.miit.gov.cn/",
            ],
        },
    },
    {
        "domain_slug": "china_market_signal",
        "url": "samr_action:china_market_signal",
        "name": "SAMR Regulatory Actions (China)",
        "source_type": "samr_action",
        "frequency_name": "daily",
        "description": (
            "State Administration for Market Regulation - antitrust"
            " investigations and penalty decisions, including those"
            " naming foreign companies. Feeds the C7 rule"
            " (a named US company is always at least a weak signal)."
        ),
        "config": {
            "source_outlet_type": "official",
            "issuing_body": "SAMR",
            # The section subpages (/xw/mtjj/, /xw/zj/) return a ~3KB
            # shell with zero article anchors - confirmed live; they
            # build their lists client-side. The HOME page carries the
            # same stories as real server-rendered links, so it is the
            # listing used instead of the sections.
            "listing_urls": [
                "https://www.samr.gov.cn/",
            ],
        },
    },
    {
        "domain_slug": "china_market_signal",
        "url": "cac_review:china_market_signal",
        "name": "CAC Security Reviews (China)",
        "source_type": "cac_review",
        "frequency_name": "daily",
        "description": (
            "Cyberspace Administration of China - cybersecurity reviews"
            " of foreign technology products. This is the mechanism used"
            " in the 2023 memory procurement case, which is the template"
            " for a single-name US earnings event."
        ),
        "config": {
            "source_outlet_type": "official",
            "issuing_body": "CAC",
            # Its section index pages 404; the home page serves the real
            # dated article links (/YYYY-MM/DD/c_<id>.htm) - confirmed
            # live, including the two 征求意见稿 consultation notices that
            # the binding-vs-non-binding classifier exists to separate.
            "listing_urls": [
                "https://www.cac.gov.cn/",
            ],
        },
    },
    {
        "domain_slug": "china_market_signal",
        "url": "cn_state_press:china_market_signal",
        "name": "Xinhua English (China)",
        "source_type": "cn_state_press",
        "frequency_name": "daily",
        "description": (
            "State news agency, English service - frequently publishes a"
            " policy announcement before the issuing ministry's own site"
            " updates, so it is the authoritative TIMESTAMP for policy."
            " Tagged state_press, never official: authoritative for what"
            " policy is, not independent confirmation of a commercial"
            " claim."
        ),
        "config": {
            "source_outlet_type": "state_press",
            # english.news.cn/business/index.htm is a DEAD page that still
            # returns 200 - its newest article is from 2021 (confirmed
            # live). A fetcher pointed at it would run clean forever and
            # produce nothing current, which is the worst kind of broken.
            # The home page carries live dated links (/YYYYMMDD/<hash>/
            # c.html) and is used instead.
            "listing_urls": [
                "https://english.news.cn/",
            ],
        },
    },
]

# hkex_filing: the six dual-listed/HK-listed companies, in English, from
# HKEX's own disclosure site. Hong Kong listing rules require English
# alongside Chinese, so this is the one company source in the China module
# needing no translation at all - which is why it is built before the
# fourteen mainland-only names.
#
# `hkex_stock_id` is HKEX's own INTERNAL issuer id and is NOT the stock
# code - SMIC's code is 00981 but its id is 7249, Lenovo's 00992 is 2325,
# and Alibaba's 09988 is 1000015694. Confirmed live 2026-10-05, and worth
# stating because passing the stock code as `stockId` returns an empty
# result set with HTTP 200 rather than an error: five of these six
# companies silently returned nothing until the real ids were resolved,
# and Tencent only worked by coincidence (its code 00700 -> 700 happened
# to be a valid, unrelated id). Resolved once from HKEX's own
# search/prefix.do autocomplete and stored, not looked up per run.
CHINA_HKEX_SOURCE: dict[str, Any] = {
    "domain_slug": "china_market_signal",
    "url": "hkex_filing:china_market_signal",
    "name": "HKEX Filings (China)",
    "source_type": "hkex_filing",
    "frequency_name": "daily",
    "description": (
        "Company filings for the Hong Kong-listed names in the China"
        " universe (SMIC, Hua Hong, Lenovo, Alibaba, Tencent, Baidu) via"
        " hkexnews.hk - English by listing requirement, so no translation"
        " layer is needed for any of them."
    ),
    "config": {
        "source_outlet_type": "official",
        "companies": [
            # hkex_stock_id is NOT carried: it is resolved per run from
            # HKEX's own autocomplete (china_code.resolve_hkex_stock_ids),
            # so a company added here needs only its listing code.
            {"code": t["code"], "company": t["company"],
             "native_name": t["native_name"], "hk_code": t["hk_code"]}
            for t in CHINA_TICKER_UNIVERSE if t.get("hk_code")
        ],
    },
}

# cninfo_filing: the officially designated disclosure site for every
# Shanghai- and Shenzhen-listed company. Confirmed live 2026-10-05 against
# all 16 mainland codes.
#
# Two findings that shape the fetcher, both confirmed live:
#   1. The announcement query needs `stock` = "CODE,ORGID". A bare code
#      returns zero rows with totalAnnouncement=0 - NOT an error, which is
#      what makes this worth stating: it fails silently and looks like a
#      company with no filings. org_id is stored per company above.
#   2. `column` must be "szse" for Shenzhen codes (000/002/300) and "sse"
#      for Shanghai ones (600/603/688). Sending the wrong one also
#      returns zero rows rather than erroring.
# Date scoping works via `seDate` as "YYYY-MM-DD~YYYY-MM-DD", so days_back
# is applied server-side rather than by walking and cutting off.
CHINA_CNINFO_SOURCE: dict[str, Any] = {
    "domain_slug": "china_market_signal",
    "url": "cninfo_filing:china_market_signal",
    "name": "CNINFO Filings (China)",
    "source_type": "cninfo_filing",
    "frequency_name": "daily",
    "description": (
        "Announcements and periodic reports for the mainland-listed"
        " China universe companies, via cninfo.com.cn - the officially"
        " designated disclosure site for the Shanghai and Shenzhen"
        " exchanges. Stores announcement metadata and the filing PDF"
        " link; the substitution signal (C2) lives in these filings."
    ),
    "config": {
        "source_outlet_type": "official",
        "companies": [
            # org_id is NOT carried: it is resolved per run from cninfo's
            # own bulk stock list (china_code.resolve_cninfo_org_ids), so
            # a company added here needs only its code.
            #
            # Membership is "has a mainland listing", which is exactly
            # "code != hk_code": SMIC and Hua Hong are dual-listed and
            # belong in BOTH this list and the HKEX one (688981/00981,
            # 688347/01347), while Lenovo, Alibaba, Tencent and Baidu
            # store their HK code in `code` because they have no mainland
            # listing at all.
            {"code": t["code"], "company": t["company"],
             "native_name": t["native_name"]}
            for t in CHINA_TICKER_UNIVERSE
            if t.get("code") and t["code"] != t.get("hk_code")
        ],
    },
}

# press_cn: Chinese-language commercial trade and financial press.
# GDELT is NOT used for this domain (explicit decision): Taiwan's GDELT
# path is scoped sourcelang:chinese sourcecountry:TW and covers
# traditional-character Taiwanese press, not simplified mainland outlets.
#
# Outlet selection is what the probe allowed, not a wish list. Confirmed
# live 2026-10-05: ITHome (691 anchors, 103 dated rows), Jiemian (169
# anchors, dated rows in the markup) and EEFocus (391 anchors) all return
# a parseable listing. Yicai and STCN serve a captcha challenge to an
# automated client and semiinsights.com refuses the connection outright -
# all three are therefore excluded rather than retried, on the same
# principle Korea applied to its blocked customs API.
CHINA_PRESS_SOURCE: dict[str, Any] = {
    "domain_slug": "china_market_signal",
    "url": "press_cn:china_market_signal",
    "name": "China Trade and Financial Press",
    "source_type": "press_cn",
    "frequency_name": "daily",
    "description": (
        "Chinese-language commercial press (ITHome, Jiemian, EEFocus),"
        " filtered client-side to the China universe by native_name or"
        " alias - none of these outlets has a per-company query. Tagged"
        " commercial_press to keep it separable from state_press: the"
        " two answer different questions and must not corroborate each"
        " other."
    ),
    "config": {
        "source_outlet_type": "commercial_press",
        # RSS where an outlet publishes a usable one. ITHome's feed
        # carries 60 dated entries with real editorial summaries against
        # 50 undated headlines from its HTML listing - strictly more,
        # and the summary removes a per-article body fetch, so the HTML
        # listing is dropped rather than kept alongside. Every other
        # Chinese outlet tried (Jiemian, EEFocus, Yicai, STCN, CLS,
        # 36kr, Sina, Huxiu, laoyaoba, eet-china, semiinsights, tmtpost)
        # returns 404, an empty feed, or is unreachable.
        "rss_urls": {
            "https://www.ithome.com/rss/": "ithome",
        },
        "listing_urls": [
            "https://www.jiemian.com/lists/116.html",
            "https://www.eefocus.com/",
            # ijiwei (集微网) newsflash - added for coverage after the
            # first three were measured at ~150 headlines per run
            # between them. This one carries 92 dated semiconductor
            # trade items on its own, and is the densest China-specific
            # source found: its homepage times out, its /news and /n
            # paths return a 1.8KB shell, and only /kuaixun serves a
            # real server-rendered list.
            "https://www.ijiwei.com/kuaixun",
        ],
        "companies": [
            {"code": t["code"], "company": t["company"],
             "native_name": t["native_name"],
             "aliases": t.get("aliases", []),
             "exclude_terms": t.get("exclude_terms", [])}
            for t in CHINA_TICKER_UNIVERSE
        ],
    },
}

# Companies whose stored name is not a usable English search query.
# Confirmed live 2026-10-05 against real search results, not guessed:
# "Amec" returned a Spanish article about an unrelated firm (there is a
# large UK engineering company of that name), and "Piotech" returned the
# German semiconductor-equipment makers PVA TePla and SUSS MicroTec. Both
# would have silently measured the English-coverage lag of the wrong
# company. Same mechanism Japan uses for Disco and Towa - ordinary words
# that a bare-name search answers wrongly - and the same reason it is
# stored per company: the ambiguity is a fact about the name.
_CHINA_ENGLISH_CHECK_QUERY_OVERRIDES: dict[str, str] = {
    "688012": "AMEC Advanced Micro-Fabrication Equipment China",
    "688072": "Piotech semiconductor China",
    "688120": "Hwatsing Technology CMP China",
    "688082": "ACM Research Shanghai",
    "603986": "GigaDevice Semiconductor",
    "688041": "Hygon Information Technology",
    "000725": "BOE Technology display",
    "600111": "China Northern Rare Earth",
    "300748": "JL MAG Rare-Earth",
    # Lenovo, Alibaba, Tencent, Baidu, SMIC, Cambricon, Naura, Inspur,
    # Luxshare, Hua Hong and Loongson are unambiguous on their own names.
}

# comtrade_china_trade: C6 (trade data), reached from the PARTNER side.
#
# The spec asks for Chinese customs data on integrated-circuit and
# semiconductor-equipment trade. China's own two publishers are both
# unreachable from here (GACC serves a JS cookie challenge, NBS is
# network-blocked), and Comtrade's CHINA-REPORTED monthly series stops at
# 2024-12 - confirmed live across 202501-202504, all count=0.
#
# What IS available, free, keyless and current to within ~2-3 months: what
# the EXPORTERS report shipping TO China. Confirmed live 2026-10-05 for
# 2026-07 - Japan $1.275bn, Netherlands $0.582bn, USA $0.082bn of
# semiconductor equipment (HS 8486).
#
# This is not a workaround, it is arguably the better measurement for the
# question C6 exists to answer. Export controls bite on the EXPORTER's
# side: Washington restricts what American and allied firms may sell, so
# the US/Japan/Netherlands export series IS the control measure, recorded
# by the governments doing the restricting rather than by the country
# being restricted. The live figures show it plainly - US equipment
# exports to China run $77-97m/month while Japan's run $0.7-1.3bn.
#
# Reporters are the three equipment-export-control jurisdictions plus
# Korea (memory). Products are the two the spec names:
#   8542 integrated circuits
#   8486 semiconductor manufacturing equipment
# Flow X (exports) only: an import flow here would be these countries
# buying FROM China, a different question.
#
# Seeded monthly (min_days_back 30): the underlying series updates
# monthly, so a daily --days-back 1 run correctly skips it, same gating
# seaj_billings uses.
CHINA_COMTRADE_SOURCE: dict[str, Any] = {
    "domain_slug": "china_market_signal",
    "url": "comtrade_china_trade:china_market_signal",
    "name": "China Semiconductor Trade (partner-reported)",
    "source_type": "comtrade_china_trade",
    "frequency_name": "monthly",
    "description": (
        "Monthly integrated-circuit (HS 8542) and semiconductor-equipment"
        " (HS 8486) exports TO China, as reported by the United States,"
        " Japan, the Netherlands and South Korea, via the UN Comtrade"
        " preview API (free, no key). Substitutes for Chinese customs"
        " data, which is unreachable - and measures export controls from"
        " the side that imposes them. ~2-3 month lag."
    ),
    "config": {
        "source_outlet_type": "official",
        # UN M49 country codes, which is what Comtrade keys on.
        # The three equipment-export-control jurisdictions. South Korea
        # was tried and dropped: its Comtrade series lags ~9 months
        # (2025-12 is its newest, vs 2026-07 for the other three), so it
        # would contribute only stale rows. Korea's own customs service
        # publishes a current semiconductor export figure and is already
        # fetched by this service under korea_market_signal
        # (kr_customs_export) - that is where a Korea read should come
        # from, not from here.
        "reporters": [
            {"code": "842", "name": "United States"},
            {"code": "392", "name": "Japan"},
            {"code": "528", "name": "Netherlands"},
        ],
        "commodities": [
            {"code": "8542", "label": "Integrated circuits"},
            {"code": "8486", "label": "Semiconductor manufacturing equipment"},
        ],
        "partner_code": "156",  # China
    },
}

# nbs_ic_output: the other half of C6 - China's own monthly integrated-
# circuit PRODUCTION volume, from the National Bureau of Statistics.
#
# Comtrade (above) measures what the world ships INTO China, in dollars.
# This measures what China itself makes, in units. Together they answer
# the two halves of C6: are export controls biting, and is domestic
# output replacing what was restricted. One without the other reads a
# fall in imports as a win for controls when it may simply be
# substitution.
#
# **Only reachable from ECS.** Confirmed live 2026-10-05: stats.gov.cn is
# blocked at the network layer from a local dev machine ("The URL has
# been blocked as per the instructions of the Competent Government
# Authority" - an ISP-level block, not a refusal by the site) but returns
# 127KB of normal HTML from the staging ECS task. A developer running
# this domain locally will see this one source fail and every other one
# work; that is expected, not a bug.
#
# NBS's structured easyquery API (data.stats.gov.cn/easyquery.htm) is WAF-
# blocked even from ECS (`reason:UrlACL`), so the figure is read from the
# monthly industrial-output press release, which carries it as a table
# row: 集成电路（亿块） followed by the month's output and its YoY percent.
# Confirmed live against the August 2026 release - 529 亿块 (52.9bn
# units), +20.6% YoY.
#
# Monthly frequency: NBS publishes this once a month, around the 15th.
CHINA_NBS_IC_OUTPUT_SOURCE: dict[str, Any] = {
    "domain_slug": "china_market_signal",
    "url": "nbs_ic_output:china_market_signal",
    "name": "NBS IC Production (China)",
    "source_type": "nbs_ic_output",
    "frequency_name": "monthly",
    "description": (
        "China's monthly integrated-circuit production volume (集成电路,"
        " 亿块) and its year-on-year change, from the National Bureau of"
        " Statistics' monthly industrial-output release. The domestic-"
        "output half of C6, paired with comtrade_china_trade's import"
        " side. Reachable from ECS only - stats.gov.cn is network-blocked"
        " from local dev machines."
    ),
    "config": {
        "source_outlet_type": "official",
        "issuing_body": "NBS",
        "index_url": "http://www.stats.gov.cn/sj/zxfb/",
    },
}

# press_cn_english_check: the spec's English-coverage-lag requirement -
# how long after a Chinese-language report does English coverage appear,
# measured per company. Same shape as Japan's press_jp_english_check and
# Korea's KOREA_GDELT_ENGLISH_SOURCE: an independent per-company search,
# not a per-article cross-reference.
CHINA_PRESS_ENGLISH_CHECK_SOURCE: dict[str, Any] = {
    "domain_slug": "china_market_signal",
    "url": "press_cn_english_check:china_market_signal",
    "name": "China Press English-Coverage Check",
    "source_type": "press_cn_english_check",
    "frequency_name": "daily",
    "description": (
        "Per-company English-language news search (DuckDuckGo News, free,"
        " no API key) for the China universe - measures how far English"
        " coverage lags the Chinese-language original, the same check"
        " Japan and Korea already run."
    ),
    "config": {
        "source_outlet_type": "commercial_press",
        # The three listed accelerator makers - Cambricon, Hygon,
        # Loongson. Their article bodies ARE fetched, unlike every other
        # company's, because this is the only source that carries their
        # news at all: they file nothing resembling a product milestone
        # (77 cninfo filings over a year, all governance) and press_cn
        # returns zero accelerator articles. The search snippet alone is
        # one truncated sentence, too thin for signal-detection-agent's
        # C4 rule, which needs a stated volume, a named customer or a
        # procurement award. Scoped to three codes rather than all
        # twenty, since this costs one page fetch per surviving article
        # and no other company's rule needs a body here.
        "fetch_body_for_codes": ["688256", "688041", "688047"],
        "companies": [
            {"code": t["code"], "company": t["company"],
             "search_query": _CHINA_ENGLISH_CHECK_QUERY_OVERRIDES.get(
                 t["code"], t["company"])}
            for t in CHINA_TICKER_UNIVERSE
        ],
    },
}

# ---------------------------------------------------------------------------
# Middle East market signal
# ---------------------------------------------------------------------------
# The spec's companies and funds (section 4), hardcoded like Taiwan's
# TAIWAN_TICKER_UNIVERSE pending research-universe, which holds neither the
# unlisted Gulf names (G42, Humain, MGX, the funds, QatarEnergy) nor their US
# counterparties yet. Source: "Middle East Market Signals for the AI Economy"
# (Ayushi Shah, 2026-10-15) - every counterparty and direction below is the
# spec's, cited by section; nothing is added from elsewhere.
#
# ticker / exchange: the company's OWN listing on its home exchange (same
#   meaning as Taiwan's). None = not listed; the spec marks everything about
#   an unlisted company "indirect" (it is seen only through others).
# aliases: other written forms press uses, for matching (same name as
#   research-universe's universe_companies.aliases). A company whose name is a
#   common word is stored under its full name ("Elm Company", "Nova Ltd") so
#   matching never fires on a tree or Nova Scotia.
# counterparties: the US companies the spec says this company's news moves.
#   counterparty_name / counterparty_ticker: the US company and its ticker.
#   relationship: one plain sentence, role then why, with the spec section.
#   share_price_on_good_news: the spec's arrow - "rise" or "fall" - for GOOD
#   news from this company; bad news has the reverse effect. The spec's
#   hypothesis, not yet tested against prices.
#   [] = the spec names a group but no US company (its wording is kept in a
#   comment); no ticker is invented for it.
def _counterparties(names: dict[str, str], relationship: str,
                    share_price_on_good_news: str) -> list[dict[str, str]]:
    """{ticker: name} -> counterparty rows sharing one relationship."""
    return [{"counterparty_name": name, "counterparty_ticker": ticker,
             "relationship": relationship,
             "share_price_on_good_news": share_price_on_good_news}
            for ticker, name in names.items()]


MIDDLE_EAST_TICKER_UNIVERSE: list[dict[str, Any]] = [
    # --- 4.1 Gulf AI companies and data-centre builders --------------------
    {"ticker": None, "company": "G42", "exchange": None, "country": "UAE",
     "aliases": ["Core42", "Khazna", "Stargate UAE"],
     "counterparties": [
         *_counterparties({"NVDA": "Nvidia", "CSCO": "Cisco", "ORCL": "Oracle"},
                          "Partner: building G42's 5-gigawatt AI campus in Abu Dhabi (spec 2.1)", "rise"),
         *_counterparties({"AMD": "AMD"},
                          "Supplier: sells into the Gulf AI data-centre projects (spec 2.1)", "rise"),
         *_counterparties({"MSFT": "Microsoft"},
                          "Investor: owns a stake in G42 (spec 4.1)", "rise"),
         *_counterparties({"CBRS": "Cerebras"},
                          "Supplier: named by the spec as affected by G42's news (spec 4.1)", "rise"),
     ]},
    {"ticker": None, "company": "Humain", "exchange": None, "country": "Saudi Arabia",
     "aliases": [],
     "counterparties": [
         *_counterparties({"NVDA": "Nvidia", "AMD": "AMD", "AMZN": "Amazon", "QCOM": "Qualcomm"},
                          "Supplier: signed part of Humain's ~$23 billion of agreements (spec 2.1)", "rise"),
         *_counterparties({"CSCO": "Cisco"},
                          "Supplier: sells into the Gulf AI data-centre projects (spec 2.1)", "rise"),
     ]},
    {"ticker": "PRESIGHT", "company": "Presight AI", "exchange": "ADX", "country": "UAE",
     "aliases": ["Presight"],
     "counterparties": _counterparties(
         {"PLTR": "Palantir"},
         "Peer: a US AI software company; Presight is G42's listed AI software arm (spec 4.1)", "rise")},
    # Spec 4.1: "US satellite and connectivity companies" - no company named.
    {"ticker": "SPACE42", "company": "Space42", "exchange": "ADX", "country": "UAE",
     "aliases": [], "counterparties": []},
    {"ticker": "EAND", "company": "e&", "exchange": "ADX", "country": "UAE",
     "aliases": ["Etisalat", "Emirates Telecommunications Group"],
     "counterparties": _counterparties(
         {"CSCO": "Cisco", "ORCL": "Oracle", "MSFT": "Microsoft"},
         "Supplier: sells into e&'s data centres and cloud services (spec 4.1)", "rise")},
    {"ticker": "7010", "company": "stc", "exchange": "Tadawul", "country": "Saudi Arabia",
     "aliases": ["Saudi Telecom", "center3"],
     "counterparties": _counterparties(
         {"NVDA": "Nvidia", "CSCO": "Cisco", "ORCL": "Oracle", "MSFT": "Microsoft"},
         "Supplier: sells into stc's center3 data centres (spec 4.1)", "rise")},
    # Spec 4.1: "US business software and cloud companies" - no company named.
    {"ticker": "7203", "company": "Elm Company", "exchange": "Tadawul", "country": "Saudi Arabia",
     "aliases": [], "counterparties": []},
    {"ticker": "ORDS", "company": "Ooredoo", "exchange": "QSE", "country": "Qatar",
     "aliases": [],
     "counterparties": _counterparties(
         {"NVDA": "Nvidia"},
         "Supplier: its GPUs are the computing Ooredoo rents out (spec 4.1)", "rise")},
    # --- 4.2 Gulf government investment funds -------------------------------
    {"ticker": None, "company": "MGX", "exchange": None, "country": "UAE",
     "aliases": [],
     "counterparties": _counterparties(
         {"NVDA": "Nvidia", "MSFT": "Microsoft"},
         "Supplier to the AI labs and data-centre companies MGX invests in (spec 4.2)", "rise")},
    {"ticker": None, "company": "Mubadala", "exchange": None, "country": "UAE",
     "aliases": ["Mubadala Investment Company"],
     "counterparties": _counterparties(
         {"GFS": "GlobalFoundries"},
         "Investee: Mubadala is its majority owner (spec 4.2)", "rise")},
    # Spec 4.2: "Data-centre and infrastructure funds" - no company named.
    {"ticker": None, "company": "Abu Dhabi Investment Authority", "exchange": None, "country": "UAE",
     "aliases": ["ADIA"], "counterparties": []},
    {"ticker": None, "company": "ADQ", "exchange": None, "country": "UAE",
     "aliases": [], "counterparties": []},
    {"ticker": None, "company": "Public Investment Fund", "exchange": None, "country": "Saudi Arabia",
     "aliases": ["PIF"],
     "counterparties": _counterparties(
         {"NVDA": "Nvidia", "AMD": "AMD", "QCOM": "Qualcomm"},
         "Supplier to Humain, which PIF owns (spec 2.1, 4.2)", "rise")},
    # Spec 4.2: "US AI labs and infrastructure companies" - no company named.
    {"ticker": None, "company": "Qatar Investment Authority", "exchange": None, "country": "Qatar",
     "aliases": ["QIA"], "counterparties": []},
    # --- 4.3 Energy, power and helium ---------------------------------------
    # QatarEnergy's good news splits: a Ras Laffan restart lifts the helium
    # buyers and lowers the helium sellers.
    {"ticker": None, "company": "QatarEnergy", "exchange": None, "country": "Qatar",
     "aliases": ["Qatar Energy", "Ras Laffan"],
     "counterparties": [
         *_counterparties({"MU": "Micron", "INTC": "Intel"},
                          "Helium buyer: its chip factories need the helium Ras Laffan produces (spec 2.4, 4.3)", "rise"),
         *_counterparties({"XOM": "ExxonMobil"},
                          "Helium seller: the largest US helium source; earns more while Ras Laffan is offline (spec 4.3)", "fall"),
         *_counterparties({"LIN": "Linde", "APD": "Air Products"},
                          "Helium seller: charges more while helium is scarce (spec 2.4, 4.3)", "fall"),
     ]},
    # Spec 4.3: "US chipmakers and data-centre operators, through energy
    # costs" - no company named.
    {"ticker": "2222", "company": "Saudi Aramco", "exchange": "Tadawul", "country": "Saudi Arabia",
     "aliases": ["Aramco"], "counterparties": []},
    {"ticker": "2082", "company": "ACWA Power", "exchange": "Tadawul", "country": "Saudi Arabia",
     "aliases": [],
     "counterparties": _counterparties(
         {"GEV": "GE Vernova", "VRT": "Vertiv", "ETN": "Eaton"},
         "Supplier: sells power equipment for ACWA's plants, including for data centres (spec 4.3)", "rise")},
    {"ticker": "TAQA", "company": "TAQA", "exchange": "ADX", "country": "UAE",
     "aliases": ["Abu Dhabi National Energy"],
     "counterparties": _counterparties(
         {"GEV": "GE Vernova", "VRT": "Vertiv"},
         "Supplier: sells power and cooling equipment to TAQA (spec 4.3)", "rise")},
    {"ticker": "ADNOCGAS", "company": "ADNOC Gas", "exchange": "ADX", "country": "UAE",
     "aliases": [],
     "counterparties": _counterparties(
         {"GEV": "GE Vernova", "VRT": "Vertiv", "ETN": "Eaton"},
         "Supplier: sells equipment for the UAE power stations ADNOC Gas supplies (spec 4.3)", "rise")},
    # Spec 4.3: "US chipmakers, through etch-gas supply" - no company named.
    {"ticker": "ICL", "company": "ICL Group", "exchange": "NYSE", "country": "Israel",
     "aliases": [], "counterparties": []},
    # --- 4.4 Israel: chip design, factories and equipment -------------------
    {"ticker": "TSEM", "company": "Tower Semiconductor", "exchange": "Nasdaq", "country": "Israel",
     "aliases": [],
     "counterparties": [
         *_counterparties({"COHR": "Coherent", "LITE": "Lumentum"},
                          "Customer: US optical networking company using the optical chips Tower makes for AI data centres (spec 4.4)", "rise"),
         *_counterparties({"GFS": "GlobalFoundries"},
                          "Competitor: also makes specialty chips (spec 4.4)", "fall"),
     ]},
    {"ticker": "NVMI", "company": "Nova Ltd", "exchange": "Nasdaq", "country": "Israel",
     "aliases": ["Nova Measuring Instruments"],
     "counterparties": _counterparties(
         {"KLAC": "KLA", "ONTO": "Onto Innovation"},
         "Peer: strong orders at Nova mean chip factories are spending (spec 4.4)", "rise")},
    {"ticker": "CAMT", "company": "Camtek", "exchange": "Nasdaq", "country": "Israel",
     "aliases": [],
     "counterparties": [
         *_counterparties({"KLAC": "KLA", "ONTO": "Onto Innovation"},
                          "Peer: chip inspection and measurement equipment (spec 4.4)", "rise"),
         *_counterparties({"MU": "Micron"},
                          "AI memory (HBM) maker: Camtek's tools are used heavily for HBM (spec 4.4)", "rise"),
     ]},
    {"ticker": "MBLY", "company": "Mobileye", "exchange": "Nasdaq", "country": "Israel",
     "aliases": [],
     "counterparties": _counterparties(
         {"INTC": "Intel"},
         "Parent: Intel majority-owns Mobileye (spec 4.4)", "rise")},
    # Spec states the bad-news side (fall when these sites are disrupted);
    # good news is its reverse, per the spec's own convention.
    {"ticker": None, "company": "Nvidia Israel and Intel Kiryat Gat", "exchange": None, "country": "Israel",
     "aliases": ["Nvidia Israel", "Intel Israel", "Kiryat Gat"],
     "counterparties": [
         *_counterparties({"NVDA": "Nvidia"},
                          "Owner: its networking products are designed in Israel (spec 2.4, 4.4)", "rise"),
         *_counterparties({"INTC": "Intel"},
                          "Owner: makes chips at its Kiryat Gat factory (spec 2.4, 4.4)", "rise"),
     ]},
]

# Terms a WAM, SPA or BIS item must contain (title, or WAM's own lead) to
# be kept. Both wires publish everything - weather, sport, condolence
# cables - so this is the same "filter before the body fetch" order Japan
# and China use. Company names come from the universe above; the topic
# terms are the spec's eight event types (M1-M8). Matched on word
# boundaries, case-insensitively, in pipeline._me_match_regex.
_ME_TOPIC_TERMS: list[str] = [
    # M1/M8 - US decisions on Gulf chip sales, enforcement
    "export control", "export controls", "export licence", "export license",
    "entity list", "Bureau of Industry and Security", "smuggling",
    # M2/M6 - AI hardware, data centres, supplier choice
    "AI", "artificial intelligence", "data centre", "data center",
    "data centres", "data centers", "chip", "chips", "semiconductor",
    "semiconductors", "GPU", "Nvidia", "AMD", "Qualcomm", "Cisco",
    "Oracle", "Microsoft", "OpenAI", "Amazon Web Services", "AWS",
    "Huawei", "cloud", "megawatt", "gigawatt",
    # M3 - Gulf money into US AI
    "sovereign wealth", "sovereign fund",
    # M4/M5 - war, shipping, helium and gas
    "Hormuz", "Red Sea", "Bab el-Mandeb", "Houthi", "Houthis", "tanker",
    "shipping", "helium", "LNG", "force majeure",
]

_ME_COMPANY_NAMES: set[str] = {
    name for c in MIDDLE_EAST_TICKER_UNIVERSE for name in [c["company"], *c["aliases"]]
}

_ME_MATCH_TERMS: list[str] = sorted(_ME_COMPANY_NAMES | set(_ME_TOPIC_TERMS))

# The four Gulf funds that file with the SEC. 13F holdings are not
# fetched: they arrive up to 45 days after quarter end, and the latest
# ones touch almost nothing in the AI universe (PIF's lists five stocks,
# none of them AI). 13D/13G stake filings arrive within days of a change
# and carry real narrative text (13D item 4, "purpose of transaction").
# MGX has no SEC filer of its own.
MIDDLE_EAST_SEC_FUNDS: list[dict[str, str]] = [
    {"cik": "1767640", "fund": "Public Investment Fund"},
    {"cik": "1704268", "fund": "Mubadala Investment Company"},
    {"cik": "1362558", "fund": "Abu Dhabi Investment Authority"},
    {"cik": "1441449", "fund": "Qatar Investment Authority"},
]

_ME_GULF_OUTLETS: list[str] = [
    "thenationalnews.com", "gulfnews.com", "khaleejtimes.com",
    "arabnews.com", "english.aawsat.com", "english.alarabiya.net",
    "saudigazette.com.sa", "thepeninsulaqatar.com", "zawya.com",
    "gulf-times.com", "dohanews.co", "qna.org.qa",
]


def _er_concepts(*titles: str) -> dict[str, Any]:
    """An Event Registry $or over English-Wikipedia concept URIs."""
    return {"$or": [
        {"conceptUri": f"http://en.wikipedia.org/wiki/{t}"} for t in titles
    ]}


def _er_keywords(terms: list[str], loc: str) -> dict[str, Any]:
    """An Event Registry $or over keywords, matched in `loc` (title/body)."""
    return {"$or": [{"keyword": t, "keywordLoc": loc} for t in terms]}


MIDDLE_EAST_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "middle_east_market_signal",
        "url": "wam_wire:middle_east_market_signal",
        "name": "WAM - Emirates News Agency (English)",
        "source_type": "wam_wire",
        "frequency_name": "daily",
        "description": (
            "UAE state news agency, English, via the JSON API its own site"
            " calls: the latest-news view for the listing, then each"
            " article by slug for the body. Official UAE announcements -"
            " G42, Stargate UAE, MGX, state AI deals. newsapi.ai's copy of"
            " WAM English was last updated ~21 days before this was built,"
            " so it is fetched directly."
        ),
        "config": {
            "source_outlet_type": "state_press",
            "match_terms": _ME_MATCH_TERMS,
        },
    },
    {
        "domain_slug": "middle_east_market_signal",
        "url": "spa_wire:middle_east_market_signal",
        "name": "SPA - Saudi Press Agency (English)",
        "source_type": "spa_wire",
        "frequency_name": "daily",
        "description": (
            "Saudi state news agency, English, via its portal API"
            " (portalapi.spa.gov.sa/api/v1/news, 20 per page, then"
            " /news/{uuid} for the body). Official Saudi announcements -"
            " Humain, PIF, Aramco. Not indexed by newsapi.ai at all."
        ),
        "config": {
            "source_outlet_type": "state_press",
            "match_terms": _ME_MATCH_TERMS,
        },
    },
    {
        "domain_slug": "middle_east_market_signal",
        "url": "substack_post:middleeastainews",
        "name": "Middle East AI News",
        "source_type": "substack_post",
        "frequency_name": "daily",
        "description": (
            "Dedicated Gulf AI newsletter linking to primary sources -"
            " deals, data-centre phases, funding. Read through Substack's"
            " own API (archive listing, then each post's body_html) rather"
            " than its RSS feed, so no body depends on a page fetch; free"
            " written posts only - podcast episodes repeat a written post"
            " as show notes. Not indexed by newsapi.ai."
        ),
        "config": {"base_url": "https://www.middleeastainews.com"},
    },
    {
        "domain_slug": "middle_east_market_signal",
        "url": "bis_press:middle_east_market_signal",
        "name": "BIS Press Releases (Gulf and Israel)",
        "source_type": "bis_press",
        "frequency_name": "daily",
        "description": (
            "Bureau of Industry and Security press releases - licence"
            " decisions, country-group changes, enforcement - read from the"
            " JSON embedded in bis.gov/news-updates, kept when tagged with a"
            " tracked country or naming a tracked company. A release whose"
            " text lives on justice.gov gets its body from the DOJ press"
            " API, matched by title and date; the few that only link a"
            " scanned PDF are skipped, since every row needs a body."
        ),
        "config": {
            "source_outlet_type": "official",
            "countries": ["United Arab Emirates", "Saudi Arabia", "Qatar", "Israel"],
            "match_terms": sorted(_ME_COMPANY_NAMES | {
                "Gulf", "Middle East", "UAE", "Emirati", "Saudi", "Qatar", "Israel"}),
        },
    },
    {
        "domain_slug": "middle_east_market_signal",
        "url": "federal_register:middle_east_market_signal",
        "name": "Federal Register - BIS rules naming Gulf countries",
        "source_type": "federal_register",
        "frequency_name": "daily",
        "description": (
            "BIS rules and notices that name a Gulf country or Israel - the"
            " gate on whether a Gulf chip order can ship. geopolitical_news"
            " already fetches every BIS document; this copy is stored under"
            " its own url scheme so the global url index does not drop it"
            " from this domain."
        ),
        "config": {
            "agencies": ["industry-and-security-bureau"],
            "type": ["RULE", "PRORULE", "NOTICE"],
            "terms": '"United Arab Emirates" | "Saudi Arabia" | Qatar | Israel',
            "url_scheme": "me-federal-register",
            "source_outlet_type": "official",
            # Keeps these rows out of the domain's title-similarity dedup
            # (see _DOMAIN_TITLE_DEDUP_EXCLUDED_CATEGORIES).
            "source_category": "me_us_policy",
            # Whole document, no 20,000-character cap: the UAE A:5 rule
            # alone is 33,187 characters.
            "body_max_chars": None,
        },
    },
    {
        "domain_slug": "middle_east_market_signal",
        "url": "sec_fund_stake:middle_east_market_signal",
        "name": "SEC 13D/13G - Gulf sovereign funds",
        "source_type": "sec_fund_stake",
        "frequency_name": "daily",
        "description": (
            "Schedule 13D and 13G stake filings by PIF, Mubadala, ADIA and"
            " QIA, read from each filing's structured primary_doc.xml -"
            " issuer, percent of class, shares, event date, and the 13D's"
            " narrative items as the body."
        ),
        "config": {
            "source_outlet_type": "official",
            "funds": MIDDLE_EAST_SEC_FUNDS,
        },
    },
    # newsapi.ai. Five queries, each pairing an entity or place with a
    # topic term - an entity alone was measured to drag in astronomy
    # (helium), Jio's IPO anchor book (the funds) and Nvidia's MGX server
    # platform ("MGX"). The free plan rejects a query with more than 15
    # keywords, counting every word of a phrase. G42, Humain, MGX and
    # Stargate UAE have no usable concept in Event Registry (G42 resolves
    # to a Chinese expressway), which is why Gulf AI news comes from the
    # Gulf-outlet query plus WAM, SPA and Middle East AI News rather than
    # an entity query.
    #
    # `url_scheme`: rows are stored as er-me://{uri} with the real link in
    # metadata.article_url. geopolitical_news runs a "Strait of Hormuz"
    # query of its own, and 28 of 100 shipping articles were measured in
    # both - under the global url index the second domain to fetch one
    # would silently lose it.
    {
        "domain_slug": "middle_east_market_signal",
        "url": "eventregistry:me_shipping",
        "name": "Event Registry - Hormuz and Red Sea shipping",
        "source_type": "eventregistry",
        "frequency_name": "daily",
        "description": "M4: Strait of Hormuz / Bab-el-Mandeb concepts with a shipping term in the title.",
        "config": {
            "url_scheme": "er-me",
            "require_body": True,
            # ~100 matches a day against a 100-article page, and a run's
            # window spans two calendar days; one extra token per run.
            "max_pages": 2,
            "query": {"$and": [
                _er_concepts("Strait_of_Hormuz", "Bab-el-Mandeb"),
                _er_keywords(["ships", "tanker", "transit", "vessel", "shipping",
                              "closed", "reopen", "mine"], "title"),
            ]},
        },
    },
    {
        "domain_slug": "middle_east_market_signal",
        "url": "eventregistry:me_helium",
        "name": "Event Registry - helium supply",
        "source_type": "eventregistry",
        "frequency_name": "daily",
        "description": "M5: helium in the title, with a supply or chip term in the body.",
        "config": {
            "url_scheme": "er-me",
            "require_body": True,
            "query": {"$and": [
                _er_keywords(["helium"], "title"),
                _er_keywords(["Qatar", "shortage", "chip", "semiconductor",
                              "supply", "price"], "body"),
            ]},
        },
    },
    {
        "domain_slug": "middle_east_market_signal",
        "url": "eventregistry:me_qatarenergy",
        "name": "Event Registry - QatarEnergy and Ras Laffan",
        "source_type": "eventregistry",
        "frequency_name": "daily",
        "description": "M5: QatarEnergy / Ras Laffan with an output term in the title.",
        "config": {
            "url_scheme": "er-me",
            "require_body": True,
            "query": {"$and": [
                _er_concepts("QatarEnergy", "Ras_Laffan_Industrial_City"),
                _er_keywords(["force majeure", "restart", "LNG", "helium",
                              "production", "output"], "title"),
            ]},
        },
    },
    {
        "domain_slug": "middle_east_market_signal",
        "url": "eventregistry:me_israel_sites",
        "name": "Event Registry - Israeli chip sites",
        "source_type": "eventregistry",
        "frequency_name": "daily",
        "description": "M7: Kiryat Gat, Tower Semiconductor, or Intel/Nvidia Israel and Mobileye in the title.",
        "config": {
            "url_scheme": "er-me",
            "require_body": True,
            "query": {"$or": [
                _er_concepts("Kiryat_Gat", "Tower_Semiconductor"),
                _er_keywords(["Intel Israel", "Nvidia Israel", "Mobileye"], "title"),
            ]},
        },
    },
    {
        "domain_slug": "middle_east_market_signal",
        "url": "eventregistry:me_gulf_outlets",
        "name": "Event Registry - Gulf outlets, AI and energy",
        "source_type": "eventregistry",
        "frequency_name": "daily",
        "description": (
            "M2/M3/M6: twelve Gulf outlets, restricted to AI, data-centre,"
            " chip, helium and LNG headlines - the route for G42, Humain and"
            " Stargate UAE coverage, which have no usable entity concept."
        ),
        "config": {
            "url_scheme": "er-me",
            "require_body": True,
            "query": {"$and": [
                {"$or": [{"sourceUri": s} for s in _ME_GULF_OUTLETS]},
                _er_keywords(["AI", "data centre", "data center", "chips",
                              "Nvidia", "G42", "Humain", "semiconductor",
                              "helium", "LNG"], "title"),
            ]},
        },
    },
]

# GDELT DOC 2.0 API, queried once per individual theme code (not OR-joined -
# only single bare "theme:X" queries are confirmed working against the live
# API; multi-theme "(theme:X OR theme:Y)" queries could not be verified and
# are avoided until confirmed). English-only. No API key required.
# Fetcher (source_type: "gdelt") implemented in pipeline.py.
GDELT_SOURCE: dict[str, Any] = {
    "domain_slug": "geopolitical_news",
    "url": "gdelt:geopolitical",
    "name": "GDELT DOC API",
    "source_type": "gdelt",
    "frequency_name": "daily",
    "description": (
        "Global conflict, terrorism, sanctions, and unrest coverage via"
        " the GDELT Project's DOC 2.0 API, English-language, US-sourced"
        " outlets only (sourcecountry:US - used as a proxy for events"
        " involving or affecting the US), queried per individual theme code."
    ),
    "config": {
        "queries": [
            "theme:ARMEDCONFLICT sourcelang:english sourcecountry:US",
            "theme:MILITARY sourcelang:english sourcecountry:US",
            "theme:CEASEFIRE sourcelang:english sourcecountry:US",
            "theme:TERROR sourcelang:english sourcecountry:US",
            "theme:WB_2467_TERRORISM sourcelang:english sourcecountry:US",
            "theme:REBELLION sourcelang:english sourcecountry:US",
            "theme:REBELS sourcelang:english sourcecountry:US",
            "theme:INSURGENCY sourcelang:english sourcecountry:US",
            "theme:SEPARATISTS sourcelang:english sourcecountry:US",
            "theme:SANCTIONS sourcelang:english sourcecountry:US",
            "theme:BLOCKADE sourcelang:english sourcecountry:US",
            "theme:PROTEST sourcelang:english sourcecountry:US",
            "theme:VIOLENT_UNREST sourcelang:english sourcecountry:US",
            "theme:KILL sourcelang:english sourcecountry:US",
            "theme:ASSASSINATION sourcelang:english sourcecountry:US",
            "theme:HUMAN_RIGHTS_ABUSES sourcelang:english sourcecountry:US",
            "theme:WB_2509_GENOCIDE sourcelang:english sourcecountry:US",
            "theme:WB_2510_WAR_CRIMES sourcelang:english sourcecountry:US",
            "theme:DISPLACED sourcelang:english sourcecountry:US",
            "theme:REFUGEES sourcelang:english sourcecountry:US",
            "theme:PEACEKEEPING sourcelang:english sourcecountry:US",
            "theme:RELEASE_HOSTAGE sourcelang:english sourcecountry:US",
            "theme:WB_2503_WEAPONS_PROLIFERATION_AND_ARMS_CONTROL sourcelang:english sourcecountry:US",
            "theme:WB_2505_WEAPONS_OF_MASS_DESTRUCTION sourcelang:english sourcecountry:US",
            "theme:CYBER_ATTACK sourcelang:english sourcecountry:US",
            "theme:BORDER sourcelang:english sourcecountry:US",
            "theme:MILITARY_COOPERATION sourcelang:english sourcecountry:US",
        ],
    },
}

# Event Registry (newsapi.ai) - the primary geopolitical_news source.
#
# One source row per query, because the free tier caps a query at 15
# keywords and a MULTI-WORD phrase counts per word ("export controls" is
# 2) - so four phrases plus lang and two dates is the practical limit.
# One token per query, five queries, against a 2,000-token allowance.
#
# Terms were chosen by measuring a real 7-day window per theme rather
# than by reputation, and matched on TITLE: the same terms against the
# body returned 11,020 results for a single day (Formula 1, local gas
# prices) against 565 title-matched.
#
#   sanctions/tariffs/embargo   3,287   the core trade-action vocabulary
#   semis/minerals              3,889
#   energy                      1,646
#   grid/power                    339   AI compute straining the grid -
#                                       uncovered by the old queries and
#                                       directly this domain's
#                                       Energy & Grid / Data Centers
#                                       categories
#   nuclear fuel                  223   Nuclear & Advanced Energy had no
#                                       query coverage at all
#
# Deliberately NOT included, each measured and rejected: nationalisation
# (82/wk, 4 of 5 sampled were duplicates of one water-industry
# petition), subsea cables (27/wk, 5 of 5 one Mediterranean story),
# CFIUS/investment screening (10/wk), data sovereignty (12/wk), and
# kinetic conflict terms (953/wk but rarely transmitting to an
# AI-economy category - Stage A/B reject most of it).
EVENT_REGISTRY_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "geopolitical_news",
        "url": "eventregistry:trade_controls",
        "name": "Event Registry - trade & controls",
        "source_type": "eventregistry",
        "frequency_name": "daily",
        "description": "newsapi.ai title search: sanctions, tariffs, embargo, export controls.",
        "config": {"keywords": ["sanctions", "tariffs", "embargo", "export controls"]},
    },
    {
        "domain_slug": "geopolitical_news",
        "url": "eventregistry:supply_chain",
        "name": "Event Registry - supply chain",
        "source_type": "eventregistry",
        "frequency_name": "daily",
        "description": "newsapi.ai title search: rare earths, critical minerals, semiconductor, chip curbs.",
        "config": {"keywords": ["rare earths", "critical minerals", "semiconductor", "chip curbs"]},
    },
    {
        "domain_slug": "geopolitical_news",
        "url": "eventregistry:energy_shipping",
        "name": "Event Registry - energy & shipping",
        "source_type": "eventregistry",
        "frequency_name": "daily",
        "description": "newsapi.ai title search: LNG, pipeline, oil exports, Strait of Hormuz.",
        "config": {"keywords": ["LNG", "pipeline", "oil exports", "Strait of Hormuz"]},
    },
    {
        "domain_slug": "geopolitical_news",
        "url": "eventregistry:grid_power",
        "name": "Event Registry - grid & power",
        "source_type": "eventregistry",
        "frequency_name": "daily",
        "description": "newsapi.ai title search: power grid, grid emergency, electricity shortage, data center.",
        "config": {"keywords": ["power grid", "grid emergency", "electricity shortage", "data center"]},
    },
    {
        "domain_slug": "geopolitical_news",
        "url": "eventregistry:bans_nuclear",
        "name": "Event Registry - bans & nuclear fuel",
        "source_type": "eventregistry",
        "frequency_name": "daily",
        "description": "newsapi.ai title search: export ban, import ban, entity list, uranium.",
        "config": {"keywords": ["export ban", "import ban", "entity list", "uranium"]},
    },
]

# Google News search for geopolitical_news via SerpAPI.
GEOPOLITICAL_GOOGLE_NEWS_SOURCE: dict[str, Any] = {
    "domain_slug": "geopolitical_news",
    "url": "google_news:geopolitical",
    "name": "Google News",
    "source_type": "google_news",
    "frequency_name": "daily",
    "description": "Google News search across geopolitics topics via SerpAPI.",
    "config": {
        "queries": [
            "geopolitics",
            "sanctions",
            "export controls",
            "trade war tariffs",
            "military conflict",
            "diplomatic relations",
            "territorial dispute",
        ],
    },
}

# VC commentary / investor blogs.
VC_COMMENTARY_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "vc_commentary",
        "url": "https://medium.com/feed/mayfield-perspectives",
        "name": "Mayfield Viewpoints",
        "frequency_name": "daily",
        "description": "Navin Chaddha / Mayfield on AI-economy theses and founder leadership.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "https://techcrunch.com/author/navin-chaddha/feed/",
        "name": "Navin Chaddha (TechCrunch)",
        "frequency_name": "daily",
        "description": "Op-eds on AI, startups, and markets from Mayfield Managing Partner.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "https://www.mayfield.com/feed",
        "name": "Mayfield",
        "frequency_name": "daily",
        "description": "Mayfield Fund firm perspectives and portfolio insights.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "http://avc.com/feed/",
        "name": "AVC",
        "frequency_name": "daily",
        "description": "Fred Wilson (USV) — daily VC commentary since 2003.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "https://abovethecrowd.com/feed",
        "name": "Above the Crowd",
        "frequency_name": "daily",
        "description": "Bill Gurley (Benchmark) on platforms, valuations, and regulation.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "https://andrewchen.com/feed",
        "name": "Andrew Chen",
        "frequency_name": "daily",
        "description": "Andrew Chen (a16z) on growth, consumer, and AI products.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "https://tomtunguz.com/index.xml",
        "name": "Tomasz Tunguz",
        "frequency_name": "daily",
        "description": "Tomasz Tunguz (Theory) — data-driven SaaS and AI market analysis.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "https://blog.eladgil.com/feed",
        "name": "Elad Gil",
        "frequency_name": "daily",
        "description": "Elad Gil on startups, AI, and high-growth company building.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "https://www.saastr.com/feed",
        "name": "SaaStr",
        "frequency_name": "daily",
        "description": "Jason Lemkin on SaaS and AI GTM strategies.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "https://www.lennysnewsletter.com/feed",
        "name": "Lenny's Newsletter",
        "frequency_name": "daily",
        "description": "Lenny Rachitsky on product, growth, and AI product strategy.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "https://www.newcomer.co/feed",
        "name": "Newcomer",
        "frequency_name": "daily",
        "description": "Eric Newcomer on VC, tech politics, and startup industry news.",
    },
    {
        "domain_slug": "vc_commentary",
        "url": "https://feld.com/feed",
        "name": "Feld Thoughts",
        "frequency_name": "daily",
        "description": "Brad Feld (Foundry) on VC, startups, and entrepreneurship.",
    },
]

# Adverse media / investigative sources.
# Kept separate so they can be toggled independently for testing.
# To enable: uncomment the *ADVERSE_SOURCES line at the bottom of SOURCES.
ADVERSE_SOURCES: list[dict[str, Any]] = [
    {
        "domain_slug": "adverse_media",
        "url": "https://thebearcave.substack.com/feed",
        "name": "The Bear Cave",
        "frequency_name": "daily",
        "description": "Edwin Dorsey's investigative research exposing corporate misconduct and short-sell targets.",
    },
    {
        "domain_slug": "adverse_media",
        "url": "https://hntrbrk.com/feed",
        "name": "Hunterbrook",
        "frequency_name": "daily",
        "description": "Investigative journalism uncovering corporate and financial misconduct.",
    },
]


SOURCES: list[dict[str, Any]] = [
    # ------------------------------------------------------------------
    # AI News
    # ------------------------------------------------------------------
    {
        "domain_slug": "ai_news",
        "url": "https://venturebeat.com/category/ai/feed/",
        "name": "VentureBeat AI",
        "frequency_name": "daily",
        "description": (
            "The leading source for transformative tech news with"
            " deep AI, machine learning, and data coverage for"
            " business leaders."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": (
            "https://www.theverge.com/rss/ai-artificial-intelligence"
            "/index.xml"
        ),
        "name": "The Verge AI",
        "frequency_name": "daily",
        "description": (
            "Technology news, reviews, and culture covering gadgets,"
            " platforms, AI tools, and how tech shapes everyday life."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": (
            "https://techcrunch.com/category/artificial-intelligence"
            "/feed/"
        ),
        "name": "TechCrunch AI",
        "frequency_name": "daily",
        "description": (
            "AI-focused startup and technology news covering funding,"
            " launches, and innovation from TechCrunch."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.wired.com/feed/tag/ai/latest/rss",
        "name": "Wired AI",
        "frequency_name": "daily",
        "description": (
            "Covers how AI affects culture, economy, and politics,"
            " from model releases to societal implications."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.technologyreview.com/feed/",
        "name": "MIT Technology Review",
        "frequency_name": "daily",
        "description": (
            "Covers emerging technologies and their impact on"
            " society, business, and the future, published by MIT."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.nextplatform.com/feed/",
        "name": "The Next Platform",
        "frequency_name": "daily",
        "description": (
            "In-depth coverage of high-end computing at large"
            " enterprises, supercomputing centres, hyperscale data"
            " centres, and public clouds."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://semiengineering.com/feed/",
        "name": "Semiconductor Engineering",
        "frequency_name": "daily",
        "description": (
            "Deep technical coverage of semiconductor design,"
            " manufacturing, verification, and EDA for chip"
            " engineers."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.servethehome.com/feed/",
        "name": "ServeTheHome",
        "frequency_name": "daily",
        "description": (
            "Covers servers, storage, networking, and high-end"
            " workstation hardware for IT professionals and"
            " enthusiasts."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.datacenterdynamics.com/en/rss/",
        "name": "Data Center Dynamics",
        "frequency_name": "daily",
        "description": (
            "News and analysis on data centre infrastructure,"
            " hyperscale cloud, colocation, AI workloads, and"
            " energy."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://news.crunchbase.com/feed/",
        "name": "Crunchbase News",
        "frequency_name": "daily",
        "description": (
            "Startup funding trends, investment rounds, and private"
            " company intelligence from Crunchbase."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://siliconangle.com/feed/",
        "name": "SiliconAngle",
        "frequency_name": "daily",
        "description": (
            "The trusted voice of enterprise technology, reporting"
            " on AI, cloud, security, and data infrastructure for"
            " IT leaders."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": (
            "https://feeds.arstechnica.com/arstechnica/technology-lab"
        ),
        "name": "Ars Technica",
        "frequency_name": "daily",
        "description": (
            "In-depth technology journalism covering science, policy,"
            " hardware, and software with academic-level rigour."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://spectrum.ieee.org/feeds/feed.rss",
        "name": "IEEE Spectrum",
        "frequency_name": "daily",
        "description": (
            "The flagship publication of IEEE, covering engineering,"
            " electronics, AI, robotics, and emerging technology."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.therobotreport.com/feed/",
        "name": "The Robot Report",
        "frequency_name": "daily",
        "description": (
            "News and analysis on commercial robotics, autonomous"
            " systems, AI for robots, and industrial automation."
        ),
    },
    # Weekly feeds - only polled when days_back >= 7
    {
        "domain_slug": "ai_news",
        "url": "https://huggingface.co/blog/feed.xml",
        "name": "Hugging Face Blog",
        "frequency_name": "weekly",
        "description": (
            "Covers machine learning research, open-source AI tools,"
            " transformers, and practical implementation guides."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.canarymedia.com/rss.xml",
        "name": "Canary Media",
        "frequency_name": "weekly",
        "description": (
            "Nonprofit journalism covering the clean energy"
            " transition, decarbonisation, and climate technology"
            " markets."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.hackster.io/feed",
        "name": "Hackster.io",
        "frequency_name": "weekly",
        "description": (
            "Community platform for hardware developers covering ML,"
            " IoT, robotics, and maker projects with emerging tech."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.quantamagazine.org/feed/",
        "name": "Quanta Magazine",
        "frequency_name": "weekly",
        "description": (
            "Science journalism covering mathematics, theoretical"
            " physics, computer science, and the basic life"
            " sciences."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.lawfaremedia.org/feed",
        "name": "Lawfare",
        "frequency_name": "weekly",
        "description": (
            "Non-partisan analysis of national security law,"
            " cybersecurity, executive powers, content moderation,"
            " and AI governance."
        ),
    },
    {
        "domain_slug": "ai_news",
        "url": "https://www.eff.org/rss/updates.xml",
        "name": "EFF",
        "frequency_name": "weekly",
        "description": (
            "The Electronic Frontier Foundation covers digital civil"
            " liberties, privacy, surveillance, and technology"
            " policy."
        ),
    },
    # ------------------------------------------------------------------
    # Smart Money
    # ------------------------------------------------------------------
    {
        "domain_slug": "smart_money",
        "url": "https://www.pymnts.com/feed/",
        "name": "PYMNTS",
        "frequency_name": "daily",
        "description": (
            "Global data, news, and insights on innovation in"
            " payments and the connected economy."
        ),
    },
    {
        "domain_slug": "smart_money",
        "url": "https://www.finextra.com/rss/rss.aspx",
        "name": "Finextra",
        "frequency_name": "daily",
        "description": (
            "The leading global newswire for financial technology"
            " professionals, covering fintech, payments, banking,"
            " and crypto."
        ),
    },
    {
        "domain_slug": "smart_money",
        "url": "https://techcrunch.com/category/fintech/feed/",
        "name": "TechCrunch Fintech",
        "frequency_name": "daily",
        "description": (
            "Fintech-focused coverage of startups, funding, and"
            " innovation in payments and financial services."
        ),
    },
    {
        "domain_slug": "smart_money",
        "url": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "name": "CoinDesk",
        "frequency_name": "daily",
        "description": (
            "Leading cryptocurrency and blockchain news, with market"
            " data, policy analysis, and DeFi coverage."
        ),
    },
    {
        "domain_slug": "smart_money",
        "url": "https://www.theblock.co/rss.xml",
        "name": "The Block",
        "frequency_name": "daily",
        "description": (
            "Breaking news, research, and analysis on Bitcoin,"
            " Ethereum, and digital assets for crypto professionals."
        ),
    },
    {
        "domain_slug": "smart_money",
        "url": "https://decrypt.co/feed",
        "name": "Decrypt",
        "frequency_name": "daily",
        "description": (
            "Independent crypto and AI news covering blockchain,"
            " digital assets, culture, and emerging technology."
        ),
    },
    {
        "domain_slug": "smart_money",
        "url": "https://a16z.com/feed/",
        "name": "a16z",
        "frequency_name": "daily",
        "description": (
            "Andreessen Horowitz publishes analysis, research, and"
            " opinion on technology, crypto, AI, and venture"
            " investing."
        ),
    },
    # ------------------------------------------------------------------
    # Substack newsletters (free tier - full body in RSS)
    # ------------------------------------------------------------------
    *SUBSTACK_SOURCES,
    # ------------------------------------------------------------------
    # SerpAPI Google News
    # ------------------------------------------------------------------
    GOOGLE_NEWS_SOURCE,
    # ------------------------------------------------------------------
    # NewsAPI top-headlines
    # ------------------------------------------------------------------
    *NEWSAPI_SOURCES,
    # ------------------------------------------------------------------
    # Adverse media / investigative 
    # ------------------------------------------------------------------
     *ADVERSE_SOURCES,
    # ------------------------------------------------------------------
    # VC commentary / investor blogs
    # ------------------------------------------------------------------
    *VC_COMMENTARY_SOURCES,
    # ------------------------------------------------------------------
    # Company-specific news (Alpha Vantage)
    # ------------------------------------------------------------------
    *COMPANY_NEWS_SOURCES,
    # ------------------------------------------------------------------
    # Geopolitical news (Federal Register + Event Registry)
    #
    # GDELT and SerpAPI/Google News are DISABLED, not deleted - their
    # definitions below are intact and restoring either is uncommenting
    # one line. Both were measured against Event Registry on the same
    # window and lost on every axis that matters here:
    #
    #   SerpAPI       its dedicated geopolitical quota is exhausted and
    #                 returns 0 articles; it also charges one call per
    #                 query, so the 7 queries cost 7 calls where Event
    #                 Registry ORs them into one request.
    #   GDELT         answers 429 under its own 15s-spaced retry policy,
    #                 returns headlines with NO body (every article then
    #                 needs a Trafilatura/curl_cffi/archive fetch), and
    #                 its 27 theme codes skew to conflict coverage
    #                 (ARMEDCONFLICT, TERROR, REBELLION) rather than the
    #                 trade, export-control and supply-chain events this
    #                 domain's taxonomy actually transmits.
    #
    # Measured over one real 7-day window:
    #   eventregistry    245 articles, 245 with body (100%), 183 domains
    #   gdelt+serpapi+rss 120 articles, 103 with body,        107 domains
    #   federal_register   16 articles,  16 with body
    # ------------------------------------------------------------------
    *FEDERAL_REGISTER_SOURCES,
    *EVENT_REGISTRY_SOURCES,
    # GEOPOLITICAL_GOOGLE_NEWS_SOURCE,  # SerpAPI - quota exhausted
    # GDELT_SOURCE,                     # GDELT - 429, and no article body
    # ------------------------------------------------------------------
    # Taiwan market signal (TWSE + TPEx OpenData)
    # ------------------------------------------------------------------
    *TAIWAN_REVENUE_SOURCES,
    *TAIWAN_MATERIAL_SOURCES,
    TAIWAN_GDELT_SOURCE,
    # ------------------------------------------------------------------
    # Korea market signal (DART + Korean-language trade press RSS + GDELT
    # + Customs export press-release scrape)
    # ------------------------------------------------------------------
    KOREA_DART_SOURCE,
    *KOREA_RSS_SOURCES,
    KOREA_GDELT_SOURCE,
    KOREA_GDELT_ENGLISH_SOURCE,
    KOREA_CUSTOMS_EXPORT_SOURCE,
    # ------------------------------------------------------------------
    # Japan market signal (more source_types pending - see
    # JAPAN_TICKER_UNIVERSE comment block above)
    # ------------------------------------------------------------------
    JAPAN_IRBANK_FINANCIALS_SOURCE,
    JAPAN_IRBANK_BUYBACK_SOURCE,
    JAPAN_IRBANK_COMPANY_REFERENCE_SOURCE,
    JAPAN_KABUTAN_TDNET_MIRROR_SOURCE,
    JAPAN_KABUTAN_BUYBACK_SOURCE,
    JAPAN_EDINET_FILING_SOURCE,
    JAPAN_EDINET_BUYBACK_STATUS_SOURCE,
    JAPAN_EDINET_EXTRAORDINARY_REPORT_SOURCE,
    JAPAN_SEAJ_BILLINGS_SOURCE,
    JAPAN_MONOIST_CAPEX_SOURCE,
    JAPAN_PRESS_JP_SOURCE,
    JAPAN_PRESS_JP_ENGLISH_CHECK_SOURCE,
    # ------------------------------------------------------------------
    # China market signal. Eight source_types, not the ten originally
    # scoped: customs (GACC) serves a JS-cookie anti-bot challenge that
    # needs a browser to defeat, and the statistics bureau (NBS) is
    # blocked at the network layer from this environment. Both were
    # confirmed live 2026-10-05 and are deliberately absent rather than
    # stubbed - see news-retrieval/CLAUDE.md for the probe record.
    # ------------------------------------------------------------------
    *CHINA_POLICY_SOURCES,
    CHINA_HKEX_SOURCE,
    CHINA_CNINFO_SOURCE,
    CHINA_COMTRADE_SOURCE,
    CHINA_NBS_IC_OUTPUT_SOURCE,
    CHINA_PRESS_SOURCE,
    CHINA_PRESS_ENGLISH_CHECK_SOURCE,
    # ------------------------------------------------------------------
    # Middle East market signal. Every source is an API or structured
    # JSON and every row carries a body - see news-retrieval/CLAUDE.md
    # for the sources that were probed and dropped (PortWatch ran 8
    # days behind; UKMTO, Tadawul and Home Front Command are blocked).
    # ------------------------------------------------------------------
    *MIDDLE_EAST_SOURCES,
]

# VC commentary / investor blogs.
# Kept separate so they can be toggled independently for testing.
# ---------------------------------------------------------------------------
# Seeding logic
# ---------------------------------------------------------------------------


def seed() -> None:
    """Insert frequencies, domains, taxonomies, and sources.

    Skips rows that already exist. Safe to call multiple times.
    All inserts run in a single transaction; a failure rolls back
    the entire seed run.
    """
    with transaction():
        # Run statuses (must be seeded before runs table is used)
        with get_db() as conn:
            conn.execute_values(
                "INSERT INTO run_statuses (name)"
                " VALUES %s ON CONFLICT (name) DO NOTHING",
                [(s,) for s in RUN_STATUSES],
            )
        logger.info("Seeded %d run statuses.", len(RUN_STATUSES))

        # Frequencies
        with get_db() as conn:
            conn.execute_values(
                "INSERT INTO frequencies (name, min_days_back)"
                " VALUES %s ON CONFLICT (name) DO NOTHING",
                [(f["name"], f["min_days_back"]) for f in FREQUENCIES],
            )
            rows = conn.execute(
                "SELECT id, name FROM frequencies"
                " WHERE name = ANY(?)",
                ([f["name"] for f in FREQUENCIES],),
            ).fetchall()
        freq_id_map = {row["name"]: row["id"] for row in rows}
        logger.info("Seeded %d frequencies.", len(freq_id_map))

        # Domains
        with get_db() as conn:
            conn.execute_values(
                "INSERT INTO domains (name, slug, description)"
                " VALUES %s ON CONFLICT (slug) DO NOTHING",
                [
                    (d["name"], d["slug"], d["description"])
                    for d in DOMAINS
                ],
            )
            rows = conn.execute(
                "SELECT id, slug FROM domains WHERE slug = ANY(?)",
                ([d["slug"] for d in DOMAINS],),
            ).fetchall()
        domain_id_map = {row["slug"]: row["id"] for row in rows}
        logger.info("Seeded %d domains.", len(domain_id_map))

        # Sources
        source_rows = [
            (
                s["url"],
                domain_id_map[s["domain_slug"]],
                freq_id_map[s.get("frequency_name", "daily")],
                s["name"],
                s["description"],
                s.get("no_fetch", False),
                s.get("source_type", "rss"),
                json.dumps(s["config"]) if s.get("config") else None,
            )
            for s in SOURCES
        ]
        with get_db() as conn:
            cur = conn.execute_values(
                "INSERT INTO sources"
                " (url, domain_id, frequency_id, name, description,"
                " no_fetch, source_type, config)"
                " VALUES %s"
                # `config` IS REFRESHED ON CONFLICT. It used to be DO
                # NOTHING for the whole row, which meant a config
                # change made in this file never reached an
                # already-seeded database - the source kept whatever
                # it was first seeded with, silently and forever.
                #
                # That cost real coverage. China's press source gained
                # an `rss_urls` entry for ITHome, and a seeded database
                # never picked it up: `config.get("rss_urls", {})`
                # iterated nothing, so the RSS path never ran and every
                # ITHome article arrived through the HTML listing with
                # no summary. The same row was also missing ijiwei,
                # added to `listing_urls` at the same time. Both were
                # invisible - the fetcher logged no error, it simply
                # had nothing to iterate.
                #
                # `config` is safe to overwrite because it is derived
                # entirely from this file; nothing writes it at
                # runtime. The operator-owned columns are deliberately
                # NOT touched: `no_fetch` is a kill switch someone may
                # have flipped, and `frequency_id` may have been tuned
                # against real volume. Those still require a manual
                # change, which is the right trade - they encode a
                # decision, where `config` encodes code.
                " ON CONFLICT (url) DO UPDATE SET"
                "   config = EXCLUDED.config,"
                "   source_type = EXCLUDED.source_type,"
                "   name = EXCLUDED.name,"
                "   description = EXCLUDED.description"
                " RETURNING id, (xmax = 0) AS was_inserted",
                source_rows,
            )
            returned = cur.fetchall()
            inserted = sum(1 for r in returned if r["was_inserted"])
        refreshed = len(source_rows) - inserted
        logger.info(
            "Seed complete: %d sources inserted,"
            " %d existing refreshed from code (config, source_type,"
            " name, description).",
            inserted,
            refreshed,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    seed()
