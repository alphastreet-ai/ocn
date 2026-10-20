# infra

Part of the [ocn monorepo](../CLAUDE.md).

## Overview

Terraform infrastructure for the OCN platform. Provisions all AWS resources - VPC, RDS, ECS Fargate cluster, ALB, security groups, IAM roles, and CloudWatch logs - across environments. Remote state is stored in S3 with DynamoDB locking.

## Jira Board
| Board | URL | Project Key |
|-------|-----|-------------|
| OCN Board | https://opengrowthventures.atlassian.net/jira/software/projects/CON/boards/34 | CON |

## Structure

See [STRUCTURE.md](STRUCTURE.md) for file-level descriptions.

```
infra/
├── bootstrap/                 # One-time state backend setup
│   └── main.tf
├── modules/
│   ├── alb/                   # Application Load Balancer
│   ├── ecs_cluster/           # ECS cluster, task definitions, IAM, logs
│   ├── rds/                   # RDS PostgreSQL instance
│   ├── security_groups/       # Per-service security groups
│   └── vpc/                   # VPC, subnets, NAT gateway
└── staging/                   # Staging environment root module
    ├── main.tf
    ├── variables.tf
    └── terraform.tfvars        # Not committed
```

## Environments

| Environment | Path | Region | State key |
|-------------|------|--------|-----------|
| staging | `staging/` | `eu-north-1` | `staging/terraform.tfstate` |

## Environment Variables injected per service

### auth-service
| Variable | Source | Notes |
|----------|--------|-------|
| `AUTH_POSTGRES_HOST` | `rds_endpoint` | RDS instance address |
| `AUTH_POSTGRES_PORT` | hardcoded | `5432` |
| `AUTH_POSTGRES_DB` | hardcoded | `auth_db` |
| `AUTH_POSTGRES_USER` | hardcoded | `auth_user` |
| `AUTH_POSTGRES_PASSWORD` | Secrets Manager | `ocn/{env}/auth-service:POSTGRES_PASSWORD` |
| `AUTH_ADMIN_API_KEY` | Secrets Manager | `ocn/{env}/auth-service:ADMIN_API_KEY` |
| `AUTH_JWT_PRIVATE_KEY` | Secrets Manager | `ocn/{env}/auth-service:JWT_PRIVATE_KEY` - PEM-encoded RS256 private key |
| `AUTH_JWT_EXPIRY_SECONDS` | hardcoded | `86400` (24 hours) — overrides default 1h TTL |
| `ADMIN_USERNAME` | Secrets Manager | `ocn/{env}/auth-service:ADMIN_USERNAME` - seed admin user |
| `ADMIN_EMAIL` | Secrets Manager | `ocn/{env}/auth-service:ADMIN_EMAIL` - seed admin user |
| `ADMIN_PASSWORD` | Secrets Manager | `ocn/{env}/auth-service:ADMIN_PASSWORD` - seed admin user |

### news-retrieval
| Variable | Source | Notes |
|----------|--------|-------|
| `POSTGRES_HOST` | `rds_endpoint` | |
| `POSTGRES_PORT` | hardcoded | `5432` |
| `POSTGRES_DB` | hardcoded | `news_retrieval_db` |
| `POSTGRES_USER` | hardcoded | `news_user` |
| `POSTGRES_PASSWORD` | Secrets Manager | `ocn/{env}/news-retrieval:POSTGRES_PASSWORD` |
| `AUTH_SERVICE_URL` | hardcoded | `http://auth-service.{env}.ocn.internal:8001` |
| `OPENROUTER_API_KEY` | Secrets Manager | `ocn/{env}/news-retrieval:OPENROUTER_API_KEY` - used for Taiwan GDELT title-dedup embeddings |
| `SERPAPI_KEY` | Secrets Manager | `ocn/{env}/news-retrieval:SERPAPI_KEY` - optional; SerpAPI key for `google_news` sources |
| `SERPAPI_KEY_GEOPOLITICAL` | Secrets Manager | `ocn/{env}/news-retrieval:SERPAPI_KEY_GEOPOLITICAL` - dedicated SerpAPI key used only for the `geopolitical_news` domain's `google_news` source (separate quota from `SERPAPI_KEY`) |
| `NEWSAPI_KEY` | Secrets Manager | `ocn/{env}/news-retrieval:NEWSAPI_KEY` - optional; NewsAPI key for `newsapi` sources (newsapi.**org**) |
| `EVENT_REGISTRY_API_KEY` | Secrets Manager | `ocn/{env}/news-retrieval:EVENT_REGISTRY_API_KEY` - newsapi.**ai** / Event Registry; a different vendor and account from `NEWSAPI_KEY`. Required for the `eventregistry` source_type - one of the two sources `geopolitical_news` fetches (the other is `federal_register`), and `middle_east_market_signal`'s `er-me://` rows. Fails OPEN: without it the fetch logs a warning and skips those sources, so the domain silently runs on half its sources rather than erroring |
| `ALPHA_VANTAGE_API_KEY` | Secrets Manager | `ocn/{env}/news-retrieval:ALPHA_VANTAGE_API_KEY` - optional; Alpha Vantage key for `company_news` domain |
| `RESEARCH_UNIVERSE_URL` | hardcoded | `http://research-universe.{env}.ocn.internal:8007` - optional; used to fetch tickers dynamically for `company_news` domain |
| `RESEARCH_UNIVERSE_API_KEY` | Secrets Manager | `ocn/{env}/news-retrieval:RESEARCH_UNIVERSE_API_KEY` - optional; service API key (ru_ prefix) for authenticating calls to research-universe |
| `AWS_REGION` | hardcoded | `eu-north-1` — DynamoDB region for market data tables |
| `DYNAMODB_TABLE_QUOTE` | hardcoded | `ocn-market-quote` |
| `DYNAMODB_TABLE_OVERVIEW` | hardcoded | `ocn-market-overview` |
| `DYNAMODB_TABLE_PRICE_HISTORY` | hardcoded | `ocn-market-price-history` |
| `DYNAMODB_TABLE_EARNINGS` | hardcoded | `ocn-market-earnings` |
| `DYNAMODB_TABLE_INDICES` | hardcoded | `ocn-market-indices` |
| `DYNAMODB_TABLE_MARKET_STATUS` | hardcoded | `ocn-market-status` |
| `DYNAMODB_TABLE_LOCK` | hardcoded | `ocn-market-lock` — distributed poller lock |
| `DYNAMODB_TABLE_SEC_FILINGS` | hardcoded | `ocn-sec-filings` — SEC EDGAR 8-K/10-Q/10-K filing metadata |
| `DYNAMODB_TABLE_MACRO` | hardcoded | `ocn-market-macro` — Fed funds rate, CPI, 10-year Treasury yield, unemployment, nonfarm payroll, real GDP, retail sales, durables, top gainers/losers/most-active (not ticker-keyed) |
| `DART_API_KEY` | Secrets Manager | `ocn/{env}/news-retrieval:DART_API_KEY` - Korea FSS OpenAPI key, free/self-service at opendart.fss.or.kr; required for the `dart_filing` source_type (`korea_market_signal` domain) |
| `FRED_API_KEY` | Secrets Manager | `ocn/{env}/news-retrieval:FRED_API_KEY` - St. Louis Fed API key, free/self-service at fred.stlouisfed.org/docs/api/api_key.html; required for `fetch-macro-signals` (`macro_signal` domain) |
| `EDINET_API_KEY` | Secrets Manager | `ocn/{env}/news-retrieval:EDINET_API_KEY` - EDINET (Financial Services Agency) v2 API subscription key, free/self-service registration at disclosure2.edinet-fsa.go.jp; required for `edinet_filing`/`edinet_buyback_status`/`edinet_extraordinary_report` source_types (`japan_market_signal` domain) - confirmed live the v2 API returns a hard 401 with no key at all |

### signal-detection
| Variable | Source | Notes |
|----------|--------|-------|
| `POSTGRES_HOST` | `rds_endpoint` | |
| `POSTGRES_PORT` | hardcoded | `5432` |
| `POSTGRES_DB` | hardcoded | `signal_detection_db` |
| `POSTGRES_USER` | hardcoded | `signal_user` |
| `POSTGRES_PASSWORD` | Secrets Manager | `ocn/{env}/signal-detection:POSTGRES_PASSWORD` |
| `AUTH_SERVICE_URL` | hardcoded | `http://auth-service.{env}.ocn.internal:8001` |
| `NEWS_RETRIEVAL_URL` | hardcoded | `http://news-retrieval.{env}.ocn.internal:8000` |
| `QDRANT_HOST` | `qdrant_host` var | |
| `QDRANT_PORT` | hardcoded | `6333` |
| `LANGFUSE_HOST` | hardcoded | `https://cloud.langfuse.com` |
| `OPENROUTER_API_KEY` | Secrets Manager | `ocn/{env}/signal-detection:OPENROUTER_API_KEY` |
| `QDRANT_API_KEY` | Secrets Manager | `ocn/{env}/signal-detection:QDRANT_API_KEY` |
| `LANGFUSE_SECRET_KEY` | Secrets Manager | `ocn/{env}/signal-detection:LANGFUSE_SECRET_KEY` |
| `LANGFUSE_PUBLIC_KEY` | Secrets Manager | `ocn/{env}/signal-detection:LANGFUSE_PUBLIC_KEY` |

## Guidance

- Read only the docs relevant to your task
- Use the Jira board (project key `CON`) to track and reference cards
- All secrets must exist in Secrets Manager before `terraform apply` - ECS task launch fails at runtime if a `valueFrom` ARN cannot be resolved
- When adding a new service, add its security group in `modules/security_groups`, its task definition and ECS service in `modules/ecs_cluster/services.tf`, and its log group in `modules/ecs_cluster/logs.tf`

### signal-detection-agent
| Variable | Source | Notes |
|----------|--------|-------|
| `POSTGRES_HOST` | `rds_endpoint` | |
| `POSTGRES_PORT` | hardcoded | `5432` |
| `POSTGRES_DB` | hardcoded | `signal_detection_db` - shared with signal-detection |
| `POSTGRES_USER` | hardcoded | `signal_user` - shared with signal-detection |
| `POSTGRES_PASSWORD` | Secrets Manager | `ocn/{env}/signal-detection:POSTGRES_PASSWORD` - shared with signal-detection |
| `PGSSLMODE` | hardcoded | `require` |
| `NEWS_RETRIEVAL_URL` | hardcoded | `http://news-retrieval.{env}.ocn.internal:8000` |
| `OPENAI_BASE_URL` | hardcoded | `https://openrouter.ai/api/v1` |
| `SIGNAL_DETECTION_MODEL` | hardcoded | `anthropic/claude-sonnet-4-6` |
| `SIGNAL_DETECTION_MODEL_V2` | hardcoded | `openai/gpt-4o-mini` - second pass only |
| `SEC_FILING_MODEL` | hardcoded | `openai/gpt-4.1` - SEC filing classification only; falls back to `SIGNAL_DETECTION_MODEL` if unset. Tested against 59 real filings: ~91-96% agreement with sonnet, plus a real hard-failure rate on NVDA 10-K/10-Q (schema-invalid `noise` + non-`none` materiality) - mitigated by normalizing `materiality` to `none` in `validate_filing_classification()` rather than rejecting. A residual ~4-9% label-disagreement rate vs sonnet on ambiguous NVDA deceleration judgment calls was not resolved by prompt tuning or a wider trailing-revenue window (sonnet itself is not fully stable on these same filings) - accepted as a known trade-off |
| `GEOPOLITICAL_SIGNAL_STAGE_C_MODEL` | hardcoded | `openai/gpt-4.1` - geopolitical_signal Stage C tagging only; falls back to `GEOPOLITICAL_SIGNAL_MODEL` → `SIGNAL_DETECTION_MODEL_V2` if unset. Set after measuring both tiers against all 19 real staging-tagged rows: gpt-4o-mini under-fills the structured arrays (empty `assets` on 9 of the 11 rows that also had no categories, e.g. no `usd` on a bank-sanctions story), while gpt-4.1 fills them correctly, recovers the Cuba shipping-container row and adds a correct second category on the Cuban nickel row. Neither tier invents categories for genuinely unmappable stories (sanctions on two named individuals stayed empty 5/5). Trade-off: gpt-4.1 is marginally less repeatable on one borderline row (4/5 runs) and ~15-20x the cost per call - immaterial at 0-3 Stage C rows/day against a 50/day cap |
| `OPENAI_API_KEY` | Secrets Manager | `ocn/{env}/signal-detection-agent:OPENAI_API_KEY` - OpenRouter key |

### api-gateway
| Variable | Source | Notes |
|----------|--------|-------|
| `GATEWAY_AUTH_URL` | hardcoded | `http://auth-service.{env}.ocn.internal:8001` |
| `GATEWAY_NEWS_URL` | hardcoded | `http://news-retrieval.{env}.ocn.internal:8000` |
| `GATEWAY_SIGNAL_URL` | hardcoded | `http://signal-detection.{env}.ocn.internal:8002` |
| `GATEWAY_SIGNAL_AGENT_URL` | hardcoded | `http://signal-detection-agent.{env}.ocn.internal:8003` |
| `GATEWAY_CORS_ORIGINS` | Terraform (`gateway_cors_origins` var) | Comma-separated list of allowed CORS origins; set to CloudFront URL in staging |

### lucky-clarke
| Variable | Source | Notes |
|----------|--------|-------|
| `SIGNAL_DETECTION_URL` | hardcoded | `http://signal-detection.{env}.ocn.internal:8002` |
| `LUCKY_CLARKE_URL` | hardcoded | `http://lucky-clarke.{env}.ocn.internal:8005` |
| `SIGNAL_CALLER_SUB` | hardcoded | `1` - internal service identity used in x-ocn-caller header |
| `OPENROUTER_MODEL` | hardcoded | `openai/gpt-4o-mini` |
| `AWS_REGION` | hardcoded | `eu-north-1` |
| `OPENROUTER_API_KEY` | Secrets Manager | `ocn/{env}/lucky-clarke:OPENROUTER_API_KEY` |
| `SMTP_HOST` | Secrets Manager | `ocn/{env}/lucky-clarke:SMTP_HOST` |
| `SMTP_USER` | Secrets Manager | `ocn/{env}/lucky-clarke:SMTP_USER` |
| `SMTP_PASSWORD` | Secrets Manager | `ocn/{env}/lucky-clarke:SMTP_PASSWORD` |
| `SMTP_FROM` | Secrets Manager | `ocn/{env}/lucky-clarke:SMTP_FROM` |

### signal-herald
| Variable | Source | Notes |
|----------|--------|-------|
| `SIGNAL_AGENT_URL` | hardcoded | `http://signal-detection-agent.{env}.ocn.internal:8003` |
| `SIGNAL_HERALD_URL` | hardcoded | `http://signal-herald.{env}.ocn.internal:8006` |
| `SIGNAL_CALLER_SUB` | hardcoded | `1` - internal service identity used in x-ocn-caller header |
| `OPENROUTER_MODEL` | hardcoded | `openai/gpt-4o-mini` |
| `OPENROUTER_API_KEY` | Secrets Manager | `ocn/{env}/signal-herald:OPENROUTER_API_KEY` |
| `SMTP_HOST` | Secrets Manager | `ocn/{env}/signal-herald:SMTP_HOST` |
| `SMTP_USER` | Secrets Manager | `ocn/{env}/signal-herald:SMTP_USER` |
| `SMTP_PASSWORD` | Secrets Manager | `ocn/{env}/signal-herald:SMTP_PASSWORD` |
| `SMTP_FROM` | Secrets Manager | `ocn/{env}/signal-herald:SMTP_FROM` |

## Maintenance

- Do not modify the Jira Board, Guidance, or Maintenance sections unless explicitly asked
### research-universe
| Variable | Source | Notes |
|----------|--------|-------|
| `POSTGRES_HOST` | `rds_endpoint` | |
| `POSTGRES_PORT` | hardcoded | `5432` |
| `POSTGRES_DB` | hardcoded | `research_universe_db` |
| `POSTGRES_USER` | hardcoded | `research_universe_user` |
| `POSTGRES_PASSWORD` | Secrets Manager | `ocn/{env}/research-universe:POSTGRES_PASSWORD` |
| `PGSSLMODE` | hardcoded | `require` |
| `OPENROUTER_MODEL` | hardcoded | `anthropic/claude-sonnet-4-6` |
| `OPENROUTER_API_KEY` | Secrets Manager | `ocn/{env}/research-universe:OPENROUTER_API_KEY` |
| `CORS_ORIGINS` | Secrets Manager | `ocn/{env}/research-universe:CORS_ORIGINS` - Lovable deployment URL |
| `API_PREFIX` | hardcoded | `/universe` - strips ALB path prefix |

**Secrets Manager pre-requisite:** Create `ocn/{env}/research-universe` with keys:
- `POSTGRES_PASSWORD` - DB password for `research_universe_user`
- `OPENROUTER_API_KEY` - OpenRouter API key
- `CORS_ORIGINS` - Lovable deployment URL (e.g. `https://your-app.lovable.app`)

**First deploy:** Run seed as a one-off ECS task after the service is up:
```bash
aws ecs run-task --cluster {env}-ocn \
  --task-definition {env}-research-universe \
  --overrides '{"containerOverrides":[{"name":"research-universe","command":["python","src/seed.py","--xlsx","/data/seed.xlsx"]}]}'
```

**Scheduled scan:** CloudWatch rule fires daily at 06:00 UTC → runs `python -m src scan-next` → scans the least recently enriched category → results land in pending_review queue.

**ALB routing:** Path prefix `/universe/*` → research-universe target group (port 8007). Lovable's API base URL is `http://{alb-dns}/universe`.

## Maintenance

- Do not modify the Jira Board, Guidance, or Maintenance sections unless explicitly asked
- Keep the Environment Variables tables above in sync with `modules/ecs_cluster/services.tf` whenever env vars are added, renamed, or removed
