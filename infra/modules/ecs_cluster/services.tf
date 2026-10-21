resource "aws_ecs_task_definition" "auth_service" {
  family                   = "${var.env}-auth-service"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn


  container_definitions = jsonencode([
    {
      name  = "auth-service"
      image = "${var.ecr_registry}/ocn/auth-service:${var.image_tag}"
      portMappings = [
        { containerPort = 8001 }
      ]
      environment = [
        { name = "AUTH_POSTGRES_HOST",       value = var.rds_endpoint },
        { name = "AUTH_POSTGRES_PORT",       value = "5432" },
        { name = "AUTH_POSTGRES_DB",         value = "auth_db" },
        { name = "AUTH_POSTGRES_USER",       value = "auth_user" },
        { name = "PGSSLMODE",                value = "require" },
        { name = "AUTH_JWT_EXPIRY_SECONDS",  value = "86400" }
      ]
      secrets = [
        {
          name      = "AUTH_POSTGRES_PASSWORD"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/auth-service:POSTGRES_PASSWORD::"
        },
        {
          name      = "AUTH_ADMIN_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/auth-service:ADMIN_API_KEY::"
        },
        {
          name      = "AUTH_JWT_PRIVATE_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/auth-service:JWT_PRIVATE_KEY::"
        },
        {
          name      = "ADMIN_USERNAME"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/auth-service:ADMIN_USERNAME::"
        },
        {
          name      = "ADMIN_EMAIL"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/auth-service:ADMIN_EMAIL::"
        },
        {
          name      = "ADMIN_PASSWORD"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/auth-service:ADMIN_PASSWORD::"
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.env}/auth-service"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  lifecycle {
    ignore_changes = [container_definitions]
  }
}


resource "aws_service_discovery_service" "auth_service" {
  name = "auth-service"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}


resource "aws_ecs_service" "auth_service" {
  name            = "${var.env}-auth-service"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.auth_service.arn
  desired_count   = 1
  launch_type     = "FARGATE"


  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.auth_sg_id]
    assign_public_ip = false
  }


  service_registries {
    registry_arn = aws_service_discovery_service.auth_service.arn
  }

  lifecycle {
    ignore_changes = [task_definition]
  }
}

resource "aws_ecs_task_definition" "news_retrieval" {
  family                   = "${var.env}-news-retrieval"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  # Bumped from 1024 after two real crashes (2026-09-23, 2026-09-24) during
  # geopolitical_news's title-dedup phase, confirmed via CloudWatch to climb
  # to ~680MB before the process died with no traceback - traced to
  # get_recent_articles_for_domain holding every embedding in its 7-day
  # comparison window (1500+ rows, ~30KB of JSON per row) fully in memory
  # for the whole dedup pass, growing as the domain's corpus grows. 2048
  # gives real headroom over the observed peak while a cheaper fix
  # (narrower window, more compact embedding storage) is still open -
  # revisit if this domain's corpus keeps growing and 2048 stops being
  # enough.
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task_exec_ssm.arn

  container_definitions = jsonencode([
    {
      name  = "news-retrieval"
      image = "${var.ecr_registry}/ocn/news-retrieval:${var.image_tag}"
      portMappings = [
        { containerPort = 8000 }
      ]
      environment = [
        { name = "POSTGRES_HOST",    value = var.rds_endpoint },
        { name = "POSTGRES_PORT",    value = "5432" },
        { name = "POSTGRES_DB",      value = "news_retrieval_db" },
        { name = "POSTGRES_USER",    value = "news_user" },
        { name = "PGSSLMODE",        value = "require" },
        { name = "AUTH_SERVICE_URL",        value = "http://auth-service.${var.env}.ocn.internal:8001" },
        { name = "NEWS_RETRIEVAL_URL",      value = "http://news-retrieval.${var.env}.ocn.internal:8000" },
        { name = "RESEARCH_UNIVERSE_URL",   value = "http://research-universe.${var.env}.ocn.internal:8007" },
        { name = "AWS_REGION",                    value = var.aws_region },
        { name = "DYNAMODB_TABLE_QUOTE",          value = "ocn-market-quote" },
        { name = "DYNAMODB_TABLE_OVERVIEW",       value = "ocn-market-overview" },
        { name = "DYNAMODB_TABLE_PRICE_HISTORY",  value = "ocn-market-price-history" },
        { name = "DYNAMODB_TABLE_EARNINGS",       value = "ocn-market-earnings" },
        { name = "DYNAMODB_TABLE_INDICES",        value = "ocn-market-indices" },
        { name = "DYNAMODB_TABLE_MARKET_STATUS",  value = "ocn-market-status" },
        { name = "DYNAMODB_TABLE_LOCK",           value = "ocn-market-lock" },
        { name = "DYNAMODB_TABLE_SEC_FILINGS",    value = "ocn-sec-filings" },
        { name = "DYNAMODB_TABLE_MACRO",          value = "ocn-market-macro" }
      ]
      secrets = [
        {
          name      = "POSTGRES_PASSWORD"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:POSTGRES_PASSWORD::"
        },
        {
          name      = "OPENROUTER_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:OPENROUTER_API_KEY::"
        },
        {
          name      = "SERPAPI_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:SERPAPI_KEY::"
        },
        {
          name      = "SERPAPI_KEY_GEOPOLITICAL"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:SERPAPI_KEY_GEOPOLITICAL::"
        },
        {
          name      = "NEWSAPI_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:NEWSAPI_KEY::"
        },
        {
          # newsapi.ai / Event Registry. NOT the same key as NEWSAPI_KEY
          # above (newsapi.org) - different vendor, different account.
          # Required for the eventregistry source_type, which is one of
          # the two sources the geopolitical_news domain fetches from
          # (the other is federal_register). Without it the fetch does
          # not fail - _fetch_sources logs "EVENT_REGISTRY_API_KEY not
          # set - skipping N eventregistry source(s)" and carries on, so
          # the domain silently runs on half its sources. Also used by
          # middle_east_market_signal's er-me:// rows.
          name      = "EVENT_REGISTRY_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:EVENT_REGISTRY_API_KEY::"
        },
        {
          name      = "ALPHA_VANTAGE_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:ALPHA_VANTAGE_API_KEY::"
        },
        {
          name      = "RESEARCH_UNIVERSE_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:RESEARCH_UNIVERSE_API_KEY::"
        },
        {
          # Required for the dart_filing source_type (korea_market_signal
          # domain) - DART (Financial Supervisory Service) OpenAPI key,
          # free/self-service at opendart.fss.or.kr. Must exist in
          # Secrets Manager under this key before terraform apply, same
          # requirement as every other secret above (infra/CLAUDE.md).
          # The Korea Customs press-release scraper (kr_customs_export
          # source_type) needs no credential at all - no-login scrape.
          name      = "DART_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:DART_API_KEY::"
        },
        {
          # Required for the edinet_filing / edinet_buyback_status /
          # edinet_extraordinary_report source_types (japan_market_signal
          # domain) - EDINET (Financial Services Agency) v2 API
          # subscription key, free/self-service at
          # disclosure2.edinet-fsa.go.jp. The v2 API returns a hard 401
          # with no key at all, so without this the three source_types
          # fetch nothing and J6's ownership rule has no rows to read.
          # Must exist in Secrets Manager under this key before terraform
          # apply, same requirement as every other secret above.
          name      = "EDINET_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:EDINET_API_KEY::"
        },
        {
          # Required for fetch-macro-signals (macro_signal domain) - St.
          # Louis Fed's FRED API key, free/self-service at
          # fred.stlouisfed.org/docs/api/api_key.html. Used for both
          # plain fred/series/observations calls and ALFRED first-print
          # vintage calls (macro_signal_fetch.py) - Treasury Fiscal
          # Data's own DTS endpoint (close_today_bal) needs no key. Must
          # exist in Secrets Manager under this key before terraform
          # apply, same requirement as every other secret above
          # (infra/CLAUDE.md).
          name      = "FRED_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:FRED_API_KEY::"
        },
        {
          # Required for edinet_filing/edinet_buyback_status/
          # edinet_extraordinary_report source_types (japan_market_signal
          # domain) - EDINET (Financial Services Agency) v2 API
          # subscription key, free/self-service registration at
          # disclosure2.edinet-fsa.go.jp - confirmed live 2026-09-27 the
          # v2 API returns a hard 401 with no key at all, unlike every
          # other Japan source_type (IRBANK/Kabutan/SEAJ/MONOist/Jiji/
          # newswitch.jp need no credential). Must exist in Secrets
          # Manager under this key before terraform apply, same
          # requirement as every other secret above (infra/CLAUDE.md).
          name      = "EDINET_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:EDINET_API_KEY::"
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.env}/news-retrieval"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  lifecycle {
    # task_role_arn is also ignored, not just container_definitions - it's a
    # sibling top-level attribute that Terraform's state has drifted on
    # (stuck pointing at ecs_task_execution while the live task now uses
    # ecs_task_exec_ssm for ECS exec support). Without this, a real
    # `terraform apply` would force-replace this task definition purely to
    # reconcile task_role_arn, rebuilding container_definitions from this
    # file's own template in the process - which bakes in image_tag's
    # unsafe "latest" default instead of the SHA-pinned image CI/CD
    # actually deployed. Same class of drift as container_definitions;
    # excluding it here closes that gap the same way.
    ignore_changes = [container_definitions, task_role_arn]
  }
}


resource "aws_service_discovery_service" "news_retrieval" {
  name = "news-retrieval"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}


resource "aws_ecs_service" "news_retrieval" {
  name                   = "${var.env}-news-retrieval"
  cluster                = aws_ecs_cluster.main.id
  task_definition        = aws_ecs_task_definition.news_retrieval.arn
  desired_count          = 1
  launch_type            = "FARGATE"
  enable_execute_command = true


  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.news_sg_id]
    assign_public_ip = false
  }


  service_registries {
    registry_arn = aws_service_discovery_service.news_retrieval.arn
  }

  lifecycle {
    ignore_changes = [task_definition]
  }
}

resource "aws_cloudwatch_event_rule" "news_retrieval_company_news_daily" {
  name                = "${var.env}-news-retrieval-company-news-daily"
  schedule_expression = "cron(0 1 * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_company_news_daily" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_company_news_daily.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "trigger", "--domain", "company_news", "--days-back", "1"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_geopolitical_news_daily" {
  name = "${var.env}-news-retrieval-geopolitical-news-daily"
  # Offset an hour after company_news (01:00 UTC) - geopolitical_news's GDELT
  # source runs 27 sequential theme queries with round-robin retries on rate
  # limiting, so it can take much longer than company_news's ticker fetch.
  schedule_expression = "cron(0 2 * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_geopolitical_news_daily" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_geopolitical_news_daily.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - CloudWatch resolves this to
    # whichever revision is currently ACTIVE at trigger time, so scheduled
    # runs never pin to a stale revision left behind in Terraform state
    # (this rule's target previously pinned revision 22 while the live
    # service had moved on to revision 38+, via CI/CD deploys outside TF).
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name = "news-retrieval"
        # --callback-url: notify signal-detection-agent the moment this
        # fetch actually finishes, so its geopolitical_signal pipeline
        # starts immediately rather than waiting on a fixed-offset guess
        # (signal_detection_agent_geopolitical_signal_pipeline_daily below
        # still runs on its own 04:00 UTC schedule too, as a fallback for
        # if this webhook delivery is ever missed - see that rule's own
        # comment and routes/webhooks.py in signal-detection-agent).
        #
        # -X faulthandler: confirmed live (2026-09-25, run_id=472) that
        # this domain's scheduled fetch can die completely silently - the
        # process's own log output and CloudWatch's lastIngestionTime both
        # stopped 11 seconds after start, with no exception, no traceback,
        # and no CloudTrail StopTask event; the run row was left stuck in
        # 'running' for 7+ hours until manually cleaned up. faulthandler
        # dumps a traceback to stderr (captured by the same awslogs driver
        # as everything else) on a fatal signal (SIGSEGV/SIGABRT/SIGBUS),
        # which a plain uncaught Python exception would already do without
        # it - so its absence here means this wasn't an ordinary Python
        # exception. Costs nothing when nothing crashes; only changes
        # behavior on an actual fatal-signal death, which is exactly the
        # case current logs give zero information about.
        command = [
          "python", "-X", "faulthandler", "__main__.py", "trigger",
          "--domain", "geopolitical_news", "--days-back", "1",
          "--callback-url", "http://signal-detection-agent.${var.env}.ocn.internal:8003/webhooks/news-retrieval-run-completed",
        ]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_fail_stuck_runs_hourly" {
  name = "${var.env}-news-retrieval-fail-stuck-runs-hourly"
  # Confirmed live (2026-09-25, run_id=472): a scheduled fetch can die
  # completely silently (no exception, no traceback, no CloudTrail
  # StopTask event) and leave its run row stuck in 'running' forever,
  # since the only existing cleanup (fail_orphaned_runs, models/runs.py)
  # runs once at server startup - useless here because this server had
  # been up 24+ hours with no restart, so run_id=472 sat stuck for 7+
  # hours until a human found and manually cleaned it up. This rule is
  # the periodic catch for that gap, independent of server restarts -
  # every domain's runs share one `runs` table, so this one schedule
  # covers all of them, not just geopolitical_news.
  schedule_expression = "cron(0 * * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_fail_stuck_runs_hourly" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_fail_stuck_runs_hourly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name = "news-retrieval"
        # 3 hours: comfortably longer than any real run has ever taken
        # (the longest observed, run_id=445, completed in under 90 min)
        # while still catching a stuck run well before a human would
        # otherwise notice.
        command = ["python", "__main__.py", "fail-stuck-runs", "--max-hours", "3"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_taiwan_market_signal" {
  name = "${var.env}-news-retrieval-taiwan-market-signal"
  # Every 4 hours, bounded to 01:00-16:00 UTC only (fires at 01,05,09,13
  # UTC), Mon-Fri (Taiwan has no weekend trading). The step must stay
  # bounded to this range, not "0/4" unbounded (which fires all 24 hours,
  # including well outside Taiwan trading/announcement hours - a mistake
  # caught before an earlier version of this schedule shipped). 01:00-05:30
  # UTC covers TWSE/TPEx's 9:00 AM-1:30 PM Taipei trading session; the
  # window extends through 16:00 UTC (midnight Taipei) because Taiwan's
  # 重大訊息 (material announcement) disclosures are confirmed to cluster
  # AFTER market close through the evening (5:30 PM Taipei / 09:30 UTC
  # onward), not during trading hours - a trading-hours-only window would
  # miss most of that feed. TWSE/TPEx's own full-dump endpoints have no
  # date/period query param (always return only the latest snapshot - see
  # pipeline.py's freshness check), so polling on a schedule + dedup on
  # ingest is the only way to capture each day's new revenue/announcement
  # rows; missing a day's poll window loses that day's data permanently,
  # there is no backfill - a 4-hour cadence (down from 30 min, then 2
  # hours) trades some of that margin for lower cost/load, on the
  # reasoning that revenue/material announcements don't need 30-minute
  # freshness the way a live price feed would.
  #
  # Does NOT account for Taiwan's own holiday calendar (e.g. multi-day
  # Lunar New Year closure), which is distinct from US holidays - on a
  # closed trading day this simply polls a market that isn't publishing
  # anything new, which is harmless (dedup prevents any duplicate
  # inserts) but not worth suppressing via a holiday-aware schedule for
  # this initial cut.
  schedule_expression = "cron(0 1-16/4 ? * MON-FRI *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_taiwan_market_signal" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_taiwan_market_signal.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "trigger", "--domain", "taiwan_market_signal", "--days-back", "1"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_korea_market_signal" {
  name = "${var.env}-news-retrieval-korea-market-signal"
  # Every 4 hours, unbounded across all 24 hours, all 7 days - unlike
  # Taiwan's schedule above, Korea's own sources don't concentrate in a
  # narrow trading-hours window. DART filings are lodged in the evening
  # KST and around weekend board meetings (KST is UTC+9, so Korea's own
  # business day runs roughly 23:00-14:00 UTC the previous calendar day);
  # the Korea Customs press-release scrape publishes at a fixed few-times-
  # a-month cadence (11th/21st/1st) with no time-of-day pattern to bound
  # around; RSS/GDELT news can break at any hour. A 4-hour, all-day, all-
  # week cadence matches the Korea Signals spec's own Step 2 instruction
  # ("Run from early morning to late evening, seven days a week") rather
  # than assuming a Taiwan-shaped trading session.
  #
  # DART's list.json IS date-range queryable (bgn_de/end_de), unlike
  # TWSE/TPEx's always-latest-snapshot endpoints - so a missed poll window
  # is recoverable on the next run (bounded by --days-back), not a
  # permanent gap the way Taiwan's revenue/material feed is. Still polled
  # on a schedule rather than backfilled after the fact, to keep same-day
  # freshness for time-sensitive items (S6 rumour-adjudication answers,
  # Tier 1 company filings) rather than relying on a later catch-up.
  schedule_expression = "cron(0 0/4 * * ? *)"
  # Paused 2026-09-23 (explicit request) - was previously disabled directly
  # via `aws events disable-rule`, which would have been silently reverted
  # by the next terraform apply touching this resource (state defaults to
  # ENABLED) - set here so the paused state actually persists through
  # Terraform. Set back to "ENABLED" (or remove this line) to resume.
  state = "DISABLED"
}

resource "aws_cloudwatch_event_target" "news_retrieval_korea_market_signal" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_korea_market_signal.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "trigger", "--domain", "korea_market_signal", "--days-back", "1"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_japan_market_signal_pre_open" {
  name = "${var.env}-news-retrieval-japan-market-signal-pre-open"
  # Twice daily, anchored to real Tokyo Stock Exchange session times
  # (09:00-15:00 JST, JST = UTC+9 year-round, no DST) rather than an
  # unbounded 24h poll - explicit user instruction 2026-09-30. None of
  # Japan's 9 (of 10) daily-frequency source_types are a live feed tied to
  # the trading session itself (IRBANK/Kabutan/EDINET/MONOist/press_jp are
  # all scraped/polled pages or a date-scoped API, not a market data feed
  # - see the design's own accepted same-day-to-next-day freshness
  # tradeoff, TDnet's real near-real-time cadence has no affordable free
  # path), but real disclosures still cluster around the session boundary
  # (overnight filings ahead of the open, same-day filings right after
  # close) the same way Taiwan's own 重大訊息 announcements cluster after
  # its market close - so this fetches ahead of the open to pick up
  # overnight filings before the session starts.
  #
  # 23:00 UTC = 08:00 JST, 1 hour before the 09:00 JST open - a deliberate
  # buffer, not a measured runtime, same "buffer, not a measurement"
  # caveat every other schedule comment in this file gives.
  schedule_expression = "cron(0 23 * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_japan_market_signal_pre_open" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_japan_market_signal_pre_open.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "trigger", "--domain", "japan_market_signal", "--days-back", "1"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_japan_market_signal_post_close" {
  name = "${var.env}-news-retrieval-japan-market-signal-post-close"
  # Second of the two daily passes - see the pre-open rule's own comment
  # above for the full real-market-hours rationale. --days-back 1 makes a
  # second same-day poll safe/idempotent (harmless re-poll, global URL
  # dedup on ingest prevents duplicate inserts - same reasoning the SEAJ
  # monthly rule's own comment already gives for why its own overlap with
  # the daily rule is harmless).
  #
  # 06:30 UTC = 15:30 JST, 30 minutes after the 15:00 JST close (TSE's
  # continuous session, inclusive of the 11:30-12:30 JST lunch break) -
  # catches same-day filings right after the session ends.
  #
  # --days-back 1 deliberately excludes seaj_billings (frequency_name
  # "monthly", min_days_back 30 - see load_sources' `f.min_days_back <=
  # days_back` gate in models/sources.py) - confirmed live this is
  # correct, not an oversight: seaj_billings is scheduled separately below
  # rather than folded into this daily --days-back 31, since EDINET's own
  # fetcher (unlike Kabutan/MONOist/press_jp) genuinely uses days_back as
  # a real per-day API loop (one documents.json call per day in the
  # window, times 3 modes: shareholding/buyback/extraordinary) - running
  # --days-back 31 daily just to catch a monthly source would multiply
  # EDINET's daily call volume 31x for no benefit, since EDINET's own
  # filings for a past day don't change after the fact.
  schedule_expression = "cron(30 6 * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_japan_market_signal_post_close" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_japan_market_signal_post_close.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "trigger", "--domain", "japan_market_signal", "--days-back", "1"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_japan_market_signal_monthly" {
  name = "${var.env}-news-retrieval-japan-market-signal-monthly"
  # This is the ONLY schedule that ever includes seaj_billings
  # (frequency_name "monthly", min_days_back 30 - see load_sources'
  # `f.min_days_back <= days_back` gate) - the daily rule above always
  # runs --days-back 1, which never satisfies that gate, so without this
  # rule seaj_billings would only ever run via a manual trigger. See the
  # daily rule's own comment for why this isn't instead folded into a
  # wider daily --days-back 31 (EDINET's fetcher genuinely loops per day
  # in the window, so that would multiply its daily API calls 31x for no
  # benefit).
  #
  # SEAJ's own fetcher (_fetch_seaj_billings) does not even take a
  # days_back parameter - it always fetches whatever the CURRENT release
  # on SEAJ's index page is (confirmed live ~6 trailing months per
  # release, so a missed month is recovered automatically once this rule
  # next fires) and relies on its own {period}-{qualifier} dedup key, so
  # --days-back 31 here exists only to satisfy load_sources' gate, not to
  # control what SEAJ itself returns.
  #
  # 25th of the month, 04:00 UTC - SEAJ's own English press release is
  # confirmed live to land on the 18th in the one real sample checked this
  # session, and this project's own SEAJ research separately found the
  # release cadence generally falls in the 20th-27th window - the 25th is
  # picked to land inside that window on a typical month, accepting that
  # an early-in-window release (like the 18th case observed) will be
  # caught up to 7 days late rather than same-day. Every OTHER source_type
  # in this domain also runs again on this date (this rule has no way to
  # fetch just seaj_billings in isolation without a pipeline.py change),
  # which is harmless - global URL dedup - but means this rule duplicates
  # the daily rule's own work once a month.
  schedule_expression = "cron(0 4 25 * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_japan_market_signal_monthly" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_japan_market_signal_monthly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "trigger", "--domain", "japan_market_signal", "--days-back", "31"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_taiwan_revenue_monthly" {
  name        = "${var.env}-news-retrieval-taiwan-revenue-monthly"
  description = "Re-read the last 3 months of Taiwan monthly revenue from MOPS CSVs into company_financials"
  # The daily taiwan_market_signal fetch reads the TWSE/TPEx OpenAPI feeds,
  # which only ever hold the latest month - a missed poll is otherwise lost
  # for good. This re-read recovers it and picks up restatements. Filings
  # are due by the 10th, so on the 15th the previous month is complete;
  # 3 months back covers a late filer plus a restatement window.
  schedule_expression = "cron(0 3 15 * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_taiwan_revenue_monthly" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_taiwan_revenue_monthly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "backfill-taiwan-revenue", "--months-back", "3"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_macro_signals_daily" {
  name        = "${var.env}-news-retrieval-macro-signals-daily"
  description = "Fetch macro_signal series (FRED + Treasury Fiscal Data) - --incremental (1-day window), twice daily"
  # 6am / 2pm Pacific (explicit request), pinned to PST (UTC-8) as fixed
  # UTC cron times - CloudWatch has no DST awareness, so this genuinely
  # drifts to 7am/3pm Pacific during PDT (summer, ~8 months of the
  # year). Accepted tradeoff (explicit choice over the PDT-pinned
  # alternative) rather than building DST-aware scheduling.
  # 14:00 UTC (6am PST) - morning fetch, ahead of US market open.
  # 22:00 UTC (2pm PST) - afternoon fetch, after FOMC decisions (14:00
  # ET / 18:00-19:00 UTC depending on DST) and market close (16:30 ET
  # stamp), so same-day moves are captured before the day is done.
  #
  # --incremental only fetches the trailing 1 day per series
  # (macro_signal_fetch.INCREMENTAL_LOOKBACK_DAYS) - stateless, upserted
  # safely over whatever's stored, so running it twice a day is exactly
  # as safe as running it once - no watermark to double-advance, no
  # duplicate-insert risk. The FULL 3-year backfill (fetch-macro-signals
  # with no flag) is NOT scheduled here - it is a one-time bootstrap,
  # run manually once per environment before this daily schedule is
  # ever enabled (see news-retrieval's CLI help text for
  # fetch-macro-signals). Enabling this rule against a database that
  # has never had the full backfill run leaves 728 of every series'
  # trailing 730-day z-score window empty.
  schedule_expression = "cron(0 14,22 * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_macro_signals_daily" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_macro_signals_daily.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "fetch-macro-signals", "--incremental"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_geopolitical_news_expire_weekly" {
  name = "${var.env}-news-retrieval-geopolitical-news-expire-weekly"
  # Sunday 04:00 UTC - 2 hours after the daily 02:00 UTC geopolitical_news
  # fetch, which typically completes by ~02:30-02:35 UTC, so this never
  # races with that day's ingest.
  schedule_expression = "cron(0 4 ? * SUN *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_geopolitical_news_expire_weekly" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_geopolitical_news_expire_weekly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "expire-articles", "--domain", "geopolitical_news", "--days", "7"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_china_market_signal_policy" {
  # Enabled 2026-10-11, after the backfill: 8,879 articles
  # (2022-08 to 2026-10), 84 baseline series, and a reclassify whose
  # distribution matched the local reference run. Explicit rather
  # than omitted - an absent `state` leaves whatever AWS already has,
  # so removing the DISABLED line did NOT re-enable these.
  state = "ENABLED"
  name = "${var.env}-news-retrieval-china-market-signal-policy"
  # Every 4 hours bounded to 00:00-12:00 UTC (08:00-20:00 Beijing, CST =
  # UTC+8 year-round, no DST), Mon-Fri. This is the one group where lag is
  # the product rather than a tolerance: the China Signals spec's own
  # Section 1 puts policy's lead time at "immediate - policy moves prices
  # the same hour", against two-to-four quarters for Japan's company
  # forecasts. A ministry announcement found eight hours late has already
  # been priced.
  #
  # Bounded rather than all-day because MOFCOM, MIIT, SAMR and CAC publish
  # during Beijing office hours - an overnight poll would re-walk the same
  # unchanged listings.
  #
  # EVERY DAY, not Mon-Fri. This rule was weekday-only on the assumption
  # that Chinese disclosure follows the working week. Measured against
  # the stored five years, it does not: 1,715 cninfo filings carry a
  # SATURDAY publication date, as many as a typical weekday (Mon 439,
  # Tue 1,795, Fri 1,830, Sat 1,758, Sun 93). A weekday-only schedule
  # left Saturday's filings to be picked up by Monday's wider window,
  # which worked but made the window carry the weekend rather than the
  # schedule. Running daily lets every window be --days-back 1.
  #
  # Not bounded to the TRADING session (01:30-07:00 UTC) the way the
  # filings rule below is: policy is published by ministries on their own
  # schedule and has no relationship to when the exchanges are open.
  schedule_expression = "cron(0 0-12/4 * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_china_market_signal_policy" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_china_market_signal_policy.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see the daily fetch target
    # above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name = "news-retrieval"
        # --days-back 2, not 1: the government listings carry no
        # publication TIME, only a date, so a run early in the UTC day
        # looking back exactly one day can miss an announcement published
        # late on the Beijing day that straddles the UTC boundary. The
        # extra day costs nothing (global URL dedup) and closes that seam.
        command = ["python", "__main__.py", "trigger", "--domain", "china_market_signal", "--days-back", "1"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_china_market_signal_filings" {
  state = "ENABLED"
  name = "${var.env}-news-retrieval-china-market-signal-filings"
  # Twice daily, anchored to the real mainland session (09:30-15:00 CST =
  # 01:30-07:00 UTC) in the same shape as Japan's pre-open/post-close
  # pair: 01:00 UTC catches anything filed overnight ahead of the open,
  # 08:00 UTC catches the post-close cluster. Chinese issuers file the
  # bulk of their announcements after the close, which is why the second
  # run sits an hour past it rather than at the bell.
  #
  # Runs the whole domain, not just the filing source_types - the trigger
  # command is domain-scoped and there is no per-source_type flag. That
  # means policy and press are re-fetched here too, which is harmless
  # (global URL dedup) and costs two extra listing walks a day.
  schedule_expression = "cron(0 1,8 * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_china_market_signal_filings" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_china_market_signal_filings.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name = "news-retrieval"
        # --days-back 1, because the rule now runs every day including
        # weekends. It was 3 to let Monday's run reach back over a
        # weekend of filings - real coverage (1,715 of the stored
        # filings are Saturday-dated) but carried by the window instead
        # of the schedule, which made every weekday run re-walk three
        # days to serve one.
        #
        # Widening costs nothing either way: cninfo and HKEX are both
        # date-RANGE queries, one request per company whatever the
        # window (measured: days_back 1, 3 and 30 all take the same
        # time). Unlike Japan's EDINET, which loops per day and is why
        # that domain keeps its window at 1 for the opposite reason.
        command = ["python", "__main__.py", "trigger", "--domain", "china_market_signal", "--days-back", "1"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_china_market_signal_monthly" {
  state = "ENABLED"
  name = "${var.env}-news-retrieval-china-market-signal-monthly"
  # The ONLY schedule that ever includes the two C6 trade sources
  # (comtrade_china_trade and nbs_ic_output, both frequency_name
  # "monthly", min_days_back 30) - the rules above run --days-back 2 and
  # 3, which never satisfy load_sources' `f.min_days_back <= days_back`
  # gate. Without this rule those two would only ever run by hand, the
  # same trap Japan's seaj_billings sits behind.
  #
  # 20th of the month, 05:00 UTC. NBS publishes the previous month's
  # industrial-output release (which carries the IC production figure)
  # around the 15th - confirmed live, the August 2026 figure landed
  # 15 September - so the 20th sits safely past it. Comtrade has no fixed
  # release day and lags 2-3 months regardless, so it is indifferent to
  # the date; the walk-back window in _fetch_comtrade_china_trade picks up
  # whatever has been published since the last run.
  #
  # Runs the whole domain again, like Japan's monthly rule - harmless
  # duplicate work once a month, resolved by global URL dedup.
  #
  # NOTE: nbs_ic_output is reachable ONLY from inside AWS. stats.gov.cn is
  # network-blocked from local dev machines (HTTP 403, a "blocked as per
  # the instructions of the Competent Government Authority" page), so this
  # scheduled run is the only path that actually produces that source - a
  # developer running the same command locally will see it fail open and
  # return nothing. See news-retrieval/CLAUDE.md.
  schedule_expression = "cron(0 5 20 * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_china_market_signal_monthly" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_china_market_signal_monthly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name = "news-retrieval"
        # 35, not 31: Comtrade's walk-back needs to reach past its own
        # 2-3 month publication lag, and _fetch_comtrade_china_trade
        # derives the number of months it attempts from days_back
        # (max(4, days_back//30 + 3)). 35 keeps that at the floor of 4
        # months while clearing the 30-day gate.
        command = ["python", "__main__.py", "trigger", "--domain", "china_market_signal", "--days-back", "35"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_middle_east_market_signal" {
  # DISABLED for now (2026-10-21): the fetch runs, but nothing classifies
  # this domain yet - signal-detection-agent's classifier is still being
  # built - so scheduled fetches would only accumulate unjudged articles
  # and spend newsapi.ai tokens. Set to "ENABLED" together with the
  # classify schedule once the classifier is live.
  state = "DISABLED"
  name  = "${var.env}-news-retrieval-middle-east-market-signal"
  # Twice daily, every day: 05:00 UTC (09:00 Gulf) picks up the overnight
  # war and shipping news and the Gulf morning's announcements; 13:00 UTC
  # (17:00 Gulf) closes the Gulf business day and lands before the US open
  # (13:30 UTC). Every day, not Mon-Fri - the Gulf week runs Sun-Thu and the
  # war does not stop at weekends.
  #
  # Two runs, not four: each run costs ~6 newsapi.ai tokens (shipping
  # takes 2 pages), and the domain shares a 2,000-token plan with
  # geopolitical_news.
  schedule_expression = "cron(0 5,13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_middle_east_market_signal" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_middle_east_market_signal.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name = "news-retrieval"
        # --days-back 1 with 12 hours between runs: each window overlaps
        # the previous run's, and the global url dedup stores nothing
        # twice. The pipeline also drops rows published more than
        # days_back + 1 days ago (Event Registry selects on crawl date).
        command = ["python", "__main__.py", "trigger", "--domain", "middle_east_market_signal", "--days-back", "1"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_company_news_expire_weekly" {
  name = "${var.env}-news-retrieval-company-news-expire-weekly"
  # Sunday 05:00 UTC - after the daily 01:00 UTC company_news fetch and
  # after geopolitical_news's own 04:00 UTC expiry job, so neither cleanup
  # job overlaps with a fetch or with each other.
  schedule_expression = "cron(0 5 ? * SUN *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_company_news_expire_weekly" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_company_news_expire_weekly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        # 30 days - matches the TTL already used for Alpha Vantage's other
        # data types (company overview, earnings) in DynamoDB, per poller.py.
        command = ["python", "__main__.py", "expire-articles", "--domain", "company_news", "--days", "30"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_vc_commentary_expire_weekly" {
  name = "${var.env}-news-retrieval-vc-commentary-expire-weekly"
  # Sunday 06:00 UTC - after the daily 03:00 UTC vc_commentary fetch and
  # after geopolitical_news (04:00 UTC) / company_news (05:00 UTC) expiry
  # jobs, so no two scheduled jobs overlap.
  schedule_expression = "cron(0 6 ? * SUN *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_vc_commentary_expire_weekly" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_vc_commentary_expire_weekly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        # 30 days - matches company_news's retention window.
        command = ["python", "__main__.py", "expire-articles", "--domain", "vc_commentary", "--days", "30"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_adverse_media_expire_weekly" {
  name = "${var.env}-news-retrieval-adverse-media-expire-weekly"
  # Sunday 06:15 UTC - offset from vc_commentary's own 06:00 UTC expiry job
  # above, so neither cleanup job overlaps with the other.
  schedule_expression = "cron(15 6 ? * SUN *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_adverse_media_expire_weekly" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_adverse_media_expire_weekly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        # 30 days - matches company_news's retention window.
        command = ["python", "__main__.py", "expire-articles", "--domain", "adverse_media", "--days", "30"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_ai_news_expire_weekly" {
  name = "${var.env}-news-retrieval-ai-news-expire-weekly"
  # Sunday 06:30 UTC - offset from adverse_media's own 06:15 UTC expiry job
  # above, so neither cleanup job overlaps with the other. ai_news has no
  # scheduled daily fetch job today (fetch is manual/CLI-only), so there is
  # no daily-job time to avoid colliding with here.
  schedule_expression = "cron(30 6 ? * SUN *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_ai_news_expire_weekly" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_ai_news_expire_weekly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        # 30 days - matches company_news's retention window.
        command = ["python", "__main__.py", "expire-articles", "--domain", "ai_news", "--days", "30"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_smart_money_expire_weekly" {
  name = "${var.env}-news-retrieval-smart-money-expire-weekly"
  # Sunday 06:45 UTC - offset from ai_news's own 06:30 UTC expiry job above,
  # so neither cleanup job overlaps with the other. smart_money has no
  # scheduled daily fetch job today (fetch is manual/CLI-only), same as
  # ai_news above.
  schedule_expression = "cron(45 6 ? * SUN *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_smart_money_expire_weekly" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_smart_money_expire_weekly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        # 30 days - matches company_news's retention window.
        command = ["python", "__main__.py", "expire-articles", "--domain", "smart_money", "--days", "30"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_vc_commentary_daily" {
  name                = "${var.env}-news-retrieval-vc-commentary-daily"
  schedule_expression = "cron(0 3 * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_vc_commentary_daily" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_vc_commentary_daily.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "trigger", "--domain", "vc_commentary", "--days-back", "1"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "news_retrieval_adverse_media_daily" {
  name                = "${var.env}-news-retrieval-adverse-media-daily"
  schedule_expression = "cron(0 3 * * ? *)"
}

resource "aws_cloudwatch_event_target" "news_retrieval_adverse_media_daily" {
  rule     = aws_cloudwatch_event_rule.news_retrieval_adverse_media_daily.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on the daily fetch
    # target above for why this is unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.news_retrieval.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets          = var.public_subnet_ids
      security_groups  = [var.news_sg_id]
      assign_public_ip = true
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "news-retrieval"
        command = ["python", "__main__.py", "trigger", "--domain", "adverse_media", "--days-back", "1"]
      }
    ]
  })
}

resource "aws_ecs_task_definition" "signal_detection" {
  family                   = "${var.env}-signal-detection"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn


  container_definitions = jsonencode([
    {
      name  = "signal-detection"
      image = "${var.ecr_registry}/ocn/signal-detection:${var.image_tag}"
      portMappings = [
        { containerPort = 8002 }
      ]
      environment = [
        { name = "POSTGRES_HOST",       value = var.rds_endpoint },
        { name = "POSTGRES_PORT",       value = "5432" },
        { name = "POSTGRES_DB",         value = "signal_detection_db" },
        { name = "POSTGRES_USER",       value = "signal_user" },
        { name = "PGSSLMODE",           value = "require" },
        { name = "AUTH_SERVICE_URL",    value = "http://auth-service.${var.env}.ocn.internal:8001" },
        { name = "NEWS_RETRIEVAL_URL",  value = "http://news-retrieval.${var.env}.ocn.internal:8000" },
        { name = "QDRANT_HOST",         value = var.qdrant_host },
        { name = "QDRANT_PORT",         value = "6333" },
        { name = "LANGFUSE_HOST",       value = "https://cloud.langfuse.com" }
      ]
      secrets = [
        {
          name      = "POSTGRES_PASSWORD"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/signal-detection:POSTGRES_PASSWORD::"
        },
        {
          name      = "OPENROUTER_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/signal-detection:OPENROUTER_API_KEY::"
        },
        {
          name      = "QDRANT_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/signal-detection:QDRANT_API_KEY::"
        },
        {
          name      = "LANGFUSE_SECRET_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/signal-detection:LANGFUSE_SECRET_KEY::"
        },
        {
          name      = "LANGFUSE_PUBLIC_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/signal-detection:LANGFUSE_PUBLIC_KEY::"
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.env}/signal-detection"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  lifecycle {
    ignore_changes = [container_definitions]
  }
}


resource "aws_service_discovery_service" "signal_detection" {
  name = "signal-detection"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}


resource "aws_ecs_service" "signal_detection" {
  name            = "${var.env}-signal-detection"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.signal_detection.arn
  # Stopped 2026-09-25 (explicit request) - was previously scaled to 0
  # directly via `aws ecs update-service`, which a plain terraform apply
  # would have silently reverted back to 1 (the state Terraform's own
  # config still expected) the next time infra touched this service -
  # set here so the paused state actually persists through Terraform,
  # same pattern as the CloudWatch schedule "Paused" comments elsewhere
  # in this file. Set back to 1 to resume.
  desired_count = 0
  launch_type   = "FARGATE"


  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.signal_sg_id]
    assign_public_ip = false
  }


  service_registries {
    registry_arn = aws_service_discovery_service.signal_detection.arn
  }

  lifecycle {
    ignore_changes = [task_definition]
  }
}

data "aws_secretsmanager_secret" "signal_detection_agent" {
  name = "ocn/${var.env}/signal-detection-agent"
}

resource "aws_ecs_task_definition" "signal_detection_agent" {
  family                   = "${var.env}-signal-detection-agent"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task_exec_ssm.arn

  container_definitions = jsonencode([
    {
      name  = "signal-detection-agent"
      image = "${var.ecr_registry}/ocn/signal-detection-agent:${var.image_tag}"
      portMappings = [
        { containerPort = 8003 }
      ]
      environment = [
        { name = "POSTGRES_HOST",          value = var.rds_endpoint },
        { name = "POSTGRES_PORT",          value = "5432" },
        { name = "POSTGRES_DB",            value = "signal_detection_db" },
        { name = "POSTGRES_USER",          value = "signal_user" },
        { name = "PGSSLMODE",              value = "require" },
        { name = "NEWS_RETRIEVAL_URL",     value = "http://news-retrieval.${var.env}.ocn.internal:8000" },
        # 30min, not the code default of 10min: an ai_news run normally takes
        # 4-7min, but a slow upstream day pushed one to 13.7min on 2026-09-30
        # and tripped the 10min ceiling, failing that day's signal-herald
        # digest even though the run itself completed fine minutes later.
        { name = "PIPELINE_POLL_TIMEOUT_SECS", value = "1800" },
        { name = "RESEARCH_UNIVERSE_URL",  value = "http://research-universe.${var.env}.ocn.internal:8007" },
        { name = "OPENAI_BASE_URL",        value = "https://openrouter.ai/api/v1" },
        { name = "SIGNAL_DETECTION_MODEL",    value = "anthropic/claude-sonnet-4-6" },
        { name = "SIGNAL_DETECTION_MODEL_V2", value = "openai/gpt-4o-mini" },
        { name = "SEC_FILING_MODEL",          value = "openai/gpt-4.1" },
        # geopolitical_signal Stage C only (channel/actors/assets/
        # impacted_categories tagging). Without this it inherits
        # SIGNAL_DETECTION_MODEL_V2 (gpt-4o-mini), which was measured
        # against all 19 real staging-tagged rows and under-fills the
        # structured array fields: gpt-4o-mini returned an empty "assets"
        # array on 9 of the 11 rows that also had no categories, including
        # obvious cases (US sanctions on Iranian banks -> no "usd"). gpt-4.1
        # populates those correctly, recovers the Cuba shipping-container
        # row (-> Compute Hardware & Edge Systems), and adds a correct
        # second category on the Cuban nickel row. It does NOT invent
        # categories for genuinely unmappable stories - Estonia's sanctions
        # on two named individuals stayed empty 5/5, which is the right
        # answer. Known trade-off: gpt-4.1 is marginally less repeatable on
        # one borderline row (Cuba shipping, 4/5 runs) where gpt-4o-mini is
        # deterministically empty, and costs ~15-20x per call - immaterial
        # here, since Stage C only runs on Stage B's HIGH rows (19 in total
        # to date, 0-3 per day) and is capped at 50/day anyway.
        { name = "GEOPOLITICAL_SIGNAL_STAGE_C_MODEL", value = "openai/gpt-4.1" }
      ]
      secrets = [
        {
          name      = "POSTGRES_PASSWORD"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/signal-detection:POSTGRES_PASSWORD::"
        },
        {
          name      = "OPENAI_API_KEY"
          valueFrom = "${data.aws_secretsmanager_secret.signal_detection_agent.arn}:OPENAI_API_KEY::"
        },
        {
          name      = "RESEARCH_UNIVERSE_API_KEY"
          valueFrom = "arn:aws:secretsmanager:${var.aws_region}:${var.aws_account_id}:secret:ocn/${var.env}/news-retrieval:RESEARCH_UNIVERSE_API_KEY::"
        }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.env}/signal-detection-agent"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  lifecycle {
    ignore_changes = [container_definitions, task_role_arn]
  }
}


resource "aws_service_discovery_service" "signal_detection_agent" {
  name = "signal-detection-agent"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}


# Daily SEC filing classification job. signal-detection-agent reads filing
# metadata (form_type, accession_number, primary_doc_url, item_codes, ...)
# and the tracked ticker list from news-retrieval's /market/* endpoints -
# it never calls SEC EDGAR's submissions API itself, only fetches each
# filing's body text directly (adapters/sec_edgar.py, text-fetch only).
# Scheduled for 13:00 UTC, one hour after news-retrieval's own sec_filings
# poller (12:00 UTC, see news-retrieval's CloudWatch rule) so this job always
# reads that day's freshly-fetched filings, not yesterday's. This runs AFTER
# signal-herald's unrelated 12:30 UTC digest run, so that day's filings won't
# appear in that day's digest. Note stage 2 (real classification) is
# currently commented out in filing_classifier.py, so this job only produces
# stage-1 narrative summaries for now.
resource "aws_cloudwatch_event_rule" "signal_detection_agent_filings_daily" {
  name                = "${var.env}-signal-detection-agent-filings-daily"
  description         = "Classify new SEC 8-K/10-Q/10-K filings (fetched by news-retrieval's poller) for the tracked ticker universe once daily"
  schedule_expression = "cron(0 13 * * ? *)"
  state               = "ENABLED"
}


resource "aws_cloudwatch_event_target" "signal_detection_agent_filings_daily" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_filings_daily.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on
    # news_retrieval_taiwan_market_signal's target above for why this is
    # unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "classify-filings"]
      }
    ]
  })
}


resource "aws_cloudwatch_event_rule" "signal_detection_agent_taiwan_signals" {
  name        = "${var.env}-signal-detection-agent-taiwan-signals"
  description = "Classify today's pooled taiwan_market_signal news-retrieval runs (rank/clause-lookup/translate/GDELT-classify), twice daily: post-Asia-close and pre-US-open"
  # 14:00 UTC = right after news-retrieval's last Taiwan fetch of the
  # window (01:00-16:00 UTC, see news_retrieval_taiwan_market_signal above)
  # has had time to land the evening 重大訊息 announcement cluster -
  # "post-Asia-close" pass.
  # 21:00 UTC = well before the next US trading day's pre-market (13:00 UTC
  # / 9am ET open) - "pre-US-open" pass, catching anything the 14:00 UTC
  # run missed plus same-day re-polls.
  # Both passes call classify-taiwan-signals with default from_date=to_date
  # =today (UTC), which pools ALL of today's completed runs so far (not
  # just the latest) and skips already-classified source_ids, so the two
  # runs are additive rather than duplicating work.
  schedule_expression = "cron(0 14,21 ? * MON-FRI *)"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_taiwan_signals" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_taiwan_signals.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on
    # news_retrieval_taiwan_market_signal's target above for why this is
    # unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "classify-taiwan-signals"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_korea_signals" {
  name        = "${var.env}-signal-detection-agent-korea-signals"
  description = "Classify today's pooled korea_market_signal news-retrieval runs (S2-S7 arithmetic/lookup + S7 model call + translate), twice daily"
  # news-retrieval fetches korea_market_signal every 4 hours, unbounded
  # across all 24h/7 days (00/04/08/12/16/20 UTC - see
  # news_retrieval_korea_market_signal above), unlike Taiwan's
  # trading-hours-bound schedule - so this rule's own timing isn't picking
  # a "market close" moment the way Taiwan's is, just spacing two passes
  # far enough apart to each pool a meaningful number of fetch cycles.
  #
  # Corrected 2026-09-23: an earlier version of this schedule (15:00/22:00
  # UTC) was designed by reasoning about UTC-relative-to-Taiwan's-own-
  # framing ("post-Asia-close, pre-US-open") without actually converting
  # to real KST (UTC+9) - converting it after the fact showed those two
  # times land at 00:00 and 07:00 KST, the middle of the Korean night and
  # early pre-dawn, not "after KST business hours close" as that version's
  # own comment incorrectly claimed. Recomputed properly below.
  #
  # 13:00 UTC = 22:00 KST (right after Korea's evening filing window - the
  # spec's own "filings lodged in the evening, Korea time" - and right
  # after the 12:00 UTC fetch has landed, pooling 00/04/08/12 UTC) AND
  # 09:00 ET / 13:30 UTC summary = right at US market open (9:30am ET) -
  # a trader has the digest before the US trading day starts.
  # 21:00 UTC = 06:00 KST (right after the 20:00 UTC fetch, catching
  # Korea's very early morning filings before that day's Korean business
  # hours even start) AND 17:00 ET / 21:30 UTC summary = end of the US
  # trading day - a second checkpoint before the desk closes out.
  # Both passes call classify-korea-signals with default from_date=to_date=
  # today (UTC), which pools ALL of today's completed runs so far (not just
  # the latest) and skips already-classified source_ids (via the
  # idx_agent_classifications_korea_source_id unique index - see db.py), so
  # the two runs are additive rather than duplicating work, same pattern as
  # Taiwan's own two-pass schedule.
  schedule_expression = "cron(0 13,21 * * ? *)"
  # Paused 2026-09-23 (explicit request) - see
  # news_retrieval_korea_market_signal's own comment on why this is set
  # here rather than left as a CLI-only disable-rule call.
  state = "DISABLED"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_korea_signals" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_korea_signals.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    # Family-only ARN (no revision suffix) - same reasoning as Taiwan's own
    # target above.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "classify-korea-signals"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_korea_signals_summary" {
  name        = "${var.env}-signal-detection-agent-korea-signals-summary"
  description = "Generate the spec Section 8.4 twice-daily trader summary from today's already-classified korea_market_signal rows"
  # 30 minutes after each classify-korea-signals pass (13:00/21:00 UTC
  # above), not at the same time - classification itself takes real time
  # (S7's own per-article model call, plus translation, over however many
  # articles pooled that run), so summarizing at the exact same timestamp
  # risks reading a DB that classification hasn't finished writing to
  # yet. 30 minutes is a deliberate buffer, not a measured worst-case
  # runtime - revisit if a real run is ever confirmed to still be
  # in-flight past this offset.
  # 13:30 UTC = 9:30am ET, right at US market open - the trader has the
  # digest before the trading day starts. 21:30 UTC = 5:30pm ET, just
  # after the US trading day ends - a second checkpoint before close-out.
  # summarize-korea-signals only reads agent_classifications (no
  # news-retrieval fetch, no LLM classification calls of its own beyond
  # the one summary-writer call) - so this is a fast job regardless of
  # how large the classify pass was.
  schedule_expression = "cron(30 13,21 * * ? *)"
  # Paused 2026-09-23 (explicit request) - see
  # news_retrieval_korea_market_signal's own comment on why this is set
  # here rather than left as a CLI-only disable-rule call.
  state = "DISABLED"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_korea_signals_summary" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_korea_signals_summary.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "summarize-korea-signals"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_japan_signals_pre_open" {
  name        = "${var.env}-signal-detection-agent-japan-signals-pre-open"
  description = "Classify pooled japan_market_signal news-retrieval runs (J1-J7 arithmetic/lookup + J7 model call + translate) - first of two daily passes, before TSE market open"
  # Twice daily now, anchored to real TSE market hours the same way the
  # two news-retrieval fetch rules above are (09:00-15:00 JST open/close,
  # JST = UTC+9) - explicit user instruction 2026-09-30, replacing the
  # earlier single 05:00 UTC pass.
  #
  # 00:00 UTC = 1 hour after the 23:00 UTC pre-open fetch (news_retrieval_
  # japan_market_signal_pre_open above) - same "generous buffer, not a
  # measured runtime" reasoning every other classify/fetch offset in this
  # file uses; real IRBANK/Kabutan/EDINET/MONOist/press_jp article volume
  # for a --days-back 1 daily run is small (a single day's worth of
  # filings across 20 tracked companies), so 1 hour is ample.
  #
  # classify-japan-signals defaults from_date=to_date=today (UTC), which
  # pools ALL of today's completed japan_market_signal runs so far and
  # skips already-classified source_ids via the same dedup convention
  # every other domain here uses (get_existing_japan_signal_source_ids),
  # so this pass and the post-close pass below are additive/idempotent,
  # not duplicative, even though both fall on the same UTC calendar day.
  schedule_expression = "cron(0 0 * * ? *)"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_japan_signals_pre_open" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_japan_signals_pre_open.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "classify-japan-signals"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_japan_signals_post_close" {
  name        = "${var.env}-signal-detection-agent-japan-signals-post-close"
  description = "Classify pooled japan_market_signal news-retrieval runs (J1-J7 arithmetic/lookup + J7 model call + translate) - second of two daily passes, after TSE market close"
  # 08:30 UTC = 2 hours after the 06:30 UTC post-close fetch
  # (news_retrieval_japan_market_signal_post_close above) - same buffer
  # reasoning as the pre-open pass above.
  schedule_expression = "cron(30 8 * * ? *)"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_japan_signals_post_close" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_japan_signals_post_close.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "classify-japan-signals"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_china_signals_filings" {
  state = "ENABLED"
  name = "${var.env}-signal-detection-agent-china-signals-filings"
  description = "Classify pooled china_market_signal news-retrieval runs (Gate 1 triage, C1-C7, read-through, confirmation, translate) - after the daily mainland filings fetches"
  # Anchored to the three news-retrieval china_market_signal fetch rules
  # above, not to a round number: without this rule the domain fetches
  # on all three and classifies on none, so articles accumulate
  # unjudged.
  #
  # 10:00 UTC = 2 hours after the 08:00 UTC post-close filings fetch
  # (news_retrieval_china_market_signal_filings, cron(0 1,8 ...)), which
  # covers the 01:30-07:00 UTC mainland session. Also lands after the
  # 08:00 UTC policy fetch in the 4-hourly series, so one pass picks up
  # both that morning's filings and the policy/press of the preceding
  # 12 hours. Two hours rather than one because cninfo and HKEX fetches
  # pull filing PDFs per company, which is slower than Japan's
  # equivalent - same "generous buffer, not a measured runtime"
  # convention every other offset in this file uses.
  #
  # classify-china-signals defaults from_date=to_date=today (UTC),
  # pooling ALL of today's completed china_market_signal runs and
  # skipping already-classified source_ids
  # (get_existing_china_signal_source_ids), so this pass and the
  # evening one below are additive and idempotent, not duplicative,
  # even though both fall on the same UTC calendar day.
  #
  # EVERY DAY, matching the fetch rules above. Both were weekday-only
  # until the stored history was checked: Chinese issuers file at
  # weekends (1,715 Saturday-dated cninfo filings against 1,830 on
  # Friday), so a Mon-Fri classify pass left Saturday's fetch unjudged
  # until Monday - and since the window selects RUNS by date, Monday's
  # pass pools Monday's runs, never the weekend's. Those rows were
  # orphaned permanently rather than merely delayed.
  schedule_expression = "cron(0 10 * * ? *)"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_china_signals_filings" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_china_signals_filings.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "classify-china-signals"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_china_baselines" {
  state = "ENABLED"
  name = "${var.env}-signal-detection-agent-china-baselines"
  description = "Recompute China C2/C3 per-company baselines from stored history, and re-fetch the underlying revenue figures"
  # THIS RULE ONLY REFETCHES. Rebuilding the baselines themselves is
  # no longer its job - classify-china-signals does that in-process
  # before every pass, so a company filing on the 1st is judged the
  # same day against a baseline that includes everything up to that
  # morning rather than waiting for the 22nd. The two cannot be
  # separate rules: a verdict is written once and the next pass skips
  # that source_id, so anything judged while the cache was stale keeps
  # that verdict permanently.
  #
  # What is left here is the one half that genuinely cannot run daily:
  # --refetch pulls revenue from cninfo and Alpha Vantage, one request
  # per company (~90s against ~5s for a rebuild from stored figures).
  # Those figures only move when a company files - quarterly at most,
  # and for half this universe twice a year - so a monthly pull is
  # already more often than the data changes.
  #
  # The 22nd: mainland interim and annual reports cluster around the
  # end of a month, and the 22nd leaves a company's newest filing
  # fetched and stored before the figures are re-pulled from it.
  #
  # Without this rule the daily rebuild still runs, but on revenue
  # figures that stop being refreshed - C2 would fall back to what it
  # can extract from filing text for the 18 companies that have a
  # structured source.
  schedule_expression = "cron(0 3 22 * ? *)"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_china_baselines" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_china_baselines.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "refresh-china-baselines", "--years", "5", "--refetch"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_china_signals_evening" {
  state = "ENABLED"
  name = "${var.env}-signal-detection-agent-china-signals-evening"
  description = "Classify pooled china_market_signal news-retrieval runs - second daily pass, catching the afternoon/evening policy and press fetches"
  # 14:00 UTC = 22:00 Beijing, after the 12:00 UTC fetch that ends the
  # policy rule's 00:00-12:00 UTC 4-hourly series (cron(0 0-12/4 ...)).
  # That series exists because MOFCOM/MIIT/SAMR publish through the
  # Beijing working day and 重大事项 filings cluster after close; a
  # single morning classify pass would leave everything published after
  # 08:00 UTC waiting until the next day.
  #
  # Also the pass that picks up the monthly comtrade_china_trade and
  # nbs_ic_output rows: that fetch runs 05:00 UTC on the 20th
  # (news_retrieval_china_market_signal_monthly), well before this.
  schedule_expression = "cron(0 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_china_signals_evening" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_china_signals_evening.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "classify-china-signals"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_japan_habits_refresh" {
  name        = "${var.env}-signal-detection-agent-japan-habits-refresh"
  description = "Recompute japan_company_habits/japan_progress_habits/japan_company_reference from the FULL pooled history - occasional, not per-classification-pass"
  # Monthly, not daily - see refresh-japan-habits' own CLI docstring
  # (__main__.py): "a company's real multi-year habit does not
  # meaningfully change day to day", so this is intentionally much rarer
  # than the daily classify pass above, same "occasional refresh, not
  # every pass" design already documented for this command.
  #
  # 25th of the month, 05:00 UTC - same DAY-of-month as news_retrieval_
  # japan_market_signal_monthly's own SEAJ recovery rule above (also the
  # 25th), so a fresh habit refresh always runs the day after that
  # month's SEAJ recovery has had a full day to land (that rule fires at
  # 04:00 UTC on the 25th; this one is a SEPARATE calendar day - the
  # 25th here refers to the day this rule's cron fires, immediately
  # following the 03:00 UTC daily fetch's own regular run, not
  # specifically chained to the SEAJ rule's exact hour) - picked for
  # calendar-alignment convenience (one memorable day-of-month for every
  # monthly Japan Signals job), not a hard dependency between the two.
  schedule_expression = "cron(0 5 25 * ? *)"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_japan_habits_refresh" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_japan_habits_refresh.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "refresh-japan-habits"]
      }
    ]
  })
}

# Weekly classification retention for the domain signal-detection-agent
# actually consumes this way (ai_news -> source_type='news'). sec_filing and
# taiwan_market_signal are intentionally excluded - neither has a
# source-side expiry in news-retrieval today. Runs 1 hour after its
# corresponding news_retrieval_*_expire_weekly rule, so classifications are
# only ever expired after their source article has already been deleted,
# never the other way around. Deliberately kept longer than its
# news-retrieval source's own article retention - news: 180 days here vs 30
# days for ai_news - a classification is allowed to outlive its source
# article.
resource "aws_cloudwatch_event_rule" "signal_detection_agent_news_expire_weekly" {
  name                = "${var.env}-signal-detection-agent-news-expire-weekly"
  description         = "Delete news classifications older than 180 days (longer than news-retrieval's 30-day ai_news article retention, by design)"
  schedule_expression = "cron(30 7 ? * SUN *)"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_news_expire_weekly" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_news_expire_weekly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "expire-classifications", "--source-type", "news", "--days", "180"]
      }
    ]
  })
}

# geopolitical_signal funnel: Stage A (rule-based) -> Stage B (talk vs
# action LLM call, the one IRREVERSIBLE verdict - weak_signal/noise never
# reach a later stage) -> Stage C (channel/actors/assets/category tagging,
# reads the geopolitical_signal_companies cache) -> Stage D (corroboration
# grading, no model call). Stages A-D are NOT independent jobs - B only
# acts on rows A just wrote ('waiting'), C only on rows B just wrote
# ('signal'/HIGH), D only on rows C tagged - so they run as one CLI command
# (run-geopolitical-signal-pipeline) inside a single scheduled task rather
# than four separately-scheduled triggers. Separate cron rules risked a
# later stage firing before the prior stage's run had actually finished
# (e.g. Stage A running long on a heavy news day); a single sequential
# command removes that race entirely.
#
# Stage E (story grouping - groups signals reporting DIFFERENT events in
# one developing situation, marking a seed row with
# metadata.story_development and the rest with metadata.in_story) runs
# LAST in this same sequence, and also in the webhook path in
# signal-detection-agent's routes/webhooks.py. Both, not one: every API
# read of this domain is expected to be post-Stage-E, because the default
# signals view filters on NOT (metadata ? 'in_story') - if E never ran,
# that filter would match every row and the reader would see one story
# repeated under several headlines. Any new trigger path must include it.
#
# Unlike A-D it is not scoped to the rows the previous stage just wrote:
# grouping is a property of the whole signal set, so it reads every
# Stage-D-graded signal and regroups from scratch, clearing its own two
# keys first. That makes a second run in the same day safe - which this
# rule relies on, since it is the fallback for the webhook and both can
# fire. Measured on the live 57-signal set: a re-run changed 0 of 57 rows.
#   01:00 - company/category cache refresh (research-universe -> cache
#            table). Kept on its own schedule, independent of the news
#            cycle - company/ticker/category data changes rarely, so it
#            doesn't need to run every time the pipeline does. It runs
#            BEFORE the 02:00 fetch so that the PRIMARY path - the
#            webhook pipeline, which starts the moment that fetch
#            finishes - reads a cache about an hour old. Refreshing
#            after the fetch instead left the primary run on a
#            ~23-hour-old cache and gave the fresh one only to the
#            fallback, which is the run that matters least. Also
#            guarantees the cache is populated before a first-ever
#            deploy's first pipeline run.
#   03:00 - Stage A -> B -> C -> D -> E, in sequence, in one task.
#            This is a FALLBACK, not the primary trigger: the
#            primary path is news-retrieval's own --callback-url webhook
#            (news_retrieval_geopolitical_news_daily above passes
#            /webhooks/news-retrieval-run-completed as its callback),
#            which kicks off this same pipeline in signal-detection-
#            agent's running server process the moment that day's fetch
#            actually completes - correct regardless of how long the
#            fetch takes, unlike a fixed offset from news-retrieval's own
#            02:00 UTC schedule. This 05:00 UTC rule stays as a safety
#            net for if that webhook delivery is ever lost (it's
#            fire-and-forget on news-retrieval's side - no retry, see
#            _fire_webhook in news-retrieval/src/controllers/run.py) -
#            running the pipeline twice in one day is harmless, since
#            every stage's worklist query only selects rows the previous
#            stage hasn't processed yet.
#            HEADROOM: 03:00 leaves ~48 minutes after the 02:00 fetch
#            starts. Measured fetch durations are 4.9-11.4 minutes, so
#            that is ample today, but the margin is the thing to check
#            first if this rule ever fires while a fetch is still
#            writing - Stage A would then pool a partial article list.
#            news-retrieval's vc_commentary and adverse_media fetches
#            also run at 03:00; different containers, so no conflict,
#            but they share the cluster.
resource "aws_cloudwatch_event_rule" "signal_detection_agent_geopolitical_signal_cache_refresh_daily" {
  name                = "${var.env}-signal-detection-agent-geo-signal-cache-refresh-daily"
  description         = "Refresh geopolitical_signal_companies cache (ticker/name/category) from research-universe"
  schedule_expression = "cron(0 1 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_geopolitical_signal_cache_refresh_daily" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_geopolitical_signal_cache_refresh_daily.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "refresh-geopolitical-category-map"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_geopolitical_signal_pipeline_daily" {
  name                = "${var.env}-signal-detection-agent-geo-signal-pipeline-daily"
  description         = "geopolitical_signal Stage A -> B -> C -> D -> E, run sequentially in one task"
  schedule_expression = "cron(0 3 * * ? *)"
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_geopolitical_signal_pipeline_daily" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_geopolitical_signal_pipeline_daily.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "run-geopolitical-signal-pipeline"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_macro_signal_fomc_calendar_refresh_weekly" {
  name        = "${var.env}-signal-detection-agent-macro-fomc-calendar-weekly"
  description = "Refresh fomc_meeting_dates from federalreserve.gov/json/calendar.json - needed for DFF's inter-meeting move detection"
  # Weekly (explicit request, downgraded from an original twice-daily
  # schedule) - the Fed's own published meeting calendar is set months
  # in advance and is effectively static day to day; refreshing more
  # than weekly buys nothing real, since there is no plausible scenario
  # where a same-day or even next-day refresh would catch something a
  # week's delay would miss. The pipeline's own staleness guard
  # (calendar_staleness_warning, see pipeline/macro_signal_fomc_
  # calendar.py) only warns after 30 days without a refresh, which
  # itself implies weekly is generous margin, not a risk.
  # Sunday 14:30 UTC (6:30am PST, pinned to PST - see
  # news_retrieval_macro_signals_daily's own comment on the DST
  # tradeoff) - ahead of Monday's first pipeline pass, same "land
  # before the run that consumes it" reasoning as the old daily
  # schedule, just no longer duplicated across every single day.
  schedule_expression = "cron(30 14 ? * SUN *)"
  state                = "ENABLED"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_macro_signal_fomc_calendar_refresh_weekly" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_macro_signal_fomc_calendar_refresh_weekly.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "refresh-fomc-calendar"]
      }
    ]
  })
}

# Two SEPARATE rules (not one twice-daily cron) because each pass needs a
# DIFFERENT date argument - run-macro-signal-pipeline_cmd's default is
# always "yesterday" (UTC), which is right for the morning pass but wrong
# for the afternoon pass (explicit request: catch a same-day FOMC/data/
# market move THE SAME DAY, not the next day) - see
# run_macro_signal_pipeline_cmd's own --today flag docstring in
# __main__.py for why a plain twice-daily default-args schedule would not
# actually deliver same-day latency.
resource "aws_cloudwatch_event_rule" "signal_detection_agent_macro_signal_pipeline_after_close" {
  name        = "${var.env}-signal-detection-agent-macro-signal-pipeline-after-close"
  description = "Z-SCORE -> TIER -> SUPPRESS -> COLLAPSE -> INTERPRET for macro_signal, evaluating TODAY's date - the same-day-latency pass"
  # 23:00 UTC (3pm PST) - 1 hour after news-retrieval's own afternoon
  # macro fetch (22:00 UTC), giving that fetch (46 series, hundreds of
  # real FRED/Treasury HTTP calls with rate-limit backoff - see
  # macro_signal_fetch.py's own _fred_get retry comment) generous
  # margin to finish before this reads from it. --today makes this
  # evaluate TODAY's UTC date rather than the plain "yesterday" default
  # - by 23:00 UTC, a market-close series' today observation already
  # exists (16:30 ET / 20:30-21:30 UTC stamp), and FOMC/most 8:30 ET
  # data releases have long since landed, so this is the pass that
  # actually catches a same-day event on the day it happened.
  schedule_expression = "cron(0 23 * * ? *)"
  state                = "ENABLED"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_macro_signal_pipeline_after_close" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_macro_signal_pipeline_after_close.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "run-macro-signal-pipeline", "--today"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_macro_signal_pipeline_morning" {
  name        = "${var.env}-signal-detection-agent-macro-signal-pipeline-morning"
  description = "Z-SCORE -> TIER -> SUPPRESS -> COLLAPSE -> INTERPRET for macro_signal, evaluating YESTERDAY's date - the morning catch-up pass"
  # 15:00 UTC (7am PST) - 1 hour after news-retrieval's own morning
  # macro fetch (14:00 UTC), same margin reasoning as the afternoon
  # pass above. Uses the plain default (no --today) - by design this
  # pass still evaluates YESTERDAY's UTC date, covering the tail end of
  # the prior US day that the 23:00 UTC pass's own knowledge_time
  # cutoff may have missed, and safely re-confirming anything the
  # 23:00 UTC pass already caught (dedup'd by source_id - see
  # insert_macro_signal_event's own docstring, harmless to re-evaluate
  # the same date twice).
  schedule_expression = "cron(0 15 * * ? *)"
  state                = "ENABLED"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_macro_signal_pipeline_morning" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_macro_signal_pipeline_morning.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "run-macro-signal-pipeline"]
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "signal_detection_agent_macro_signal_confirmation_recheck_daily" {
  name        = "${var.env}-signal-detection-agent-macro-confirmation-recheck-daily"
  description = "Suppression mechanism C - retroactively suppress weekly/monthly macro_signal events that have since fully reversed at their next observation"
  # Once daily (not twice) - unlike the main pipeline, this pass is
  # retroactive by nature (see run_macro_signal_confirmation_recheck's
  # own docstring: checking already-stored events against a next
  # observation that usually doesn't exist yet for hours-to-weeks after
  # the original event), so there is no same-day-latency argument for
  # running it twice - a weekly/monthly series' next print isn't going
  # to arrive in the hours between the morning and afternoon passes.
  # 16:00 UTC (8am PST) - 1 hour after the morning pipeline pass (15:00
  # UTC above), the same relative offset the original single-daily
  # schedule used. default --since-date is 60 days ago when omitted,
  # generous enough to cover even a monthly series' full confirmation
  # window with margin.
  schedule_expression = "cron(0 16 * * ? *)"
  state                = "ENABLED"
}

resource "aws_cloudwatch_event_target" "signal_detection_agent_macro_signal_confirmation_recheck_daily" {
  rule     = aws_cloudwatch_event_rule.signal_detection_agent_macro_signal_confirmation_recheck_daily.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_detection_agent.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_detection_agent_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-detection-agent"
        command = ["python", "-m", "src", "recheck-macro-signal-confirmations"]
      }
    ]
  })
}


resource "aws_ecs_service" "signal_detection_agent" {
  name            = "${var.env}-signal-detection-agent"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.signal_detection_agent.arn
  desired_count   = 1
  launch_type     = "FARGATE"
  # enable_execute_command / deployment_maximum_percent /
  # deployment_minimum_healthy_percent set explicitly here (2026-09-25) to
  # match what was actually live in AWS (confirmed via aws ecs
  # describe-services) rather than the reverse - this service's real
  # settings had drifted from the previous config's own defaults, and
  # applying that stale config would have silently disabled ECS Exec
  # (shell/debug access to the running containers) and changed rollout
  # behavior as an unrelated side effect of this same apply.
  enable_execute_command             = true
  deployment_maximum_percent         = 100
  deployment_minimum_healthy_percent = 0

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.signal_detection_agent_sg_id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.signal_detection_agent.arn
  }

  lifecycle {
    ignore_changes = [task_definition]
  }
}



resource "aws_iam_role" "ecs_events" {
  name = "${var.env}-ecs-events-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}


resource "aws_iam_role_policy" "ecs_events_run_task" {
  name = "${var.env}-ecs-events-run-task"
  role = aws_iam_role.ecs_events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["ecs:RunTask"]
        Resource = [
          "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/staging-signal-detection:*",
          "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/staging-signal-herald:*",
          "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/staging-research-universe:*",
          "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/staging-news-retrieval:*",
          "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/staging-signal-detection-agent:*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [
          aws_iam_role.ecs_task_execution.arn,
          aws_iam_role.ecs_task_exec_ssm.arn,
        ]
      }
    ]
  })
}




resource "aws_ecs_task_definition" "api_gateway" {
  family                   = "${var.env}-api-gateway"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn


  container_definitions = jsonencode([
    {
      name  = "api-gateway"
      image = "${var.ecr_registry}/ocn/api-gateway:${var.image_tag}"
      portMappings = [
        { containerPort = 8004 }
      ]
      environment = [
        { name = "GATEWAY_AUTH_URL",         value = "http://auth-service.${var.env}.ocn.internal:8001" },
        { name = "GATEWAY_NEWS_URL",         value = "http://news-retrieval.${var.env}.ocn.internal:8000" },
        { name = "GATEWAY_SIGNAL_URL",       value = "http://signal-detection.${var.env}.ocn.internal:8002" },
        { name = "GATEWAY_SIGNAL_AGENT_URL", value = "http://signal-detection-agent.${var.env}.ocn.internal:8003" },
        { name = "GATEWAY_CORS_ORIGINS",     value = var.gateway_cors_origins }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.env}/api-gateway"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  lifecycle {
    ignore_changes = [container_definitions]
  }
}


resource "aws_service_discovery_service" "api_gateway" {
  name = "api-gateway"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}


resource "aws_ecs_service" "api_gateway" {
  name            = "${var.env}-api-gateway"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api_gateway.arn
  desired_count   = 1
  launch_type     = "FARGATE"


  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.gateway_sg_id]
    assign_public_ip = false
  }


  load_balancer {
    target_group_arn = var.api_gateway_tg_arn
    container_name   = "api-gateway"
    container_port   = 8004
  }


  service_registries {
    registry_arn = aws_service_discovery_service.api_gateway.arn
  }

  lifecycle {
    ignore_changes = [task_definition]
  }
}


data "aws_secretsmanager_secret" "lucky_clarke" {
  name = "ocn/${var.env}/lucky-clarke"
}

resource "aws_ecs_task_definition" "lucky_clarke" {
  family                   = "${var.env}-lucky-clarke"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn


  container_definitions = jsonencode([
    {
      name  = "lucky-clarke"
      image = "${var.ecr_registry}/ocn/lucky-clarke:${var.image_tag}"
      portMappings = [
        { containerPort = 8005 }
      ]
      environment = [
        { name = "SIGNAL_DETECTION_URL",  value = "http://signal-detection.${var.env}.ocn.internal:8002" },
        { name = "LUCKY_CLARKE_URL",      value = "http://lucky-clarke.${var.env}.ocn.internal:8005" },
        { name = "SIGNAL_CALLER_SUB",     value = "1" },
        { name = "OPENROUTER_MODEL",      value = "openai/gpt-4o-mini" },
        { name = "AWS_REGION",            value = var.aws_region },
      ]
      secrets = [
        {
          name      = "OPENROUTER_API_KEY"
          valueFrom = "${data.aws_secretsmanager_secret.lucky_clarke.arn}:OPENROUTER_API_KEY::"
        },
        {
          name      = "SMTP_HOST"
          valueFrom = "${data.aws_secretsmanager_secret.lucky_clarke.arn}:SMTP_HOST::"
        },
        {
          name      = "SMTP_USER"
          valueFrom = "${data.aws_secretsmanager_secret.lucky_clarke.arn}:SMTP_USER::"
        },
        {
          name      = "SMTP_PASSWORD"
          valueFrom = "${data.aws_secretsmanager_secret.lucky_clarke.arn}:SMTP_PASSWORD::"
        },
        {
          name      = "SMTP_FROM"
          valueFrom = "${data.aws_secretsmanager_secret.lucky_clarke.arn}:SMTP_FROM::"
        },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.env}/lucky-clarke"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  lifecycle {
    ignore_changes = [container_definitions]
  }
}


resource "aws_service_discovery_service" "lucky_clarke" {
  name = "lucky-clarke"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}


resource "aws_ecs_service" "lucky_clarke" {
  name            = "${var.env}-lucky-clarke"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.lucky_clarke.arn
  # Stopped 2026-09-25 (explicit request) - was previously scaled to 0
  # directly via `aws ecs update-service`, which a plain terraform apply
  # would have silently reverted back to 1 (the state Terraform's own
  # config still expected) the next time infra touched this service -
  # set here so the paused state actually persists through Terraform,
  # same pattern as the CloudWatch schedule "Paused" comments elsewhere
  # in this file. Set back to 1 to resume.
  desired_count = 0
  launch_type   = "FARGATE"


  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.lucky_clarke_sg_id]
    assign_public_ip = false
  }


  service_registries {
    registry_arn = aws_service_discovery_service.lucky_clarke.arn
  }

  lifecycle {
    ignore_changes = [task_definition]
  }
}




data "aws_secretsmanager_secret" "signal_herald" {
  name = "ocn/${var.env}/signal-herald"
}

resource "aws_ecs_task_definition" "signal_herald" {
  family                   = "${var.env}-signal-herald"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn

  container_definitions = jsonencode([
    {
      name  = "signal-herald"
      image = "${var.ecr_registry}/ocn/signal-herald:${var.image_tag}"
      portMappings = [
        { containerPort = 8006 }
      ]
      environment = [
        { name = "SIGNAL_AGENT_URL",   value = "http://signal-detection-agent.${var.env}.ocn.internal:8003" },
        { name = "SIGNAL_HERALD_URL",  value = "http://signal-herald.${var.env}.ocn.internal:8006" },
        { name = "SIGNAL_CALLER_SUB",  value = "1" },
        { name = "OPENROUTER_MODEL",   value = "openai/gpt-4o-mini" },
      ]
      secrets = [
        {
          name      = "OPENROUTER_API_KEY"
          valueFrom = "${data.aws_secretsmanager_secret.signal_herald.arn}:OPENROUTER_API_KEY::"
        },
        {
          name      = "SMTP_HOST"
          valueFrom = "${data.aws_secretsmanager_secret.signal_herald.arn}:SMTP_HOST::"
        },
        {
          name      = "SMTP_USER"
          valueFrom = "${data.aws_secretsmanager_secret.signal_herald.arn}:SMTP_USER::"
        },
        {
          name      = "SMTP_PASSWORD"
          valueFrom = "${data.aws_secretsmanager_secret.signal_herald.arn}:SMTP_PASSWORD::"
        },
        {
          name      = "SMTP_FROM"
          valueFrom = "${data.aws_secretsmanager_secret.signal_herald.arn}:SMTP_FROM::"
        },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.env}/signal-herald"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  lifecycle {
    ignore_changes = [container_definitions]
  }
}


resource "aws_service_discovery_service" "signal_herald" {
  name = "signal-herald"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}


resource "aws_ecs_service" "signal_herald" {
  name            = "${var.env}-signal-herald"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.signal_herald.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.signal_herald_sg_id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.signal_herald.arn
  }

  lifecycle {
    ignore_changes = [task_definition]
  }
}


resource "aws_cloudwatch_event_rule" "signal_herald_daily" {
  name                = "${var.env}-signal-herald-daily"
  schedule_expression = "cron(30 12 ? * MON-FRI *)"
}


resource "aws_cloudwatch_event_target" "signal_herald_daily" {
  rule     = aws_cloudwatch_event_rule.signal_herald_daily.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn
  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on
    # news_retrieval_taiwan_market_signal's target above for why this is
    # unpinned rather than a specific revision (CI/CD registers new task-def
    # revisions directly, bypassing terraform apply, so a pinned ARN here
    # drifts stale the moment CI/CD deploys next).
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.signal_herald.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.signal_herald_sg_id]
    }
  }
  input = jsonencode({
    containerOverrides = [
      {
        name    = "signal-herald"
        command = ["python", "-m", "src", "run", "--force"]
      }
    ]
  })
}




# ---------------------------------------------------------------------------
# research-universe (port 8007)
# ---------------------------------------------------------------------------

data "aws_secretsmanager_secret" "research_universe" {
  name = "ocn/${var.env}/research-universe"
}

resource "aws_ecs_task_definition" "research_universe" {
  family                   = "${var.env}-research-universe"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "512"
  memory                   = "1024"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task_exec_ssm.arn

  container_definitions = jsonencode([
    {
      name  = "research-universe"
      image = "${var.ecr_registry}/ocn/research-universe:${var.image_tag}"
      portMappings = [
        { containerPort = 8007 }
      ]
      environment = [
        { name = "POSTGRES_HOST",      value = var.rds_endpoint },
        { name = "POSTGRES_PORT",      value = "5432" },
        { name = "POSTGRES_DB",        value = "research_universe_db" },
        { name = "POSTGRES_USER",      value = "research_universe_user" },
        { name = "PGSSLMODE",          value = "require" },
        { name = "OPENROUTER_MODEL",   value = "anthropic/claude-sonnet-4-6" },
        { name = "API_PREFIX",         value = "/universe" },
      ]
      secrets = [
        {
          name      = "POSTGRES_PASSWORD"
          valueFrom = "${data.aws_secretsmanager_secret.research_universe.arn}:POSTGRES_PASSWORD::"
        },
        {
          name      = "OPENROUTER_API_KEY"
          valueFrom = "${data.aws_secretsmanager_secret.research_universe.arn}:OPENROUTER_API_KEY::"
        },
        {
          name      = "CORS_ORIGINS"
          valueFrom = "${data.aws_secretsmanager_secret.research_universe.arn}:CORS_ORIGINS::"
        },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/${var.env}/research-universe"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
    }
  ])

  lifecycle {
    ignore_changes = [container_definitions]
  }
}


resource "aws_service_discovery_service" "research_universe" {
  name = "research-universe"
  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id
    dns_records {
      ttl  = 10
      type = "A"
    }
  }
}


resource "aws_ecs_service" "research_universe" {
  name                   = "${var.env}-research-universe"
  cluster                = aws_ecs_cluster.main.id
  task_definition        = aws_ecs_task_definition.research_universe.arn
  desired_count          = 1
  launch_type            = "FARGATE"
  enable_execute_command = true

  load_balancer {
    target_group_arn = var.research_universe_tg_arn
    container_name   = "research-universe"
    container_port   = 8007
  }

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.research_universe_sg_id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.research_universe.arn
  }

  lifecycle {
    ignore_changes = [task_definition]
  }
}


resource "aws_cloudwatch_event_rule" "research_universe_scan" {
  name                = "${var.env}-research-universe-scan"
  description         = "Universe enrichment: full 19-category scan every 15 days"
  schedule_expression = "rate(15 days)"  # every 15 days at 09:00 UTC
}


resource "aws_cloudwatch_event_target" "research_universe_scan" {
  rule     = aws_cloudwatch_event_rule.research_universe_scan.name
  arn      = aws_ecs_cluster.main.arn
  role_arn = aws_iam_role.ecs_events.arn

  ecs_target {
    # Family-only ARN (no revision suffix) - see comment on
    # news_retrieval_taiwan_market_signal's target above for why this is
    # unpinned rather than a specific revision.
    task_definition_arn = "arn:aws:ecs:${var.aws_region}:${var.aws_account_id}:task-definition/${aws_ecs_task_definition.research_universe.family}"
    launch_type         = "FARGATE"
    network_configuration {
      subnets         = var.private_subnet_ids
      security_groups = [var.research_universe_sg_id]
    }
  }

  input = jsonencode({
    containerOverrides = [
      {
        name    = "research-universe"
        command = ["python", "-m", "src", "scan-all"]
      }
    ]
  })
}
