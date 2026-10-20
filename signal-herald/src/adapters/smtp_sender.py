"""SMTP email sender: AlphaStreet.ai signal intelligence digest."""
import logging
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import config

logger = logging.getLogger(__name__)


def _html_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


_GREEN_LINE = '<span style="color:#1a7a4a;font-weight:600">'

def _time_aware_title() -> str:
    return (
        "The AI economy is shifting<br>"
        f'{_GREEN_LINE}Here&#39;s what mattered</span>'
    )


def _top_row(article: dict[str, Any], cat_label: str) -> str:
    title = _html_escape(article.get("title") or article.get("url", "Untitled"))
    url = article.get("url", "#")
    score = float(article.get("signal_score") or 0)
    return (
        f'<tr>'
        f'<td style="padding:6px 0;width:36px;vertical-align:top;font-family:\'Courier New\',monospace;'
        f'font-size:11px;font-weight:600;color:#1a7a4a">{score:.2f}</td>'
        f'<td style="padding:6px 14px 6px 10px;vertical-align:top">'
        f'<a href="{url}" style="font-size:13px;color:#111;text-decoration:none;line-height:1.35">'
        f'{title}</a></td>'
        f'<td style="padding:6px 0;text-align:right;vertical-align:top;white-space:nowrap;'
        f'font-size:10px;font-weight:600;letter-spacing:0.5px;text-transform:uppercase;color:#bbb">'
        f'{cat_label}</td>'
        f'</tr>'
    )


def _article_row(article: dict[str, Any]) -> str:
    title = _html_escape(article.get("title") or article.get("url", "Untitled"))
    url = article.get("url", "#")
    tier = article.get("signal_detection", "weak_signal")
    score = float(article.get("signal_score") or 0)
    entities = [e.get("name", "") for e in (article.get("entities") or [])]
    entity_str = " · ".join(entities[:4]) if entities else ""

    dot_color = "#1a7a4a" if tier == "signal" else "#c97a12"
    score_color = "#1a7a4a" if tier == "signal" else "#c97a12"

    return (
        f'<tr style="border-top:0.5px solid #EEEDE8">'
        f'<td style="padding:8px 10px 8px 0;width:12px;vertical-align:top;font-size:10px;line-height:1.8;color:{dot_color}">&#9679;</td>'
        f'<td style="padding:8px 0;vertical-align:top">'
        f'<a href="{url}" style="font-size:13px;color:#111;text-decoration:none;'
        f'line-height:1.4;display:block;margin-bottom:2px">{title}</a>'
        f'<span style="font-size:11px;font-weight:700;color:#555">{_html_escape(entity_str)}</span>'
        f'</td>'
        f'<td style="padding:8px 0 8px 10px;vertical-align:top;text-align:right;white-space:nowrap;'
        f'font-family:\'Courier New\',monospace;font-size:11px;font-weight:600;color:{score_color}">'
        f'{score:.2f}</td>'
        f'</tr>'
    )


def _render_html(
    summaries: dict[str, str],
    visible: dict[str, list[dict[str, Any]]],
    ordered_categories: list[str],
    top_articles: list[dict[str, Any]],
    all_categorised: dict[str, list[dict[str, Any]]] | None = None,
) -> str:
    now = datetime.now(tz=timezone.utc)
    date_str = now.strftime("%A, %B %-d, %Y")
    title_html = _time_aware_title()

    parts: list[str] = [
        "<!DOCTYPE html>",
        '<html lang="en"><head>',
        '<meta charset="UTF-8">',
        '<meta name="viewport" content="width=device-width,initial-scale=1.0">',
        "<style>",
        "body{margin:0;padding:0;background:#FAFAF8;font-family:'IBM Plex Sans',Arial,sans-serif;-webkit-text-size-adjust:100%;text-align:left}",
        "a{color:inherit}",
        "@media only screen and (max-width:620px){",
        ".wrap{padding:0!important}",
        ".email{border-left:none!important;border-right:none!important}",
        ".hdr,.top-block,.cat-block,.footer{padding-left:16px!important;padding-right:16px!important}",
        ".hdr-title{font-size:22px!important;letter-spacing:-0.2px!important}",
        ".date-desktop{display:none!important}",
        ".date-mobile{display:block!important}",
        "td.cat-label-col{display:none!important;width:0!important;overflow:hidden!important}",
        "span.cat-label-col{display:block!important;margin-top:3px!important}",
        ".score-col{width:28px!important;font-size:10px!important;padding-top:14px!important}",
        ".legend{display:block!important}",
        ".legend span{display:block!important;margin-bottom:2px!important}",
        "}",
        "</style>",
        "</head><body>",
        '<div class="wrap" style="padding:0;background:#FAFAF8">',
        '<div class="email" style="max-width:100%;margin:0 auto;background:#FAFAF8">',
    ]

    # ── HEADER ──
    parts.append(
        f'<div class="hdr" style="padding:28px 32px 24px;border-bottom:1px solid #E0E0DB;text-align:left">'
        f'<div style="margin-bottom:20px">'
        f'<span style="font-size:11px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#1a7a4a;display:block">AlphaStreet.ai</span>'
        f'<span style="font-size:9px;letter-spacing:1px;text-transform:uppercase;color:#111;font-weight:500;display:block;margin-top:3px">OCN · Signal Intelligence</span>'
        f'</div>'
        f'<div class="hdr-title" style="font-size:27px;font-weight:600;color:#111;letter-spacing:-0.4px;line-height:1.2;margin-bottom:10px">'
        f'{title_html}'
        f'</div>'
        f'<div style="font-size:15px;color:#111;line-height:1.5;letter-spacing:0.2px">Daily signal intelligence across the AI economy</div>'
        f'<div style="margin-top:16px;font-size:11px;color:#111;line-height:1.6">'
        f'Score reflects how strongly an article qualifies as a signal, based on whether it describes a concrete AI-economy event, how relevant and important that event is, how new it is, and how well-supported it is.'
        f'</div>'
        f'</div>'
    )

    # ── HIGHEST CONVICTION ──
    parts.append(
        '<div class="top-block" style="padding:20px 32px;border-bottom:1px solid #E0E0DB;background:#F4F4F1">'
        '<div style="font-size:11px;font-weight:800;letter-spacing:3px;text-transform:uppercase;color:#111;margin-bottom:12px">Highest Conviction Today</div>'
        '<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse">'
    )
    for i, article in enumerate(top_articles):
        # A list since `category` became TEXT[]; a row can carry several.
        # Joined for the label rather than showing only the first, which
        # would silently drop the others. A bare string still renders,
        # for rows written before the column changed type.
        raw_cat = article.get("category") or ""
        cat = raw_cat if isinstance(raw_cat, str) else ", ".join(raw_cat)
        cat_label = _html_escape(cat)
        border = "border-bottom:0.5px solid #E8E8E3" if i < len(top_articles) - 1 else ""
        parts.append(f'<tr style="{border}">')
        parts.append(
            f'<td class="score-col" style="padding:10px 12px 8px 0;width:32px;vertical-align:top;font-family:\'Courier New\',monospace;'
            f'font-size:11px;font-weight:600;color:#1a7a4a;white-space:nowrap;line-height:1.6">{float(article.get("signal_score") or 0):.2f}</td>'
            f'<td style="padding:8px 10px 8px 0;vertical-align:top">'
            f'<a href="{article.get("url","#")}" style="font-size:13px;color:#111;text-decoration:none;line-height:1.6;display:block">'
            f'{_html_escape(article.get("title") or article.get("url","Untitled"))}</a>'
            f'<span class="cat-label-col" style="display:none;font-size:9px;font-weight:700;letter-spacing:0.5px;text-transform:uppercase;color:#555;margin-top:2px">'
            f'{cat_label}</span>'
            f'</td>'
            f'<td class="cat-label-col" style="padding:8px 0;text-align:right;vertical-align:top;white-space:nowrap;'
            f'font-size:10px;font-weight:700;letter-spacing:0.5px;text-transform:uppercase;color:#555;line-height:1.35">'
            f'{cat_label}</td>'
        )
        parts.append("</tr>")
    parts.append("</table></div>")

    # ── CATEGORY BLOCKS ──
    for i, cat in enumerate(ordered_categories):
        articles = visible.get(cat, [])
        summary = summaries.get(cat, "")
        cat_label = _html_escape(cat)
        is_last = i == len(ordered_categories) - 1
        bottom_pad = "12px" if is_last else "28px"

        parts.append(
            f'<div class="cat-block" style="padding:24px 32px {bottom_pad}">'
            f'<div style="font-size:11px;font-weight:800;letter-spacing:3px;text-transform:uppercase;color:#111;margin-bottom:3px">'
            f'{cat_label}</div>'
            f'<hr style="width:100%;height:1px;background:#E8E8E4;border:none;margin:6px 0 14px 0">'
        )

        if summary:
            parts.append(
                f'<p style="font-size:13.5px;font-weight:300;line-height:1.72;color:#111;margin:0 0 18px 0">'
                f'{summary}</p>'
            )

        parts.append(
            '<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse">'
        )
        for article in articles:
            parts.append(_article_row(article))
        parts.append("</table>")
        parts.append("</div>")

    # ── FOOTER ──
    parts.append(
        '<div class="footer" style="padding:14px 32px;border-top:1px solid #E0E0DB">'
        '<span style="font-size:10px;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;color:#1a7a4a">AlphaStreet.ai</span>'
        '</div>'
    )

    parts.append("</div></div></body></html>")
    return "\n".join(parts)


def send_digest(
    summaries: dict[str, str],
    visible: dict[str, list[dict[str, Any]]],
    ordered_categories: list[str],
    top_articles: list[dict[str, Any]],
    all_categorised: dict[str, list[dict[str, Any]]] | None = None,
) -> None:
    """Send the AlphaStreet.ai signal intelligence digest via SMTP."""
    html_body = _render_html(summaries, visible, ordered_categories, top_articles, all_categorised)

    now = datetime.now(tz=timezone.utc)
    date_str = now.strftime("%-d %B %Y")
    subject = f"OCN Daily Signal Intelligence Digest - {date_str}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.SMTP_FROM
    msg["To"] = ", ".join(config.EMAIL_RECIPIENTS)
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as smtp:
        smtp.starttls()
        smtp.login(config.SMTP_USER, config.SMTP_PASSWORD)
        smtp.sendmail(config.SMTP_FROM, config.EMAIL_RECIPIENTS, msg.as_string())

    logger.info(
        "Digest email sent to %d recipients via %s",
        len(config.EMAIL_RECIPIENTS),
        config.SMTP_HOST,
    )
