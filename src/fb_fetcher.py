from __future__ import annotations

"""Reusable helper module for fetching Facebook Ads Insights data.

This wraps common logic so the rest of the application (CLI, Streamlit GUI, etc.)
can call `fetch_insights()` with plain Python primitives and receive a
`pandas.DataFrame` ready for display / export.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional, Sequence, Tuple

import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

# ---------------------------------------------------------------------------
# SDK initialisation helpers
# ---------------------------------------------------------------------------

def init_api(*, app_id: str, app_secret: str, access_token: str) -> None:
    """Initialise the Facebook Marketing API SDK.

    This is safe to call multiple times; the SDK keeps global state.
    """
    if not all([app_id, app_secret, access_token]):
        raise ValueError("Missing Facebook API credentials")

    # Calling init repeatedly is OK – the SDK just overwrites the global config
    FacebookAdsApi.init(app_id=app_id, app_secret=app_secret, access_token=access_token)


# ---------------------------------------------------------------------------
# Introspection utilities
# ---------------------------------------------------------------------------

def list_available_fields() -> List[str]:
    """Return all valid insight field names supported by the SDK version in use."""
    # Filter dunder names and ensure value is str
    return [
        value
        for name, value in AdsInsights.Field.__dict__.items()
        if not name.startswith("__") and isinstance(value, str)
    ]


# ---------------------------------------------------------------------------
# Core fetch routine
# ---------------------------------------------------------------------------

def _date_range_iterator(start: datetime, end: datetime, step_days: int = 7) -> Tuple[datetime, datetime]:
    """Generator yielding (period_start, period_end) date tuples.

    The Marketing API allows queries up to 37 months in one request, but smaller
    windows can avoid timeouts and large payloads. We default to 7‑day chunks.
    """
    current = start
    delta = timedelta(days=step_days)
    while current < end:
        period_end = min(current + delta, end)
        yield current, period_end
        current = period_end


def fetch_insights(
    *,
    ad_account_id: str,
    fields: Sequence[str],
    since: str,
    until: str,
    breakdowns: Optional[Sequence[str]] = None,
    attribution_windows: Optional[Sequence[str]] = None,
    conversion_events: Optional[Sequence[str]] = None,
    api_credentials: Optional[dict] = None,
    step_days: int = 7,
) -> pd.DataFrame:
    """Fetch Insights as a pandas DataFrame.

    Parameters
    ----------
    ad_account_id : str
        The numeric account id (can include or exclude "act_" prefix).
    fields : list[str]
        The insight columns to request – should match AdsInsights.Field constants.
    since, until : str
        Date range in YYYY-MM-DD format (inclusive of since, *exclusive* of until).
    breakdowns : list[str] | None
        Supported breakdown dimensions (age, gender, etc.).
    attribution_windows : list[str] | None
        E.g. ["1d_click", "7d_click"]. Defaults to ["1d_click"].
    conversion_events : list[str] | None
        List of Facebook action_type strings (e.g., "offsite_conversion.fb_pixel_purchase").
        For each event, the resulting DataFrame will include a column with the
        1‑day click count. Internally we must request the "actions" field from
        the API and post‑process the array.
    api_credentials : dict | None
        If provided, should contain access_token, app_id, app_secret. If omitted,
        expects the SDK to already be initialised elsewhere.
    step_days : int
        Query window size in days – reduces risk of large‑response errors.
    """

    # Initialise SDK if creds supplied
    if api_credentials:
        init_api(
            app_id=api_credentials.get("app_id"),
            app_secret=api_credentials.get("app_secret"),
            access_token=api_credentials.get("access_token"),
        )

    if not ad_account_id:
        raise ValueError("ad_account_id cannot be empty")

    # Ensure account id has act_ prefix
    if not ad_account_id.startswith("act_"):
        ad_account_id = f"act_{ad_account_id}"

    ad_account = AdAccount(ad_account_id)

    # Convert dates
    try:
        start_dt = datetime.strptime(since, "%Y-%m-%d")
        end_dt = datetime.strptime(until, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("Dates must be in YYYY-MM-DD format") from exc

    if start_dt >= end_dt:
        raise ValueError("`since` must be before `until` date")

    attribution_windows = attribution_windows or ["1d_click"]
    conversion_events = list(conversion_events) if conversion_events else []
    breakdowns = list(breakdowns) if breakdowns else []

    all_rows = []

    # Ensure we request actions when conversion columns are desired
    req_fields = list(fields)
    if conversion_events and "actions" not in req_fields:
        req_fields.append("actions")

    for period_start, period_end in _date_range_iterator(start_dt, end_dt, step_days):
        params = {
            "time_range": {
                "since": period_start.strftime("%Y-%m-%d"),
                "until": period_end.strftime("%Y-%m-%d"),
            },
            "level": "ad",
            "action_attribution_windows": attribution_windows,
            "action_report_time": "conversion",
            "breakdowns": breakdowns,
            "limit": 1000,
        }

        logging.info(
            "Fetching insights %s → %s (fields=%d, breakdowns=%s)",
            params["time_range"]["since"],
            params["time_range"]["until"],
            len(req_fields),
            ",".join(breakdowns) if breakdowns else "none",
        )

        insights = ad_account.get_insights(fields=req_fields, params=params)
        all_rows.extend([row.export_all_data() for row in insights])

    if not all_rows:
        return pd.DataFrame()

    df = pd.json_normalize(all_rows)

    # Extract conversion event counts
    if conversion_events and "actions" in df.columns:
        def _extract(row_actions):
            counts = {}
            if not isinstance(row_actions, list):
                return pd.Series({evt: 0 for evt in conversion_events})
            for action in row_actions:
                a_type = action.get("action_type")
                if a_type in conversion_events:
                    try:
                        counts[a_type] = int(float(action.get("1d_click", 0)))
                    except (ValueError, TypeError):
                        counts[a_type] = 0
            # Ensure all events present
            for evt in conversion_events:
                counts.setdefault(evt, 0)
            return pd.Series(counts)

        df_conversions = df["actions"].apply(_extract)
        df = pd.concat([df.drop(columns=["actions"]), df_conversions], axis=1)

    return df


# ---------------------------------------------------------------------------
# Convenience CLI (optional usage)
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Quick test fetch for Ads Insights")
    parser.add_argument("--access-token", required=True)
    parser.add_argument("--app-id", required=True)
    parser.add_argument("--app-secret", required=True)
    parser.add_argument("--ad-account-id", required=True)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True)
    parser.add_argument("--fields", required=True, nargs="+", help="Insight field names")
    parser.add_argument("--breakdowns", nargs="*")
    args = parser.parse_args()

    init_api(app_id=args.app_id, app_secret=args.app_secret, access_token=args.access_token)

    df = fetch_insights(
        ad_account_id=args.ad_account_id,
        fields=args.fields,
        since=args.since,
        until=args.until,
        breakdowns=args.breakdowns,
    )
    print(df.head()) 