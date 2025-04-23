import os, sys
from pathlib import Path
# Ensure project root and src package are on sys.path when running via Streamlit
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
for p in (ROOT_DIR, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import json
from datetime import date
from io import BytesIO
from typing import List
from dotenv import load_dotenv

import streamlit as st
import pandas as pd

try:
    from src.fb_fetcher import fetch_insights, list_available_fields, init_api
except ModuleNotFoundError:
    # Fallback if the package import fails but module is directly on path
    from fb_fetcher import fetch_insights, list_available_fields, init_api

st.set_page_config(page_title="Facebook Insights JSON Builder", layout="wide")

# Load env vars (supports both root .env and facebook_api_to_json/.env)
load_dotenv()  # try default first
FB_ENV = ROOT_DIR / "facebook_api_to_json" / ".env"
if FB_ENV.exists():
    load_dotenv(dotenv_path=FB_ENV, override=True)

# -----------------------------------------------------------------------------
# Sidebar – credentials & query parameters
# -----------------------------------------------------------------------------

def _strip_act_prefix(acc_id: str | None) -> str:
    if not acc_id:
        return ""
    return acc_id.replace("act_", "")

def sidebar_inputs():
    st.sidebar.header("Credentials")

    access_token = st.sidebar.text_input(
        "Access Token",
        value=os.getenv("FACEBOOK_ACCESS_TOKEN", ""),
        type="password",
    )
    app_id = st.sidebar.text_input("App ID", value=os.getenv("FACEBOOK_APP_ID", ""))
    app_secret = st.sidebar.text_input(
        "App Secret",
        value=os.getenv("FACEBOOK_APP_SECRET", ""),
        type="password",
    )
    ad_account_id = st.sidebar.text_input(
        "Ad Account ID (numeric)",
        value=_strip_act_prefix(os.getenv("FACEBOOK_AD_ACCOUNT_ID")),
    )

    st.sidebar.header("Date Range")
    col1, col2 = st.sidebar.columns(2)
    since = col1.date_input("Since", value=date.today().replace(day=1))
    until = col2.date_input("Until", value=date.today())

    st.sidebar.header("Parameters")
    all_fields: List[str] = list_available_fields()
    selected_fields = st.sidebar.multiselect(
        "Metric Fields",
        options=sorted(all_fields),
        default=["ad_id", "ad_name", "spend", "impressions"],
    )

    conv_options = [
        "offsite_conversion.fb_pixel_custom",
        "offsite_conversion.fb_pixel_purchase",
        "offsite_conversion.fb_pixel_add_to_cart",
        "offsite_conversion.fb_pixel_initiate_checkout",
        "offsite_conversion.fb_pixel_lead",
        "offsite_conversion.fb_pixel_complete_registration",
        "offsite_conversion.fb_pixel_view_content",
    ]
    conv_selected = st.sidebar.multiselect(
        "Conversion Events (1‑day click count)",
        options=conv_options,
        default=["offsite_conversion.fb_pixel_custom"],
    )

    breakdown_options = [
        "age",
        "gender",
        "country",
        "region",
        "impression_device",
        "device_platform",
        "placement",
    ]
    selected_breakdowns = st.sidebar.multiselect("Breakdowns (optional)", breakdown_options)

    return {
        "access_token": access_token,
        "app_id": app_id,
        "app_secret": app_secret,
        "ad_account_id": ad_account_id,
        "since": since.strftime("%Y-%m-%d"),
        "until": until.strftime("%Y-%m-%d"),
        "fields": selected_fields,
        "conversion_events": conv_selected,
        "breakdowns": selected_breakdowns,
    }


query_params = sidebar_inputs()

st.title("📊 Facebook Ads Insights JSON Builder")

# Fetch button
if st.button("Fetch Insights"):
    # Combine fields from both selections
    selected_fields_combined = query_params["fields"]

    if not selected_fields_combined:
        st.error("Please select at least one metric field.")
    else:
        def _run_fetch():
            if not all([
                query_params["access_token"],
                query_params["app_id"],
                query_params["app_secret"],
                query_params["ad_account_id"],
            ]):
                st.error("Please fill in all credentials.")
                return
            with st.spinner("Fetching data from Facebook Marketing API…"):
                try:
                    init_api(
                        app_id=query_params["app_id"],
                        app_secret=query_params["app_secret"],
                        access_token=query_params["access_token"],
                    )

                    df: pd.DataFrame = fetch_insights(
                        ad_account_id=query_params["ad_account_id"],
                        fields=selected_fields_combined,
                        since=query_params["since"],
                        until=query_params["until"],
                        breakdowns=query_params["breakdowns"],
                        conversion_events=query_params.get("conversion_events") or None,
                    )

                    if df.empty:
                        st.warning("No data returned for the chosen parameters.")
                    else:
                        st.success(f"Retrieved {len(df)} rows.")
                        st.dataframe(df, use_container_width=True)
                        json_str = df.to_json(orient="records", date_format="iso")
                        b = BytesIO(); b.write(json_str.encode())
                        st.download_button("Download JSON", b.getvalue(), "fb_insights.json", "application/json")
                except Exception as exc:
                    st.error(f"Error fetching insights: {exc}")
        _run_fetch() 