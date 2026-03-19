#!/usr/bin/env python3
"""
Phase 1 admin dashboard for agent usage analytics.

Usage:
    streamlit run app/streamlit_dashboard.py
"""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Agent Usage Admin",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&family=Space+Grotesk:wght@500;700&display=swap');

        :root {
            --bg-a: #f6f8f4;
            --bg-b: #dcefe6;
            --panel: rgba(255, 255, 255, 0.82);
            --ink: #0d2118;
            --muted: #47685b;
            --accent: #0f7d63;
            --accent-2: #d75b2f;
            --edge: rgba(15, 125, 99, 0.18);
        }

        .stApp {
            background:
                radial-gradient(1100px 600px at 95% -5%, rgba(215, 91, 47, 0.14), transparent 60%),
                radial-gradient(800px 500px at -10% 10%, rgba(15, 125, 99, 0.14), transparent 60%),
                linear-gradient(160deg, var(--bg-a), var(--bg-b));
            color: var(--ink);
            font-family: 'Manrope', sans-serif;
        }

        h1, h2, h3 {
            font-family: 'Space Grotesk', sans-serif;
            letter-spacing: 0.2px;
        }

        .dashboard-title {
            padding: 0.75rem 1rem;
            border-radius: 14px;
            background: linear-gradient(90deg, rgba(15, 125, 99, 0.14), rgba(215, 91, 47, 0.12));
            border: 1px solid var(--edge);
            margin-bottom: 1rem;
        }

        .metric-card {
            background: var(--panel);
            border: 1px solid var(--edge);
            border-radius: 14px;
            padding: 0.85rem 1rem;
            box-shadow: 0 8px 22px rgba(8, 35, 27, 0.06);
        }

        .small-label {
            font-size: 0.78rem;
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.08em;
            margin-bottom: 0.2rem;
        }

        .big-value {
            font-size: 1.55rem;
            font-weight: 800;
            line-height: 1.1;
            color: var(--ink);
        }

        .stDataFrame, div[data-testid='stMetric'] {
            background: var(--panel);
            border-radius: 10px;
        }

        /* Keep tab labels readable even before hover */
        button[data-baseweb='tab'] {
            color: var(--muted) !important;
            opacity: 1 !important;
            font-weight: 700 !important;
        }

        button[data-baseweb='tab']:hover {
            color: var(--ink) !important;
        }

        button[data-baseweb='tab'][aria-selected='true'] {
            color: var(--accent-2) !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def sql_quote(value: str) -> str:
    return value.replace("'", "''")


def find_parquet_sources(output_dir: Path) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str, float]] = []

    for file_path in output_dir.glob("*.parquet"):
        label = f"File: {file_path.name}"
        glob_path = file_path.as_posix()
        mtime = file_path.stat().st_mtime
        candidates.append((label, glob_path, mtime))

    for directory in output_dir.iterdir() if output_dir.exists() else []:
        if not directory.is_dir():
            continue
        parquet_children = list(directory.rglob("*.parquet"))
        if not parquet_children:
            continue
        newest = max(path.stat().st_mtime for path in parquet_children)
        label = f"Dataset: {directory.name}"
        glob_path = f"{directory.as_posix()}/**/*.parquet"
        candidates.append((label, glob_path, newest))

    candidates.sort(key=lambda item: item[2], reverse=True)
    return [(label, glob_path) for label, glob_path, _ in candidates]


@st.cache_data(ttl=300, show_spinner=False)
def load_sessions(source_glob: str) -> pd.DataFrame:
    query = f"SELECT * FROM read_parquet('{sql_quote(source_glob)}', union_by_name=true)"
    con = duckdb.connect(database=":memory:")
    try:
        df = con.execute(query).df()
    finally:
        con.close()

    if "start_time_aedt" in df.columns:
        df["start_time_dt"] = pd.to_datetime(
            df["start_time_aedt"].astype(str).str.replace(" AEDT", "", regex=False),
            errors="coerce",
        )
    else:
        df["start_time_dt"] = pd.NaT

    if "start_date" in df.columns:
        date_series = pd.to_datetime(df["start_date"], errors="coerce")
        df["start_date"] = date_series.dt.date
    else:
        df["start_date"] = df["start_time_dt"].dt.date

    bool_cols = [
        "implied_success",
        "knowledge_gap_detected",
        "zero_result_all_searches",
        "bot_responded",
        "answered_without_filter",
        "re_planned",
    ]
    for col in bool_cols:
        if col in df.columns:
            df[col] = coerce_bool_series(df[col])

    return df


def pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 1)


def parse_pipe_values(values: Iterable[str]) -> list[str]:
    items: list[str] = []
    for value in values:
        if value is None:
            continue
        for part in str(value).split("|"):
            cleaned = part.strip()
            if cleaned:
                items.append(cleaned)
    return items


def coerce_bool_series(series: pd.Series) -> pd.Series:
    lowered = series.astype(str).str.strip().str.lower()
    mapped = lowered.map({"true": True, "false": False, "1": True, "0": False})
    if series.dtype == bool:
        return series
    return mapped.fillna(False).astype(bool)


def normalized_text_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def build_filter_options(series: pd.Series, empty_label: str = "(empty)") -> list[str]:
    values = normalized_text_series(series)
    unique_values = sorted({v for v in values.unique() if v})
    if (values == "").any():
        unique_values.append(empty_label)
    return unique_values


def apply_text_multiselect_filter(
    df: pd.DataFrame,
    column: str,
    label: str,
    empty_label: str = "(empty)",
) -> pd.DataFrame:
    if column not in df.columns:
        return df

    values = normalized_text_series(df[column])
    options = build_filter_options(values, empty_label=empty_label)
    selected = st.sidebar.multiselect(label, options, default=options)

    if options and not selected:
        return df.iloc[0:0]

    if not selected:
        return df

    normalized_values = values.replace("", empty_label)
    mask = normalized_values.isin(selected)
    return df[mask]


def top_terms(texts: Iterable[str], top_n: int = 15) -> pd.DataFrame:
    stop_words = {
        "the", "and", "for", "with", "that", "this", "from", "have", "your", "you", "are", "was",
        "were", "not", "but", "can", "cannot", "could", "should", "would", "about", "into", "than",
        "then", "when", "what", "where", "which", "while", "after", "before", "they", "them", "their",
        "there", "here", "been", "being", "had", "has", "did", "does", "its", "our", "out", "all",
        "too", "very", "just", "some", "more", "like", "need", "please", "agent", "copilot", "chat",
    }
    counter: Counter = Counter()

    for text in texts:
        if text is None:
            continue
        tokens = re.findall(r"[a-zA-Z]{3,}", str(text).lower())
        for token in tokens:
            if token in stop_words:
                continue
            counter[token] += 1

    if not counter:
        return pd.DataFrame(columns=["term", "count"])

    rows = counter.most_common(top_n)
    return pd.DataFrame(rows, columns=["term", "count"])


def render_metric_card(label: str, value: str) -> None:
    st.markdown(
        f"""
        <div class='metric-card'>
            <div class='small-label'>{label}</div>
            <div class='big-value'>{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.subheader("Filters")

    if df.empty:
        return df

    filtered = df.copy()

    if "start_date" in filtered.columns and filtered["start_date"].notna().any():
        min_date = filtered["start_date"].dropna().min()
        max_date = filtered["start_date"].dropna().max()
        date_range = st.sidebar.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        if isinstance(date_range, (tuple, list)) and len(date_range) == 2:
            start_date, end_date = date_range
            filtered = filtered[(filtered["start_date"] >= start_date) & (filtered["start_date"] <= end_date)]

    filtered = apply_text_multiselect_filter(filtered, "outcome", "Outcome")
    filtered = apply_text_multiselect_filter(filtered, "channel", "Channel")
    filtered = apply_text_multiselect_filter(filtered, "user_department", "Department")
    filtered = apply_text_multiselect_filter(filtered, "user_feedback_reaction", "Feedback reaction")

    return filtered


def render_overview(df: pd.DataFrame) -> None:
    st.subheader("Usage Overview")

    total_sessions = len(df)
    resolved = int((df.get("outcome", pd.Series(dtype=str)) == "Resolved").sum())
    implied_success = int(df.get("implied_success", pd.Series(dtype=bool)).fillna(False).astype(bool).sum())
    with_feedback = int(df.get("user_feedback_reaction", pd.Series(dtype=str)).astype(str).str.len().gt(0).sum())
    avg_latency = float(pd.to_numeric(df.get("total_response_latency_sec", pd.Series(dtype=float)), errors="coerce").dropna().mean() or 0.0)

    like_count = int((df.get("user_feedback_reaction", pd.Series(dtype=str)).astype(str).str.lower() == "like").sum())

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        render_metric_card("Total sessions", f"{total_sessions:,}")
    with c2:
        render_metric_card("Resolved rate", f"{pct(resolved, total_sessions)}%")
    with c3:
        render_metric_card("Implied success", f"{pct(implied_success, total_sessions)}%")
    with c4:
        render_metric_card("Avg response latency", f"{avg_latency:.1f}s")
    with c5:
        render_metric_card("Like rate", f"{pct(like_count, with_feedback)}%")

    c6, c7 = st.columns((3, 2))
    with c6:
        if "start_date" in df.columns and df["start_date"].notna().any():
            trend = (
                df.dropna(subset=["start_date"])
                .groupby("start_date", as_index=False)
                .size()
                .rename(columns={"size": "sessions"})
                .sort_values("start_date")
            )
            st.markdown("Sessions over time")
            st.line_chart(trend.set_index("start_date"))

    with c7:
        if "outcome" in df.columns:
            out = (
                df["outcome"]
                .astype(str)
                .replace("", "(empty)")
                .value_counts(dropna=False)
                .rename_axis("outcome")
                .reset_index(name="sessions")
            )
            st.markdown("Outcome distribution")
            st.bar_chart(out.set_index("outcome"))


def render_feedback(df: pd.DataFrame) -> None:
    st.subheader("Feedback")

    reaction_series = df.get("user_feedback_reaction", pd.Series(dtype=str)).astype(str).str.lower()
    feedback_df = df[reaction_series.isin(["like", "dislike"])].copy()

    total_feedback = len(feedback_df)
    likes = int((feedback_df["user_feedback_reaction"].astype(str).str.lower() == "like").sum()) if total_feedback else 0
    dislikes = int((feedback_df["user_feedback_reaction"].astype(str).str.lower() == "dislike").sum()) if total_feedback else 0

    c1, c2, c3 = st.columns(3)
    with c1:
        render_metric_card("Feedback events", f"{total_feedback:,}")
    with c2:
        render_metric_card("Likes", f"{likes:,}")
    with c3:
        render_metric_card("Dislikes", f"{dislikes:,}")

    if total_feedback:
        chart_df = pd.DataFrame(
            {
                "reaction": ["like", "dislike"],
                "count": [likes, dislikes],
            }
        )
        st.markdown("Feedback mix")
        st.bar_chart(chart_df.set_index("reaction"))

    comments = feedback_df[
        feedback_df.get("user_feedback_text", pd.Series(dtype=str)).astype(str).str.strip().str.len() > 0
    ].copy()

    if not comments.empty:
        st.markdown("Feedback comments")
        display_cols = [
            col
            for col in [
                "start_time_aedt",
                "user_feedback_reaction",
                "user_name",
                "user_department",
                "first_user_query",
                "user_feedback_text",
            ]
            if col in comments.columns
        ]
        st.dataframe(comments[display_cols], width="stretch", hide_index=True)

        dislike_texts = comments[
            comments["user_feedback_reaction"].astype(str).str.lower() == "dislike"
        ].get("user_feedback_text", pd.Series(dtype=str))
        term_df = top_terms(dislike_texts)
        if not term_df.empty:
            st.markdown("Top terms in dislike comments")
            st.bar_chart(term_df.set_index("term"))
    else:
        st.info("No free-text feedback comments in the current filter scope.")


def render_quality(df: pd.DataFrame) -> None:
    st.subheader("Quality Diagnostics")

    total = len(df)
    has_search = df.get("total_search_calls", pd.Series(dtype=float)).fillna(0).astype(float) > 0
    searched_df = df[has_search].copy()

    gaps = int(searched_df.get("knowledge_gap_detected", pd.Series(dtype=bool)).fillna(False).astype(bool).sum())
    zero_results = int(searched_df.get("zero_result_all_searches", pd.Series(dtype=bool)).fillna(False).astype(bool).sum())
    raw_only_answers = int(searched_df.get("answered_without_filter", pd.Series(dtype=bool)).fillna(False).astype(bool).sum())

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_metric_card("Sessions with search", f"{len(searched_df):,}")
    with c2:
        render_metric_card("Knowledge gap rate", f"{pct(gaps, max(len(searched_df), 1))}%")
    with c3:
        render_metric_card("Zero-result rate", f"{pct(zero_results, max(len(searched_df), 1))}%")
    with c4:
        render_metric_card("Answered w/o filter", f"{pct(raw_only_answers, max(len(searched_df), 1))}%")

    c5, c6 = st.columns((2, 3))
    with c5:
        if "failure_category" in df.columns:
            failure = (
                df["failure_category"]
                .astype(str)
                .replace("", "none")
                .value_counts()
                .rename_axis("failure_category")
                .reset_index(name="sessions")
            )
            st.markdown("Failure category distribution")
            st.bar_chart(failure.set_index("failure_category"))

    with c6:
        failed_queries = parse_pipe_values(df.get("failed_queries", pd.Series(dtype=str)).dropna().astype(str).tolist())
        if failed_queries:
            top_failed = pd.DataFrame(Counter(failed_queries).most_common(15), columns=["query", "count"])
            st.markdown("Top failed queries")
            st.dataframe(top_failed, width="stretch", hide_index=True)

    if total == 0:
        st.info("No sessions available after filters.")


def render_session_explorer(df: pd.DataFrame) -> None:
    st.subheader("Session Explorer")

    search_text = st.text_input("Search in query/response", value="").strip().lower()
    view_df = df.copy()

    if search_text:
        query_series = view_df.get("first_user_query", pd.Series(index=view_df.index, dtype=str)).astype(str).str.lower()
        response_series = view_df.get("bot_response_text", pd.Series(index=view_df.index, dtype=str)).astype(str).str.lower()
        mask = query_series.str.contains(search_text, na=False, regex=False) | response_series.str.contains(search_text, na=False, regex=False)
        view_df = view_df[mask]

    preferred_columns = [
        "start_time_aedt",
        "session_id",
        "user_name",
        "user_department",
        "channel",
        "outcome",
        "implied_success",
        "total_response_latency_sec",
        "total_search_calls",
        "search_hit_rate",
        "knowledge_gap_detected",
        "user_feedback_reaction",
        "first_user_query",
        "user_feedback_text",
    ]
    selectable = [col for col in preferred_columns if col in view_df.columns]
    selected = st.multiselect("Columns", options=selectable, default=selectable)

    if not selected:
        st.info("Select at least one column to display session rows.")
        return

    st.dataframe(view_df[selected], width="stretch", hide_index=True)


def main() -> None:
    apply_theme()

    st.markdown(
        """
        <div class='dashboard-title'>
            <h1 style='margin: 0;'>Agent Admin Console</h1>
            <p style='margin: 0.35rem 0 0 0; color: #35594c;'>
                Track usage, response quality, and user feedback across transcript analytics Parquet outputs.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    project_root = Path(__file__).resolve().parents[1]
    output_dir = project_root / "output"

    st.sidebar.header("Data source")
    sources = find_parquet_sources(output_dir)

    source_map = {label: glob_path for label, glob_path in sources}
    source_labels = list(source_map.keys())
    source_labels.append("Custom path...")
    source_choice = st.sidebar.selectbox("Parquet source", source_labels, index=0 if sources else len(source_labels) - 1)

    if source_choice == "Custom path...":
        custom_path = st.sidebar.text_input("Path or glob", value=str((output_dir / "*.parquet").as_posix())).strip()
        source_glob = custom_path
    else:
        source_glob = source_map[source_choice]

    st.sidebar.caption(f"Using: {source_glob}")

    try:
        df = load_sessions(source_glob)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Failed to load Parquet source: {exc}")
        st.stop()

    if df.empty:
        st.warning("No rows found in the selected source.")
        st.stop()

    filtered_df = apply_filters(df)

    tabs = st.tabs(["Overview", "Feedback", "Quality", "Sessions"])
    with tabs[0]:
        render_overview(filtered_df)
    with tabs[1]:
        render_feedback(filtered_df)
    with tabs[2]:
        render_quality(filtered_df)
    with tabs[3]:
        render_session_explorer(filtered_df)


if __name__ == "__main__":
    main()
