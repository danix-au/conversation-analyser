#!/usr/bin/env python3
"""
Copilot Studio Conversation Transcript Analytics
=================================================
Parses one or more agentPDS transcript JSON files and computes
per-session and aggregate KPIs for measuring agent effectiveness.

Usage:
    python tools/transcript_analytics.py                          # all JSON in Processing/Inbox/
    python tools/transcript_analytics.py path/to/transcript.json  # single file
    python tools/transcript_analytics.py dir/                     # directory of JSON files
    python tools/transcript_analytics.py --csv kpis.csv           # export aggregate KPIs to CSV
    python tools/transcript_analytics.py --parquet kpis.parquet   # export per-session KPIs to Parquet
"""

import json
import sys
import csv
import os
import subprocess
import time
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional
from collections import Counter

import requests

# Australia/Sydney AEDT = UTC+11 (summer), AEST = UTC+10 (winter)
# For analysis purposes we use a fixed +11 offset (AEDT)
AEDT = timezone(timedelta(hours=11))


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class SearchStepMetrics:
    step_id: str
    query: str
    keywords: str
    execution_time_sec: float
    raw_result_count: int
    filtered_result_count: int
    output_knowledge_sources: int
    unique_docs_raw: list = field(default_factory=list)
    has_errors: bool = False
    knowledge_sources: list = field(default_factory=list)  # KS IDs queried for this step


@dataclass
class SessionMetrics:
    # Identity
    file: str
    session_id: str = ""
    plan_identifier: str = ""
    channel: str = ""
    locale: str = ""
    aad_object_id: str = ""
    user_name: str = ""
    user_department: str = ""

    # Timing (AEDT)
    start_time_aedt: str = ""
    end_time_aedt: str = ""
    duration_sec: float = 0.0

    # Session outcome
    session_type: str = ""          # e.g. Engaged
    outcome: str = ""               # Resolved | Abandoned | Escalated
    outcome_reason: str = ""        # UserExit | etc.
    implied_success: bool = False
    turn_count: int = 0
    design_mode: bool = False

    # User messages
    user_messages: list = field(default_factory=list)
    user_message_count: int = 0

    # Planning
    plan_iterations: int = 0        # number of DynamicPlanReceived events
    total_steps_planned: int = 0
    steps_executed: int = 0
    planning_latency_sec: float = 0.0   # user msg → first plan

    # Search tool metrics
    total_search_calls: int = 0
    total_raw_results: int = 0
    total_filtered_results: int = 0
    total_output_knowledge_sources: int = 0
    search_hit_rate: float = 0.0        # % calls with filteredResults > 0
    total_search_execution_sec: float = 0.0
    unique_docs_surfaced: int = 0       # unique docs in raw results
    relevant_docs_promoted: int = 0     # unique docs in filteredResults (placeholder: 0 if empty)
    zero_result_all_searches: bool = False

    # Total response latency
    total_response_latency_sec: float = 0.0

    # Derived quality flags
    re_planned: bool = False
    knowledge_gap_detected: bool = False   # all searches returned 0 filtered results

    # Knowledge gap diagnostics
    failure_category: str = ""             # ""|"no_search"|"no_raw_results"|"filtered_out"|"partial_filtered"
    failed_queries: list = field(default_factory=list)          # queries that returned 0 filtered results
    top_raw_docs_not_promoted: list = field(default_factory=list)  # docs returned raw but filtered out
    knowledge_sources_queried: list = field(default_factory=list)  # unique KS IDs searched
    search_error_count: int = 0            # steps that had search_errors
    max_raw_per_step: int = 0             # highest raw result count across steps

    # Bot response diagnostics
    bot_responded: bool = False            # True if agent generated any answer text
    answered_without_filter: bool = False  # True if bot answered but filteredResults=0 (used raw docs)
    gpt_answer_state: str = ""            # Answered | NotAnswered (from GPTAnswer trace)
    completion_state: str = ""            # Answered | etc. (from KnowledgeTraceData)
    cited_knowledge_sources: list = field(default_factory=list)  # KS IDs cited in final answer
    bot_response_text: str = ""           # full LLM-generated answer text (from VariableAssignment)

    # User feedback (thumbs up/down)
    user_feedback_reaction: str = ""       # "like" | "dislike" (from message/submitAction)
    user_feedback_text: str = ""           # free-text comment submitted with the reaction

    # Search details (list of SearchStepMetrics)
    search_steps: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def _parse_exec_time(exec_time_str: str) -> float:
    """Convert '00:00:05.5524061' to seconds as float."""
    if not exec_time_str:
        return 0.0
    parts = exec_time_str.split(":")
    try:
        h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
        return h * 3600 + m * 60 + s
    except Exception:
        return 0.0


def _ts_to_aedt(unix_ts: int) -> str:
    """Convert unix timestamp to AEDT string."""
    dt = datetime.fromtimestamp(unix_ts, tz=AEDT)
    return dt.strftime("%Y-%m-%d %H:%M:%S AEDT")


def parse_transcript_data(data: dict, file_label: str) -> SessionMetrics:
    """Parse a single transcript payload dictionary into SessionMetrics."""
    m = SessionMetrics(file=file_label)

    activities = data.get("activities", [])

    user_msg_timestamp: Optional[int] = None
    first_plan_timestamp: Optional[int] = None
    plan_finished_timestamp: Optional[int] = None
    search_calls_with_filtered: int = 0

    # Buffer for BindUpdate queries — keyed by stepId (arrives before StepFinished)
    pending_queries: dict = {}  # stepId → {query, keywords}
    pending_trace: dict = {}    # stepId → {filtered, full, output_ks} from TraceData
    current_step_id: str = ""   # set by StepTriggered, used to correlate TraceData

    for act in activities:
        act_type = act.get("type", "")
        act_name = act.get("name", "")
        value = act.get("value", {})
        value_type = act.get("valueType", "")
        ts = act.get("timestamp", 0)
        channel = act.get("channelId", "")

        if channel:
            m.channel = channel

        # ── ConversationInfo ──────────────────────────────────────────────
        if value_type == "ConversationInfo" or (act_type == "trace" and isinstance(value, dict) and "lastSessionOutcome" in value):
            m.outcome = value.get("lastSessionOutcome", "")
            m.outcome_reason = value.get("lastSessionOutcomeReason", "")
            m.design_mode = value.get("isDesignMode", False)
            m.locale = value.get("locale", "")

        # ── SessionInfo ───────────────────────────────────────────────────
        if value_type == "SessionInfo":
            start_utc = value.get("startTimeUtc", "")
            end_utc = value.get("endTimeUtc", "")
            if start_utc:
                dt_start = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(AEDT)
                m.start_time_aedt = dt_start.strftime("%Y-%m-%d %H:%M:%S AEDT")
            if end_utc:
                dt_end = datetime.fromisoformat(end_utc.replace("Z", "+00:00")).astimezone(AEDT)
                m.end_time_aedt = dt_end.strftime("%Y-%m-%d %H:%M:%S AEDT")
            if start_utc and end_utc:
                m.duration_sec = (datetime.fromisoformat(end_utc.replace("Z", "+00:00")) -
                                  datetime.fromisoformat(start_utc.replace("Z", "+00:00"))).total_seconds()
            m.session_type = value.get("type", "")
            m.outcome = value.get("outcome", m.outcome)  # override from SessionInfo if present
            m.implied_success = value.get("impliedSuccess", False)
            m.turn_count = value.get("turnCount", 0)
            m.outcome_reason = value.get("outcomeReason", m.outcome_reason)

        # ── User messages ─────────────────────────────────────────────────
        if act_type == "message" and isinstance(value, dict) and act.get("from", {}).get("role") == 1:
            text = act.get("text", "")
            if text:
                m.user_messages.append(text)
                if user_msg_timestamp is None:
                    user_msg_timestamp = ts

        # Also check plain string format
        if act_type == "message" and act.get("from", {}).get("role") == 1:
            text = act.get("text", "")
            if text and text not in m.user_messages:
                m.user_messages.append(text)
                if user_msg_timestamp is None:
                    user_msg_timestamp = ts
            if not m.aad_object_id:
                m.aad_object_id = act.get("from", {}).get("aadObjectId", "")

        # ── DynamicPlanStepTriggered (track current in-flight stepId) ────────
        if act_name == "DynamicPlanStepTriggered":
            current_step_id = value.get("stepId", "")

        # ── DynamicPlanReceived ───────────────────────────────────────────
        if value_type == "DynamicPlanReceived" or act_name == "DynamicPlanReceived":
            steps = value.get("steps", [])
            m.plan_iterations += 1
            m.total_steps_planned += len(steps)
            plan_id = value.get("planIdentifier", "")
            if plan_id:
                m.plan_identifier = plan_id
            if first_plan_timestamp is None:
                first_plan_timestamp = ts

        # ── DynamicPlanFinished ───────────────────────────────────────────
        if value_type == "DynamicPlanFinished" or act_name == "DynamicPlanFinished":
            plan_finished_timestamp = ts
            plan_id = value.get("planId", "")
            if plan_id:
                m.session_id = plan_id

        # ── DynamicPlanStepFinished (UniversalSearchTool) ─────────────────
        if act_name == "DynamicPlanStepFinished":
            step_id = value.get("stepId", "")
            task_dialog = value.get("taskDialogId", "")
            exec_time = _parse_exec_time(value.get("executionTime", ""))
            obs = value.get("observation") or {}
            search_result = obs.get("search_result") or {}
            raw_results = search_result.get("search_results", [])
            errors = search_result.get("search_errors", [])

            # Collect unique document names
            unique_docs = list({r.get("Name", r.get("Url", "?")) for r in raw_results if isinstance(r, dict)})

            # Check filteredResults from the companion UniversalSearchToolTraceData
            # (The filteredResults field is on the TraceData event, not StepFinished)
            filtered_count = len(value.get("filteredResults", []))
            output_ks = len(value.get("outputKnowledgeSources", []))

            step = SearchStepMetrics(
                step_id=step_id,
                query="",  # filled from DynamicPlanStepBindUpdate
                keywords="",
                execution_time_sec=exec_time,
                raw_result_count=len(raw_results),
                filtered_result_count=filtered_count,
                output_knowledge_sources=output_ks,
                unique_docs_raw=unique_docs,
                has_errors=len(errors) > 0,
            )

            if "UniversalSearch" in task_dialog or "Search" in task_dialog:
                m.search_steps.append(step)
                m.total_search_execution_sec += exec_time
                # Apply any buffered query/keywords from BindUpdate
                if step_id in pending_queries:
                    step.query = pending_queries[step_id]["query"]
                    step.keywords = pending_queries[step_id]["keywords"]
                # Apply buffered TraceData (filteredResults, etc.)
                if step_id in pending_trace:
                    t = pending_trace[step_id]
                    step.filtered_result_count = t["filtered"]
                    step.output_knowledge_sources = t["output_ks"]
                    if t["full"] > 0:
                        step.raw_result_count = t["full"]

        # ── UniversalSearchToolTraceData (has filteredResults) ─────────────
        if act_name == "UniversalSearchToolTraceData":
            filtered_results = value.get("filteredResults", [])
            full_results = value.get("fullResults", [])
            output_ks = value.get("outputKnowledgeSources", [])
            knowledge_sources = value.get("knowledgeSources", [])
            # Buffer against current_step_id (set by StepTriggered)
            pending_trace[current_step_id] = {
                "filtered": len(filtered_results),
                "full": len(full_results),
                "output_ks": len(output_ks),
                "knowledge_sources": knowledge_sources,
            }

        # ── DynamicPlanStepBindUpdate (has search query/keywords) ─────────
        if act_name == "DynamicPlanStepBindUpdate":
            args = value.get("arguments", {})
            step_id = value.get("stepId", "")
            pending_queries[step_id] = {
                "query": args.get("search_query", ""),
                "keywords": args.get("search_keywords", ""),
            }
            # Also try to apply to an already-created step
            for step in reversed(m.search_steps):
                if step.step_id == step_id:
                    step.query = args.get("search_query", "")
                    step.keywords = args.get("search_keywords", "")
                    break

        # ── VariableAssignment — captures the LLM-generated answer text ───
        if value_type == "VariableAssignment" and value.get("id") == "Topic.Answer":
            if value.get("newValue"):
                m.bot_responded = True
                m.bot_response_text = value["newValue"]

        # ── KnowledgeTraceData — final completion state + cited sources ────
        if value_type == "KnowledgeTraceData":
            state = value.get("completionState", "")
            if state:
                m.completion_state = state
            cited = value.get("citedKnowledgeSources", [])
            if cited:
                m.cited_knowledge_sources = cited

        # ── GPTAnswer — whether the LLM returned an answer ────────────────
        if value_type == "GPTAnswer":
            m.gpt_answer_state = value.get("gptAnswerState", "")
            if m.gpt_answer_state == "Answered":
                m.bot_responded = True

        # ── User feedback (thumbs up/down + optional text) ─────────────────
        if act_type == "invoke" and act_name == "message/submitAction":
            if isinstance(value, dict) and value.get("actionName") == "feedback":
                action_value = value.get("actionValue", {})
                m.user_feedback_reaction = action_value.get("reaction", "")
                m.user_feedback_text = action_value.get("feedback", {}).get("feedbackText", "")

    # ── Post-processing ───────────────────────────────────────────────────
    # UniversalSearchToolTraceData arrives AFTER DynamicPlanStepFinished in the
    # activity stream, so pending_trace is always empty when StepFinished runs.
    # Apply any buffered trace data now that the full loop has completed.
    for step in m.search_steps:
        if step.step_id in pending_trace:
            t = pending_trace[step.step_id]
            step.filtered_result_count = t["filtered"]
            step.output_knowledge_sources = t["output_ks"]
            if t["full"] > 0:
                step.raw_result_count = t["full"]
            step.knowledge_sources = t.get("knowledge_sources", [])

    m.user_message_count = len(m.user_messages)
    m.total_search_calls = len(m.search_steps)
    m.total_raw_results = sum(s.raw_result_count for s in m.search_steps)
    m.total_filtered_results = sum(s.filtered_result_count for s in m.search_steps)
    m.total_output_knowledge_sources = sum(s.output_knowledge_sources for s in m.search_steps)
    m.steps_executed = len(m.search_steps)
    m.re_planned = m.plan_iterations > 1

    # Unique docs across all raw results
    all_docs = set()
    for step in m.search_steps:
        all_docs.update(step.unique_docs_raw)
    m.unique_docs_surfaced = len(all_docs)

    # Search hit rate: % searches that returned filtered results
    if m.total_search_calls > 0:
        hits = sum(1 for s in m.search_steps if s.filtered_result_count > 0)
        search_calls_with_filtered = hits
        m.search_hit_rate = hits / m.total_search_calls
    else:
        m.search_hit_rate = 0.0

    # Knowledge gap: all searches returned zero filtered results
    m.zero_result_all_searches = (
        m.total_search_calls > 0 and m.total_filtered_results == 0
    )
    m.knowledge_gap_detected = m.zero_result_all_searches

    # ── Knowledge gap diagnostics ─────────────────────────────────────────
    # Failure category: classify WHY zero filtered results occurred
    if m.total_search_calls == 0:
        m.failure_category = "no_search" if m.user_message_count > 0 else ""
    elif m.total_raw_results == 0:
        m.failure_category = "no_raw_results"   # KS empty / query completely off-topic
    elif m.total_filtered_results == 0:
        m.failure_category = "filtered_out"     # content found but relevance filter rejected all
    elif m.search_hit_rate < 1.0:
        m.failure_category = "partial_filtered" # some steps hit, some didn't
    else:
        m.failure_category = ""

    # Queries that produced zero filtered results
    m.failed_queries = [
        s.query or s.keywords
        for s in m.search_steps
        if s.filtered_result_count == 0 and (s.query or s.keywords)
    ]

    # Docs returned by raw search but rejected by relevance filter
    rejected_docs: Counter = Counter()
    for step in m.search_steps:
        if step.filtered_result_count == 0 and step.unique_docs_raw:
            rejected_docs.update(step.unique_docs_raw)
    m.top_raw_docs_not_promoted = [doc for doc, _ in rejected_docs.most_common(5)]

    # Unique knowledge source IDs queried across all search steps
    all_ks: set = set()
    for step in m.search_steps:
        all_ks.update(step.knowledge_sources)
    m.knowledge_sources_queried = sorted(all_ks)

    # Steps with search errors
    m.search_error_count = sum(1 for s in m.search_steps if s.has_errors)

    # Highest raw result count across steps (distinguishes "narrow miss" from "total miss")
    m.max_raw_per_step = max((s.raw_result_count for s in m.search_steps), default=0)

    # Answered without relevance filter: bot responded but no filtered results were promoted
    m.answered_without_filter = m.bot_responded and m.zero_result_all_searches

    # Refine failure_category now that we know whether the bot actually responded:
    # "filtered_out" + bot_responded  → agent answered using ONLY raw (unfiltered) docs
    # "filtered_out" + not bot_responded → agent found raw docs but generated no reply
    if m.failure_category == "filtered_out" and not m.bot_responded:
        m.failure_category = "filtered_out_no_reply"

    # Latency calculations (from unix timestamps)
    if user_msg_timestamp and first_plan_timestamp:
        m.planning_latency_sec = first_plan_timestamp - user_msg_timestamp
    if user_msg_timestamp and plan_finished_timestamp:
        m.total_response_latency_sec = plan_finished_timestamp - user_msg_timestamp

    return m


def parse_transcript(filepath: str) -> SessionMetrics:
    """Parse a single transcript JSON file into SessionMetrics."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return parse_transcript_data(data, os.path.basename(filepath))


def parse_transcripts_from_jsonl(filepath: str) -> list[SessionMetrics]:
    """Parse one extracted-content JSONL file into many SessionMetrics rows."""
    sessions: list[SessionMetrics] = []
    file_name = os.path.basename(filepath)
    with open(filepath, "r", encoding="utf-8") as f:
        for line_number, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSONL in {file_name} line {line_number}: {exc.msg}"
                ) from exc

            if not isinstance(record, dict):
                continue

            if isinstance(record.get("content_json"), dict):
                payload = record["content_json"]
                transcript_id = str(record.get("conversationtranscriptid") or "").strip()
                label = f"{transcript_id or file_name}:{line_number}"
            elif isinstance(record.get("activities"), list):
                payload = record
                label = f"{file_name}:{line_number}"
            else:
                continue

            sessions.append(parse_transcript_data(payload, label))

    return sessions


# ---------------------------------------------------------------------------
# Multi-file aggregation
# ---------------------------------------------------------------------------

def aggregate_sessions(sessions: list[SessionMetrics]) -> dict:
    """Compute aggregate KPIs across multiple sessions."""
    n = len(sessions)
    if n == 0:
        return {}

    # Outcome distribution
    outcomes = Counter(s.outcome for s in sessions)
    outcome_reasons = Counter(s.outcome_reason for s in sessions)
    channels = Counter(s.channel for s in sessions)

    # Rates
    resolved_rate = outcomes.get("Resolved", 0) / n
    abandoned_rate = outcomes.get("Abandoned", 0) / n
    implied_success_rate = sum(1 for s in sessions if s.implied_success) / n
    re_plan_rate = sum(1 for s in sessions if s.re_planned) / n
    # Knowledge gap and zero-result rates are only meaningful for sessions that
    # actually triggered a search.  Using all sessions as the denominator inflates
    # the apparent pass rate (e.g. 13/67 = 19% instead of the true 13/13 = 100%).
    sessions_with_search = [s for s in sessions if s.total_search_calls > 0]
    n_search = len(sessions_with_search) or 1  # avoid division by zero
    knowledge_gap_rate = sum(1 for s in sessions_with_search if s.knowledge_gap_detected) / n_search
    zero_result_rate = sum(1 for s in sessions_with_search if s.zero_result_all_searches) / n_search

    def safe_mean(vals):
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else 0.0

    def safe_median(vals):
        vals = sorted(v for v in vals if v is not None)
        if not vals:
            return 0.0
        mid = len(vals) // 2
        return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2

    # Search performance
    all_hit_rates = [s.search_hit_rate for s in sessions if s.total_search_calls > 0]
    all_search_calls = [s.total_search_calls for s in sessions]
    all_filtered = [s.total_filtered_results for s in sessions]
    all_raw = [s.total_raw_results for s in sessions]

    # Top queries (user messages)
    all_queries = [msg for s in sessions for msg in s.user_messages]

    # Document frequency (which docs appear most in raw search results)
    doc_freq: Counter = Counter()
    for s in sessions:
        for step in s.search_steps:
            doc_freq.update(step.unique_docs_raw)

    # Failure category breakdown
    fail_cats = Counter(s.failure_category for s in sessions if s.failure_category)

    # Top failed queries (queries that returned 0 filtered results)
    all_failed_queries = [q for s in sessions for q in s.failed_queries]
    top_failed_queries = Counter(all_failed_queries).most_common(10)

    # Top rejected docs (raw results that were never promoted to filtered)
    rejected_doc_freq: Counter = Counter()
    for s in sessions:
        for step in s.search_steps:
            if step.filtered_result_count == 0 and step.unique_docs_raw:
                rejected_doc_freq.update(step.unique_docs_raw)
    top_rejected_docs = rejected_doc_freq.most_common(10)

    # Unique knowledge sources queried across all sessions
    all_ks_queried: set = set()
    for s in sessions:
        all_ks_queried.update(s.knowledge_sources_queried)

    # Bot response rates
    bot_response_rate = sum(1 for s in sessions if s.bot_responded) / n
    answered_without_filter_rate = sum(1 for s in sessions_with_search if s.answered_without_filter) / n_search

    return {
        "total_sessions": n,
        "sessions_with_search": len(sessions_with_search),
        "outcome_distribution": dict(outcomes),
        "outcome_reason_distribution": dict(outcome_reasons),
        "channel_distribution": dict(channels),
        "resolved_rate_pct": round(resolved_rate * 100, 1),
        "abandoned_rate_pct": round(abandoned_rate * 100, 1),
        "implied_success_rate_pct": round(implied_success_rate * 100, 1),
        "re_plan_rate_pct": round(re_plan_rate * 100, 1),
        "knowledge_gap_rate_pct": round(knowledge_gap_rate * 100, 1),
        "zero_result_rate_pct": round(zero_result_rate * 100, 1),
        "avg_duration_sec": round(safe_mean([s.duration_sec for s in sessions]), 1),
        "median_duration_sec": round(safe_median([s.duration_sec for s in sessions]), 1),
        "avg_turn_count": round(safe_mean([s.turn_count for s in sessions]), 2),
        "avg_planning_latency_sec": round(safe_mean([s.planning_latency_sec for s in sessions if s.planning_latency_sec > 0]), 1),
        "avg_total_response_latency_sec": round(safe_mean([s.total_response_latency_sec for s in sessions if s.total_response_latency_sec > 0]), 1),
        "avg_search_calls_per_session": round(safe_mean(all_search_calls), 1),
        "avg_filtered_results_per_session": round(safe_mean(all_filtered), 2),
        "avg_raw_results_per_session": round(safe_mean(all_raw), 1),
        "avg_search_hit_rate_pct": round(safe_mean(all_hit_rates) * 100, 1),
        "avg_plan_iterations": round(safe_mean([s.plan_iterations for s in sessions]), 2),
        "avg_unique_docs_surfaced": round(safe_mean([s.unique_docs_surfaced for s in sessions]), 1),
        "top_10_documents_surfaced": doc_freq.most_common(10),
        "sample_user_queries": all_queries[:20],
        "total_user_turns": sum(s.user_message_count for s in sessions),
        # Knowledge gap diagnostics
        "failure_category_distribution": dict(fail_cats),
        "top_failed_queries": top_failed_queries,
        "top_rejected_docs": top_rejected_docs,
        "knowledge_sources_queried": sorted(all_ks_queried),
        # Bot response rates
        "bot_response_rate_pct": round(bot_response_rate * 100, 1),
        "answered_without_filter_rate_pct": round(answered_without_filter_rate * 100, 1),
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

SEPARATOR = "─" * 70

def print_session_report(m: SessionMetrics):
    print(f"\n{SEPARATOR}")
    print(f"  SESSION: {m.file}")
    print(SEPARATOR)

    print(f"\n{'SESSION METADATA':}")
    print(f"  Channel           : {m.channel}")
    print(f"  Locale            : {m.locale}")
    print(f"  User (aadObjectId): {m.aad_object_id or '(not captured)'}")
    print(f"  Session ID        : {m.session_id or '(not captured)'}")
    print(f"  Plan identifier   : {m.plan_identifier}")
    print(f"  Design mode       : {m.design_mode}")
    print(f"  Start (AEDT)      : {m.start_time_aedt}")
    print(f"  End (AEDT)        : {m.end_time_aedt}")
    print(f"  Duration          : {m.duration_sec:.0f}s ({m.duration_sec/60:.1f} min)")

    print(f"\n{'OUTCOME KPIs':}")
    print(f"  Outcome           : {m.outcome}")
    print(f"  Outcome reason    : {m.outcome_reason}")
    print(f"  Session type      : {m.session_type}")
    print(f"  Implied success   : {m.implied_success}")
    print(f"  Turn count        : {m.turn_count}")
    print(f"  User messages     : {m.user_message_count}")
    for i, msg in enumerate(m.user_messages):
        print(f"    [{i+1}] \"{msg}\"")

    print(f"\n{'PLANNING METRICS':}")
    print(f"  Plan iterations   : {m.plan_iterations}")
    print(f"  Re-planned        : {'YES ⚠' if m.re_planned else 'No'}")
    print(f"  Total steps plan  : {m.total_steps_planned}")
    print(f"  Steps executed    : {m.steps_executed}")
    print(f"  Planning latency  : {m.planning_latency_sec:.1f}s (user msg → first plan)")

    print(f"\n{'SEARCH TOOL PERFORMANCE':}")
    print(f"  Total search calls  : {m.total_search_calls}")
    print(f"  Total exec time     : {m.total_search_execution_sec:.2f}s")
    print(f"  Total raw results   : {m.total_raw_results}")
    print(f"  Total filtered      : {m.total_filtered_results}")
    print(f"  Search hit rate     : {m.search_hit_rate*100:.0f}%")
    print(f"  Unique docs (raw)   : {m.unique_docs_surfaced}")
    print(f"  Zero result (all)   : {'YES ⚠ KNOWLEDGE GAP' if m.zero_result_all_searches else 'No'}")

    if m.search_steps:
        print(f"\n  {'#':<3} {'Exec(s)':<8} {'Raw':<5} {'Filt':<5} {'OutKS':<6}  Query")
        print(f"  {'─'*3} {'─'*7} {'─'*4} {'─'*4} {'─'*5}  {'─'*40}")
        for i, s in enumerate(m.search_steps, 1):
            query_short = (s.query or s.keywords)[:50]
            print(f"  {i:<3} {s.exec_time_str()} {s.raw_result_count:<5} {s.filtered_result_count:<5} {s.output_knowledge_sources:<6}  {query_short}")

    print(f"\n{'LATENCY':}")
    print(f"  Planning latency    : {m.planning_latency_sec:.1f}s")
    print(f"  Total response time : {m.total_response_latency_sec:.1f}s")
    print(f"  Search execution    : {m.total_search_execution_sec:.2f}s")
    print(f"  Reasoning overhead  : {max(0, m.total_response_latency_sec - m.total_search_execution_sec):.1f}s")

    print(f"\n{'RESPONSE':}")
    print(f"  Bot responded         : {'YES' if m.bot_responded else 'NO ⚠'}")
    print(f"  GPT answer state      : {m.gpt_answer_state or '(not captured)'}")
    print(f"  Completion state      : {m.completion_state or '(not captured)'}")
    print(f"  Cited knowledge srcs  : {', '.join(m.cited_knowledge_sources) if m.cited_knowledge_sources else '(none)'}")
    if m.answered_without_filter:
        print(f"  ⚠ Answered without filter: bot used raw (unfiltered) docs only")
    if m.bot_response_text:
        # Display full response text, wrapped at 120 chars
        lines = []
        words = m.bot_response_text.split()
        line = ""
        for word in words:
            if len(line) + len(word) + 1 > 116:
                lines.append(line)
                line = word
            else:
                line = (line + " " + word).strip()
        if line:
            lines.append(line)
        print(f"  Response text:")
        for ln in lines:
            print(f"    {ln}")

    print(f"\n{'DIAGNOSIS':}")
    issues = []
    if m.failure_category == "no_raw_results":
        issues.append("⚠ CRITICAL [no_raw_results]: Searches returned 0 raw results — knowledge source may be empty, misconfigured, or query is completely off-topic")
    elif m.failure_category == "filtered_out":
        issues.append(f"⚠ WARN [filtered_out+answered]: {m.max_raw_per_step} raw docs found, relevance filter rejected all, bot answered using UNFILTERED docs — answer quality/grounding at risk")
        if m.top_raw_docs_not_promoted:
            issues.append("  Docs used (raw, unfiltered): " + " | ".join(m.top_raw_docs_not_promoted[:3]))
    elif m.failure_category == "filtered_out_no_reply":
        issues.append(f"⚠ CRITICAL [filtered_out_no_reply]: {m.max_raw_per_step} raw docs found but relevance filter rejected all AND bot sent no reply — user received silence")
        if m.top_raw_docs_not_promoted:
            issues.append("  Docs returned raw but rejected: " + " | ".join(m.top_raw_docs_not_promoted[:3]))
    elif m.failure_category == "partial_filtered":
        issues.append(f"⚠ WARN [partial_filtered]: {m.search_hit_rate*100:.0f}% of searches returned filtered results — some queries not matching indexed content")
    elif m.failure_category == "no_search":
        issues.append("⚠ INFO [no_search]: User sent messages but agent did not trigger any search steps")
    if m.failed_queries:
        issues.append("  Failed queries: " + " | ".join(q[:60] for q in m.failed_queries[:3]))
    if m.knowledge_sources_queried:
        issues.append("  Knowledge sources: " + ", ".join(m.knowledge_sources_queried))
    if m.search_error_count > 0:
        issues.append(f"⚠ ERRORS: {m.search_error_count} search step(s) reported errors")
    if m.re_planned:
        issues.append("⚠ INFO: Agent re-planned mid-conversation — initial results insufficient")
    if m.total_response_latency_sec > 60:
        issues.append(f"⚠ LATENCY: Total response time {m.total_response_latency_sec:.0f}s exceeds 60s threshold")
    if m.outcome == "Abandoned" and not m.implied_success:
        issues.append("⚠ UX: Session abandoned with no implied success — user likely unsatisfied")
    if not issues:
        issues.append("✓ No critical issues detected")
    for issue in issues:
        print(f"  {issue}")


def _exec_time_str(self):
    return f"{self.execution_time_sec:<7.2f}"

SearchStepMetrics.exec_time_str = _exec_time_str


def print_aggregate_report(agg: dict):
    n = agg["total_sessions"]
    print(f"\n{'═' * 70}")
    print(f"  AGGREGATE KPIs — {n} session(s)")
    print(f"{'═' * 70}")

    print(f"\n{'ENGAGEMENT & OUTCOME':}")
    print(f"  Total sessions        : {n}")
    print(f"  Resolved rate         : {agg['resolved_rate_pct']}%")
    print(f"  Abandoned rate        : {agg['abandoned_rate_pct']}%")
    print(f"  Implied success rate  : {agg['implied_success_rate_pct']}%")
    print(f"  Outcome distribution  : {agg['outcome_distribution']}")
    print(f"  Outcome reasons       : {agg['outcome_reason_distribution']}")
    print(f"  Channel distribution  : {agg['channel_distribution']}")

    print(f"\n{'SESSION DURATION':}")
    print(f"  Avg duration          : {agg['avg_duration_sec']:.0f}s")
    print(f"  Median duration       : {agg['median_duration_sec']:.0f}s")
    print(f"  Avg turns/session     : {agg['avg_turn_count']:.1f}")
    print(f"  Total user turns      : {agg['total_user_turns']}")

    print(f"\n{'SEARCH PERFORMANCE':}")
    print(f"  Avg search calls      : {agg['avg_search_calls_per_session']:.1f}")
    print(f"  Avg filtered results  : {agg['avg_filtered_results_per_session']:.2f}")
    print(f"  Avg raw results       : {agg['avg_raw_results_per_session']:.1f}")
    print(f"  Avg search hit rate   : {agg['avg_search_hit_rate_pct']:.0f}%")
    n_search = agg.get('sessions_with_search', '?')
    print(f"  Knowledge gap rate    : {agg['knowledge_gap_rate_pct']}%  (of {n_search} sessions with search)")
    print(f"  Zero result rate      : {agg['zero_result_rate_pct']}%  (of {n_search} sessions with search)")
    print(f"  Bot response rate     : {agg['bot_response_rate_pct']}%")
    print(f"  Answered w/o filter   : {agg['answered_without_filter_rate_pct']}%  (of {n_search} sessions with search — used raw docs only)")
    print(f"  Re-plan rate          : {agg['re_plan_rate_pct']}%")
    print(f"  Avg plan iterations   : {agg['avg_plan_iterations']:.1f}")

    print(f"\n{'LATENCY':}")
    print(f"  Avg planning latency  : {agg['avg_planning_latency_sec']:.1f}s")
    print(f"  Avg total response    : {agg['avg_total_response_latency_sec']:.1f}s")

    print(f"\n{'KNOWLEDGE COVERAGE':}")
    print(f"  Avg unique docs/sess  : {agg['avg_unique_docs_surfaced']:.1f}")
    print(f"\n  Top 10 documents returned by raw search:")
    for doc, count in agg["top_10_documents_surfaced"]:
        print(f"    {count:>3}x  {doc}")

    print(f"\n{'KNOWLEDGE GAP DIAGNOSIS':}")
    fail_cats = agg.get("failure_category_distribution", {})
    if fail_cats:
        print(f"  Failure category breakdown:")
        cat_labels = {
            "filtered_out":          "Relevance filter rejected all — bot answered using raw (unfiltered) docs",
            "filtered_out_no_reply": "Relevance filter rejected all — bot sent NO reply (user received silence)",
            "no_raw_results":        "Search returned zero raw results",
            "partial_filtered":      "Some steps got results, some didn't",
            "no_search":             "User messaged but no search triggered",
        }
        total_fail = sum(fail_cats.values())
        for cat, cnt in sorted(fail_cats.items(), key=lambda x: -x[1]):
            label = cat_labels.get(cat, cat)
            print(f"    {cnt:>3}x  [{cat}]  {label}")
        print(f"  (Total sessions with a failure category: {total_fail})")
    else:
        print(f"  No failure categories detected across sessions.")
    ks_list = agg.get("knowledge_sources_queried", [])
    if ks_list:
        print(f"\n  Knowledge sources queried:")
        for ks in ks_list:
            print(f"    · {ks}")
    top_failed = agg.get("top_failed_queries", [])
    if top_failed:
        print(f"\n  Top failed search queries (0 filtered results):")
        for q, cnt in top_failed:
            print(f"    {cnt:>3}x  {q[:70]}")
    top_rejected = agg.get("top_rejected_docs", [])
    if top_rejected:
        print(f"\n  Top docs returned raw but rejected by relevance filter:")
        for doc, cnt in top_rejected:
            print(f"    {cnt:>3}x  {doc}")

    print(f"\n{'SAMPLE QUERIES':}")
    for q in agg["sample_user_queries"][:10]:
        print(f"    · {q}")

    # Agent effectiveness scorecard
    print(f"\n{'═' * 70}")
    print("  AGENT EFFECTIVENESS SCORECARD")
    print(f"{'═' * 70}")
    print(f"  {'Metric':<40} {'Score':<15} {'Benchmark'}")
    print(f"  {'─'*39} {'─'*14} {'─'*15}")
    score_rows = [
        ("Implied Success Rate",     f"{agg['implied_success_rate_pct']}%",    "Target: ≥70%",  agg['implied_success_rate_pct'] >= 70),
        ("Resolution Rate",          f"{agg['resolved_rate_pct']}%",           "Target: ≥60%",  agg['resolved_rate_pct'] >= 60),
        ("Knowledge Gap Rate",       f"{agg['knowledge_gap_rate_pct']}%",      "Target: ≤10%",  agg['knowledge_gap_rate_pct'] <= 10),
        ("Search Hit Rate",          f"{agg['avg_search_hit_rate_pct']}%",     "Target: ≥80%",  agg['avg_search_hit_rate_pct'] >= 80),
        ("Avg Response Latency",     f"{agg['avg_total_response_latency_sec']:.0f}s",  "Target: ≤30s",  agg['avg_total_response_latency_sec'] <= 30),
        ("Avg Turns to Resolution",  f"{agg['avg_turn_count']:.1f}",           "Target: ≤3",    agg['avg_turn_count'] <= 3),
        ("Re-plan Rate",             f"{agg['re_plan_rate_pct']}%",            "Target: ≤20%",  agg['re_plan_rate_pct'] <= 20),
    ]
    pass_count = 0
    for metric, score, benchmark, passed in score_rows:
        status = "✓ PASS" if passed else "✗ FAIL"
        if passed:
            pass_count += 1
        print(f"  {metric:<40} {score:<15} {benchmark:<15}  {status}")
    overall = pass_count / len(score_rows) * 100
    print(f"\n  Overall score: {pass_count}/{len(score_rows)} ({overall:.0f}%)")


def export_csv(sessions: list[SessionMetrics], agg: dict, output_path: str):
    """Export per-session KPIs to CSV."""
    fieldnames = [
        "file", "session_id", "aad_object_id", "user_name", "user_department", "channel", "locale", "start_time_aedt", "end_time_aedt",
        "duration_sec", "outcome", "outcome_reason", "implied_success", "turn_count",
        "user_message_count", "first_user_query",
        "plan_iterations", "re_planned", "total_steps_planned", "steps_executed",
        "planning_latency_sec", "total_response_latency_sec",
        "total_search_calls", "total_raw_results", "total_filtered_results",
        "total_output_knowledge_sources", "search_hit_rate", "total_search_execution_sec",
        "unique_docs_surfaced", "zero_result_all_searches", "knowledge_gap_detected",
        # Knowledge gap diagnostics
        "failure_category", "search_error_count", "max_raw_per_step",
        "failed_queries", "knowledge_sources_queried", "top_raw_docs_not_promoted",
        # Bot response diagnostics
        "bot_responded", "answered_without_filter", "gpt_answer_state",
        "completion_state", "cited_knowledge_sources", "bot_response_text",
        # User feedback
        "user_feedback_reaction", "user_feedback_text",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for s in sessions:
            row = {k: getattr(s, k, "") for k in fieldnames}
            row["first_user_query"] = s.user_messages[0] if s.user_messages else ""
            # Serialize list fields as pipe-delimited strings
            row["failed_queries"] = " | ".join(s.failed_queries)
            row["knowledge_sources_queried"] = " | ".join(s.knowledge_sources_queried)
            row["top_raw_docs_not_promoted"] = " | ".join(s.top_raw_docs_not_promoted)
            row["cited_knowledge_sources"] = " | ".join(s.cited_knowledge_sources)
            writer.writerow(row)
    print(f"\n  CSV exported to: {output_path}")


def _session_rows_for_export(sessions: list[SessionMetrics]) -> list[dict]:
    rows: list[dict] = []
    for s in sessions:
        start_date = s.start_time_aedt[:10] if s.start_time_aedt else ""
        row = {
            "file": s.file,
            "session_id": s.session_id,
            "aad_object_id": s.aad_object_id,
            "user_name": s.user_name,
            "user_department": s.user_department,
            "channel": s.channel,
            "locale": s.locale,
            "start_time_aedt": s.start_time_aedt,
            "end_time_aedt": s.end_time_aedt,
            "start_date": start_date,
            "duration_sec": s.duration_sec,
            "outcome": s.outcome,
            "outcome_reason": s.outcome_reason,
            "implied_success": s.implied_success,
            "turn_count": s.turn_count,
            "user_message_count": s.user_message_count,
            "first_user_query": s.user_messages[0] if s.user_messages else "",
            "plan_iterations": s.plan_iterations,
            "re_planned": s.re_planned,
            "total_steps_planned": s.total_steps_planned,
            "steps_executed": s.steps_executed,
            "planning_latency_sec": s.planning_latency_sec,
            "total_response_latency_sec": s.total_response_latency_sec,
            "total_search_calls": s.total_search_calls,
            "total_raw_results": s.total_raw_results,
            "total_filtered_results": s.total_filtered_results,
            "total_output_knowledge_sources": s.total_output_knowledge_sources,
            "search_hit_rate": s.search_hit_rate,
            "total_search_execution_sec": s.total_search_execution_sec,
            "unique_docs_surfaced": s.unique_docs_surfaced,
            "zero_result_all_searches": s.zero_result_all_searches,
            "knowledge_gap_detected": s.knowledge_gap_detected,
            "failure_category": s.failure_category,
            "search_error_count": s.search_error_count,
            "max_raw_per_step": s.max_raw_per_step,
            "failed_queries": " | ".join(s.failed_queries),
            "knowledge_sources_queried": " | ".join(s.knowledge_sources_queried),
            "top_raw_docs_not_promoted": " | ".join(s.top_raw_docs_not_promoted),
            "bot_responded": s.bot_responded,
            "answered_without_filter": s.answered_without_filter,
            "gpt_answer_state": s.gpt_answer_state,
            "completion_state": s.completion_state,
            "cited_knowledge_sources": " | ".join(s.cited_knowledge_sources),
            "bot_response_text": s.bot_response_text,
            "user_feedback_reaction": s.user_feedback_reaction,
            "user_feedback_text": s.user_feedback_text,
        }
        rows.append(row)
    return rows


def export_parquet(sessions: list[SessionMetrics], output_path: str):
    """Export per-session KPIs to Parquet. If output path is a directory, write a partitioned dataset by start_date."""
    try:
        import pyarrow as pa
        import pyarrow.dataset as ds
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Parquet export requires 'pyarrow'. Install dependencies from requirements.txt."
        ) from exc

    rows = _session_rows_for_export(sessions)
    if not rows:
        print("\n  Parquet export skipped: no sessions to export")
        return

    table = pa.Table.from_pylist(rows)
    output = Path(output_path)

    if output.suffix.lower() == ".parquet":
        output.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, str(output), compression="snappy")
        print(f"\n  Parquet exported to: {output}")
        return

    output.mkdir(parents=True, exist_ok=True)
    ds.write_dataset(
        data=table,
        base_dir=str(output),
        format="parquet",
        partitioning=["start_date"],
        existing_data_behavior="overwrite_or_ignore",
    )
    print(f"\n  Parquet dataset exported to: {output} (partitioned by start_date)")


# ---------------------------------------------------------------------------
# Entra enrichment (via existing Azure CLI login)
# ---------------------------------------------------------------------------

GRAPH_BATCH_URL = "https://graph.microsoft.com/v1.0/$batch"


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _load_entra_cache(cache_path: Path) -> dict:
    if not cache_path.exists():
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload.get("entries", {}) if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_entra_cache(cache_path: Path, entries: dict) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "entries": entries,
    }
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _is_cache_fresh(entry: dict, ttl_hours: int) -> bool:
    refreshed_at = entry.get("refreshed_at_utc", "")
    if not refreshed_at:
        return False
    try:
        refreshed_dt = datetime.fromisoformat(refreshed_at)
    except Exception:
        return False
    age_sec = (datetime.now(timezone.utc) - refreshed_dt).total_seconds()
    return age_sec <= ttl_hours * 3600


def _get_graph_token_from_az() -> str:
    az_candidates = []
    detected_az = shutil.which("az")
    if detected_az:
        az_candidates.append(detected_az)
    # Common Windows install path for Azure CLI.
    az_candidates.append(r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd")
    az_candidates.append("az")

    commands = [
        [
            "account",
            "get-access-token",
            "--resource-type",
            "ms-graph",
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        [
            "account",
            "get-access-token",
            "--resource",
            "https://graph.microsoft.com",
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
    ]
    last_error = ""
    for az_cmd in az_candidates:
        if az_cmd != "az" and not Path(az_cmd).exists():
            continue
        for cmd in commands:
            try:
                full_cmd = [az_cmd] + cmd
                result = subprocess.run(full_cmd, capture_output=True, text=True, check=True)
                token = (result.stdout or "").strip()
                if token:
                    return token
            except Exception as exc:
                last_error = str(exc)
    raise RuntimeError(
        "Unable to acquire Microsoft Graph token from Azure CLI. "
        "Ensure 'az login' has been completed and your account can access Graph. "
        f"Last error: {last_error}"
    )


def _get_graph_token_from_app_registration() -> str:
    tenant_id = _require_env("ENTRA_TENANT_ID")
    client_id = _require_env("ENTRA_CLIENT_ID")
    client_secret = _require_env("ENTRA_CLIENT_SECRET")

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }
    resp = requests.post(token_url, data=payload, timeout=60)
    if resp.status_code >= 400:
        raise RuntimeError(
            "Failed to acquire Microsoft Graph token using app registration. "
            f"HTTP {resp.status_code}: {resp.text}"
        )
    body = resp.json()
    access_token = body.get("access_token", "")
    if not access_token:
        raise RuntimeError("App registration token response did not include access_token")
    return access_token


def _get_graph_token(auth_mode: str) -> str:
    mode = (auth_mode or "az").strip().lower()
    if mode == "az":
        return _get_graph_token_from_az()
    if mode == "app":
        return _get_graph_token_from_app_registration()
    raise RuntimeError(
        "Unsupported Entra auth mode. Use 'az' or 'app'. "
        f"Received: {auth_mode}"
    )


def _graph_batch_lookup_users(
    object_ids: list[str],
    token: str,
    retries: int = 3,
) -> tuple[dict[str, dict], dict[str, object]]:
    if not object_ids:
        return {}, {
            "resolved": 0,
            "not_found": 0,
            "invalid_id": 0,
            "permission_denied": 0,
            "transient_errors": 0,
            "other_errors": 0,
            "permission_denied_samples": [],
        }

    resolved: dict[str, dict] = {}
    stats: dict[str, object] = {
        "resolved": 0,
        "not_found": 0,
        "invalid_id": 0,
        "permission_denied": 0,
        "transient_errors": 0,
        "other_errors": 0,
        "permission_denied_samples": [],
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Microsoft Graph batch API supports up to 20 requests per batch.
    batch_size = 20
    for i in range(0, len(object_ids), batch_size):
        batch_ids = object_ids[i:i + batch_size]
        requests_payload = []
        for idx, object_id in enumerate(batch_ids):
            requests_payload.append({
                "id": str(idx),
                "method": "GET",
                "url": f"/users/{object_id}?$select=id,displayName,department",
            })

        payload = {"requests": requests_payload}

        attempt = 0
        while True:
            attempt += 1
            resp = requests.post(GRAPH_BATCH_URL, headers=headers, json=payload, timeout=60)
            if resp.status_code == 429 and attempt <= retries:
                retry_after = int(resp.headers.get("Retry-After", "2"))
                time.sleep(retry_after)
                continue
            if resp.status_code in (401, 403):
                raise RuntimeError(
                    "Microsoft Graph access denied (401/403). The current az login user likely "
                    "lacks directory read permissions to resolve Entra user object IDs."
                )
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("responses", []):
                req_id = item.get("id", "")
                try:
                    oid = batch_ids[int(req_id)]
                except Exception:
                    continue
                status = item.get("status", 0)
                body = item.get("body") or {}
                if status == 200:
                    resolved[oid] = {
                        "user_name": body.get("displayName", "") or "",
                        "user_department": body.get("department", "") or "",
                        "refreshed_at_utc": datetime.now(timezone.utc).isoformat(),
                    }
                    stats["resolved"] = int(stats["resolved"]) + 1
                elif status in (401, 403):
                    stats["permission_denied"] = int(stats["permission_denied"]) + 1
                    samples = stats["permission_denied_samples"]
                    if isinstance(samples, list) and len(samples) < 5:
                        samples.append(oid)
                elif status == 404:
                    # User ID does not exist in tenant: safe to negative-cache.
                    resolved[oid] = {
                        "user_name": "",
                        "user_department": "",
                        "refreshed_at_utc": datetime.now(timezone.utc).isoformat(),
                    }
                    stats["not_found"] = int(stats["not_found"]) + 1
                elif status == 400:
                    # Invalid object ID format/request: safe to negative-cache.
                    resolved[oid] = {
                        "user_name": "",
                        "user_department": "",
                        "refreshed_at_utc": datetime.now(timezone.utc).isoformat(),
                    }
                    stats["invalid_id"] = int(stats["invalid_id"]) + 1
                elif status in (429, 500, 502, 503, 504):
                    # Transient failures should not be negative-cached.
                    stats["transient_errors"] = int(stats["transient_errors"]) + 1
                else:
                    # Unknown failures should not be negative-cached.
                    stats["other_errors"] = int(stats["other_errors"]) + 1
            break

    return resolved, stats


def enrich_sessions_from_entra_via_az_login(
    sessions: list[SessionMetrics],
    cache_path: Path,
    auth_mode: str = "az",
    cache_ttl_hours: int = 24,
) -> None:
    unique_ids = sorted({s.aad_object_id for s in sessions if s.aad_object_id})
    if not unique_ids:
        print("\n  Entra enrichment skipped: no aad_object_id values found")
        return

    cache_entries = _load_entra_cache(cache_path)
    unresolved: list[str] = []

    for object_id in unique_ids:
        entry = cache_entries.get(object_id)
        if entry and _is_cache_fresh(entry, cache_ttl_hours):
            continue
        unresolved.append(object_id)

    if unresolved:
        print(
            "\n  Entra enrichment: resolving "
            f"{len(unresolved)} users from Microsoft Graph via auth mode '{auth_mode}'..."
        )
        token = _get_graph_token(auth_mode)
        fetched, lookup_stats = _graph_batch_lookup_users(unresolved, token)
        if fetched:
            cache_entries.update(fetched)
            _save_entra_cache(cache_path, cache_entries)
        print(f"  Entra enrichment: refreshed {len(fetched)} cache entries")

        permission_denied = int(lookup_stats.get("permission_denied", 0))
        if permission_denied > 0:
            print(
                "  WARNING: Entra enrichment permission denied for "
                f"{permission_denied} user lookups (401/403)."
            )
            sample_ids = lookup_stats.get("permission_denied_samples", [])
            if isinstance(sample_ids, list) and sample_ids:
                print(f"  Sample denied aad_object_id values: {' | '.join(sample_ids)}")
            print(
                "  The current az login user likely needs directory read access "
                "(for example, permission to read users in Entra ID)."
            )

        transient_errors = int(lookup_stats.get("transient_errors", 0))
        if transient_errors > 0:
            print(
                "  WARNING: Entra enrichment had transient Graph failures for "
                f"{transient_errors} user lookups. These were not cached and will retry next run."
            )

        other_errors = int(lookup_stats.get("other_errors", 0))
        if other_errors > 0:
            print(
                "  WARNING: Entra enrichment had "
                f"{other_errors} unexpected Graph errors. These were not cached."
            )
    else:
        print("\n  Entra enrichment: all users served from cache")

    enriched = 0
    for s in sessions:
        if not s.aad_object_id:
            continue
        entry = cache_entries.get(s.aad_object_id, {})
        s.user_name = entry.get("user_name", "") or ""
        s.user_department = entry.get("user_department", "") or ""
        if s.user_name or s.user_department:
            enriched += 1
    print(f"  Entra enrichment applied to {enriched}/{len(sessions)} sessions")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def collect_files(args: list[str]) -> list[str]:
    """Collect JSON/JSONL transcript inputs from args or default extracted content JSONL files."""
    files = []
    if not args:
        default_dir = Path(__file__).parent.parent / "output"
        if default_dir.exists():
            files = [str(p) for p in default_dir.glob("*_content.jsonl")]
        if not files:
            print(f"No transcript input files found in {default_dir}")
    else:
        for arg in args:
            p = Path(arg)
            if p.is_dir():
                files.extend(str(x) for x in p.glob("*.json"))
                files.extend(str(x) for x in p.glob("*.jsonl"))
            elif p.is_file() and p.suffix in {".json", ".jsonl"}:
                files.append(str(p))
    return files


def main():
    # Ensure Unicode output works on Windows consoles
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    # Keep .env behavior consistent with other scripts in this repo.
    _load_dotenv(Path(__file__).parent.parent / ".env")

    csv_path = None
    parquet_path = None
    enrich_entra = False
    entra_auth_mode = os.getenv("ENTRA_AUTH_MODE", "az").strip().lower() or "az"
    entra_cache_path: Optional[str] = None
    entra_cache_ttl_hours = 24
    raw_args = sys.argv[1:]
    skip_next = False
    args = []
    for i, a in enumerate(raw_args):
        if skip_next:
            skip_next = False
            continue
        if a == "--csv":
            if i + 1 < len(raw_args):
                csv_path = raw_args[i + 1]
            skip_next = True
            continue
        if a == "--parquet":
            if i + 1 < len(raw_args):
                parquet_path = raw_args[i + 1]
            skip_next = True
            continue
        if a == "--enrich-entra":
            enrich_entra = True
            continue
        if a == "--entra-auth-mode":
            if i + 1 < len(raw_args):
                entra_auth_mode = raw_args[i + 1].strip().lower()
            skip_next = True
            continue
        if a == "--entra-cache":
            if i + 1 < len(raw_args):
                entra_cache_path = raw_args[i + 1]
            skip_next = True
            continue
        if a == "--entra-cache-ttl-hours":
            if i + 1 < len(raw_args):
                try:
                    entra_cache_ttl_hours = int(raw_args[i + 1])
                except ValueError:
                    print("Invalid value for --entra-cache-ttl-hours; using default 24")
            skip_next = True
            continue
        args.append(a)

    files = collect_files(args)
    if not files:
        print("Usage: python tools/transcript_analytics.py [file_or_dir] [--csv output.csv] [--parquet output.parquet|output_dir]")
        sys.exit(1)

    sessions: list[SessionMetrics] = []
    for fp in sorted(files):
        try:
            if Path(fp).suffix.lower() == ".jsonl":
                batch = parse_transcripts_from_jsonl(fp)
                sessions.extend(batch)
                for m in batch:
                    print_session_report(m)
            else:
                m = parse_transcript(fp)
                sessions.append(m)
                print_session_report(m)
        except Exception as e:
            print(f"  ERROR parsing {fp}: {e}")
            import traceback; traceback.print_exc()

    if not sessions:
        print("No valid sessions were parsed. Nothing to aggregate.")
        sys.exit(1)

    if len(sessions) > 1 or True:  # always show aggregate for single too
        agg = aggregate_sessions(sessions)
        print_aggregate_report(agg)

    if enrich_entra:
        default_cache = Path(__file__).parent.parent / "output" / "entra_user_cache.json"
        cache_path = Path(entra_cache_path).resolve() if entra_cache_path else default_cache
        try:
            enrich_sessions_from_entra_via_az_login(
                sessions=sessions,
                cache_path=cache_path,
                auth_mode=entra_auth_mode,
                cache_ttl_hours=entra_cache_ttl_hours,
            )
        except Exception as exc:
            print(f"\n  WARNING: Entra enrichment failed: {exc}")
            print("  Continuing without user name/department enrichment.")

    if csv_path:
        agg = agg if 'agg' in dir() else aggregate_sessions(sessions)
        export_csv(sessions, agg, csv_path)

    if parquet_path:
        export_parquet(sessions, parquet_path)


if __name__ == "__main__":
    main()
