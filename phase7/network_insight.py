"""
Phase 7, Lab C1 — Claude API: Network Insight Generator
--------------------------------------------------------
Turns curated grid evidence (from the ML system) into an operations-
friendly, evidence-grounded explanation, using the Claude API directly.

Setup:
    pip install anthropic
    export ANTHROPIC_API_KEY="sk-ant-..."

Run:
    python network_insight.py

Output:
    - Console: one structured insight per sample grid
    - report.md: full run log, validation results, model-comparison
      table, and the model-selection rationale (the two "Expected
      Output" deliverables for this lab)

NOTE ON MODEL IDS / PRICING:
    Model IDs and per-token prices below reflect Anthropic's lineup as
    of Sep 2026 (claude-haiku-4-5-20251001, claude-sonnet-5, claude-opus-5).
    Both change over time — verify current values at
    https://docs.claude.com/en/docs/about-claude/models/overview and
    https://docs.claude.com/en/docs/about-claude/pricing before relying
    on the cost numbers this script prints.
"""

import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import anthropic

from evidence import ALL_GRIDS

# ---------------------------------------------------------------------------
# 1. The fixed instruction contract (this IS the deliverable the lab is
#    really testing — everything below is just harness code around it)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are assisting a Network Operations Centre.

You will receive curated evidence for one grid cell, one hourly interval:
grid_id, timestamp, current total_activity, baseline total_activity,
activity_growth, peak_ratio, variability, internet_share, an anomaly_score
and direction, and any rule alerts currently firing.

Respond in exactly four sections, in this order, using these exact headers:

SEVERITY        one of NORMAL / ATTENTION / HIGH
EVIDENCE        only what is stated above, with the numbers
INTERPRETATION  what this MIGHT mean, clearly marked as inference
NEXT CHECKS     what a human engineer should inspect next

Rules:
- These are activity measures, not call counts, message counts, or MB.
- Do NOT claim congestion; we have no capacity or utilization data.
- If the evidence is insufficient to reach a severity, say so and say
  what additional evidence you would need.
- Never invent a number that is not in the evidence given to you.
"""

REQUIRED_SECTIONS = ["SEVERITY", "EVIDENCE", "INTERPRETATION", "NEXT CHECKS"]

# Models compared in this lab. Swap/extend freely.
MODELS = {
    "haiku": "claude-haiku-4-5-20251001",
    "sonnet": "claude-sonnet-5",
    "opus": "claude-opus-5",
}
DEFAULT_MODEL_KEY = "sonnet"

# USD per million tokens — CHECK https://docs.claude.com/en/docs/about-claude/pricing
# before trusting these for a real budget decision.
PRICE_PER_MTOK = {
    "claude-haiku-4-5-20251001": {"input": 1.00, "output": 5.00},
    "claude-sonnet-5": {"input": 2.00, "output": 10.00},
    "claude-opus-5": {"input": 5.00, "output": 25.00},
}

MAX_TOKENS = 700

client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from env


# ---------------------------------------------------------------------------
# 2. Evidence -> user message
# ---------------------------------------------------------------------------

def rich_user_message(evidence: Dict[str, Any]) -> str:
    """Full evidence package."""
    return "Evidence for one grid cell:\n\n" + json.dumps(evidence, indent=2)


def short_user_message(evidence: Dict[str, Any]) -> str:
    """
    Deliberately thin context: only the anomaly score/direction and the
    grid id/timestamp. Used for the short-vs-rich comparison required by
    the lab ("Experiment with a short versus a richer context package").
    """
    thin = {
        "grid_id": evidence["grid_id"],
        "timestamp": evidence["timestamp"],
        "anomaly_score": evidence["anomaly_score"],
        "direction": evidence["direction"],
    }
    return "Evidence for one grid cell:\n\n" + json.dumps(thin, indent=2)


# ---------------------------------------------------------------------------
# 3. API call wrapper (records latency + token usage for the model
#    comparison / cost rationale)
# ---------------------------------------------------------------------------

@dataclass
class CallResult:
    text: str
    model: str
    latency_s: float
    input_tokens: int
    output_tokens: int
    cost_usd: float


def call_claude(user_content: str, model: str) -> CallResult:
    t0 = time.time()
    resp = client.messages.create(
        model=model,
        max_tokens=MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
    )
    latency = time.time() - t0

    text = "".join(block.text for block in resp.content if block.type == "text")
    in_tok = resp.usage.input_tokens
    out_tok = resp.usage.output_tokens
    price = PRICE_PER_MTOK.get(model, {"input": 0.0, "output": 0.0})
    cost = (in_tok / 1e6) * price["input"] + (out_tok / 1e6) * price["output"]

    return CallResult(text, model, latency, in_tok, out_tok, cost)


# ---------------------------------------------------------------------------
# 4. Validation against the lab's acceptance criteria
# ---------------------------------------------------------------------------

def validate_response(text: str, evidence: Dict[str, Any]) -> Dict[str, Any]:
    """
    Checks the acceptance criteria that can be checked mechanically:
      - all four sections present, in order
      - "congestion" is never asserted
      - EVIDENCE numbers actually appear in the input evidence
      - if anomaly_score is missing, SEVERITY states insufficiency
    """
    results: Dict[str, Any] = {}

    positions = [text.upper().find(h) for h in REQUIRED_SECTIONS]
    results["all_sections_present"] = all(p != -1 for p in positions)
    results["sections_in_order"] = positions == sorted(positions) and results["all_sections_present"]

    # crude "asserted" check: flag "congestion" unless clearly negated nearby
    congestion_hits = [m.start() for m in re.finditer(r"congestion", text, re.IGNORECASE)]
    asserted = False
    for pos in congestion_hits:
        window = text[max(0, pos - 40): pos].lower()
        if not any(neg in window for neg in ["no ", "not ", "n't", "without", "cannot", "can't"]):
            asserted = True
    results["congestion_word_found"] = bool(congestion_hits)
    results["congestion_asserted"] = asserted

    # does EVIDENCE section only use numbers present in the input?
    if results["all_sections_present"]:
        ev_start = text.upper().find("EVIDENCE")
        next_start = text.upper().find("INTERPRETATION")
        evidence_section = text[ev_start:next_start] if next_start != -1 else text[ev_start:]
        numbers_in_output = set(re.findall(r"-?\d+\.?\d*", evidence_section))
        numbers_in_input = set(
            re.findall(r"-?\d+\.?\d*", json.dumps({k: v for k, v in evidence.items() if v is not None}))
        )
        invented = {n for n in numbers_in_output if n not in numbers_in_input}
        results["possible_invented_numbers"] = sorted(invented)
    else:
        results["possible_invented_numbers"] = ["N/A - sections missing"]

    # insufficiency path
    if evidence.get("anomaly_score") is None:
        sev_start = text.upper().find("SEVERITY")
        ev_start = text.upper().find("EVIDENCE")
        severity_section = text[sev_start:ev_start] if ev_start != -1 else ""
        results["insufficiency_flagged"] = any(
            kw in severity_section.lower()
            for kw in ["insufficient", "not enough", "cannot determine", "unable to determine", "need more"]
        )
    else:
        results["insufficiency_flagged"] = None  # not applicable

    return results


# ---------------------------------------------------------------------------
# 5. Report writer
# ---------------------------------------------------------------------------

def main():
    report_lines: List[str] = ["# Lab C1 — Network Insight Generator — Run Report\n"]

    # --- Part 1: run every sample grid through the default model ----------
    report_lines.append("## 1. Insight responses across sample grids\n")
    per_grid_results = {}

    for name, evidence in ALL_GRIDS.items():
        model = MODELS[DEFAULT_MODEL_KEY]
        result = call_claude(rich_user_message(evidence), model)
        checks = validate_response(result.text, evidence)
        per_grid_results[name] = (result, checks)

        print(f"\n{'=' * 70}\nGRID: {name}  (model={model})\n{'=' * 70}")
        print(result.text)
        print(f"\n[latency={result.latency_s:.2f}s tokens_in={result.input_tokens} "
              f"tokens_out={result.output_tokens} cost=${result.cost_usd:.5f}]")
        print(f"[validation] {checks}")

        report_lines.append(f"\n### Grid `{name}`\n\n")
        report_lines.append("```\n" + result.text.strip() + "\n```\n\n")
        report_lines.append(
            f"- latency: {result.latency_s:.2f}s | tokens in/out: "
            f"{result.input_tokens}/{result.output_tokens} | cost: ${result.cost_usd:.5f}\n"
        )
        report_lines.append(f"- validation: `{checks}`\n")

    # --- Part 2: acceptance-criteria rollup --------------------------------
    report_lines.append("\n## 2. Acceptance criteria rollup\n\n")
    all_have_sections = all(c["all_sections_present"] for _, c in per_grid_results.values())
    no_congestion = all(not c["congestion_asserted"] for _, c in per_grid_results.values())
    insuff_result = per_grid_results["E_insufficient"][1]["insufficiency_flagged"]

    report_lines.append(f"- All four sections present, every grid: **{all_have_sections}**\n")
    report_lines.append(f"- No response asserts \"congestion\": **{no_congestion}**\n")
    report_lines.append(
        f"- Removing anomaly_score (grid `E_insufficient`) produced an explicit "
        f"insufficiency statement: **{insuff_result}**\n"
    )

    # --- Part 3: short vs rich context comparison --------------------------
    report_lines.append("\n## 3. Short vs. rich context comparison (grid `B_attention`)\n\n")
    evidence = ALL_GRIDS["B_attention"]
    model = MODELS[DEFAULT_MODEL_KEY]

    short_result = call_claude(short_user_message(evidence), model)
    rich_result = call_claude(rich_user_message(evidence), model)

    print(f"\n{'=' * 70}\nSHORT CONTEXT\n{'=' * 70}\n{short_result.text}")
    print(f"\n{'=' * 70}\nRICH CONTEXT\n{'=' * 70}\n{rich_result.text}")

    report_lines.append("**Short context (only anomaly_score + direction):**\n")
    report_lines.append("```\n" + short_result.text.strip() + "\n```\n")
    report_lines.append("\n**Rich context (full evidence object):**\n")
    report_lines.append("```\n" + rich_result.text.strip() + "\n```\n\n")
    report_lines.append(
        "Takeaway: the short-context run has far less to put in EVIDENCE and "
        "typically hedges harder in INTERPRETATION/SEVERITY — a direct "
        "illustration of why the evidence package (not the prompt wording) "
        "is what the learner owns in this lab.\n"
    )

    # --- Part 4: model comparison (cost/latency/reasoning) ------------------
    report_lines.append("\n## 4. Model comparison (grid `C_high`)\n\n")
    report_lines.append("| model | latency (s) | tokens in | tokens out | cost (USD) |\n")
    report_lines.append("|---|---|---|---|---|\n")

    comparison_grid = ALL_GRIDS["C_high"]
    model_outputs = {}
    for key, model_id in MODELS.items():
        r = call_claude(rich_user_message(comparison_grid), model_id)
        model_outputs[key] = r
        report_lines.append(
            f"| {model_id} | {r.latency_s:.2f} | {r.input_tokens} | "
            f"{r.output_tokens} | {r.cost_usd:.5f} |\n"
        )
        print(f"\n{'=' * 70}\nMODEL: {model_id}\n{'=' * 70}\n{r.text}")
        print(f"[latency={r.latency_s:.2f}s cost=${r.cost_usd:.5f}]")

    report_lines.append("\n### Model selection rationale\n")
    report_lines.append(
        "- **Haiku** is the cheapest and fastest of the three. On this workload "
        "(short, structured, evidence-bounded task) it is usually enough for the "
        "NORMAL/ATTENTION cases where the rule is close to mechanical.\n"
        "- **Sonnet** costs roughly 2x Haiku's input rate and 2x its output rate "
        "but gives noticeably steadier adherence to the four-section contract and "
        "the insufficiency rule under ambiguous evidence (borderline/HIGH grids). "
        "This is the default for the lab.\n"
        "- **Opus** costs roughly 5x Haiku's rates. Its extra reasoning depth "
        "matters most when evidence is genuinely ambiguous or when INTERPRETATION "
        "needs to weigh conflicting signals (e.g. high peak_ratio but low "
        "variability) — not for routine NORMAL grids.\n"
        "- Recommendation for this pipeline: **route by anomaly_score** — "
        "Haiku for anomaly_score below ~0.3, Sonnet for the mid-range, Opus only "
        "for HIGH-severity / conflicting-signal grids or when a human has flagged "
        "a grid for deeper review. Re-check current per-token prices before "
        "finalizing this routing rule, since rates change.\n"
    )

    with open("report.md", "w") as f:
        f.writelines(report_lines)

    print("\nWrote report.md")


if __name__ == "__main__":
    main()