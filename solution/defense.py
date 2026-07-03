"""
Your defense. Implement register(ctx) and a handler per event type.
See ../README.md for the full interface + toolkit reference, and
../RULES.md before you start.
"""
from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def check_data_batch(payload, ctx):
    profile = ctx.tools.batch_profile(payload["batch_id"])
    if "error" in profile:
        return Verdict(alert=False, pillar="checks")

    b = ctx.baseline
    reasons = []
    rc = profile["row_count"]
    if rc < b["row_count_min"] * 0.85 or rc > b["row_count_max"] * 1.15:
        reasons.append("volume")
    nr = profile["null_rate"]["customer_id"]
    if nr > b["null_rate_max"] * 2.0:
        reasons.append("null_spike")
    ma = profile["mean_amount"]
    if ma < b["mean_amount_min"] * 0.85 or ma > b["mean_amount_max"] * 1.15:
        reasons.append("distribution")
    sm = profile["staleness_min"]
    if sm > b["staleness_min_max"] * 1.2:
        reasons.append("freshness")
    return Verdict(alert=len(reasons) > 0, pillar="checks", reason=";".join(reasons))


def check_contract_checkpoint(payload, ctx):
    diff = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if "error" in diff:
        return Verdict(alert=False, pillar="contracts")
    reasons = []
    if diff.get("violations"):
        reasons.extend(diff["violations"])
    fd = diff.get("freshness_delay_min", 0)
    if fd > ctx.baseline["freshness_delay_max_min"] * 1.2:
        reasons.append("stale")
    return Verdict(alert=len(reasons) > 0, pillar="contracts", reason=";".join(reasons))


def check_lineage_run(payload, ctx):
    slc = ctx.tools.lineage_graph_slice(payload["run_id"])
    if "error" in slc:
        return Verdict(alert=False, pillar="lineage")

    b = ctx.baseline
    reasons = []
    dur = slc["duration_ms"]
    up = slc.get("actual_upstream", [])
    dc = slc.get("actual_downstream_count", 0)

    if dur > b["lineage_duration_ms_max"]:
        reasons.append("runtime")
    if not up:
        reasons.append("missing_upstream")
    if dc == 0:
        reasons.append("orphan_output")

    return Verdict(alert=len(reasons) > 0, pillar="lineage", reason=";".join(reasons))


def check_feature_materialization(payload, ctx):
    drift = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if "error" in drift:
        return Verdict(alert=False, pillar="ai_infra")
    sigma = drift.get("mean_shift_sigma", 0)
    if sigma > ctx.baseline["feature_mean_shift_sigma_max"] * 0.65:
        return Verdict(alert=True, pillar="ai_infra", reason=f"skew sigma={sigma:.3f}")
    return Verdict(alert=False, pillar="ai_infra")


def check_embedding_batch(payload, ctx):
    drift = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if "error" in drift:
        return Verdict(alert=False, pillar="ai_infra")
    b = ctx.baseline
    reasons = []
    cs = drift.get("centroid_shift", 0)
    if cs > b["embedding_centroid_shift_max"] * 0.8:
        reasons.append("drift")
    age = drift.get("avg_doc_age_days", 0)
    if age > b["corpus_avg_doc_age_days_max"] * 1.1:
        reasons.append("stale")
    return Verdict(alert=len(reasons) > 0, pillar="ai_infra", reason=";".join(reasons))
