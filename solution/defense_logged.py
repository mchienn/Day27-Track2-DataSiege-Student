import sys, json
from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)



# ---------------------------------------------------------------------------
#  ADAPTIVE STATISTICS HELPERS
# ---------------------------------------------------------------------------

def _ensure_stats(s, key):
    """Initialise a running-statistics accumulator in ctx.state if absent."""
    if key not in s:
        s[key] = {"n": 0, "sum": 0.0, "sum2": 0.0,
                  "min": float("inf"), "max": float("-inf")}


def _update_stat(s, key, value):
    """Update running mean / variance accumulator (clean events only)."""
    acc = s[key]
    acc["n"] += 1
    acc["sum"] += value
    acc["sum2"] += value * value
    if value < acc["min"]:
        acc["min"] = value
    if value > acc["max"]:
        acc["max"] = value


def _get_z(s, key, value):
    """Return z-score of value vs running stats.  None if too few samples."""
    acc = s.get(key)
    if acc is None or acc["n"] < 8:
        return None
    mean = acc["sum"] / acc["n"]
    var = acc["sum2"] / acc["n"] - mean * mean
    if var <= 0:
        return None
    std = var ** 0.5
    if std < 1e-9:
        return None
    return (value - mean) / std


# ---------------------------------------------------------------------------
#  DATA BATCH (checks pillar)
# ---------------------------------------------------------------------------

def check_data_batch(payload, ctx):
    profile = ctx.tools.batch_profile(payload["batch_id"])
    if "error" in profile:
        print("LOG_DATA_BATCH_ERROR:" + json.dumps({"payload": payload, "profile": profile}), file=sys.stderr)
        return Verdict(alert=False, pillar="checks")

    b = ctx.baseline
    s = ctx.state
    reasons = []
    score = 0.0

    rc = profile["row_count"]
    nr = profile["null_rate"]["customer_id"]
    ma = profile["mean_amount"]
    sa = profile["std_amount"]
    sm = profile["staleness_min"]

    # --- Static threshold checks ---
    rc_lo, rc_hi = b["row_count_min"], b["row_count_max"]
    if rc < rc_lo * 0.85 or rc > rc_hi * 1.15:
        reasons.append("volume")
        score += 3.0
    elif rc < rc_lo * 0.92 or rc > rc_hi * 1.08:
        score += 1.0

    nr_max = b["null_rate_max"]
    if nr > nr_max * 2.0:
        reasons.append("null_spike")
        score += 3.0
    elif nr > nr_max * 1.5:
        score += 1.0

    ma_lo, ma_hi = b["mean_amount_min"], b["mean_amount_max"]
    if ma < ma_lo * 0.85 or ma > ma_hi * 1.15:
        reasons.append("distribution")
        score += 3.0
    elif ma < ma_lo * 0.92 or ma > ma_hi * 1.08:
        score += 1.0

    sm_max = b["staleness_min_max"]
    if sm > sm_max * 1.2:
        reasons.append("freshness")
        score += 3.0
    elif sm > sm_max * 1.05:
        score += 1.0

    # --- Adaptive z-score checks (only boost, never standalone trigger) ---
    adaptive_boost = 0.0
    for metric_name, value in [("db_rc", rc), ("db_nr", nr), ("db_ma", ma),
                                ("db_sa", sa), ("db_sm", sm)]:
        _ensure_stats(s, metric_name)
        z = _get_z(s, metric_name, value)
        if z is not None:
            az = abs(z)
            if az > 4.0:
                adaptive_boost += 1.5
            elif az > 3.5:
                adaptive_boost += 0.8
            elif az > 3.0:
                adaptive_boost += 0.3

    # Adaptive boost only helps if there's already some static evidence
    if score >= 1.0:
        score += adaptive_boost
    elif adaptive_boost >= 3.0:
        # Multiple metrics all very anomalous — likely a real fault
        score += adaptive_boost
        reasons.append("multi_adaptive")

    alerted = score >= 2.5
    if not alerted:
        for metric_name, value in [("db_rc", rc), ("db_nr", nr), ("db_ma", ma),
                                    ("db_sa", sa), ("db_sm", sm)]:
            _update_stat(s, metric_name, value)

    _v = Verdict(alert=alerted, pillar="checks", reason=";".join(reasons) if reasons else "")
    print("LOG_DATA_BATCH:" + json.dumps({"payload": payload, "profile": profile, "verdict": {"alert": _v.alert, "reason": _v.reason}}), file=sys.stderr)
    return _v


# ---------------------------------------------------------------------------
#  CONTRACT CHECKPOINT (contracts pillar)
# ---------------------------------------------------------------------------

def check_contract_checkpoint(payload, ctx):
    diff = ctx.tools.contract_diff(payload["contract_id"],
                                    payload["checkpoint_batch_id"])
    if "error" in diff:
        print("LOG_CONTRACT_ERROR:" + json.dumps({"payload": payload, "diff": diff}), file=sys.stderr)
        return Verdict(alert=False, pillar="contracts")

    reasons = []
    score = 0.0

    # Schema/type violations are binary — always alert
    if diff.get("violations"):
        reasons.extend(diff["violations"])
        score += 5.0

    # Freshness delay
    fd = diff.get("freshness_delay_min", 0)
    fd_max = ctx.baseline["freshness_delay_max_min"]
    if fd > fd_max * 1.2:
        reasons.append("stale")
        score += 3.0
    elif fd > fd_max * 1.0:
        score += 1.0

    # Adaptive freshness delay
    s = ctx.state
    _ensure_stats(s, "ct_fd")
    z = _get_z(s, "ct_fd", fd)
    if z is not None and z > 4.0 and score >= 1.0:
        score += 1.5
        reasons.append("adaptive_stale")

    alerted = score >= 2.5
    if not alerted:
        _update_stat(s, "ct_fd", fd)

    _v = Verdict(alert=alerted, pillar="contracts", reason=";".join(reasons) if reasons else "")
    print("LOG_CONTRACT:" + json.dumps({"payload": payload, "diff": diff, "verdict": {"alert": _v.alert, "reason": _v.reason}}), file=sys.stderr)
    return _v


# ---------------------------------------------------------------------------
#  LINEAGE RUN (lineage pillar)
# ---------------------------------------------------------------------------

def check_lineage_run(payload, ctx):
    slc = ctx.tools.lineage_graph_slice(payload["run_id"])
    if "error" in slc:
        print("LOG_LINEAGE_ERROR:" + json.dumps({"payload": payload, "slc": slc}), file=sys.stderr)
        return Verdict(alert=False, pillar="lineage")

    s = ctx.state
    b = ctx.baseline
    reasons = []
    score = 0.0

    dur = slc["duration_ms"]
    up = slc.get("actual_upstream", [])
    dc = slc.get("actual_downstream_count", 0)
    n_up = len(up)

    # --- Static checks ---
    dur_max = b["lineage_duration_ms_max"]
    if dur > dur_max:
        reasons.append("runtime")
        score += 3.0
    elif dur > dur_max * 0.9:
        score += 1.0

    if not up:
        reasons.append("missing_upstream")
        score += 3.0

    if dc == 0:
        reasons.append("orphan_output")
        score += 3.0

    # --- Rolling statistics checks (ratio-based) ---
    _ensure_stats(s, "lk_up_n")
    _ensure_stats(s, "lk_dn_n")
    _ensure_stats(s, "lk_dur")

    # Upstream count anomaly via ratio
    if n_up > 0:
        acc_up = s["lk_up_n"]
        if acc_up["n"] >= 3:
            avg_up = acc_up["sum"] / acc_up["n"]
            if avg_up > 0 and n_up <= avg_up * 0.55:
                if "missing_upstream" not in reasons:
                    reasons.append("missing_upstream")
                score += 2.5

        # z-score check
        z_up = _get_z(s, "lk_up_n", n_up)
        if z_up is not None and z_up < -2.5:
            if "missing_upstream" not in reasons:
                reasons.append("missing_upstream")
            score += 1.5

    # Downstream count anomaly
    if dc > 0:
        acc_dn = s["lk_dn_n"]
        if acc_dn["n"] >= 3:
            avg_dn = acc_dn["sum"] / acc_dn["n"]
            if avg_dn > 0 and dc <= avg_dn * 0.50:
                if "orphan_output" not in reasons:
                    reasons.append("orphan_output")
                score += 2.5

        z_dn = _get_z(s, "lk_dn_n", dc)
        if z_dn is not None and z_dn < -2.5:
            if "orphan_output" not in reasons:
                reasons.append("orphan_output")
            score += 1.5

    # Runtime z-score boost
    z_dur = _get_z(s, "lk_dur", dur)
    if z_dur is not None and z_dur > 3.5:
        if "runtime" not in reasons:
            reasons.append("runtime")
        score += 1.5

    alerted = score >= 2.5

    if not alerted:
        if n_up > 0:
            _update_stat(s, "lk_up_n", n_up)
        if dc > 0:
            _update_stat(s, "lk_dn_n", dc)
        _update_stat(s, "lk_dur", dur)

    _v = Verdict(alert=alerted, pillar="lineage", reason=";".join(reasons) if reasons else "")
    print("LOG_LINEAGE:" + json.dumps({"payload": payload, "slc": slc, "verdict": {"alert": _v.alert, "reason": _v.reason}}), file=sys.stderr)
    return _v


# ---------------------------------------------------------------------------
#  FEATURE MATERIALIZATION (ai_infra pillar)
# ---------------------------------------------------------------------------

def check_feature_materialization(payload, ctx):
    drift = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if "error" in drift:
        print("LOG_FEATURE_ERROR:" + json.dumps({"payload": payload, "drift": drift}), file=sys.stderr)
        return Verdict(alert=False, pillar="ai_infra")

    s = ctx.state
    b = ctx.baseline
    reasons = []
    score = 0.0

    sigma = drift.get("mean_shift_sigma", 0)
    serve_mean = drift.get("serve_mean", 0)
    train_mean = drift.get("train_mean", 0)
    train_std = drift.get("train_std", 1)

    # --- Static threshold ---
    sigma_max = b["feature_mean_shift_sigma_max"]  # 0.4095

    if sigma > sigma_max * 0.92:
        reasons.append("skew")
        score += 3.0
    elif sigma > sigma_max * 0.80:
        score += 1.0
    elif sigma > sigma_max * 0.65:
        score += 0.3

    # --- Adaptive z-score on sigma ---
    _ensure_stats(s, "ft_sigma")
    z_sigma = _get_z(s, "ft_sigma", sigma)
    if z_sigma is not None:
        if z_sigma > 4.5:
            score += 2.0
            reasons.append("adaptive_skew")
        elif z_sigma > 4.0:
            score += 1.0
        elif z_sigma > 3.5 and score >= 0.5:
            score += 0.5

    # --- Adaptive z-score on serve_mean ---
    _ensure_stats(s, "ft_serve")
    z_serve = _get_z(s, "ft_serve", serve_mean)
    if z_serve is not None and abs(z_serve) > 4.5:
        score += 1.0

    alerted = score >= 2.5

    if not alerted:
        _update_stat(s, "ft_sigma", sigma)
        _update_stat(s, "ft_serve", serve_mean)

    _v = Verdict(alert=alerted, pillar="ai_infra", reason=";".join(reasons) if reasons else f"sigma={sigma:.3f}")
    print("LOG_FEATURE:" + json.dumps({"payload": payload, "drift": drift, "verdict": {"alert": _v.alert, "reason": _v.reason}}), file=sys.stderr)
    return _v


# ---------------------------------------------------------------------------
#  EMBEDDING BATCH (ai_infra pillar)
# ---------------------------------------------------------------------------

def check_embedding_batch(payload, ctx):
    drift = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if "error" in drift:
        print("LOG_FEATURE_ERROR:" + json.dumps({"payload": payload, "drift": drift}), file=sys.stderr)
        return Verdict(alert=False, pillar="ai_infra")

    s = ctx.state
    b = ctx.baseline
    reasons = []
    score = 0.0

    cs = drift.get("centroid_shift", 0)
    age = drift.get("avg_doc_age_days", 0)

    # --- Static thresholds ---
    cs_max = b["embedding_centroid_shift_max"]  # 0.0435
    age_max = b["corpus_avg_doc_age_days_max"]  # 49.7955

    if cs > cs_max * 0.95:
        reasons.append("drift")
        score += 3.0
    elif cs > cs_max * 0.85:
        score += 1.0
    elif cs > cs_max * 0.70:
        score += 0.3

    if age > age_max * 1.0:
        reasons.append("stale")
        score += 3.0
    elif age > age_max * 0.92:
        score += 1.0
    elif age > age_max * 0.82:
        score += 0.3

    # --- Adaptive z-scores ---
    _ensure_stats(s, "eb_cs")
    _ensure_stats(s, "eb_age")

    z_cs = _get_z(s, "eb_cs", cs)
    if z_cs is not None:
        if z_cs > 4.5:
            score += 1.5
            reasons.append("adaptive_drift")
        elif z_cs > 4.0 and score >= 1.0:
            score += 0.8

    z_age = _get_z(s, "eb_age", age)
    if z_age is not None:
        if z_age > 4.5:
            score += 1.5
            reasons.append("adaptive_stale")
        elif z_age > 4.0 and score >= 1.0:
            score += 0.8

    alerted = score >= 2.5

    if not alerted:
        _update_stat(s, "eb_cs", cs)
        _update_stat(s, "eb_age", age)

    _v = Verdict(alert=alerted, pillar="ai_infra", reason=";".join(reasons) if reasons else "")
    print("LOG_EMBEDDING:" + json.dumps({"payload": payload, "drift": drift, "verdict": {"alert": _v.alert, "reason": _v.reason}}), file=sys.stderr)
    return _v
