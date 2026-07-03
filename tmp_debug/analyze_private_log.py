import json
import statistics

def mean(vals):
    return sum(vals) / len(vals) if vals else 0

def stdev(vals):
    return statistics.stdev(vals) if len(vals) > 1 else 0

def main():
    events = []
    with open("tmp_debug/private_stderr.log", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("LOG_DATA_BATCH:"):
                data = json.loads(line[len("LOG_DATA_BATCH:"):])
                data["event_type"] = "data_batch"
                events.append(data)
            elif line.startswith("LOG_CONTRACT:"):
                data = json.loads(line[len("LOG_CONTRACT:"):])
                data["event_type"] = "contract"
                events.append(data)
            elif line.startswith("LOG_LINEAGE:"):
                data = json.loads(line[len("LOG_LINEAGE:"):])
                data["event_type"] = "lineage"
                events.append(data)
            elif line.startswith("LOG_FEATURE:"):
                data = json.loads(line[len("LOG_FEATURE:"):])
                data["event_type"] = "feature"
                events.append(data)
            elif line.startswith("LOG_EMBEDDING:"):
                data = json.loads(line[len("LOG_EMBEDDING:"):])
                data["event_type"] = "embedding"
                events.append(data)
            elif "ERROR" in line:
                print("Error log line:", line)

    print(f"Total events parsed: {len(events)}")
    
    # Analyze by event type
    types = sorted(list(set(e["event_type"] for e in events)))
    for etype in types:
        etype_events = [e for e in events if e["event_type"] == etype]
        print(f"\n--- {etype.upper()} ({len(etype_events)} events) ---")
        
        if etype == "data_batch":
            rcs = [e["profile"]["row_count"] for e in etype_events]
            nrs = [e["profile"]["null_rate"]["customer_id"] for e in etype_events]
            mas = [e["profile"]["mean_amount"] for e in etype_events]
            sas = [e["profile"]["std_amount"] for e in etype_events]
            sms = [e["profile"]["staleness_min"] for e in etype_events]
            
            # Print statistics for each metric
            for name, vals in [("row_count", rcs), ("null_rate", nrs), ("mean_amount", mas), ("std_amount", sas), ("staleness_min", sms)]:
                print(f"  {name:15s}: min={min(vals):.4f}, max={max(vals):.4f}, mean={mean(vals):.4f}, std={stdev(vals):.4f}")
            
            # Print alerted ones
            print("  Alerted:")
            for e in etype_events:
                if e["verdict"]["alert"]:
                    print(f"    Batch {e['payload']['batch_id']}: Profile={e['profile']} | Alert={e['verdict']['alert']} | Reason={e['verdict']['reason']}")
                    
        elif etype == "contract":
            fds = [e["diff"].get("freshness_delay_min", 0) for e in etype_events]
            vios = [len(e["diff"].get("violations", [])) for e in etype_events]
            for name, vals in [("freshness_delay", fds), ("violations_count", vios)]:
                print(f"  {name:15s}: min={min(vals):.4f}, max={max(vals):.4f}, mean={mean(vals):.4f}, std={stdev(vals):.4f}")
            print("  Alerted:")
            for e in etype_events:
                if e["verdict"]["alert"]:
                    print(f"    Contract {e['payload']['contract_id']}: Diff={e['diff']} | Reason={e['verdict']['reason']}")
                    
        elif etype == "lineage":
            durs = [e["slc"]["duration_ms"] for e in etype_events]
            ups = [len(e["slc"].get("actual_upstream", [])) for e in etype_events]
            dcs = [e["slc"].get("actual_downstream_count", 0) for e in etype_events]
            for name, vals in [("duration_ms", durs), ("upstream_count", ups), ("downstream_count", dcs)]:
                print(f"  {name:15s}: min={min(vals):.4f}, max={max(vals):.4f}, mean={mean(vals):.4f}, std={stdev(vals):.4f}")
            print("  Alerted:")
            for e in etype_events:
                if e["verdict"]["alert"]:
                    print(f"    Run {e['payload']['run_id']}: Slice={e['slc']} | Reason={e['verdict']['reason']}")
                    
        elif etype == "feature":
            sigmas = [e["drift"].get("mean_shift_sigma", 0) for e in etype_events]
            smeans = [e["drift"].get("serve_mean", 0) for e in etype_events]
            tmeans = [e["drift"].get("train_mean", 0) for e in etype_events]
            tstds = [e["drift"].get("train_std", 1) for e in etype_events]
            for name, vals in [("mean_shift_sigma", sigmas), ("serve_mean", smeans), ("train_mean", tmeans), ("train_std", tstds)]:
                print(f"  {name:15s}: min={min(vals):.4f}, max={max(vals):.4f}, mean={mean(vals):.4f}, std={stdev(vals):.4f}")
            print("  Alerted:")
            for e in etype_events:
                if e["verdict"]["alert"]:
                    print(f"    Feature {e['payload']['feature_view']}: Drift={e['drift']} | Reason={e['verdict']['reason']}")
                    
        elif etype == "embedding":
            css = [e["drift"].get("centroid_shift", 0) for e in etype_events]
            ages = [e["drift"].get("avg_doc_age_days", 0) for e in etype_events]
            for name, vals in [("centroid_shift", css), ("avg_doc_age_days", ages)]:
                print(f"  {name:15s}: min={min(vals):.4f}, max={max(vals):.4f}, mean={mean(vals):.4f}, std={stdev(vals):.4f}")
            print("  Alerted:")
            for e in etype_events:
                if e["verdict"]["alert"]:
                    print(f"    Embedding {e['payload']['corpus']}: Drift={e['drift']} | Reason={e['verdict']['reason']}")

if __name__ == "__main__":
    main()
