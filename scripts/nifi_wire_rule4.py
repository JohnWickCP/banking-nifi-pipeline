#!/usr/bin/env python3
"""
nifi_wire_rule4.py
------------------
Wire CheckDuplicate_Rule4 ExecuteScript into the live NiFi flow via REST API.

Flow position (after wiring):
  CheckVelocity_Rule1 SUCCESS
    -> CheckDuplicate_Rule4
       -> SUCCESS -> QueryRecord (Rule 3)
       -> FAILURE -> PublishKafka (txn.alert)
                  -> PutDatabaseRecord (fact_txn)
                  -> PutSQL (fact_alert, duplicate SQL)

Requires:
  pip install requests
  NiFi running at https://localhost:8443
"""

import json
import sys
import time
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_BASE = "https://localhost:8443/nifi-api"
USERNAME  = "admin"
PASSWORD  = "Banking@Admin1"

# Controller service IDs (created in the original session)
DMC_CLIENT_ID  = "8ffa4eb2-019e-1000-fd0c-6065521f62ae"
DMC_SERVER_ID  = "8ff96699-019e-1000-0d05-3562596a66c8"
DBCP_ID        = None   # discovered at runtime
JSON_READER_ID = None   # discovered at runtime
JSON_WRITER_ID = None   # discovered at runtime
DB_LOOKUP_ID   = None   # discovered at runtime

SCRIPT_PATH = "/opt/nifi/nifi-current/scripts/duplicate_check.groovy"


# ─── Auth ───────────────────────────────────────────────────────────────────

def get_token():
    r = requests.post(
        f"{NIFI_BASE}/access/token",
        data={"username": USERNAME, "password": PASSWORD},
        verify=False,
        timeout=10,
    )
    if r.status_code != 201:
        raise RuntimeError(f"Auth failed: {r.status_code} {r.text}")
    return r.text.strip()


def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ─── Generic helpers ─────────────────────────────────────────────────────────

def get(path, token, **kwargs):
    r = requests.get(f"{NIFI_BASE}{path}", headers=headers(token), verify=False, timeout=15, **kwargs)
    r.raise_for_status()
    return r.json()


def post(path, token, body):
    r = requests.post(f"{NIFI_BASE}{path}", headers=headers(token), json=body, verify=False, timeout=15)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"POST {path} -> {r.status_code}: {r.text[:400]}")
    return r.json()


def put(path, token, body):
    r = requests.put(f"{NIFI_BASE}{path}", headers=headers(token), json=body, verify=False, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"PUT {path} -> {r.status_code}: {r.text[:400]}")
    return r.json()


def delete(path, token, params=None):
    r = requests.delete(f"{NIFI_BASE}{path}", headers=headers(token), params=params, verify=False, timeout=15)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"DELETE {path} -> {r.status_code}: {r.text[:400]}")
    return r


# ─── Flow inspection ─────────────────────────────────────────────────────────

def get_root_group_id(token):
    d = get("/process-groups/root", token)
    return d["id"]


def list_processors(group_id, token):
    d = get(f"/process-groups/{group_id}/processors", token)
    return d.get("processors", [])


def list_connections(group_id, token):
    d = get(f"/process-groups/{group_id}/connections", token)
    return d.get("connections", [])


def list_controller_services(group_id, token):
    d = get(f"/flow/process-groups/{group_id}/controller-services", token)
    return d.get("controllerServices", [])


def find_by_name(items, name_substr):
    matches = [i for i in items if name_substr.lower() in i["component"]["name"].lower()]
    return matches[0] if len(matches) == 1 else (matches if matches else None)


def find_connection(connections, src_id, relationship):
    for c in connections:
        comp = c["component"]
        if (comp["source"]["id"] == src_id
                and relationship in comp.get("selectedRelationships", [])):
            return c
    return None


# ─── Processor creation ──────────────────────────────────────────────────────

def create_execute_script(group_id, token, name, script_path, dmc_client_id, position):
    body = {
        "revision": {"version": 0},
        "component": {
            "type": "org.apache.nifi.processors.script.ExecuteScript",
            "name": name,
            "position": position,
            "config": {
                "schedulingStrategy": "TIMER_DRIVEN",
                "schedulingPeriod": "0 sec",
                "properties": {
                    "Script Engine":  "Groovy",
                    "Script File":    script_path,
                    "Script Body":    None,
                    "Module Directory": None,
                    "DMCServiceId":   dmc_client_id,
                },
                "autoTerminatedRelationships": [],
            },
        },
    }
    r = post(f"/process-groups/{group_id}/processors", token, body)
    return r


def create_put_sql(group_id, token, name, dbcp_id, sql, position):
    body = {
        "revision": {"version": 0},
        "component": {
            "type": "org.apache.nifi.processors.standard.PutSQL",
            "name": name,
            "position": position,
            "config": {
                "schedulingStrategy": "TIMER_DRIVEN",
                "schedulingPeriod": "0 sec",
                "properties": {
                    "JDBC Connection Pool": dbcp_id,
                    "SQL Statement":        sql,
                    "Batch Size":           "100",
                    "Support Fragmented Transactions": "true",
                    "Transaction Timeout":  None,
                    "Rollback On Failure":  "false",
                },
                "autoTerminatedRelationships": ["success", "retry", "failure"],
            },
        },
    }
    r = post(f"/process-groups/{group_id}/processors", token, body)
    return r


# ─── Connection creation ──────────────────────────────────────────────────────

def create_connection(group_id, token, src_id, src_type, dst_id, dst_type, relationships, name=""):
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "source": {"id": src_id, "groupId": group_id, "type": src_type},
            "destination": {"id": dst_id, "groupId": group_id, "type": dst_type},
            "selectedRelationships": relationships,
            "backPressureObjectThreshold": "10000",
            "backPressureDataSizeThreshold": "1 GB",
            "flowFileExpiration": "0 sec",
        },
    }
    r = post(f"/process-groups/{group_id}/connections", body=body, token=token)
    return r


# ─── Start processor ──────────────────────────────────────────────────────────

def start_processor(proc_id, token, version):
    return put(f"/processors/{proc_id}/run-status", token, {
        "revision": {"version": version},
        "state": "RUNNING",
    })


# ─── Main wiring logic ────────────────────────────────────────────────────────

def main():
    print("==> Authenticating with NiFi...")
    token = get_token()
    print("    OK")

    root_id = get_root_group_id(token)
    print(f"==> Root process group: {root_id}")

    # ── Inspect existing flow ─────────────────────────────────────────────────
    procs = list_processors(root_id, token)
    print(f"\n==> Found {len(procs)} processors:")
    for p in procs:
        print(f"    [{p['component']['state']:8s}] {p['component']['name']}")

    if not procs:
        print("\nERROR: NiFi flow is empty — the flow was lost when the container was recreated.")
        print("       Run scripts/nifi_full_setup.py to rebuild the full flow from scratch.")
        sys.exit(1)

    # ── Find processors we need ───────────────────────────────────────────────
    velocity_proc  = find_by_name(procs, "CheckVelocity")
    query_proc     = find_by_name(procs, "QueryRecord")
    publish_alert  = find_by_name(procs, "txn.alert")
    put_db_fact    = find_by_name(procs, "fact_txn")

    # PutSQL for velocity fact_alert (to find DBCP service ID from it)
    put_sql_vel    = find_by_name(procs, "PutSQL")

    for label, obj in [
        ("CheckVelocity_Rule1", velocity_proc),
        ("QueryRecord (Rule3)", query_proc),
        ("PublishKafka (txn.alert)", publish_alert),
        ("PutDatabaseRecord (fact_txn)", put_db_fact),
    ]:
        if obj is None:
            print(f"\nERROR: '{label}' not found in flow — cannot wire Rule 4.")
            print("       Existing processor names:")
            for p in procs:
                print(f"         {p['component']['name']}")
            sys.exit(1)
        print(f"    Found: {label} -> id={obj['id']}")

    # ── Check if Rule 4 already wired ─────────────────────────────────────────
    dup_existing = find_by_name(procs, "CheckDuplicate")
    if dup_existing:
        print("\nINFO: CheckDuplicate_Rule4 already exists in flow. Skipping creation.")
        print("      Verify connections manually if needed.")
        sys.exit(0)

    # ── Get DBCP service ID from PutDatabaseRecord config ─────────────────────
    dbcp_id = None
    if put_db_fact:
        props = put_db_fact["component"]["config"]["properties"]
        dbcp_id = props.get("JDBC Connection Pool") or props.get("dbcp-connection-pool")
    if not dbcp_id and put_sql_vel:
        props = put_sql_vel["component"]["config"]["properties"]
        dbcp_id = props.get("JDBC Connection Pool")

    if not dbcp_id:
        # Fall back to discovering from controller services
        services = list_controller_services(root_id, token)
        for s in services:
            if "DBCP" in s["component"]["type"] or "ConnectionPool" in s["component"]["type"]:
                dbcp_id = s["id"]
                break

    if not dbcp_id:
        print("\nERROR: Cannot find DBCP Connection Pool service ID.")
        sys.exit(1)
    print(f"\n==> DBCP Connection Pool ID: {dbcp_id}")

    # ── Get position for new processor (near velocity) ────────────────────────
    vel_pos = velocity_proc["component"]["position"]
    dup_pos = {"x": vel_pos["x"] + 400, "y": vel_pos["y"]}

    # ── Stop flow before wiring ───────────────────────────────────────────────
    print("\n==> Stopping flow processors for safe wiring...")
    try:
        put(f"/flow/process-groups/{root_id}", token, {
            "id": root_id,
            "state": "STOPPED",
        })
        time.sleep(3)
    except Exception as e:
        print(f"    Warning: could not stop process group: {e}")

    # ── Delete existing connection: CheckVelocity SUCCESS -> QueryRecord ───────
    conns = list_connections(root_id, token)
    vel_to_qr_conn = find_connection(conns, velocity_proc["id"], "success")
    if vel_to_qr_conn:
        conn_id = vel_to_qr_conn["id"]
        conn_ver = vel_to_qr_conn["revision"]["version"]
        print(f"\n==> Deleting connection: CheckVelocity SUCCESS -> QueryRecord (id={conn_id})")
        delete(f"/connections/{conn_id}", token, params={"version": conn_ver})
        print("    Deleted.")
    else:
        print("\nWARN: Connection CheckVelocity SUCCESS -> QueryRecord not found. May already be replaced.")

    # ── Create CheckDuplicate_Rule4 processor ─────────────────────────────────
    print("\n==> Creating CheckDuplicate_Rule4 (ExecuteScript)...")
    dup_proc_resp = create_execute_script(
        group_id=root_id,
        token=token,
        name="CheckDuplicate_Rule4",
        script_path=SCRIPT_PATH,
        dmc_client_id=DMC_CLIENT_ID,
        position=dup_pos,
    )
    dup_id = dup_proc_resp["id"]
    dup_ver = dup_proc_resp["revision"]["version"]
    print(f"    Created: id={dup_id}")

    # ── Read duplicate SQL for PutSQL ─────────────────────────────────────────
    try:
        with open("nifi/sql/fact_alert_duplicate_insert.sql") as f:
            dup_sql = f.read().strip()
    except FileNotFoundError:
        dup_sql = ("INSERT INTO fact_alert (alert_id, transaction_id, rule_triggered, severity, detected_at) "
                   "VALUES ('${alert_id}', '${txn_id}', '${rule_triggered}', '${severity}', NOW()) "
                   "ON CONFLICT (alert_id) DO NOTHING")

    # ── Create PutSQL for fact_alert (duplicate) ──────────────────────────────
    put_sql_pos = {"x": dup_pos["x"] + 200, "y": dup_pos["y"] + 200}
    print("\n==> Creating PutSQL_FactAlert_Duplicate...")
    put_sql_dup_resp = create_put_sql(
        group_id=root_id,
        token=token,
        name="PutSQL_FactAlert_Duplicate",
        dbcp_id=dbcp_id,
        sql=dup_sql,
        position=put_sql_pos,
    )
    put_sql_dup_id = put_sql_dup_resp["id"]
    print(f"    Created: id={put_sql_dup_id}")

    # ── Create connections ────────────────────────────────────────────────────
    print("\n==> Wiring connections...")

    # 1. CheckVelocity SUCCESS -> CheckDuplicate_Rule4
    create_connection(root_id, token,
        src_id=velocity_proc["id"], src_type="PROCESSOR",
        dst_id=dup_id,             dst_type="PROCESSOR",
        relationships=["success"],
        name="velocity_ok -> dup_check",
    )
    print("    CheckVelocity SUCCESS -> CheckDuplicate_Rule4")

    # 2. CheckDuplicate SUCCESS -> QueryRecord (Rule 3)
    create_connection(root_id, token,
        src_id=dup_id,             src_type="PROCESSOR",
        dst_id=query_proc["id"],   dst_type="PROCESSOR",
        relationships=["success"],
        name="dup_ok -> query_rule3",
    )
    print("    CheckDuplicate SUCCESS -> QueryRecord")

    # 3. CheckDuplicate FAILURE -> PublishKafka (txn.alert)
    create_connection(root_id, token,
        src_id=dup_id,              src_type="PROCESSOR",
        dst_id=publish_alert["id"], dst_type="PROCESSOR",
        relationships=["failure"],
        name="dup_alert -> publish_kafka_alert",
    )
    print("    CheckDuplicate FAILURE -> PublishKafka (txn.alert)")

    # 4. CheckDuplicate FAILURE -> PutDatabaseRecord (fact_txn)
    create_connection(root_id, token,
        src_id=dup_id,            src_type="PROCESSOR",
        dst_id=put_db_fact["id"], dst_type="PROCESSOR",
        relationships=["failure"],
        name="dup_alert -> put_fact_txn",
    )
    print("    CheckDuplicate FAILURE -> PutDatabaseRecord (fact_txn)")

    # 5. CheckDuplicate FAILURE -> PutSQL (fact_alert duplicate)
    create_connection(root_id, token,
        src_id=dup_id,        src_type="PROCESSOR",
        dst_id=put_sql_dup_id, dst_type="PROCESSOR",
        relationships=["failure"],
        name="dup_alert -> put_sql_fact_alert_dup",
    )
    print("    CheckDuplicate FAILURE -> PutSQL_FactAlert_Duplicate")

    # ── Start the new processors ──────────────────────────────────────────────
    print("\n==> Starting new processors...")
    # Re-fetch versions after creation
    dup_updated  = get(f"/processors/{dup_id}", token)
    sql_updated  = get(f"/processors/{put_sql_dup_id}", token)

    start_processor(dup_id,        token, dup_updated["revision"]["version"])
    print("    CheckDuplicate_Rule4: RUNNING")
    start_processor(put_sql_dup_id, token, sql_updated["revision"]["version"])
    print("    PutSQL_FactAlert_Duplicate: RUNNING")

    # ── Restart the whole process group ──────────────────────────────────────
    print("\n==> Restarting process group...")
    try:
        put(f"/flow/process-groups/{root_id}", token, {
            "id": root_id,
            "state": "RUNNING",
        })
        print("    Process group: RUNNING")
    except Exception as e:
        print(f"    Warning: {e}")

    print("\n==> Rule 4 wiring COMPLETE.")
    print(f"    CheckDuplicate_Rule4 processor id: {dup_id}")
    print(f"    PutSQL_FactAlert_Duplicate processor id: {put_sql_dup_id}")
    print("\n    Verify with:")
    print("    python tests/fraud/send_duplicate_test.py")
    print("    psql -U banking -d banking_dw -c \"SELECT * FROM fact_alert WHERE rule_triggered='duplicate' ORDER BY detected_at DESC LIMIT 3;\"")


if __name__ == "__main__":
    main()
