#!/usr/bin/env python3
"""
nifi_full_setup.py
------------------
Rebuild the complete NiFi banking pipeline flow from scratch via REST API.

Run ONCE on a fresh NiFi instance (empty flow).
Idempotent for controller services but NOT for processors — run on a clean canvas only.

Flow built:
  ConsumeKafka_2_6 (txn.raw)
    -> LookupRecord  (enrich from dim_customer)
      -> CheckVelocity_Rule1   (ExecuteScript, DMC)
           FAILURE -> PublishKafka(alert) + PutDatabaseRecord(fact_txn) + PutSQL(fact_alert)
           SUCCESS -> CheckDuplicate_Rule4 (ExecuteScript, DMC)
                        FAILURE -> PublishKafka(alert) + PutDatabaseRecord(fact_txn) + PutSQL(fact_alert)
                        SUCCESS -> QueryRecord  (Rule 3: off_hours_large)
                                     original    -> PutDatabaseRecord(fact_txn)
                                     fraud_rule3 -> EvaluateJsonPath + UpdateAttribute
                                                     -> PutSQL(fact_alert) + PublishKafka(alert)
                                                     -> PutDatabaseRecord(fact_txn)

Usage:
    pip install requests
    python scripts/nifi_full_setup.py
"""

import json
import sys
import time
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── Config ───────────────────────────────────────────────────────────────────

NIFI_BASE = "https://localhost:8443/nifi-api"
USERNAME  = "admin"
PASSWORD  = "Banking@Admin1"

PG_URL    = "jdbc:postgresql://postgres:5432/banking_dw"
PG_DRIVER = "org.postgresql.Driver"
PG_USER   = "banking"
PG_PASS   = "banking123"

KAFKA_BOOTSTRAP  = "kafka:29092"
KAFKA_TOPIC_RAW  = "txn.raw"
KAFKA_TOPIC_ALERT = "txn.alert"
KAFKA_GROUP_ID   = "nifi-banking-consumer"

DMC_PORT = 4557

VELOCITY_SCRIPT  = "/opt/nifi/nifi-current/scripts/velocity_check.groovy"
DUPLICATE_SCRIPT = "/opt/nifi/nifi-current/scripts/duplicate_check.groovy"

# ─── HTTP helpers ─────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.verify = False


def get_token():
    r = SESSION.post(f"{NIFI_BASE}/access/token",
                     data={"username": USERNAME, "password": PASSWORD},
                     timeout=15)
    if r.status_code != 201:
        raise RuntimeError(f"Auth failed {r.status_code}: {r.text}")
    return r.text.strip()


def h(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def get(path, token):
    r = SESSION.get(f"{NIFI_BASE}{path}", headers=h(token), timeout=20)
    r.raise_for_status()
    return r.json()


def post(path, token, body):
    r = SESSION.post(f"{NIFI_BASE}{path}", headers=h(token), json=body, timeout=20)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"POST {path} {r.status_code}: {r.text[:500]}")
    return r.json()


def put(path, token, body):
    r = SESSION.put(f"{NIFI_BASE}{path}", headers=h(token), json=body, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"PUT {path} {r.status_code}: {r.text[:500]}")
    return r.json()


def delete(path, token, params=None):
    r = SESSION.delete(f"{NIFI_BASE}{path}", headers=h(token), params=params, timeout=20)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"DELETE {path} {r.status_code}: {r.text[:200]}")


# ─── Controller service helpers ───────────────────────────────────────────────

def create_cs(group_id, token, cs_type, name, properties):
    body = {
        "revision": {"version": 0},
        "component": {
            "type": cs_type,
            "name": name,
            "properties": properties,
        },
    }
    r = post(f"/process-groups/{group_id}/controller-services", token, body)
    print(f"  Created CS: {name} -> {r['id']}")
    return r


def enable_cs(cs_id, version, token):
    r = put(f"/controller-services/{cs_id}/run-status", token, {
        "revision": {"version": version},
        "state": "ENABLED",
        "disconnectedNodeAcknowledged": False,
    })
    return r


def wait_cs_enabled(cs_id, token, timeout=60):
    for _ in range(timeout // 3):
        d = get(f"/controller-services/{cs_id}", token)
        if d["component"]["state"] == "ENABLED":
            return d
        time.sleep(3)
    raise TimeoutError(f"Controller service {cs_id} did not enable within {timeout}s")


# ─── Processor helpers ────────────────────────────────────────────────────────

def create_proc(group_id, token, proc_type, name, properties,
                auto_terminate=None, position=None, scheduling_period="0 sec"):
    if position is None:
        position = {"x": 0, "y": 0}
    config = {
        "schedulingStrategy": "TIMER_DRIVEN",
        "schedulingPeriod": scheduling_period,
        "properties": properties,
    }
    if auto_terminate:
        config["autoTerminatedRelationships"] = auto_terminate
    body = {
        "revision": {"version": 0},
        "component": {
            "type": proc_type,
            "name": name,
            "position": position,
            "config": config,
        },
    }
    r = post(f"/process-groups/{group_id}/processors", token, body)
    print(f"  Created proc: {name} -> {r['id']}")
    return r


def connect(group_id, token, src_id, dst_id, rels, name=""):
    body = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "source": {"id": src_id, "groupId": group_id, "type": "PROCESSOR"},
            "destination": {"id": dst_id, "groupId": group_id, "type": "PROCESSOR"},
            "selectedRelationships": rels,
            "backPressureObjectThreshold": "10000",
            "backPressureDataSizeThreshold": "1 GB",
            "flowFileExpiration": "0 sec",
        },
    }
    r = post(f"/process-groups/{group_id}/connections", token, body)
    return r


def start_proc(proc_id, token):
    d = get(f"/processors/{proc_id}", token)
    version = d["revision"]["version"]
    put(f"/processors/{proc_id}/run-status", token, {
        "revision": {"version": version},
        "state": "RUNNING",
    })


# ─── Main setup ───────────────────────────────────────────────────────────────

def main():
    print("==> NiFi Full Flow Setup")
    print("==> Authenticating...")
    token = get_token()
    print("    OK\n")

    root_id = get("/process-groups/root", token)["id"]
    print(f"==> Root process group: {root_id}\n")

    # Verify flow is empty
    existing = get(f"/process-groups/{root_id}/processors", token)["processors"]
    if existing:
        print(f"WARNING: {len(existing)} processors already exist. Aborting to avoid duplicates.")
        print("  Run nifi_wire_rule4.py instead, or clear the canvas first.")
        for p in existing:
            print(f"    - {p['component']['name']}")
        sys.exit(1)

    # ─── Phase 1: Controller Services ────────────────────────────────────────

    print("==> Phase 1: Creating controller services...")

    json_reader = create_cs(root_id, token,
        "org.apache.nifi.json.JsonTreeReader",
        "JsonTreeReader",
        {"schema-access-strategy": "infer-schema"})

    json_writer = create_cs(root_id, token,
        "org.apache.nifi.json.JsonRecordSetWriter",
        "JsonRecordSetWriter",
        {"schema-access-strategy": "inherit-record-schema",
         "Schema Write Strategy": "no-schema"})

    dbcp = create_cs(root_id, token,
        "org.apache.nifi.dbcp.DBCPConnectionPool",
        "DBCPConnectionPool",
        {
            "Database Connection URL":       PG_URL,
            "Database Driver Class Name":    PG_DRIVER,
            "database-driver-locations":     "/opt/nifi/nifi-current/lib/postgresql-42.5.0.jar",
            "Database User":                 PG_USER,
            "Password":                      PG_PASS,
            "Max Wait Time":                 "500 millis",
            "Max Total Connections":         "8",
            "Validation-query":              "SELECT 1",
        })

    db_lookup = create_cs(root_id, token,
        "org.apache.nifi.lookup.db.DatabaseRecordLookupService",
        "DatabaseRecordLookupService",
        {
            "dbrecord-lookup-dbcp-service": dbcp["id"],
            "dbrecord-lookup-table-name":   "dim_customer",
            "dbrecord-lookup-key-column":   "customer_id",
        })

    dmc_server = create_cs(root_id, token,
        "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "VelocityCacheServer",
        {
            "Port":                 str(DMC_PORT),
            "Maximum Cache Entries": "10000",
            "Eviction Strategy":    "First In, First Out",
            "Persistence Directory": None,
        })

    dmc_client = create_cs(root_id, token,
        "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
        "VelocityCacheClient",
        {
            "Server Hostname":      "localhost",
            "Server Port":          str(DMC_PORT),
            "Communications Timeout": "30 secs",
        })

    print()

    # ─── Phase 2: Enable controller services ─────────────────────────────────

    print("==> Phase 2: Enabling controller services...")

    # Must enable DMC server before client, DBCP before DBLookup
    for cs in [dmc_server, dmc_client, dbcp, db_lookup, json_reader, json_writer]:
        d = get(f"/controller-services/{cs['id']}", token)
        version = d["revision"]["version"]
        enable_cs(cs["id"], version, token)
        print(f"  Enabling: {d['component']['name']}...", end=" ")
        try:
            wait_cs_enabled(cs["id"], token, timeout=90)
            print("ENABLED")
        except TimeoutError as e:
            print(f"TIMEOUT: {e}")
            d2 = get(f"/controller-services/{cs['id']}", token)
            print(f"  Current state: {d2['component']['state']}")

    reader_id  = json_reader["id"]
    writer_id  = json_writer["id"]
    dbcp_id    = dbcp["id"]
    dblookup_id = db_lookup["id"]
    dmc_client_id = dmc_client["id"]

    print()

    # ─── Phase 3: Processors ─────────────────────────────────────────────────

    print("==> Phase 3: Creating processors...")

    # Layout: processors spread horizontally, alerts branch downward
    X0, Y0 = 100, 100
    DX = 400

    # 1. ConsumeKafka_2_6
    consume = create_proc(root_id, token,
        "org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6",
        "ConsumeKafka_txn_raw",
        {
            "bootstrap.servers":        KAFKA_BOOTSTRAP,
            "topic":                    KAFKA_TOPIC_RAW,
            "group.id":                 KAFKA_GROUP_ID,
            "auto.offset.reset":        "latest",
            "max.poll.records":         "500",
            "max-uncommit-offset-wait": "1 secs",
            "honor-transactions":       "false",
            "message-demarcator":       None,
            "separate-by-key":          "false",
        },
        position={"x": X0, "y": Y0},
    )

    # 2. LookupRecord (enrich from dim_customer)
    lookup = create_proc(root_id, token,
        "org.apache.nifi.processors.standard.LookupRecord",
        "LookupRecord_Customer",
        {
            "record-reader":      reader_id,
            "record-writer":      writer_id,
            "lookup-service":     dblookup_id,
            "result-record-path": "/",
            "routing-strategy":   "route-to-success",
            "key":                "/customer_id",
        },
        auto_terminate=["failure"],
        position={"x": X0 + DX, "y": Y0},
    )

    # 3. CheckVelocity_Rule1 (ExecuteScript Groovy)
    velocity = create_proc(root_id, token,
        "org.apache.nifi.processors.script.ExecuteScript",
        "CheckVelocity_Rule1",
        {
            "Script Engine": "Groovy",
            "Script File":   VELOCITY_SCRIPT,
            "Script Body":   None,
            "Module Directory": None,
            "DMCServiceId":  dmc_client_id,
        },
        position={"x": X0 + 2 * DX, "y": Y0},
    )

    # 4. CheckDuplicate_Rule4 (ExecuteScript Groovy)
    duplicate = create_proc(root_id, token,
        "org.apache.nifi.processors.script.ExecuteScript",
        "CheckDuplicate_Rule4",
        {
            "Script Engine": "Groovy",
            "Script File":   DUPLICATE_SCRIPT,
            "Script Body":   None,
            "Module Directory": None,
            "DMCServiceId":  dmc_client_id,
        },
        position={"x": X0 + 3 * DX, "y": Y0},
    )

    # 5. QueryRecord (Rule 3 — off-hours large transaction)
    # SUBSTRING extracts hour from ISO 8601 "2026-06-07T02:06:29" at position 12-13
    # because Calcite CAST("timestamp" AS TIMESTAMP) fails on the T-separator format
    rule3_sql = (
        "SELECT * FROM FLOWFILE WHERE "
        "CAST(\"amount\" AS BIGINT) > 50000000 "
        "AND ("
        "CAST(SUBSTRING(\"timestamp\", 12, 2) AS INTEGER) >= 22 "
        "OR CAST(SUBSTRING(\"timestamp\", 12, 2) AS INTEGER) < 6"
        ")"
    )
    query = create_proc(root_id, token,
        "org.apache.nifi.processors.standard.QueryRecord",
        "QueryRecord_Rule3_OffHours",
        {
            "record-reader":                 reader_id,
            "record-writer":                 writer_id,
            "include-zero-record-flowfiles": "false",
            "fraud_rule3":                   rule3_sql,
        },
        auto_terminate=["failure"],
        position={"x": X0 + 4 * DX, "y": Y0},
    )

    # 6. PutDatabaseRecord — fact_txn (shared by clean + alert paths)
    put_fact_txn = create_proc(root_id, token,
        "org.apache.nifi.processors.standard.PutDatabaseRecord",
        "PutDatabaseRecord_fact_txn",
        {
            "put-db-record-dcbp-service":    dbcp_id,
            "put-db-record-record-reader":   reader_id,
            "db-type":                       "PostgreSQL",
            "put-db-record-statement-type":  "INSERT",
            "put-db-record-table-name":      "fact_txn",
            "Unmatched Field Behavior":      "Ignore Unmatched Fields",
            "Unmatched Column Behavior":     "Warning Unmatched Columns",
            "Rollback On Failure":           "false",
        },
        auto_terminate=["success", "failure", "retry"],
        position={"x": X0 + 5 * DX, "y": Y0},
    )

    # 7. PublishKafka_2_6 — txn.alert (shared alert publisher)
    publish_alert = create_proc(root_id, token,
        "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        "PublishKafka_txn_alert",
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "topic":             KAFKA_TOPIC_ALERT,
            "acks":              "all",
            "use-transactions":  "false",
        },
        auto_terminate=["success", "failure"],
        position={"x": X0 + 3 * DX, "y": Y0 + 400},
    )

    # PutSQL shared SQL — inline to avoid multiline comment parsing issues
    alert_sql = (
        "INSERT INTO fact_alert (alert_id, transaction_id, rule_triggered, severity, detected_at) "
        "VALUES ('${alert_id}', '${txn_id}', '${rule_triggered}', '${severity}', NOW()) "
        "ON CONFLICT (alert_id) DO NOTHING"
    )

    # 8. PutSQL — fact_alert for Rule 1 (velocity)
    put_alert_vel = create_proc(root_id, token,
        "org.apache.nifi.processors.standard.PutSQL",
        "PutSQL_FactAlert_Velocity",
        {
            "JDBC Connection Pool":  dbcp_id,
            "putsql-sql-statement":  alert_sql,
            "Batch Size":            "1",
        },
        auto_terminate=["success", "failure", "retry"],
        position={"x": X0 + 2 * DX, "y": Y0 + 400},
    )

    # 9. PutSQL — fact_alert for Rule 4 (duplicate)
    put_alert_dup = create_proc(root_id, token,
        "org.apache.nifi.processors.standard.PutSQL",
        "PutSQL_FactAlert_Duplicate",
        {
            "JDBC Connection Pool":  dbcp_id,
            "putsql-sql-statement":  alert_sql,
            "Batch Size":            "1",
        },
        auto_terminate=["success", "failure", "retry"],
        position={"x": X0 + 4 * DX, "y": Y0 + 400},
    )

    # 10. EvaluateJsonPath — extract txn_id for Rule 3
    eval_json = create_proc(root_id, token,
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        "EvaluateJsonPath_Rule3",
        {
            "Destination":             "flowfile-attribute",
            "Return Type":             "auto-detect",
            "Path Not Found Behavior": "ignore",
            "Null Value Representation": "empty string",
            "txn_id":                  "$[0].transaction_id",
        },
        auto_terminate=["failure"],
        position={"x": X0 + 5 * DX, "y": Y0 + 300},
    )

    # 11. UpdateAttribute — tag Rule 3 alert attributes
    update_attr = create_proc(root_id, token,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        "UpdateAttribute_Rule3_Alert",
        {
            "rule_triggered": "off_hours_large",
            "severity":       "HIGH",
            "fraud_flag":     "true",
            "alert_id":       "ALT-R3-${UUID():replace('-',''):substring(0,8):toUpper()}",
        },
        position={"x": X0 + 5 * DX, "y": Y0 + 450},
    )

    # 12. PutSQL — fact_alert for Rule 3 (off_hours)
    put_alert_r3 = create_proc(root_id, token,
        "org.apache.nifi.processors.standard.PutSQL",
        "PutSQL_FactAlert_OffHours",
        {
            "JDBC Connection Pool":  dbcp_id,
            "putsql-sql-statement":  alert_sql,
            "Batch Size":            "1",
        },
        auto_terminate=["success", "failure", "retry"],
        position={"x": X0 + 5 * DX, "y": Y0 + 600},
    )

    print()

    # ─── Phase 4: Connections ─────────────────────────────────────────────────

    print("==> Phase 4: Creating connections...")

    vid = velocity["id"]
    did = duplicate["id"]

    # ConsumeKafka -> LookupRecord
    connect(root_id, token, consume["id"], lookup["id"], ["success"], "raw->lookup")
    print("  ConsumeKafka -> LookupRecord")

    # LookupRecord -> CheckVelocity
    connect(root_id, token, lookup["id"], velocity["id"], ["success", "unmatched"], "lookup->velocity")
    print("  LookupRecord -> CheckVelocity_Rule1")

    # CheckVelocity FAILURE -> alerts
    connect(root_id, token, vid, publish_alert["id"], ["failure"], "vel_fail->kafka_alert")
    connect(root_id, token, vid, put_fact_txn["id"],  ["failure"], "vel_fail->fact_txn")
    connect(root_id, token, vid, put_alert_vel["id"], ["failure"], "vel_fail->fact_alert_vel")
    print("  CheckVelocity FAILURE -> PublishKafka + PutDatabaseRecord + PutSQL")

    # CheckVelocity SUCCESS -> CheckDuplicate
    connect(root_id, token, vid, did, ["success"], "vel_ok->dup")
    print("  CheckVelocity SUCCESS -> CheckDuplicate_Rule4")

    # CheckDuplicate FAILURE -> alerts
    connect(root_id, token, did, publish_alert["id"], ["failure"], "dup_fail->kafka_alert")
    connect(root_id, token, did, put_fact_txn["id"],  ["failure"], "dup_fail->fact_txn")
    connect(root_id, token, did, put_alert_dup["id"], ["failure"], "dup_fail->fact_alert_dup")
    print("  CheckDuplicate FAILURE -> PublishKafka + PutDatabaseRecord + PutSQL")

    # CheckDuplicate SUCCESS -> QueryRecord
    connect(root_id, token, did, query["id"], ["success"], "dup_ok->query_rule3")
    print("  CheckDuplicate SUCCESS -> QueryRecord (Rule 3)")

    # QueryRecord original -> PutDatabaseRecord (clean records)
    connect(root_id, token, query["id"], put_fact_txn["id"], ["original"], "qr_clean->fact_txn")
    print("  QueryRecord original -> PutDatabaseRecord")

    # QueryRecord fraud_rule3 -> EvaluateJsonPath -> UpdateAttribute -> outputs
    connect(root_id, token, query["id"],      eval_json["id"],   ["fraud_rule3"], "qr_fraud->eval_json")
    connect(root_id, token, eval_json["id"],  update_attr["id"], ["matched", "unmatched"], "eval->update_attr")
    connect(root_id, token, update_attr["id"], publish_alert["id"], ["success"], "r3_alert->kafka")
    connect(root_id, token, update_attr["id"], put_fact_txn["id"],  ["success"], "r3_alert->fact_txn")
    connect(root_id, token, update_attr["id"], put_alert_r3["id"],  ["success"], "r3_alert->fact_alert_r3")
    print("  QueryRecord fraud_rule3 -> EvalJsonPath -> UpdateAttr -> Kafka + fact_txn + fact_alert")

    print()

    # ─── Phase 5: Start processors ────────────────────────────────────────────

    print("==> Phase 5: Starting processors...")
    for proc in [consume, lookup, velocity, duplicate, query, eval_json, update_attr]:
        start_proc(proc["id"], token)
        print(f"  Started: {proc['component']['name']}")

    print()
    print("==> Flow setup COMPLETE.")
    print(f"    NiFi UI: https://localhost:8443/nifi")
    print()
    print("  Processor IDs for reference:")
    for proc in [consume, lookup, velocity, duplicate, query,
                 put_fact_txn, publish_alert, put_alert_vel, put_alert_dup, put_alert_r3]:
        print(f"    {proc['component']['name']:45s} {proc['id']}")
    print()
    print("  Controller Service IDs:")
    for cs in [json_reader, json_writer, dbcp, db_lookup, dmc_server, dmc_client]:
        print(f"    {cs['component']['name']:45s} {cs['id']}")
    print()
    print("  Next steps:")
    print("    1. Verify flow in NiFi UI: https://localhost:8443/nifi")
    print("    2. Run: python tests/fraud/send_duplicate_test.py")
    print("    3. Verify: psql -U banking -d banking_dw -c \"SELECT rule_triggered, COUNT(*) FROM fact_alert GROUP BY 1;\"")


if __name__ == "__main__":
    main()
