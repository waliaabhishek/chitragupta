"""Qualify public PyIceberg operations without changing the worker."""

from __future__ import annotations

import argparse
import ast
import json
import os
import subprocess
import sys
import time
from contextlib import ExitStack, chdir
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from datetime import time as day_time
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from tempfile import TemporaryDirectory, TemporaryFile
from unittest.mock import patch

import pyarrow as pa
from fixture import (
    DAY,
    ECOSYSTEM,
    ICEBERG_EPOCH,
    NEIGHBOR,
    WAREHOUSE,
    active_data_file_identity,
    admin_catalog,
    append_fixture_rows,
    chargeback,
    duckdb_connection,
    file_partition,
    large_day_row,
    large_day_total,
    main_retention_settings,
    projected_row,
    provision_storage,
    provision_tables,
    qualified,
    reset_table,
    seed,
    seed_large_day,
    source_backend,
    trino_query,
    verify_object_access,
    wait_for_trino,
)
from gateway import (
    ADMIN_KEY,
    ADMIN_SECRET,
    CATALOG_TOKEN,
    REGION,
    S3_URL,
    catalog_gateway,
    storage_gateway,
    table_secret,
    table_token,
)
from pyiceberg.expressions import And, EqualTo
from pyiceberg.manifest import read_manifest_list
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import MapType, StringType

from core.config.loader import load_config
from core.storage.registry import create_storage_backend
from plugins.storage_modules import get_storage_module_for_ecosystem

HERE = Path(__file__).parent
MIB = 1024 * 1024
THREE_TABLES = ("ccloud_chargebacks", "kafka_chargebacks", "postgres_chargebacks")
UNIT = Decimal("0.00001")


SDK_IMPORTS = {
    "pyiceberg.catalog.rest": {"RestCatalog"},
    "pyiceberg.expressions": {"And", "EqualTo"},
    "pyiceberg.io.pyarrow": {"PyArrowFileIO"},
    "pyiceberg.manifest": {"read_manifest_list"},
    "pyiceberg.partitioning": {"PartitionField", "PartitionSpec"},
    "pyiceberg.schema": {"Schema"},
    "pyiceberg.transforms": {"IdentityTransform"},
    "pyiceberg.types": {"DateType", "DecimalType", "MapType", "NestedField", "StringType", "TimestamptzType"},
    "pyiceberg.table": {"Table"},
}
BLOCKED_MODULES = {"core.emitters.iceberg_transport", "core.emitters.iceberg_file_io", "check"}


def redact(value, *extra):
    words = (
        ADMIN_KEY,
        ADMIN_SECRET,
        CATALOG_TOKEN,
        "fixture-cloud-key",
        "fixture-cloud-secret",
        *(secret for table in ("alpha", *THREE_TABLES) for secret in (table_token(table), table_secret(table))),
        *extra,
    )
    text = str(value)
    for word in sorted(set(words) - {""}, key=len, reverse=True):
        text = text.replace(word, "<redacted>")
    return text


class Verdict:
    """Keep collecting independent evidence after a qualification gate fails."""

    def __init__(self):
        self.failed: list[str] = []
        self.unproved: list[str] = []

    def probe(self, label, operation, *args):
        count = len(self.failed) + len(self.unproved)
        try:
            operation(*args)
        except Exception as exc:
            self.gate(label, False, f"{type(exc).__name__}: {redact(exc)[:700]}")
        else:
            marker = "PASS" if count == len(self.failed) + len(self.unproved) else "OBSERVED"
            print(f"{marker} {label}")

    def gate(self, label, passed, evidence):
        marker = "PASS" if passed else "UNMET"
        print(f"{marker} {label}: {redact(evidence)}")
        if not passed:
            self.failed.append(label)

    def untested(self, label, evidence):
        print(f"UNPROVED {label}: {redact(evidence)}")
        self.unproved.append(label)


def audit_api(source, filename):
    def imports(module, names):
        assert not any(module == blocked or module.startswith(blocked + ".") for blocked in BLOCKED_MODULES)
        assert not module.startswith(("importlib", "inspect"))
        if module.startswith("pyiceberg"):
            assert names and set(names) <= SDK_IMPORTS.get(module, set()), (filename, module, names)

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            imports(node.module or "", [alias.name for alias in node.names])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imports(alias.name, [])
        elif isinstance(node, ast.Attribute):
            builtin_dunder = (
                isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Name)
                and (node.value.func.id, node.attr) in {("super", "__init__"), ("type", "__name__")}
            )
            if node.attr != "__csv_fields__" and not builtin_dunder:
                assert not node.attr.startswith("_") and node.attr != "update_snapshot", (filename, node.lineno)
        elif isinstance(node, ast.Name):
            assert node.id not in {"getattr", "setattr", "delattr", "vars", "eval", "exec", "__import__"}, (
                filename,
                node.lineno,
            )


def verify_api_boundary():
    for path in (HERE / name for name in ("public_writer.py", "public_api.py", "fixture.py")):
        audit_api(path.read_text(), str(path))
    for snippet in (
        "from pyiceberg.io.pyarrow import _dataframe_to_data_files",
        "import pyiceberg.table.update.snapshot as sdk",
        "table._do_commit(None)",
        "table.transaction().update_snapshot().overwrite()",
        "getattr(table, '_do_commit')",
        "from core.emitters.iceberg_transport import IcebergTable",
    ):
        try:
            audit_api(snippet, "private API boundary fixture")
        except AssertionError:
            continue
        raise AssertionError(f"API admission let a private SDK operation through: {snippet}")
    assert BLOCKED_MODULES.isdisjoint(sys.modules), BLOCKED_MODULES & sys.modules.keys()
    marker = "proof-secret-marker"
    assert redact(ValueError(f"Secret was {marker}"), marker) == "Secret was <redacted>"


def load_table(name):
    with admin_catalog() as catalog:
        return catalog.load_table(qualified(name))


def live_snapshot(table):
    snapshot = table.current_snapshot()
    return snapshot.snapshot_id if snapshot is not None else None


def settings(gateway, root, table="alpha", tenant="alpha", ecosystem=ECOSYSTEM, **overrides):
    options = {
        "catalog_uri": gateway.uri,
        "token": CATALOG_TOKEN,
        "warehouse": WAREHOUSE,
        "ca_file": str(gateway.ca),
        "s3_endpoint": S3_URL,
        "region": REGION,
        "table": qualified(table),
        "connection": f"sqlite:///{root / f'{tenant}.sqlite'}",
        "ecosystem": ecosystem,
        "tenant_id": tenant,
        "day": DAY.isoformat(),
    }
    options.update(overrides)
    return options


def run_writer(options, env, scratch, deadline, pause, interrupt, staged):
    """Own the process, including time spent holding batches for readers."""
    with TemporaryFile(mode="w+", dir=scratch) as payload:
        json.dump({**options, "deadline": deadline, "hold_batches": bool(pause or interrupt)}, payload)
        payload.seek(0)
        with subprocess.Popen(
            [sys.executable, str(HERE / "public_writer.py")],
            stdin=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        ) as writer:
            try:
                for index in range(1, 3) if pause or interrupt else ():
                    marker = scratch / f"stage-{index}.json"
                    while not marker.is_file() and writer.poll() is None:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("waiting for a staged append")
                        time.sleep(0.01)
                    if not marker.is_file():
                        break
                    evidence = json.loads(marker.read_text())
                    staged.append(evidence)
                    if pause:
                        pause(evidence, deadline)
                    if interrupt:
                        writer.kill()
                        break
                    (scratch / f"resume-{index}").touch()
                stdout, stderr = writer.communicate(timeout=max(0, deadline - time.monotonic()))
                if writer.returncode == -9:
                    return {"status": "killed"}
                assert writer.returncode == 0, redact(stderr, options["token"])[-1000:]
                return json.loads(stdout)
            finally:
                if writer.poll() is None:
                    writer.kill()
                    writer.communicate()
                for evidence_file in scratch.glob("stage-*.*"):
                    evidence_file.unlink()
                for resume in scratch.glob("resume-*"):
                    resume.unlink()


def run_attempt(gateway, root, options, *, timeout=60, ambient=False, pause=None, before_commit=None, interrupt=False):
    """Use one deadline from process startup to reaping and scratch cleanup."""
    start = time.monotonic()
    deadline = start + timeout
    initial = len(gateway.requests)
    initial_commit = len(gateway.commit_payloads)
    initial_receipts = len(gateway.commit_responses)
    initial_wire = len(gateway.commit_seconds)
    env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
    if ambient:
        env.update(AWS_ACCESS_KEY_ID=ADMIN_KEY, AWS_SECRET_ACCESS_KEY=ADMIN_SECRET)
    env["AWS_EC2_METADATA_DISABLED"] = "true"
    staged = []
    commit_observations = []

    def commit_observer(_table, _payload):
        try:
            before_commit(deadline - 1)
        except Exception as exc:
            commit_observations.append(redact(exc, options["token"]))
        else:
            commit_observations.append(None)

    def observe_stage(evidence, limit):
        if pause:
            pause(evidence, limit)
        assert not any(method == "POST" for method, _, _ in gateway.requests[initial:])

    if before_commit:
        assert gateway.before_next_commit is None
        gateway.before_next_commit = commit_observer
    with TemporaryDirectory(dir=root, prefix="writer-scratch-") as scratch:
        env["SQLITE_TMPDIR"] = scratch
        try:
            result = run_writer(
                options, env, Path(scratch), deadline - 1, observe_stage if pause else None, interrupt, staged
            )
        except subprocess.TimeoutExpired, TimeoutError:
            result = {"status": "timed_out"}
        finally:
            if before_commit:
                gateway.before_next_commit = None
        assert not list(Path(scratch).iterdir()), f"writer left scratch files in {scratch}"
    elapsed = time.monotonic() - start
    assert elapsed <= timeout, (elapsed, timeout, result)
    table = options["table"].rsplit(".", 1)[1]
    requests = gateway.requests[initial:]

    def role(name, auth):
        if auth == f"Bearer {options['token']}":
            return "catalog"
        if name and auth == f"Bearer {table_token(name)}":
            return "table"
        return "unexpected"

    result.update(
        event_seconds=elapsed,
        posts=[(name, role(name, auth)) for method, name, auth in requests if method == "POST"],
        table_gets=[role(name, auth) for method, name, auth in requests if method == "GET" and name == table],
        payloads=gateway.commit_payloads[initial_commit:],
        receipts=gateway.commit_responses[initial_receipts:],
        commit_seconds=sum(gateway.commit_seconds[initial_wire:]),
        staged=staged,
        commit_observations=commit_observations,
    )
    return result


def require_success(result, table):
    assert result["status"] == "ok", result
    assert result["posts"] == [(table, "table")], result
    assert result["receipts"] == [(table, 200)], result
    assert result["table_gets"] and set(result["table_gets"]) == {"catalog"}, result
    assert result["snapshot"] == result["intended_snapshot"], result
    assert result["table_uuid"] == result["intended_uuid"], result


def show_result(table, rows, result, snapshots):
    print(
        f"DATA {table}: rows={rows:,} batches={result['batches']} files={result['files']} snapshots={snapshots} "
        f"capture={result['capture_seconds']:.2f}s append_staging={result['write_seconds']:.2f}s "
        f"transaction={result['transaction_seconds']:.2f}s catalog={result['commit_seconds']:.3f}s "
        f"proof_elapsed={result['event_seconds']:.2f}s held={result['pause_seconds']:.2f}s "
        f"max_arrow_batch={result['max_arrow_bytes'] / MIB:.1f}MiB "
        f"peak_rss={result['peak_rss_mib']:.1f}MiB scratch={result['scratch_bytes'] / MIB:.1f}MiB"
    )


def scope_rows(gateway, table, ecosystem=ECOSYSTEM, tenant="alpha", day=DAY):
    with duckdb_connection(gateway, table) as db:
        return db.execute(
            f"SELECT amount FROM warehouse.{qualified(table)} "
            "WHERE ecosystem = ? AND tenant_id = ? AND chargeback_date = ? ORDER BY amount",
            [ecosystem, tenant, day],
        ).fetchall()


def verify_auth(gateway, root, backend):
    seed(backend, "alpha", "tls-first", chargeback("alpha", DAY, "1.00001"))
    opts = settings(gateway, root, revision="tls-first")
    first = run_attempt(gateway, root, {**opts, "legacy_token": True})
    assert first["status"] == "error", first
    assert first["table_gets"] == ["catalog"]
    assert first["posts"] and set(first["posts"]) == {("alpha", "catalog")}, first
    assert live_snapshot(load_table("alpha")) is None
    second = run_attempt(gateway, root, opts)
    require_success(second, "alpha")
    assert scope_rows(gateway, "alpha") == [(Decimal("1.00001"),)]
    fresh = run_attempt(gateway, root, {**opts, "load_only": True})
    assert fresh["status"] == "ok" and fresh["posts"] == [], fresh
    assert fresh["table_gets"] == ["catalog"], fresh
    print(
        f"PASS TLS smoke: legacy auth replaced the table bearer on {len(first['posts'])} POSTs; "
        "documented noop auth preserved it on one POST"
    )


def sql_string(value: str | None) -> str:
    return "NULL" if value is None else "'" + value.replace("'", "''") + "'"


def scope_summary(gateway, table, tenant, ecosystem, *, snapshot=None):
    condition = (
        f"WHERE ecosystem = {sql_string(ecosystem)} AND tenant_id = {sql_string(tenant)} "
        f"AND chargeback_date = DATE '{DAY}'"
    )
    fields = "count(*), sum(amount), min(calculation_id), max(calculation_id)"
    if snapshot is not None:
        observed = trino_query(
            f"SELECT {fields} FROM chargeback.{qualified(table)} FOR VERSION AS OF {snapshot} {condition}"
        )[0]
        return observed[0], Decimal(observed[1] or 0), observed[2], observed[3]
    trino = trino_query(f"SELECT {fields} FROM chargeback.{qualified(table)} {condition}")[0]
    with duckdb_connection(gateway, table) as db:
        duckdb = db.execute(f"SELECT {fields} FROM warehouse.{qualified(table)} {condition}").fetchone()
    expected = (trino[0], Decimal(trino[1] or 0), trino[2], trino[3])
    actual = (duckdb[0], duckdb[1] or Decimal(0), duckdb[2], duckdb[3])
    assert actual == expected, (actual, expected)
    return actual


def observe_current(gateway, table, tenant, ecosystem, deadline):
    """Run both readers in a reaped process under the writer's absolute deadline."""
    if time.monotonic() >= deadline:
        raise TimeoutError("deadline before verifying the staged transaction")
    program = """
import json, sys
from types import SimpleNamespace
from public_api import load_table, live_snapshot, scope_summary
args = json.load(sys.stdin)
table = load_table(args["table"])
summary = scope_summary(SimpleNamespace(**args["gateway"]), args["table"], args["tenant"], args["ecosystem"])
print(json.dumps([str(table.metadata.table_uuid), live_snapshot(table), *summary], default=str))
"""
    result = subprocess.run(
        [sys.executable, "-c", program],
        cwd=HERE,
        input=json.dumps(
            {
                "gateway": {"uri": gateway.uri, "ca": str(gateway.ca)},
                "table": table,
                "tenant": tenant,
                "ecosystem": ecosystem,
            }
        ),
        capture_output=True,
        text=True,
        timeout=max(0, deadline - time.monotonic()),
        check=False,
    )
    assert result.returncode == 0, redact(result.stderr)[-1000:]
    uuid, head, rows, amount, min_revision, max_revision = json.loads(result.stdout)
    return uuid, head, rows, Decimal(amount), min_revision, max_revision


def verify_large_rows(gateway, table, backend, tenant, ecosystem, revision, row_count, *, changed, tag_value):
    expected_amount = (large_day_total(row_count) + (Decimal("0.01") if changed else 0)).quantize(
        UNIT, rounding=ROUND_HALF_UP
    )
    if row_count == 100_001:
        assert expected_amount == Decimal("50001.23457")
    noon = datetime.combine(DAY, day_time(12), tzinfo=UTC)
    with backend.create_read_only_unit_of_work() as uow:
        facts = uow.chargebacks.find_by_range(ecosystem, tenant, noon, noon + timedelta(seconds=1))
        state = uow.pipeline_state.get(ecosystem, tenant, DAY)
        assert state is not None and state.calculation_id == revision and state.calculation_completed_at is not None
    assert len(facts) == 128 and all(fact.dimension_id is not None for fact in facts)
    dimension_order = [int(fact.identity_id[-3:]) for fact in sorted(facts, key=lambda fact: fact.dimension_id)]
    assert set(dimension_order) == set(range(128))
    position = ",".join(f"({suffix},{rank})" for rank, suffix in enumerate(dimension_order))
    site = sql_string(ecosystem)
    who = sql_string(tenant)
    calculation = sql_string(revision)
    tag_override = sql_string(tag_value)
    completed_at = state.calculation_completed_at
    expected_floor = Decimal("1.24456" if changed else "1.23456")

    def statement(*, trino):
        catalog = "chargeback" if trino else "warehouse"
        if trino:
            seconds = (
                f"SELECT sec*128+dim AS i, sec FROM UNNEST(sequence(0,{(row_count - 1) // 128})) seconds(sec) "
                f"CROSS JOIN UNNEST(sequence(0,127)) dimensions(dim) WHERE sec*128+dim < {row_count}"
            )
            timestamp = f"date_add('second',sec,TIMESTAMP '{DAY} 12:00:00 UTC')"
            calculation_time = f"from_iso8601_timestamp_nanos('{completed_at.isoformat()}')"
        else:
            seconds = f"SELECT i, i // 128 AS sec FROM range({row_count}) AS source(i)"
            timestamp = f"CAST('{noon.isoformat()}' AS TIMESTAMPTZ) + to_seconds(sec)"
            calculation_time = f"CAST('{completed_at.isoformat()}' AS TIMESTAMPTZ)"
        wide_tags = " OR ".join(
            f"actual.tags['wide_{index:02d}'] IS DISTINCT FROM "
            f"actual.tenant_id || '-' || expected.resource_id || '-tag-{index:02d}-xxxxxxxx'"
            for index in range(1, 8)
        )
        # Every input has a half-unit remainder. Its exact five-place value
        # equals the floor plus one unit for the first ceil(N/2) ranked rows.
        return f"""
            WITH dimensions(suffix, pos) AS (VALUES {position}),
            ranked AS (
                SELECT i, i % 128 AS dim, {timestamp} AS timestamp,
                       'resource-' || lpad(CAST(i % 128 AS VARCHAR), 3, '0') AS resource_id,
                       'identity-' || lpad(CAST(i % 128 AS VARCHAR), 3, '0') AS identity_id,
                       row_number() OVER (ORDER BY sec, pos) AS rank
                FROM ({seconds}) AS generated JOIN dimensions ON i % 128 = dimensions.suffix
            ),
            expected AS (
                SELECT *, CAST(
                    CASE WHEN i = 0 THEN {expected_floor} WHEN i % 2 = 0 THEN 1.23456 ELSE -0.23457 END
                    + CASE WHEN rank <= {(row_count + 1) // 2} THEN 0.00001 ELSE 0 END AS DECIMAL(38,5)
                ) AS amount,
                CASE WHEN {site} = 'confluent_cloud'
                    THEN 'env-' || lpad(CAST(dim % 8 AS VARCHAR), 2, '0') END AS env_id,
                CASE WHEN {site} = 'self_managed_kafka'
                    THEN 'team-' || lpad(CAST(dim % 8 AS VARCHAR), 2, '0') END AS team
                FROM ranked
            ),
            actual AS (
                SELECT * FROM {catalog}.{qualified(table)}
                WHERE ecosystem = {site} AND tenant_id = {who} AND chargeback_date = DATE '{DAY}'
            )
            SELECT count(*), CAST(sum(actual.amount) AS DECIMAL(38,5)), lower(any_value(typeof(actual.amount))),
                   count(*) FILTER (
                       WHERE expected.i IS NULL OR actual.timestamp IS NULL
                           OR actual.amount IS DISTINCT FROM expected.amount
                           OR actual.env_id IS DISTINCT FROM expected.env_id
                           OR actual.team IS DISTINCT FROM expected.team
                           OR actual.product_category IS DISTINCT FROM 'compute'
                           OR actual.product_type IS DISTINCT FROM 'storage'
                           OR actual.cost_type IS DISTINCT FROM 'usage'
                           OR actual.allocation_method IS NOT NULL OR actual.allocation_detail IS NOT NULL
                           OR actual.calculation_id IS DISTINCT FROM {calculation}
                           OR actual.calculation_completed_at IS DISTINCT FROM {calculation_time}
                           OR cardinality(actual.tags) IS DISTINCT FROM 8
                           OR actual.tags['owner'] IS DISTINCT FROM CASE
                               WHEN expected.dim = 0 AND {tag_override} IS NOT NULL THEN {tag_override}
                               ELSE actual.tenant_id || '-' || expected.resource_id || '-tag-00-xxxxxxxx' END
                           OR {wide_tags}
                   ) AS mismatched_rows
            FROM actual FULL OUTER JOIN expected
              ON actual.timestamp = expected.timestamp
             AND actual.resource_id = expected.resource_id
             AND actual.identity_id = expected.identity_id
            """

    with duckdb_connection(gateway, table) as db:
        observed = db.execute(statement(trino=False)).fetchone()
    assert observed == (row_count, expected_amount, "decimal(38,5)", 0), observed
    trino = trino_query(statement(trino=True))[0]
    assert (trino[0], Decimal(trino[1]), trino[2], trino[3]) == observed, trino


def add_neighbors(table_name, ecosystem, tenant):
    with admin_catalog() as catalog:
        table = catalog.load_table(qualified(table_name))
        append_fixture_rows(
            table,
            [
                {**projected_row(tenant, NEIGHBOR, "2.00001"), "ecosystem": ecosystem},
                {**projected_row("other-tenant", DAY, "3.00001"), "ecosystem": ecosystem},
                {**projected_row(tenant, DAY, "4.00001"), "ecosystem": "other-ecosystem"},
            ],
        )
        return active_data_file_identity(table)


def check_neighbors(gateway, table, ecosystem, tenant, neighbors):
    latest = load_table(table)
    files = active_data_file_identity(latest)
    assert {path: files[path] for path in neighbors} == neighbors
    assert scope_rows(gateway, table, ecosystem, tenant, NEIGHBOR) == [(Decimal("2.00001"),)]
    assert scope_rows(gateway, table, ecosystem, "other-tenant") == [(Decimal("3.00001"),)]
    assert scope_rows(gateway, table, "other-ecosystem", tenant) == [(Decimal("4.00001"),)]
    trino = trino_query(
        f"SELECT ecosystem, tenant_id, CAST(chargeback_date AS VARCHAR), amount FROM chargeback.{qualified(table)} "
        f"WHERE (ecosystem = {sql_string(ecosystem)} AND tenant_id = {sql_string(tenant)} "
        f"AND chargeback_date = DATE '{NEIGHBOR}') "
        f"OR (ecosystem = {sql_string(ecosystem)} AND tenant_id = 'other-tenant' AND chargeback_date = DATE '{DAY}') "
        f"OR (ecosystem = 'other-ecosystem' AND tenant_id = {sql_string(tenant)} AND chargeback_date = DATE '{DAY}')"
    )
    assert sorted(trino) == sorted(
        [
            [ecosystem, tenant, str(NEIGHBOR), "2.00001"],
            [ecosystem, "other-tenant", str(DAY), "3.00001"],
            ["other-ecosystem", tenant, str(DAY), "4.00001"],
        ]
    ), trino
    return latest


def verify_large_publication(
    verdict, gateway, root, backend, opts, neighbors, rows, previous, diagnostics, *, tag_value=None
):
    table = opts["table"].rsplit(".", 1)[1]
    tenant, ecosystem, revision = opts["tenant_id"], opts["ecosystem"], opts["revision"]
    before = load_table(table)
    retained = (
        str(before.metadata.table_uuid),
        live_snapshot(before),
        *scope_summary(gateway, table, tenant, ecosystem),
    )
    assert retained[2] == previous, retained

    def verify_retained(deadline):
        actual = observe_current(gateway, table, tenant, ecosystem, deadline)
        assert actual == retained, (retained, actual)

    def held(evidence, deadline):
        assert 0 < evidence["staged_rows"] < evidence["total_rows"] == rows and evidence["files"], evidence
        verify_retained(deadline)

    result = run_attempt(gateway, root, opts, pause=held, before_commit=verify_retained)
    require_success(result, table)
    assert result["batches"] > 2 and result["files"] >= result["batches"] and result["rows"] == rows, result
    early, middle = result["staged"]
    assert early["batches"] == 1 and early["staged_rows"] < rows / 2 <= middle["staged_rows"], result
    assert result["commit_observations"] == [None], result
    verify_large_rows(
        gateway, table, backend, tenant, ecosystem, revision, rows, changed=bool(previous), tag_value=tag_value
    )
    current = check_neighbors(gateway, table, ecosystem, tenant, neighbors)
    new_files = {path: record for path, record in active_data_file_identity(current).items() if path not in neighbors}
    assert len(new_files) == result["files"]
    assert {record[2] for record in new_files.values()} == {(ecosystem, tenant, (DAY - ICEBERG_EPOCH).days)}
    added = current.metadata.snapshots[len(before.metadata.snapshots) :]
    assert (result["table_uuid"], result["snapshot"]) == (str(current.metadata.table_uuid), live_snapshot(current))
    assert added and added[-1].snapshot_id == result["snapshot"]
    show_result(table, rows, result, len(added))
    label = f"{table} {rows}" + (" replay" if tag_value else "")
    print(
        f"PASS {label} all-row output and neighboring partitions in both readers; one POST, one 200 receipt; "
        f"retained day visible at {early['staged_rows']:,} and {middle['staged_rows']:,} staged rows and at POST"
    )
    if diagnostics:
        verdict.gate(f"{label} single snapshot", len(added) == 1, f"one POST, {len(added)} snapshots")


def verify_large_tenant(verdict, gateway, root, config, diagnostics):
    spec = config.plugin_settings.iceberg_emitter
    assert spec is not None
    ecosystem, tenant = config.ecosystem, config.tenant_id
    table = spec.params.table.rsplit(".", 1)[1]
    catalog_config = spec.params.catalog
    assert table in THREE_TABLES and catalog_config.uri == gateway.uri
    assert catalog_config.s3 is not None and catalog_config.s3.access_key_id is None
    assert (
        catalog_config.ca_file and catalog_config.warehouse and catalog_config.s3.endpoint and catalog_config.s3.region
    )
    connection = config.storage.connection_string.get_secret_value()
    assert connection.startswith("sqlite:///")
    backend = create_storage_backend(config.storage, storage_module=get_storage_module_for_ecosystem(ecosystem))
    try:
        backend.create_tables()
        neighbors = add_neighbors(table, ecosystem, tenant)
        opts = settings(
            gateway,
            root,
            table,
            tenant,
            ecosystem,
            connection=connection,
            catalog_uri=catalog_config.uri,
            token=catalog_config.token.get_secret_value(),
            ca_file=catalog_config.ca_file,
            warehouse=catalog_config.warehouse,
            s3_endpoint=catalog_config.s3.endpoint,
            region=catalog_config.s3.region,
        )
        for previous, rows in ((0, 100_001), (100_001, 150_000)):
            revision = f"{ecosystem}-calculation-{rows}"
            seed_large_day(backend, tenant, revision, rows, ecosystem=ecosystem, previous_count=previous)
            if previous:
                changed = replace(large_day_row(tenant, DAY, 0, ecosystem=ecosystem), amount=Decimal("1.244565"))
                with backend.create_unit_of_work() as uow:
                    uow.chargebacks.upsert(changed)
                    uow.commit()
            verdict.probe(
                f"{table} qualification case at {rows}",
                verify_large_publication,
                verdict,
                gateway,
                root,
                backend,
                {**opts, "revision": revision},
                neighbors,
                rows,
                previous,
                diagnostics,
            )

        if ecosystem == "confluent_cloud" and diagnostics:
            with backend.create_unit_of_work() as uow:
                uow.tags.bulk_add_tags(
                    tenant,
                    [
                        {
                            "entity_type": "resource",
                            "entity_id": "resource-000",
                            "tag_key": "owner",
                            "tag_value": "replay-owner",
                        },
                    ],
                    override_existing=True,
                    created_by="iceberg-docker-fixture",
                )
                uow.commit()
            verify_large_publication(
                verdict,
                gateway,
                root,
                backend,
                {**opts, "revision": revision},
                neighbors,
                rows,
                rows,
                diagnostics,
                tag_value="replay-owner",
            )
            print("PASS same calculation ID tag replay")
    finally:
        backend.dispose()


def verify_large(verdict, gateway, root, diagnostics):
    original = Path("/example/config.yaml").read_text()
    enabled = original
    for setting in (
        '# warehouse: "${ICEBERG_CATALOG_WAREHOUSE}"',
        '# ca_file: "${ICEBERG_CATALOG_CA_FILE}"',
        "# s3:",
        '#   endpoint: "${ICEBERG_S3_ENDPOINT}"',
        "#   region: us-east-1",
    ):
        assert enabled.count(setting) == 1, setting
        enabled = enabled.replace(setting, setting[2:], 1)
    proof_yaml = root / "public-example.yaml"
    proof_yaml.write_text(enabled)
    with patch.dict(
        os.environ,
        {
            "CCLOUD_API_KEY": "fixture-cloud-key",
            "CCLOUD_API_SECRET": "fixture-cloud-secret",
            "ICEBERG_CATALOG_URI": gateway.uri,
            "ICEBERG_CATALOG_TOKEN": CATALOG_TOKEN,
            "ICEBERG_CATALOG_WAREHOUSE": WAREHOUSE,
            "ICEBERG_CATALOG_CA_FILE": str(gateway.ca),
            "ICEBERG_S3_ENDPOINT": S3_URL,
        },
    ):
        configured = load_config(proof_yaml, Path("/example/.env.example"))
    assert {config.ecosystem for config in configured.tenants.values()} == {
        "confluent_cloud",
        "self_managed_kafka",
        ECOSYSTEM,
    }
    for config in configured.tenants.values():
        verdict.probe(
            f"{config.ecosystem} source cases", verify_large_tenant, verdict, gateway, root, config, diagnostics
        )


def set_main_retention():
    with admin_catalog() as catalog:
        table = catalog.load_table("finance.alpha")
        table.manage_snapshots().create_branch(
            live_snapshot(table),
            "main",
            max_ref_age_ms=2_592_000_000,
            max_snapshot_age_ms=604_800_000,
            min_snapshots_to_keep=30,
        ).commit()


def verify_repeated_clear(verdict, gateway, root, backend):
    reset_table("alpha", full_metrics=True)
    neighbors = add_neighbors("alpha", ECOSYSTEM, "alpha")
    seed(backend, "alpha", "repeat", chargeback("alpha", DAY, "5.00001"))
    opts = settings(gateway, root, revision="repeat")
    first = run_attempt(gateway, root, opts)
    require_success(first, "alpha")
    set_main_retention()
    retained = main_retention_settings(load_table("alpha"))
    manifest_counts = []
    for _ in range(4):
        result = run_attempt(gateway, root, opts)
        require_success(result, "alpha")
        current = load_table("alpha")
        assert scope_rows(gateway, "alpha") == [(Decimal("5.00001"),)]
        manifest_counts.append(len(current.current_snapshot().manifests(current.io)))
        verdict.gate(
            f"main retention after replacement {len(manifest_counts)}",
            main_retention_settings(current) == retained,
            f"before={retained} after={main_retention_settings(current)}",
        )
    print(f"EVIDENCE repeated high-level replacements manifest counts={manifest_counts}")
    print(f"EVIDENCE explicit metadata scan reads on final replacement={result['metadata_reads']}")
    assert max(manifest_counts) <= manifest_counts[0] + 4, manifest_counts
    for revision in ("clear", "clear-again"):
        seed(backend, "alpha", revision)
        before = load_table("alpha")
        latest = run_attempt(gateway, root, {**opts, "revision": revision})
        require_success(latest, "alpha")
        current = check_neighbors(gateway, "alpha", ECOSYSTEM, "alpha", neighbors)
        assert scope_rows(gateway, "alpha") == []
        count = len(current.metadata.snapshots) - len(before.metadata.snapshots)
        assert live_snapshot(current) != live_snapshot(before)
        verdict.gate(f"{revision} single snapshot", count == 1, f"one POST, {count} snapshots")


def verify_new_metrics(verdict, gateway, root, backend, mode):
    reset_table("alpha", full_metrics=True)
    before = add_neighbors("alpha", ECOSYSTEM, "alpha")
    prefix = "s3://warehouse/finance/alpha/data/custom-prefix"
    with admin_catalog() as catalog:
        table = catalog.load_table("finance.alpha")
        with table.transaction() as tx:
            tx.set_properties(**{"write.metadata.metrics.default": mode, "write.data.path": prefix})
    prior = live_snapshot(load_table("alpha"))
    seed(backend, "alpha", f"metrics-{mode}", chargeback("alpha", DAY, "6.00000"))
    result = run_attempt(gateway, root, settings(gateway, root, revision=f"metrics-{mode}"))
    after = check_neighbors(gateway, "alpha", ECOSYSTEM, "alpha", before)
    if result["status"] != "ok":
        assert live_snapshot(after) == prior, result
        verdict.gate(f"new files metrics {mode}", False, f"rejected atomically: {result['error_type']}")
        return
    require_success(result, "alpha")
    new_files = {path: info for path, info in active_data_file_identity(after).items() if path not in before}
    assert all(path.startswith(f"{prefix}/") for path in new_files), new_files
    expected = (ECOSYSTEM, "alpha", (DAY - ICEBERG_EPOCH).days)
    exact = bool(new_files) and all(info[2] == expected for info in new_files.values())
    observed = scope_rows(gateway, "alpha")
    verdict.gate(
        f"new files metrics {mode}",
        exact and observed == [(Decimal("6.00000"),)],
        f"partitions={[info[2] for info in new_files.values()]} predicate_rows={observed}",
    )


def compacted_table(metric_kind):
    labels = (("chargeback_date", "utc_day"), ("tenant_id", "tenant_partition"), ("ecosystem", "ecosystem_partition"))
    if metric_kind == "truncated":
        reset_table("alpha", full_metrics=True, partition_fields=labels)
    else:
        reset_table("alpha", metrics_default="none", partition_fields=labels)
    with admin_catalog() as catalog:
        table = catalog.load_table("finance.alpha")
        prior_tag_id = table.schema().find_field("tags").field_id
        table.update_schema().delete_column("tags").commit()
        table.update_schema().add_column("tags", MapType(1001, StringType(), 1002, StringType())).commit()
        assert table.schema().find_field("tags").field_id != prior_tag_id
        append_fixture_rows(
            table,
            [
                projected_row("alpha", DAY, "1.00001"),
                projected_row("alpha", DAY, "2.00002"),
                projected_row("alpha", DAY, "3.00003"),
                projected_row("alpha", NEIGHBOR, "4.00004"),
                projected_row("other-tenant", DAY, "5.00005"),
                {**projected_row("alpha", DAY, "6.00006"), "ecosystem": "other-ecosystem"},
            ],
        )
    if metric_kind == "truncated":
        trino_query(f"ALTER TABLE chargeback.finance.alpha EXECUTE optimize WHERE chargeback_date = DATE '{DAY}'")
    trino_query("ALTER TABLE chargeback.finance.alpha EXECUTE optimize_manifests")
    table = load_table("alpha")
    target = ((DAY - ICEBERG_EPOCH).days, "alpha", ECOSYSTEM)
    manifests = table.current_snapshot().manifests(table.io)
    candidates = [
        [e.data_file for e in m.fetch_manifest_entry(table.io) if e.status.name != "DELETED"] for m in manifests
    ]
    exact_targets = [f for files in candidates for f in files if file_partition(f, 3) == target]
    assert exact_targets, candidates
    scope_ids = tuple(
        table.schema().find_field(name).field_id for name in ("ecosystem", "tenant_id", "chargeback_date")
    )
    if metric_kind == "truncated":
        assert len(exact_targets) == 1
        ecosystem_id = scope_ids[0]
        assert exact_targets[0].lower_bounds.get(ecosystem_id) != ECOSYSTEM.encode(), exact_targets[0].lower_bounds
    else:
        assert all(
            fid not in (f.lower_bounds or {}) and fid not in (f.upper_bounds or {})
            for f in exact_targets
            for fid in scope_ids
        )
    assert any(
        target in {file_partition(f, 3) for f in files} and len({file_partition(f, 3) for f in files}) > 1
        for files in candidates
    ), "expected a mixed Trino manifest"
    return table, target


def verify_old_metrics(verdict, gateway, root, backend, mode):
    for rows in ((chargeback("alpha", DAY, "7.00007"),), ()):
        baseline, target = compacted_table(mode)
        old_files = {p: record for p, record in active_data_file_identity(baseline).items() if record[2] != target}
        retained = live_snapshot(baseline)
        assert scope_rows(gateway, "alpha") == [(Decimal("1.00001"),), (Decimal("2.00002"),), (Decimal("3.00003"),)]
        action = "replace" if rows else "clear"
        revision = f"{mode}-{action}"
        seed(backend, "alpha", revision, *rows)
        prior = live_snapshot(load_table("alpha"))
        result = run_attempt(gateway, root, settings(gateway, root, revision=revision, deny_old_parquet=True))
        after = load_table("alpha")
        files = active_data_file_identity(after)
        assert {path: files[path] for path in old_files} == old_files
        assert trino_query(
            f"SELECT amount FROM chargeback.finance.alpha FOR VERSION AS OF {retained} "
            f"WHERE ecosystem='{ECOSYSTEM}' AND tenant_id='alpha' AND chargeback_date=DATE '{DAY}' ORDER BY amount"
        ) == [["1.00001"], ["2.00002"], ["3.00003"]]
        tags = baseline.schema().find_field("tags")
        assert result["field_ids"]["tags"] == tags.field_id
        assert result["field_ids"]["key"] == tags.field_type.key_id
        assert result["field_ids"]["value"] == tags.field_type.value_id
        if result["status"] != "ok":
            assert not result["posts"] and live_snapshot(after) == prior, result
        passed = result["status"] == "ok" and result["old_data_reads"] == 0
        if passed:
            require_success(result, "alpha")
            assert scope_rows(gateway, "alpha") == ([(Decimal("7.00007"),)] if rows else [])
        verdict.gate(
            f"{action} {mode} metrics without old Parquet reads",
            passed,
            f"source={'Trino compacted files' if mode == 'truncated' else 'PyIceberg files without metrics'} "
            f"result={result['status']} reads={result['old_data_reads']} posts={len(result['posts'])}",
        )


def verify_empty_table_reads(verdict, gateway, label):
    def read_trino():
        observed = trino_query("SELECT count(*) FROM chargeback.finance.alpha")
        assert observed == [[0]], observed

    def read_duckdb():
        observed = scope_rows(gateway, "alpha")
        assert observed == [], observed

    verdict.probe(f"{label} Trino read", read_trino)
    verdict.probe(f"{label} DuckDB read", read_duckdb)


def verify_atomicity_races(verdict, gateway, root, backend, mutation):
    reset_table("alpha", full_metrics=True)
    empty = mutation == "already-empty main"
    rows = () if empty else (chargeback("alpha", DAY, "2.00000"),)
    seed(backend, "alpha", "race-base", *rows)
    require_success(run_attempt(gateway, root, settings(gateway, root, revision="race-base")), "alpha")
    before = load_table("alpha")
    if empty:
        verify_empty_table_reads(verdict, gateway, "already-empty candidate transaction snapshot")
    attempted = () if empty else (chargeback("alpha", DAY, "3.00000"),)
    seed(backend, "alpha", "race-attempt", *attempted)
    changed = []

    def mutate(table, _request):
        assert table == "alpha"
        with admin_catalog() as catalog:
            current = catalog.load_table("finance.alpha")
            if empty:
                current.overwrite(
                    pa.Table.from_batches([], schema=current.schema().as_arrow()),
                    overwrite_filter=And(
                        EqualTo("ecosystem", ECOSYSTEM),
                        EqualTo("tenant_id", "alpha"),
                        EqualTo("chargeback_date", DAY),
                    ),
                )
            elif mutation == "main":
                append_fixture_rows(current, [projected_row("other-tenant", DAY, "0.00001")])
            elif mutation == "schema":
                current.update_schema().add_column("extra", StringType()).commit()
            else:
                current.update_spec().add_field("resource_id", IdentityTransform()).commit()
            changed.append(live_snapshot(current))

    gateway.before_next_commit = mutate
    try:
        result = run_attempt(gateway, root, settings(gateway, root, revision="race-attempt"))
    finally:
        gateway.before_next_commit = None
    assert len(changed) == 1 and len(result["posts"]) == 1, result
    assert (changed[0] == live_snapshot(before)) == (mutation in {"schema", "spec"})
    requirements = {item["type"]: item for item in result["payloads"][0][1]["requirements"]}
    expected = {
        "assert-table-uuid": {"uuid": str(before.metadata.table_uuid)},
        "assert-ref-snapshot-id": {"ref": "main", "snapshot-id": live_snapshot(before)},
        "assert-current-schema-id": {"current-schema-id": before.metadata.current_schema_id},
        "assert-default-spec-id": {"default-spec-id": before.metadata.default_spec_id},
    }
    guards = {
        kind: {field: requirements.get(kind, {}).get(field) for field in values} for kind, values in expected.items()
    }
    mismatch = {kind: (values, guards[kind]) for kind, values in expected.items() if guards[kind] != values}
    verdict.gate(f"{mutation} race guards", not mismatch, f"expected and observed discrepancies={mismatch}")
    unchanged = live_snapshot(load_table("alpha")) == changed[0]
    stale = result["status"] == "error" and result["receipts"] == [("alpha", 409)] and unchanged
    evidence = f"receipts={result['receipts']} unchanged_after_race={unchanged}"
    if not empty:
        observed = scope_rows(gateway, "alpha")
        stale = stale and observed == [(Decimal("2.00000"),)]
        evidence += f" rows={observed}"
    verdict.gate(f"{mutation} race stale attempt rejected", stale, evidence)
    if empty and unchanged:
        verify_empty_table_reads(verdict, gateway, "already-empty competing overwrite snapshot")


def verify_lost(gateway, root, backend, fault):
    reset_table("alpha", full_metrics=True)
    seed(backend, "alpha", "before-loss", chargeback("alpha", DAY, "8.00000"))
    require_success(run_attempt(gateway, root, settings(gateway, root, revision="before-loss")), "alpha")
    retained = live_snapshot(load_table("alpha"))
    seed(backend, "alpha", "after-loss", chargeback("alpha", DAY, "9.00000"))
    opts = settings(gateway, root, revision="after-loss")
    gateway.next_commit_fault = fault
    try:
        ambiguous = run_attempt(gateway, root, opts)
    finally:
        gateway.next_commit_fault = None
    assert ambiguous["status"] == "error", ambiguous
    assert ambiguous["posts"] == [("alpha", "table")], ambiguous
    committed = load_table("alpha")
    assert committed.metadata_location == gateway.lost_commit_metadata, ambiguous
    uncertain_snapshot = live_snapshot(committed)
    assert uncertain_snapshot != retained

    def verify_reads():
        assert scope_rows(gateway, "alpha") == [(Decimal("9.00000"),)]
        for snapshot, amount in ((retained, "8.00000"), (uncertain_snapshot, "9.00000")):
            assert trino_query(
                f"SELECT amount FROM chargeback.finance.alpha FOR VERSION AS OF {snapshot} "
                f"WHERE tenant_id='alpha' AND chargeback_date=DATE '{DAY}'"
            ) == [[amount]]

    verify_reads()
    require_success(run_attempt(gateway, root, opts), "alpha")
    verify_reads()
    print(f"EVIDENCE {fault}: one physical POST, committed and retained data survived replay")


def verify_microbatch_recovery(gateway, root, backend, fault):
    rows = 150_000
    reset_table("alpha", full_metrics=True)
    neighbors = add_neighbors("alpha", ECOSYSTEM, "alpha")
    with admin_catalog() as catalog:
        table = catalog.load_table("finance.alpha")
        append_fixture_rows(table, [projected_row("alpha", DAY, "2.00000")])
    baseline = load_table("alpha")
    original_files = active_data_file_identity(baseline)
    retained = live_snapshot(baseline)
    assert scope_summary(gateway, "alpha", "alpha", ECOSYSTEM) == (1, Decimal("2.00000"), None, None)
    revision = f"microbatch-{fault}"
    seed_large_day(backend, "alpha", revision, rows, previous_count=rows)
    opts = settings(gateway, root, revision=revision)
    historical = {}
    historical_objects = {}
    abandoned = set()

    def read_version(snapshot):
        return scope_summary(gateway, "alpha", "alpha", ECOSYSTEM, snapshot=snapshot)

    if fault in {"interrupt", "short-iterator"}:
        if fault == "interrupt":
            stopped = run_attempt(gateway, root, opts, interrupt=True)
            assert stopped["status"] == "killed", stopped
            (stage,) = stopped["staged"]
            assert stage["batches"] == 1 and 0 < stage["staged_rows"] < stage["total_rows"] == rows, stage
            abandoned = set(stage["files"])
            staged_rows = stage["staged_rows"]
        else:
            stopped = run_attempt(gateway, root, {**opts, "short_batch_iterator": True})
            assert stopped["status"] == "error" and stopped["error_type"] == "IncompleteDayError", stopped
            assert stopped["batches"] == 1 and stopped["files"] > 0, stopped
            staged_rows = stopped["rows"]
            assert 0 < staged_rows < stopped["source_rows"] == rows, stopped
        assert stopped["posts"] == [] and stopped["receipts"] == [], stopped
        current = load_table("alpha")
        assert live_snapshot(current) == retained and active_data_file_identity(current) == original_files
        if fault == "interrupt":
            assert abandoned and all(current.io.new_input(path).exists() for path in abandoned), abandoned
        assert scope_summary(gateway, "alpha", "alpha", ECOSYSTEM) == (1, Decimal("2.00000"), None, None)
        historical[retained] = read_version(retained)
        print(f"EVIDENCE {fault} after {staged_rows:,} staged rows; zero POSTs; local scratch cleared")
    else:
        gateway.lost_commit_metadata = None
        gateway.next_commit_fault = fault
        try:
            ambiguous = run_attempt(gateway, root, opts)
        finally:
            gateway.next_commit_fault = None
        status = 503 if fault == "lose-response-with-retry" else 200
        assert ambiguous["status"] == "error" and ambiguous["batches"] > 2, ambiguous
        assert ambiguous["posts"] == [("alpha", "table")] and ambiguous["receipts"] == [("alpha", status)], ambiguous
        committed = load_table("alpha")
        assert committed.metadata_location == gateway.lost_commit_metadata, ambiguous
        assert (str(committed.metadata.table_uuid), live_snapshot(committed)) == (
            ambiguous["intended_uuid"],
            ambiguous["intended_snapshot"],
        )
        added = committed.metadata.snapshots[len(baseline.metadata.snapshots) :]
        assert len(added) > 1 and added[-1].snapshot_id == live_snapshot(committed)
        for snapshot in (baseline.current_snapshot(), *added):
            assert snapshot is not None
            historical_objects[snapshot.manifest_list] = "manifest list"
            for manifest in read_manifest_list(committed.io.new_input(snapshot.manifest_list)):
                if manifest.manifest_path in historical_objects:
                    continue
                historical_objects[manifest.manifest_path] = "manifest file"
                for entry in manifest.fetch_manifest_entry(committed.io):
                    if entry.status.name != "DELETED":
                        historical_objects[str(entry.data_file.file_path)] = "live data file"
        snapshot_ids = [retained, *(item.snapshot_id for item in added)]
        historical = {snapshot: read_version(snapshot) for snapshot in snapshot_ids}
        assert historical[retained] == (1, Decimal("2.00000"), None, None)
        assert historical[added[-1].snapshot_id] == (rows, large_day_total(rows).quantize(UNIT), revision, revision)
        assert scope_summary(gateway, "alpha", "alpha", ECOSYSTEM) == historical[added[-1].snapshot_id]
        print(
            f"EVIDENCE {fault} one physical POST; readable retained and uncertain snapshot row counts "
            f"{[values[0] for values in historical.values()]}"
        )

    replay = run_attempt(gateway, root, opts)
    require_success(replay, "alpha")
    assert replay["batches"] > 2 and replay["rows"] == rows
    current = check_neighbors(gateway, "alpha", ECOSYSTEM, "alpha", neighbors)
    assert (str(current.metadata.table_uuid), live_snapshot(current)) == (replay["table_uuid"], replay["snapshot"])
    assert not abandoned.intersection(active_data_file_identity(current))
    verify_large_rows(gateway, "alpha", backend, "alpha", ECOSYSTEM, revision, rows, changed=False, tag_value=None)
    for path, kind in historical_objects.items():
        assert current.io.new_input(path).exists(), f"Replay lost retained {kind} {path}"
    if historical_objects:
        print(f"EVIDENCE {fault} verified {len(historical_objects)} retained objects directly in S3 after replay")
    assert {snapshot: read_version(snapshot) for snapshot in historical} == historical
    print(
        f"PASS {fault} fresh replay {rows:,} exact rows; retained data readable; "
        f"unreferenced interrupted Parquet files={len(abandoned)}"
    )


def verify_missing_grant(verdict, gateway, root, backend, grant):
    reset_table("alpha", full_metrics=True)
    prior = live_snapshot(load_table("alpha"))
    seed(backend, "alpha", "missing-grant", chargeback("alpha", DAY, "6.00000"))
    with storage_gateway() as storage:
        storage.release.set()
        opts = settings(gateway, root, revision="missing-grant", s3_endpoint=storage.endpoint)
        gateway.storage_grant = grant
        try:
            result = run_attempt(gateway, root, opts, ambient=True)
        finally:
            gateway.storage_grant = "table"
        s3_requests = len(storage.request_lines)
    unchanged = live_snapshot(load_table("alpha")) == prior
    loaded = result["table_gets"] == ["catalog"] and "field_ids" in result
    posts = len(result["posts"])
    label = f"{grant} vended grant rejected before S3"
    evidence = (
        f"table_load={loaded} S3 requests={s3_requests} posts={posts} unchanged_main={unchanged} "
        f"result={result.get('error_type', result['status'])}"
    )
    if s3_requests or posts or not unchanged:
        verdict.gate(label, False, evidence)
    else:
        verdict.untested(label, f"{evidence}; no identifiable credential-admission refusal")


def avro_long(value):
    encoded = bytearray()
    shifted = value << 1
    while shifted > 127:
        encoded.append((shifted & 127) | 128)
        shifted >>= 7
    encoded.append(shifted)
    return bytes(encoded)


def pad_manifest(body, length):
    """Prepend a valid Avro metadata map block without changing manifest entries."""
    assert body.startswith(b"Obj\x01")
    key = b"fixture.padding"
    block = avro_long(1) + avro_long(len(key)) + key
    for width in range(1, 11):
        value_length = length - len(body) - len(block) - width
        if value_length >= 0 and len(avro_long(value_length)) == width:
            return body[:4] + block + avro_long(value_length) + b"x" * value_length + body[4:]
    raise AssertionError("No valid header padding for the requested Avro length")


def verify_manifest_size(verdict, gateway, root, backend, size):
    from fixture import mc

    reset_table("alpha", full_metrics=True)
    seed(backend, "alpha", "metadata-before", chargeback("alpha", DAY, "1.10000"))
    require_success(run_attempt(gateway, root, settings(gateway, root, revision="metadata-before")), "alpha")
    baseline = load_table("alpha")
    committed = live_snapshot(baseline)
    manifest = baseline.current_snapshot().manifest_list
    assert manifest.startswith("s3://warehouse/finance/alpha/metadata/")
    object_name = "admin/" + manifest.removeprefix("s3://")
    baseline_files = active_data_file_identity(baseline)
    manifest_paths = {item.manifest_path for item in baseline.current_snapshot().manifests(baseline.io)}
    seed(backend, "alpha", "metadata-after", chargeback("alpha", DAY, "2.20000"))

    with TemporaryDirectory(dir=root, prefix="manifest-bound-") as temporary:
        original = Path(temporary) / "original.avro"
        padded = Path(temporary) / "padded.avro"
        mc("cp", object_name, str(original))
        padded.write_bytes(pad_manifest(original.read_bytes(), size))
        assert padded.stat().st_size == size
        mc("cp", str(padded), object_name)
        try:
            loaded = load_table("alpha")
            assert {item.manifest_path for item in read_manifest_list(loaded.io.new_input(manifest))} == manifest_paths
            assert active_data_file_identity(loaded) == baseline_files
            result = run_attempt(
                gateway, root, settings(gateway, root, revision="metadata-after", observe_metadata=True)
            )
            after = live_snapshot(load_table("alpha"))
            lengths = result["metadata_lengths"]
            if result["status"] == "ok":
                require_success(result, "alpha")
                assert after != committed
                assert scope_rows(gateway, "alpha") == [(Decimal("2.20000"),)]
                manifest_reads = lengths.get(manifest, [])
                assert manifest_reads and set(manifest_reads) == {size}, lengths
                verdict.gate(
                    f"4 MiB metadata file limit at {size} bytes",
                    size <= 4 * MIB,
                    f"valid published fixture, manifest reads={len(manifest_reads)}, "
                    f"all metadata opens={sum(map(len, lengths.values()))}, "
                    f"reopens={sum(len(values) - 1 for values in lengths.values())}",
                )
            else:
                assert after == committed and result["posts"] == []
                assert scope_rows(gateway, "alpha") == [(Decimal("1.10000"),)]
                verdict.untested(
                    f"4 MiB metadata file limit at {size} bytes",
                    f"publication rejected with {result['error_type']} and unchanged main; no limit error verified",
                )
        finally:
            mc("cp", str(original), object_name)


def verify_rest_size(verdict, gateway, root):
    opts = settings(gateway, root, load_only=True)
    for size in (8 * MIB, 8 * MIB + 1):
        gateway.load_response_size = size
        try:
            observed = run_attempt(gateway, root, opts)
        finally:
            gateway.load_response_size = 0
        assert gateway.last_table_response_size == size and not observed["posts"], observed
        if observed["status"] == "ok":
            verdict.gate(f"8 MiB REST response limit at {size} bytes", size <= 8 * MIB, "valid response loaded")
        else:
            verdict.untested(
                f"8 MiB REST response limit at {size} bytes",
                f"retrieval rejected with {observed.get('error_type', observed['status'])}; no limit error verified",
            )


def verify_bounds(verdict, gateway, root, backend):
    reset_table("alpha", full_metrics=True)
    verdict.probe("REST response boundaries", verify_rest_size, verdict, gateway, root)
    for size in (4 * MIB, 4 * MIB + 1):
        verdict.probe(f"manifest boundary {size}", verify_manifest_size, verdict, gateway, root, backend, size)
    for limit in ("64 MiB encoded and expanded metadata", "512 metadata reads", "100,000 metadata records"):
        verdict.untested(limit, "no quantitative admission check in candidate; no over-limit fixture executed")

    reset_table("alpha", full_metrics=True)
    seed(backend, "alpha", "limit-base", chargeback("alpha", DAY, "1.10000"))
    require_success(run_attempt(gateway, root, settings(gateway, root, revision="limit-base")), "alpha")
    prior = live_snapshot(load_table("alpha"))
    seed(backend, "alpha", "over-limits", chargeback("alpha", DAY, "2.20000"))
    opts = settings(gateway, root, revision="over-limits")

    scratch = run_attempt(gateway, root, {**opts, "scratch_limit": 1})
    assert scratch["status"] == "error" and scratch["error_type"] == "IcebergScratchError", scratch
    assert scratch["posts"] == [] and live_snapshot(load_table("alpha")) == prior
    print("PASS scratch exhaustion rejected before commit")
    killed = run_attempt(gateway, root, opts, interrupt=True)
    assert killed["status"] == "killed" and killed["posts"] == [], killed
    assert live_snapshot(load_table("alpha")) == prior
    print("PASS interrupted file staging left the prior snapshot and no local scratch")
    gateway.stall_lookup = True
    gateway.lookup_received.clear()
    gateway.release_lookup.clear()
    try:
        catalog = run_attempt(gateway, root, opts, timeout=8)
    finally:
        gateway.stall_lookup = False
        gateway.release_lookup.set()
    assert gateway.lookup_received.is_set() and catalog["status"] == "timed_out", catalog
    assert catalog["posts"] == [] and live_snapshot(load_table("alpha")) == prior
    print("PASS catalog stall worker process reaped within proof deadline")
    with storage_gateway() as storage:
        stopped = run_attempt(gateway, root, {**opts, "s3_endpoint": storage.endpoint}, timeout=8)
        assert storage.stalled.is_set() and stopped["status"] in {"timed_out", "error"}, stopped
        assert stopped["posts"] == [] and live_snapshot(load_table("alpha")) == prior
    print("PASS storage stall ended without commit and without local scratch")
    assert scope_rows(gateway, "alpha") == [(Decimal("1.10000"),)]


def main():
    verify_api_boundary()
    parser = argparse.ArgumentParser()
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--smoke", action="store_true", help="Run an isolated TLS development check")
    modes.add_argument("--microbatch", action="store_true", help="Check microbatch publication and recovery")
    modes.add_argument("--public-api", action="store_true", help="Run the wider public API diagnostics")
    mode = parser.parse_args()
    verdict = Verdict()
    tables = ("alpha",) if mode.smoke else ("alpha", *THREE_TABLES)
    with TemporaryDirectory(dir="/state", prefix="public-iceberg-") as directory, ExitStack() as stack:
        root = Path(directory)
        stack.enter_context(chdir(root))
        (root / "data").mkdir()
        stack.enter_context(patch("gateway.TABLES", tables))
        provision_storage(root, tables)
        provision_tables(tables, full_metrics=True)
        gateway = stack.enter_context(catalog_gateway(root / "ca.pem"))
        alpha = source_backend(stack, root, "alpha", ECOSYSTEM)
        if mode.smoke:
            verify_auth(gateway, root, alpha)
            return
        wait_for_trino()
        verdict.probe(
            "all three production SQLite plug-in sources", verify_large, verdict, gateway, root, not mode.microbatch
        )
        if mode.microbatch:
            seed_large_day(alpha, "alpha", "recovery-source", 150_000)
            for fault in ("lose-response", "lose-response-with-retry", "interrupt", "short-iterator"):
                verdict.probe(f"150,000 row {fault}", verify_microbatch_recovery, gateway, root, alpha, fault)
        else:
            verify_diagnostics(verdict, gateway, root, alpha, tables)
    verify_api_boundary()
    label = "MICROBATCH PROOF" if mode.microbatch else "PUBLIC API DIAGNOSTICS"
    if verdict.failed or verdict.unproved:
        print(
            f"{label} UNMET ({len(verdict.failed)} failed, {len(verdict.unproved)} unproved): "
            f"{', '.join([*verdict.failed, *verdict.unproved])}"
        )
        raise SystemExit(1)
    print(f"{label} PASSED")


def verify_diagnostics(verdict, gateway, root, alpha, tables):
    verdict.probe("TLS and actual table-local bearer", verify_auth, gateway, root, alpha)
    verdict.probe("MinIO credential isolation", lambda: verify_object_access({t: load_table(t) for t in tables}))
    verdict.probe("repeated replacement and clear", verify_repeated_clear, verdict, gateway, root, alpha)
    for mode in ("none", "counts", "full"):
        verdict.probe(f"new {mode} metrics case", verify_new_metrics, verdict, gateway, root, alpha, mode)
    for mode in ("truncated", "absent"):
        verdict.probe(f"old {mode} metrics case", verify_old_metrics, verdict, gateway, root, alpha, mode)
    for mutation in ("main", "already-empty main", "schema", "spec"):
        verdict.probe(f"{mutation} race case", verify_atomicity_races, verdict, gateway, root, alpha, mutation)
    for fault in ("lose-response", "lose-response-with-retry"):
        verdict.probe(f"{fault} case", verify_lost, gateway, root, alpha, fault)
    for grant in ("missing", "metadata-only"):
        verdict.probe(f"{grant} grant case", verify_missing_grant, verdict, gateway, root, alpha, grant)
    verdict.probe("bound and interruption cases", verify_bounds, verdict, gateway, root, alpha)


if __name__ == "__main__":
    main()
