import asyncio

import psycopg
from psycopg import sql

from absurd_sdk import Absurd, AsyncAbsurd, effect_workflow_execution_id


def test_effect_workflow_execution_id_matches_effect_workflow() -> None:
    assert (
        effect_workflow_execution_id(
            "ShippingBroker/Finance/IssueSalesInvoice",
            "123",
        )
        == "3cf216794c6e5c9e7422efff15806610"
    )


def test_effect_workflow_execution_id_hashes_utf8_and_truncates_to_128_bits() -> None:
    execution_id = effect_workflow_execution_id("workflow-zażółć", "klucz-🚚")

    assert execution_id == "444a0466cfbeccfe2cdc7953618be84b"


def test_spawn_workflow_sets_effect_defaults_and_is_idempotent(
    conn, queue_name
) -> None:
    queue = queue_name("effect_workflow")
    client = Absurd(conn, queue_name=queue, default_max_attempts=9)
    client.create_queue()

    first = client.spawnWorkflow(
        "ShippingBroker/Finance/IssueSalesInvoice",
        {"attemptId": 123},
        "invoice-123",
    )
    second = client.spawnWorkflow(
        "ShippingBroker/Finance/IssueSalesInvoice",
        {"attemptId": 456},
        "invoice-123",
    )

    assert first == second
    assert first["execution_id"] == effect_workflow_execution_id(
        "ShippingBroker/Finance/IssueSalesInvoice", "invoice-123"
    )

    task = conn.execute(
        sql.SQL(
            """
            SELECT task_name, params, idempotency_key, retry_strategy, max_attempts
            FROM absurd.{table}
            WHERE task_id = %s
            """
        ).format(table=sql.Identifier(f"t_{queue}")),
        (first["task_id"],),
    ).fetchone()

    assert task == (
        "ShippingBroker/Finance/IssueSalesInvoice",
        {"attemptId": 123},
        first["execution_id"],
        {"kind": "fixed", "base_seconds": 1},
        5,
    )


def test_async_spawn_workflow_sets_effect_defaults_and_returns_ids(
    db_dsn, queue_name
) -> None:
    queue = queue_name("async_effect_workflow")

    with psycopg.connect(db_dsn, autocommit=True) as setup_conn:
        Absurd(setup_conn, queue_name=queue).create_queue()

    async def run():
        client = AsyncAbsurd(db_dsn, queue_name=queue, default_max_attempts=9)
        try:
            return await client.spawnWorkflow("workflow", {"value": 42}, "order-42")
        finally:
            await client.close()

    result = asyncio.run(run())

    assert result["execution_id"] == effect_workflow_execution_id(
        "workflow", "order-42"
    )
    with psycopg.connect(db_dsn) as verify_conn:
        max_attempts = verify_conn.execute(
            sql.SQL(
                "SELECT max_attempts FROM absurd.{table} WHERE task_id = %s"
            ).format(table=sql.Identifier(f"t_{queue}")),
            (result["task_id"],),
        ).fetchone()
    assert max_attempts == (5,)
