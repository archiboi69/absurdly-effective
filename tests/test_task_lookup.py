from datetime import datetime, timedelta, timezone

import pytest


def _task_by_idempotency_key(client, queue, idempotency_key):
    return client.conn.execute(
        """
        select task_id, last_attempt_run
          from absurd.get_task_by_idempotency_key(%s, %s)
        """,
        (queue, idempotency_key),
    ).fetchone()


@pytest.mark.parametrize("storage_mode", ["unpartitioned", "partitioned"])
def test_task_identity_round_trips_in_every_storage_mode(client, storage_mode):
    queue = f"task_identity_{storage_mode}"
    client.create_queue(queue, storage_mode=storage_mode)

    spawned = client.spawn_task(
        queue,
        "identified-task",
        {"value": 1},
        {"idempotency_key": "execution-123"},
    )

    task = _task_by_idempotency_key(client, queue, "execution-123")
    execution = client.conn.execute(
        "select idempotency_key from absurd.get_task_idempotency_key(%s, %s)",
        (queue, spawned.task_id),
    ).fetchone()

    assert task == (spawned.task_id, spawned.run_id)
    assert execution == ("execution-123",)


@pytest.mark.parametrize("storage_mode", ["unpartitioned", "partitioned"])
def test_task_identity_disappears_when_task_cleanup_releases_the_key(
    client, storage_mode
):
    queue = f"task_identity_cleanup_{storage_mode}"
    client.create_queue(queue, storage_mode=storage_mode)
    completed_at = datetime(2024, 7, 1, 10, 0, tzinfo=timezone.utc)
    client.set_fake_now(completed_at)

    spawned = client.spawn_task(
        queue,
        "identified-task",
        {"value": 1},
        {"idempotency_key": "reusable-execution"},
    )
    claimed = client.claim_tasks(queue)[0]
    client.complete_run(queue, claimed["run_id"], {"ok": True})

    client.set_fake_now(completed_at + timedelta(days=2))
    assert client.cleanup_tasks(queue, ttl_seconds=3600) == 1

    assert _task_by_idempotency_key(client, queue, "reusable-execution") is None
    assert (
        client.conn.execute(
            "select idempotency_key from absurd.get_task_idempotency_key(%s, %s)",
            (queue, spawned.task_id),
        ).fetchone()
        is None
    )
