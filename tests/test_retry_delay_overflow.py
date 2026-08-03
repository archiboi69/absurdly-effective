from datetime import datetime, timedelta, timezone

import pytest
from psycopg.types.json import Jsonb


MAX_RETRY_DELAY = timedelta(days=1)


@pytest.mark.parametrize(
    "strategy",
    [
        {"kind": "fixed", "base_seconds": -1},
        {"kind": "fixed", "base_seconds": 86401},
        {"kind": "fixed", "base_seconds": "invalid"},
        {"kind": "exponential", "max_seconds": 86401},
        {"kind": "invalid"},
    ],
)
def test_invalid_retry_strategy_is_rejected_on_spawn(client, strategy):
    queue = "invalid_retry_strategy"
    client.create_queue(queue)

    with pytest.raises(Exception) as exc_info:
        client.spawn_task(
            queue,
            "task",
            {},
            {"retry_strategy": strategy, "max_attempts": 2},
        )
    assert exc_info.value.sqlstate == "AB003"


def test_unrelated_retry_scheduling_errors_propagate(client):
    queue = "retry_scheduling_error"
    client.create_queue(queue)

    client.set_fake_now(datetime(2024, 1, 1, tzinfo=timezone.utc))
    client.spawn_task(
        queue,
        "task",
        {},
        {
            "retry_strategy": {"kind": "fixed", "base_seconds": 86400},
            "max_attempts": 2,
        },
    )
    claim = client.claim_tasks(queue, worker="worker-a")[0]

    client.set_fake_now("294276-12-31 00:00:00+00")
    with pytest.raises(Exception) as exc_info:
        client.fail_run(queue, claim["run_id"], {"message": "failure"})
    assert exc_info.value.sqlstate == "22008"


def test_exponential_retry_underflow_becomes_zero_delay(client):
    row = client.conn.execute(
        "select absurd.retry_delay_seconds(%s, 3)",
        (Jsonb({"kind": "exponential", "base_seconds": 30, "factor": 1e-308}),),
    ).fetchone()
    assert row == (0.0,)


def test_exponential_retry_overflow_uses_one_day_cap(client):
    queue = "exponential_retry_delay_overflow"
    client.create_queue(queue)

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    client.set_fake_now(start)

    task = client.spawn_task(
        queue,
        "task",
        {},
        {
            "retry_strategy": {
                "kind": "exponential",
                "base_seconds": 30,
                "factor": 1e308,
            },
            "max_attempts": 3,
        },
    )

    first_claim = client.claim_tasks(queue, worker="worker-a")[0]
    client.fail_run(queue, first_claim["run_id"], {"message": "first failure"})

    second_at = start + timedelta(seconds=30)
    client.set_fake_now(second_at)
    second_claim = client.claim_tasks(queue, worker="worker-b")[0]
    assert second_claim["attempt"] == 2

    client.fail_run(queue, second_claim["run_id"], {"message": "second failure"})

    runs = client.get_runs(queue, task.task_id)
    assert [run["state"] for run in runs] == ["failed", "failed", "sleeping"]
    assert runs[2]["available_at"] == second_at + MAX_RETRY_DELAY


def test_invalid_legacy_retry_strategy_does_not_wedge_queue(client):
    queue = "legacy_retry_delay_overflow"
    client.create_queue(queue)

    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    client.set_fake_now(start)

    poison = client.spawn_task(
        queue,
        "poison",
        {},
        {
            "retry_strategy": {"kind": "fixed", "base_seconds": 30},
            "max_attempts": 3,
        },
    )
    claim = client.claim_tasks(queue, worker="worker-a", claim_timeout=30, qty=1)[0]
    assert claim["task_id"] == poison.task_id

    # Simulate a task created before retry strategy validation was installed.
    client.conn.execute(
        "update absurd.t_legacy_retry_delay_overflow "
        "set retry_strategy = %s where task_id = %s",
        (Jsonb({"kind": "fixed", "base_seconds": 1e18}), poison.task_id),
    )
    healthy = client.spawn_task(queue, "healthy", {})

    client.set_fake_now(start + timedelta(seconds=31))
    next_claim = client.claim_tasks(queue, worker="worker-b", qty=1)
    assert [item["task_id"] for item in next_claim] == [healthy.task_id]

    assert client.get_task(queue, poison.task_id)["state"] == "failed"
    assert [run["state"] for run in client.get_runs(queue, poison.task_id)] == [
        "failed"
    ]
