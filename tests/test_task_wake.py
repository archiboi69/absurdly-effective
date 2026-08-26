"""SQL contract tests for absurd.wake_task."""

import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb


def _set_application_name(conn, name):
    conn.execute(sql.SQL("set application_name = {}").format(sql.Literal(name)))


def _wait_for_lock(db_dsn, application_name, timeout_seconds=5.0):
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            row = conn.execute(
                """
                select wait_event_type
                  from pg_stat_activity
                 where application_name = %s
                """,
                (application_name,),
            ).fetchone()
            if row is not None and row[0] == "Lock":
                return
            time.sleep(0.01)
    raise AssertionError(f"{application_name} did not block on a lock")


def _wake_task(client, queue, task_id):
    row = client.conn.execute(
        """
        select run_id, previous_state
          from absurd.wake_task(%s, %s)
        """,
        (queue, task_id),
    ).fetchone()
    assert row is not None
    return row


def _create_running_task(db_dsn, queue):
    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute("select absurd.create_queue(%s)", (queue,))
        task_id, run_id = conn.execute(
            """
            select task_id, run_id
              from absurd.spawn_task(%s, %s, %s, %s)
            """,
            (queue, "running-task", Jsonb({"value": 1}), Jsonb({})),
        ).fetchone()
        claimed_run_id = conn.execute(
            "select run_id from absurd.claim_task(%s, %s, %s, %s)",
            (queue, "worker", 60, 1),
        ).fetchone()[0]
        assert claimed_run_id == run_id
    return task_id, run_id


def _terminal_transition(conn, operation, queue, task_id, run_id):
    if operation == "complete":
        conn.execute(
            "select absurd.complete_run(%s, %s, %s)",
            (queue, run_id, Jsonb({"ok": True})),
        )
    else:
        conn.execute("select absurd.cancel_task(%s, %s)", (queue, task_id))


def test_wake_task_requeues_sleeping_event_wait_and_cleans_registration(client):
    queue = "wake_sleeping_wait"
    client.create_queue(queue)

    spawned = client.spawn_task(queue, "waiting-task", {"value": 1})
    claimed = client.claim_tasks(queue)[0]
    suspended = client.await_event(
        queue,
        spawned.task_id,
        claimed["run_id"],
        "wait-step",
        "some-event",
    )
    assert suspended["should_suspend"] is True

    run_id, previous_state = _wake_task(client, queue, spawned.task_id)

    assert run_id == claimed["run_id"]
    assert previous_state == "sleeping"
    assert client.get_run(queue, run_id)["state"] == "pending"
    wait_count = client.conn.execute(
        sql.SQL("select count(*) from absurd.{waits} where task_id = %s").format(
            waits=client.get_table("w", queue)
        ),
        (spawned.task_id,),
    ).fetchone()[0]
    assert wait_count == 0
    replay = client.claim_tasks(queue, worker="replay-worker")
    assert [row["run_id"] for row in replay] == [claimed["run_id"]]


def test_repeated_wake_is_idempotent_and_does_not_create_a_run(client):
    queue = "wake_repeated"
    client.create_queue(queue)
    spawned = client.spawn_task(queue, "wake-me", {"value": 1})

    first_run_id, first_state = _wake_task(client, queue, spawned.task_id)
    second_run_id, second_state = _wake_task(client, queue, spawned.task_id)

    assert first_run_id == second_run_id == spawned.run_id
    assert first_state == second_state == "pending"
    claimed = client.claim_tasks(queue, worker="replay-worker")
    assert [row["run_id"] for row in claimed] == [spawned.run_id]
    assert client.claim_tasks(queue, worker="other-worker") == []


def test_wake_task_reports_missing_task_like_other_task_operations(client):
    queue = "wake_missing"
    client.create_queue(queue)

    with pytest.raises(psycopg.errors.RaiseException, match="Task .* not found"):
        _wake_task(client, queue, uuid.uuid4())


def test_wake_task_makes_delayed_pending_run_claimable(client):
    queue = "wake_delayed_pending"
    client.create_queue(queue)
    spawned = client.spawn_task(queue, "retrying-task", {"value": 1})
    delayed_until = datetime.now(timezone.utc) + timedelta(hours=1)
    client.conn.execute(
        sql.SQL("update absurd.{runs} set available_at = %s where run_id = %s").format(
            runs=client.get_table("r", queue)
        ),
        (delayed_until, spawned.run_id),
    )

    assert client.claim_tasks(queue, worker="early-worker") == []
    run_id, previous_state = _wake_task(client, queue, spawned.task_id)

    assert previous_state == "pending"
    assert run_id == spawned.run_id
    replay = client.claim_tasks(queue, worker="replay-worker")
    assert [row["run_id"] for row in replay] == [run_id]


def test_wake_task_keeps_a_running_claim_unchanged(client):
    queue = "wake_running"
    client.create_queue(queue)
    spawned = client.spawn_task(queue, "running-task", {"value": 1})
    claimed = client.claim_tasks(queue)[0]

    run_id, previous_state = _wake_task(client, queue, spawned.task_id)
    run = client.get_run(queue, claimed["run_id"])
    task = client.get_task(queue, spawned.task_id)

    assert run_id == claimed["run_id"]
    assert previous_state == "running"
    assert run["state"] == "running"
    assert run["claimed_by"] == "worker"
    assert task["state"] == "running"


@pytest.mark.parametrize("terminal_state", ["completed", "failed", "cancelled"])
def test_wake_task_is_a_no_op_for_terminal_tasks(client, terminal_state):
    queue = f"wake_terminal_{terminal_state}"
    client.create_queue(queue)
    options = {"max_attempts": 1} if terminal_state == "failed" else None
    spawned = client.spawn_task(queue, "terminal-task", {"value": 1}, options)
    claimed = client.claim_tasks(queue)[0]

    if terminal_state == "completed":
        client.complete_run(queue, claimed["run_id"], {"ok": True})
    elif terminal_state == "failed":
        client.fail_run(queue, claimed["run_id"], {"message": "boom"})
    else:
        client.cancel_task(queue, spawned.task_id)

    run_id, previous_state = _wake_task(client, queue, spawned.task_id)

    assert run_id == claimed["run_id"]
    assert previous_state == terminal_state
    assert client.get_task(queue, spawned.task_id)["state"] == terminal_state
    assert client.get_runs(queue, spawned.task_id)[-1]["state"] == terminal_state


@pytest.mark.parametrize("storage_mode", ["unpartitioned", "partitioned"])
def test_wake_task_supports_both_queue_storage_modes(client, storage_mode):
    queue = f"wake_storage_{storage_mode}"
    client.create_queue(queue, storage_mode=storage_mode)
    spawned = client.spawn_task(queue, "waiting-task", {"value": 1})
    claimed = client.claim_tasks(queue)[0]
    client.await_event(
        queue,
        spawned.task_id,
        claimed["run_id"],
        "wait-step",
        "some-event",
    )

    run_id, previous_state = _wake_task(client, queue, spawned.task_id)

    assert run_id == claimed["run_id"]
    assert previous_state == "sleeping"
    assert client.claim_tasks(queue, worker="replay-worker")[0]["run_id"] == run_id


def test_wake_task_preserves_payload_already_delivered_to_pending_run(client):
    queue = "wake_preserve_payload"
    event_name = "some-event"
    client.create_queue(queue)
    spawned = client.spawn_task(queue, "waiting-task", {"value": 1})
    claimed = client.claim_tasks(queue)[0]
    client.await_event(
        queue,
        spawned.task_id,
        claimed["run_id"],
        "wait-step",
        event_name,
    )
    client.emit_event(queue, event_name, {"value": 42})
    before = client.get_run(queue, claimed["run_id"])
    assert before["state"] == "pending"
    assert before["event_payload"] == {"value": 42}

    _wake_task(client, queue, spawned.task_id)

    after = client.get_run(queue, claimed["run_id"])
    assert after["state"] == "pending"
    assert after["event_payload"] == {"value": 42}


def test_event_emission_racing_wake_cannot_leave_task_asleep(db_dsn):
    queue = "wake_event_race"
    event_name = "wake-event"
    results = {}
    threads = []
    lock_conn = None

    try:
        with psycopg.connect(db_dsn, autocommit=True) as setup:
            setup.execute("select absurd.create_queue(%s)", (queue,))
            task_id, run_id = setup.execute(
                """
                select task_id, run_id
                  from absurd.spawn_task(%s, %s, %s, %s)
                """,
                (queue, "waiting-task", Jsonb({"value": 1}), Jsonb({})),
            ).fetchone()
            setup.execute(
                "select run_id from absurd.claim_task(%s, %s, %s, %s)",
                (queue, "worker", 60, 1),
            ).fetchone()
            suspended = setup.execute(
                """
                select should_suspend
                  from absurd.await_event(%s, %s, %s, %s, %s)
                """,
                (queue, task_id, run_id, "wait-step", event_name),
            ).fetchone()[0]
            assert suspended is True

        lock_conn = psycopg.connect(db_dsn)
        lock_conn.execute(
            sql.SQL("select 1 from absurd.{runs} where run_id = %s for update").format(
                runs=sql.Identifier(f"r_{queue}")
            ),
            (run_id,),
        )

        def run_operation(name, statement, params):
            try:
                with psycopg.connect(db_dsn, autocommit=True) as conn:
                    _set_application_name(conn, name)
                    conn.execute("set statement_timeout = '5s'")
                    conn.execute(statement, params)
            except psycopg.Error as exc:  # pragma: no cover - surfaced below
                results[name] = exc

        operations = [
            (
                "absurd-wake-event-race",
                "select absurd.wake_task(%s, %s)",
                (queue, task_id),
            ),
            (
                "absurd-emit-wake-race",
                "select absurd.emit_event(%s, %s, %s)",
                (queue, event_name, Jsonb({"value": 42})),
            ),
        ]
        for operation in operations:
            thread = threading.Thread(target=run_operation, args=operation, daemon=True)
            thread.start()
            threads.append(thread)

        for application_name, _, _ in operations:
            _wait_for_lock(db_dsn, application_name)

        lock_conn.commit()
        lock_conn.close()
        lock_conn = None

        for thread in threads:
            thread.join(timeout=5)
            assert not thread.is_alive()
        if results:
            raise next(iter(results.values()))

        with psycopg.connect(db_dsn, autocommit=True) as check:
            task_state = check.execute(
                "select state from absurd.get_task_result(%s, %s)",
                (queue, task_id),
            ).fetchone()[0]
            replay_run_id = check.execute(
                "select run_id from absurd.claim_task(%s, %s, %s, %s)",
                (queue, "replay-worker", 60, 1),
            ).fetchone()[0]
            wait_count = check.execute(
                sql.SQL(
                    "select count(*) from absurd.{waits} where task_id = %s"
                ).format(waits=sql.Identifier(f"w_{queue}")),
                (task_id,),
            ).fetchone()[0]

        assert task_state == "pending"
        assert replay_run_id == run_id
        assert wait_count == 0
    finally:
        if lock_conn is not None:
            lock_conn.rollback()
            lock_conn.close()
        for thread in threads:
            thread.join(timeout=1)
        with psycopg.connect(db_dsn, autocommit=True) as cleanup:
            cleanup.execute("select absurd.drop_queue(%s)", (queue,))


@pytest.mark.parametrize(
    ("operation", "terminal_state"),
    [("complete", "completed"), ("cancel", "cancelled")],
)
def test_terminal_transition_winning_race_makes_wake_a_no_op(
    db_dsn, operation, terminal_state
):
    queue = f"wake_{operation}_wins"
    task_id, run_id = _create_running_task(db_dsn, queue)
    wake_done = threading.Event()
    wake_error = []
    lock_conn = None
    thread = None

    try:
        lock_conn = psycopg.connect(db_dsn)
        lock_conn.execute(
            sql.SQL("select 1 from absurd.{runs} where run_id = %s for update").format(
                runs=sql.Identifier(f"r_{queue}")
            ),
            (run_id,),
        )

        def wake_worker():
            try:
                with psycopg.connect(db_dsn, autocommit=True) as conn:
                    _set_application_name(conn, f"absurd-wake-{operation}-wins")
                    conn.execute("set statement_timeout = '5s'")
                    conn.execute("select absurd.wake_task(%s, %s)", (queue, task_id))
            except psycopg.Error as exc:  # pragma: no cover - surfaced below
                wake_error.append(exc)
            finally:
                wake_done.set()

        thread = threading.Thread(target=wake_worker, daemon=True)
        thread.start()
        _wait_for_lock(db_dsn, f"absurd-wake-{operation}-wins")

        _terminal_transition(lock_conn, operation, queue, task_id, run_id)
        lock_conn.commit()
        lock_conn.close()
        lock_conn = None

        assert wake_done.wait(5)
        thread.join(timeout=1)
        if wake_error:
            raise wake_error[0]

        with psycopg.connect(db_dsn, autocommit=True) as check:
            state = check.execute(
                "select state from absurd.get_task_result(%s, %s)",
                (queue, task_id),
            ).fetchone()[0]
        assert state == terminal_state
    finally:
        if lock_conn is not None:
            lock_conn.rollback()
            lock_conn.close()
        if thread is not None:
            thread.join(timeout=5)
        with psycopg.connect(db_dsn, autocommit=True) as cleanup:
            cleanup.execute("select absurd.drop_queue(%s)", (queue,))


@pytest.mark.parametrize(
    ("operation", "terminal_state"),
    [("complete", "completed"), ("cancel", "cancelled")],
)
def test_wake_winning_race_allows_terminal_transition_to_finish(
    db_dsn, operation, terminal_state
):
    queue = f"wake_wins_{operation}"
    task_id, run_id = _create_running_task(db_dsn, queue)
    terminal_done = threading.Event()
    terminal_error = []
    lock_conn = None
    thread = None

    try:
        lock_conn = psycopg.connect(db_dsn)
        lock_conn.execute(
            sql.SQL("select 1 from absurd.{runs} where run_id = %s for update").format(
                runs=sql.Identifier(f"r_{queue}")
            ),
            (run_id,),
        )

        def terminal_worker():
            try:
                with psycopg.connect(db_dsn, autocommit=True) as conn:
                    _set_application_name(conn, f"absurd-{operation}-wake-wins")
                    conn.execute("set statement_timeout = '5s'")
                    _terminal_transition(conn, operation, queue, task_id, run_id)
            except psycopg.Error as exc:  # pragma: no cover - surfaced below
                terminal_error.append(exc)
            finally:
                terminal_done.set()

        thread = threading.Thread(target=terminal_worker, daemon=True)
        thread.start()
        _wait_for_lock(db_dsn, f"absurd-{operation}-wake-wins")

        wake_row = lock_conn.execute(
            "select run_id, previous_state from absurd.wake_task(%s, %s)",
            (queue, task_id),
        ).fetchone()
        assert wake_row == (run_id, "running")
        lock_conn.commit()
        lock_conn.close()
        lock_conn = None

        assert terminal_done.wait(5)
        thread.join(timeout=1)
        if terminal_error:
            raise terminal_error[0]

        with psycopg.connect(db_dsn, autocommit=True) as check:
            state = check.execute(
                "select state from absurd.get_task_result(%s, %s)",
                (queue, task_id),
            ).fetchone()[0]
        assert state == terminal_state
    finally:
        if lock_conn is not None:
            lock_conn.rollback()
            lock_conn.close()
        if thread is not None:
            thread.join(timeout=5)
        with psycopg.connect(db_dsn, autocommit=True) as cleanup:
            cleanup.execute("select absurd.drop_queue(%s)", (queue,))
