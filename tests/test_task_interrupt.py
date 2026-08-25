import threading
import time

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

INTERRUPT_CHECKPOINT = "$absurd:interrupt"


def _request_interrupt(client, queue, task_id):
    client.conn.execute(
        "select absurd.request_task_interrupt(%s, %s)",
        (queue, task_id),
    )


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


def test_interrupting_event_wait_removes_obsolete_wait_registration(client):
    queue = "interrupt_wait_cleanup"
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

    _request_interrupt(client, queue, spawned.task_id)

    wait_count = client.conn.execute(
        sql.SQL("select count(*) from absurd.{waits} where task_id = %s").format(
            waits=client.get_table("w", queue)
        ),
        (spawned.task_id,),
    ).fetchone()[0]
    assert wait_count == 0

    run = client.get_run(queue, claimed["run_id"])
    assert run is not None
    assert run["state"] == "pending"

    checkpoint = client.get_checkpoint(queue, spawned.task_id, INTERRUPT_CHECKPOINT)
    assert checkpoint is not None
    assert checkpoint["state"] is True


def test_repeated_interruption_creates_one_replayable_request(client):
    queue = "interrupt_repeated"
    client.create_queue(queue)

    spawned = client.spawn_task(queue, "interruptible-task", {"value": 1})
    claimed = client.claim_tasks(queue)[0]
    client.await_event(
        queue,
        spawned.task_id,
        claimed["run_id"],
        "wait-step",
        "some-event",
    )

    _request_interrupt(client, queue, spawned.task_id)
    _request_interrupt(client, queue, spawned.task_id)

    checkpoint = client.get_checkpoint(queue, spawned.task_id, INTERRUPT_CHECKPOINT)
    assert checkpoint is not None
    assert checkpoint["state"] is True

    replay = client.claim_tasks(queue, worker="replay-worker")
    assert [row["run_id"] for row in replay] == [claimed["run_id"]]
    assert client.claim_tasks(queue, worker="other-worker") == []


@pytest.mark.parametrize("terminal_state", ["completed", "failed", "cancelled"])
def test_interruption_is_a_no_op_for_terminal_tasks(client, terminal_state):
    queue = f"interrupt_terminal_{terminal_state}"
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

    _request_interrupt(client, queue, spawned.task_id)

    task = client.get_task(queue, spawned.task_id)
    assert task is not None
    assert task["state"] == terminal_state
    assert client.get_checkpoint(queue, spawned.task_id, INTERRUPT_CHECKPOINT) is None


def test_interruption_wakes_a_partitioned_queue_task(client):
    queue = "interrupt_partitioned"
    client.create_queue(queue, storage_mode="partitioned")

    spawned = client.spawn_task(queue, "partitioned-task", {"value": 1})
    claimed = client.claim_tasks(queue)[0]
    suspended = client.await_event(
        queue,
        spawned.task_id,
        claimed["run_id"],
        "wait-step",
        "partitioned-event",
    )
    assert suspended["should_suspend"] is True

    _request_interrupt(client, queue, spawned.task_id)

    checkpoint = client.get_checkpoint(queue, spawned.task_id, INTERRUPT_CHECKPOINT)
    assert checkpoint is not None
    assert checkpoint["state"] is True

    replay = client.claim_tasks(queue, worker="replay-worker")
    assert [row["run_id"] for row in replay] == [claimed["run_id"]]


def test_event_emission_racing_interruption_cannot_leave_the_task_asleep(db_dsn):
    queue = "interrupt_event_race"
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
            should_suspend = setup.execute(
                """
                select should_suspend
                  from absurd.await_event(%s, %s, %s, %s, %s)
                """,
                (queue, task_id, run_id, "wait-step", event_name),
            ).fetchone()[0]
            assert should_suspend is True

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
                "absurd-interrupt-event-race",
                "select absurd.request_task_interrupt(%s, %s)",
                (queue, task_id),
            ),
            (
                "absurd-emit-interrupt-race",
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
            interrupt_state = check.execute(
                """
                select state
                  from absurd.get_task_checkpoint_state(%s, %s, %s)
                """,
                (queue, task_id, INTERRUPT_CHECKPOINT),
            ).fetchone()[0]
            wait_count = check.execute(
                sql.SQL(
                    "select count(*) from absurd.{waits} where task_id = %s"
                ).format(waits=sql.Identifier(f"w_{queue}")),
                (task_id,),
            ).fetchone()[0]

        assert task_state == "pending"
        assert replay_run_id == run_id
        assert interrupt_state is True
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
def test_terminal_transition_winning_race_makes_interruption_a_no_op(
    db_dsn, operation, terminal_state
):
    queue = f"interrupt_{operation}_wins"
    task_id, run_id = _create_running_task(db_dsn, queue)
    interrupt_done = threading.Event()
    interrupt_error = []
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

        def interrupt_worker():
            try:
                with psycopg.connect(db_dsn, autocommit=True) as conn:
                    _set_application_name(conn, f"absurd-interrupt-{operation}-wins")
                    conn.execute("set statement_timeout = '5s'")
                    conn.execute(
                        "select absurd.request_task_interrupt(%s, %s)",
                        (queue, task_id),
                    )
            except psycopg.Error as exc:  # pragma: no cover - surfaced below
                interrupt_error.append(exc)
            finally:
                interrupt_done.set()

        thread = threading.Thread(target=interrupt_worker, daemon=True)
        thread.start()
        _wait_for_lock(db_dsn, f"absurd-interrupt-{operation}-wins")

        _terminal_transition(lock_conn, operation, queue, task_id, run_id)
        lock_conn.commit()
        lock_conn.close()
        lock_conn = None

        assert interrupt_done.wait(5)
        thread.join(timeout=1)
        if interrupt_error:
            raise interrupt_error[0]

        with psycopg.connect(db_dsn, autocommit=True) as check:
            state = check.execute(
                "select state from absurd.get_task_result(%s, %s)",
                (queue, task_id),
            ).fetchone()[0]
            checkpoint = check.execute(
                "select state from absurd.get_task_checkpoint_state(%s, %s, %s)",
                (queue, task_id, INTERRUPT_CHECKPOINT),
            ).fetchone()

        assert state == terminal_state
        assert checkpoint is None
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
def test_interruption_winning_race_is_preserved_by_terminal_transition(
    db_dsn, operation, terminal_state
):
    queue = f"interrupt_wins_{operation}"
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
                    _set_application_name(conn, f"absurd-{operation}-interrupt-wins")
                    conn.execute("set statement_timeout = '5s'")
                    _terminal_transition(conn, operation, queue, task_id, run_id)
            except psycopg.Error as exc:  # pragma: no cover - surfaced below
                terminal_error.append(exc)
            finally:
                terminal_done.set()

        thread = threading.Thread(target=terminal_worker, daemon=True)
        thread.start()
        _wait_for_lock(db_dsn, f"absurd-{operation}-interrupt-wins")

        lock_conn.execute(
            "select absurd.request_task_interrupt(%s, %s)",
            (queue, task_id),
        )
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
            checkpoint = check.execute(
                "select state from absurd.get_task_checkpoint_state(%s, %s, %s)",
                (queue, task_id, INTERRUPT_CHECKPOINT),
            ).fetchone()

        assert state == terminal_state
        assert checkpoint == (True,)
    finally:
        if lock_conn is not None:
            lock_conn.rollback()
            lock_conn.close()
        if thread is not None:
            thread.join(timeout=5)
        with psycopg.connect(db_dsn, autocommit=True) as cleanup:
            cleanup.execute("select absurd.drop_queue(%s)", (queue,))
