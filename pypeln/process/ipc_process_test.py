"""Tests for the optional Arrow IPC file transport of ``pypeln.process``."""

import gc
import os
import pickle

import pytest

import pypeln as pl

pa = pytest.importorskip("pyarrow")


SCHEMA = pa.schema(
    [
        pa.field("b", pa.string(), nullable=True, metadata={b"fm": b"fv"}),
        pa.field("a", pa.int64(), nullable=True),
    ],
    metadata={b"sm": b"sv"},
)


def make_batch(values, schema=SCHEMA):
    return pa.RecordBatch.from_arrays(
        [
            pa.array(
                [str(x) if x is not None else None for x in values], type=pa.string()
            ),
            pa.array(values, type=pa.int64()),
        ],
        schema=schema,
    )


def make_empty_batch(schema=SCHEMA):
    return pa.RecordBatch.from_arrays(
        [pa.array([], type=pa.string()), pa.array([], type=pa.int64())],
        schema=schema,
    )


def assert_batch_equal(x, y):
    assert isinstance(x, pa.RecordBatch)
    assert isinstance(y, pa.RecordBatch)
    assert x.schema.equals(y.schema, check_metadata=True)
    assert x.num_rows == y.num_rows
    assert x.to_pydict() == y.to_pydict()


def batches():
    return [
        make_batch([1, None, 3]),
        make_batch([4, 5, None]),
        make_empty_batch(),
        make_batch([6]),
    ]


def dir_contents(transport):
    if not transport.run_dir or not os.path.exists(transport.run_dir):
        return []
    return sorted(os.listdir(transport.run_dir))


# ----------------------------------------------------------------
# fidelity
# ----------------------------------------------------------------


def test_schema_values_nulls_and_empty_batches_survive():
    data = batches()
    result = list(
        pl.process.map(lambda b: b, data, workers=2, ipc=pl.process.ArrowIPCTransport())
    )

    assert len(result) == len(data)

    by_signature = {(b.num_rows, tuple(b.to_pydict()["a"])): b for b in result}
    for expected in data:
        matched = [
            b
            for b in result
            if b.num_rows == expected.num_rows
            and b.schema.equals(expected.schema, check_metadata=True)
            and b.to_pydict() == expected.to_pydict()
        ]
        assert matched, expected

    # Metadata and nullability are preserved exactly.
    for b in result:
        assert b.schema.metadata == {b"sm": b"sv"}
        assert b.schema.field("b").metadata == {b"fm": b"fv"}
        assert b.schema.field("b").nullable is True

    empty = [b for b in result if b.num_rows == 0]
    assert len(empty) == 1
    assert empty[0].num_columns == 2
    assert empty[0].to_pydict() == {"b": [], "a": []}


def test_zero_column_empty_batch():
    batch = pa.RecordBatch.from_arrays([], names=[])
    result = list(
        pl.process.map(lambda b: b, [batch], ipc=pl.process.ArrowIPCTransport())
    )
    assert len(result) == 1
    assert result[0].num_rows == 0
    assert result[0].num_columns == 0


def test_columns_and_order_are_preserved():
    schema = pa.schema([pa.field("z", pa.int32()), pa.field("a", pa.string())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2], type=pa.int32()), pa.array(["x", "y"])], schema=schema
    )
    [out] = list(pl.process.map(lambda b: b, [batch], ipc=True))
    assert out.schema.names == ["z", "a"]
    assert out.to_pydict() == {"z": [1, 2], "a": ["x", "y"]}


# ----------------------------------------------------------------
# callbacks and terminal consumers see real batches
# ----------------------------------------------------------------


def test_all_callbacks_receive_real_batches():
    data = batches()

    def identity(b):
        assert isinstance(b, pa.RecordBatch)
        return b

    result = list(pl.process.map(identity, data, ipc=pl.process.ArrowIPCTransport()))
    assert all(isinstance(b, pa.RecordBatch) for b in result)

    def predicate(b):
        assert isinstance(b, pa.RecordBatch)
        return b.num_rows > 0

    result = list(
        pl.process.filter(predicate, data, ipc=pl.process.ArrowIPCTransport())
    )
    assert len(result) == 3
    assert all(isinstance(b, pa.RecordBatch) for b in result)

    def expand(b):
        assert isinstance(b, pa.RecordBatch)
        yield b
        yield b

    result = list(pl.process.flat_map(expand, data, ipc=pl.process.ArrowIPCTransport()))
    assert len(result) == 2 * len(data)
    assert all(isinstance(b, pa.RecordBatch) for b in result)

    seen = []

    def sink(b):
        assert isinstance(b, pa.RecordBatch)
        seen.append(b.num_rows)

    pl.process.each(sink, data, ipc=pl.process.ArrowIPCTransport(), run=True)


def test_element_index_and_order_semantics():
    data = batches()
    stage = pl.process.map(
        lambda b: b, data, workers=2, ipc=pl.process.ArrowIPCTransport()
    )
    stage = pl.process.ordered(stage)

    result = list(pl.process.to_iterable(stage, return_index=True))

    assert [elem.index for elem in result] == [(i,) for i in range(len(data))]
    for elem, expected in zip(result, data):
        assert_batch_equal(elem.value, expected)


# ----------------------------------------------------------------
# transport mechanics
# ----------------------------------------------------------------


def test_only_small_descriptor_is_pickled():
    batch = make_batch(list(range(10000)))
    transport = pl.process.ArrowIPCTransport()
    execution = transport.new_execution()

    descriptor = execution.encode(batch)

    assert isinstance(descriptor, pl.process.BatchDescriptor)
    assert len(pickle.dumps(descriptor)) < len(pickle.dumps(batch))
    assert_batch_equal(descriptor.read(), batch)

    execution.finish()
    transport.close()


def test_element_is_wrapped_not_replaced():
    from pypeln.utils import Element

    batch = make_batch([1, 2])
    transport = pl.process.ArrowIPCTransport()
    execution = transport.new_execution()

    encoded = execution.encode(Element(index=(3,), value=batch))

    assert isinstance(encoded, Element)
    assert encoded.index == (3,)
    assert isinstance(encoded.value, pl.process.BatchDescriptor)
    assert_batch_equal(execution.decode(encoded).value, batch)

    execution.finish()
    transport.close()


def test_write_failure_publishes_nothing():
    class FailingStore(pl.process.ArrowIPCStore):
        def create_prefix(self, prefix):
            pass

        def write(self, prefix, batch):
            raise RuntimeError("disk full")

        def delete_prefix(self, prefix):
            pass

    transport = pl.process.ArrowIPCTransport(store=FailingStore())
    stage = pl.process.map(lambda b: b, [make_batch([1])], ipc=transport)

    with pytest.raises(RuntimeError):
        list(stage)

    transport.close()


def test_non_batch_values_use_normal_path():
    transport = pl.process.ArrowIPCTransport()

    result = list(pl.process.map(lambda x: x + 1, range(10), ipc=transport))
    assert result == list(range(1, 11))

    values = [{"a": 1}, "x", None, 3]
    result = list(pl.process.map(lambda x: x, values, ipc=transport))
    assert result == values

    transport.close()


# ----------------------------------------------------------------
# lifecycle / cleanup
# ----------------------------------------------------------------


def test_cleanup_after_normal_completion():
    transport = pl.process.ArrowIPCTransport()
    result = list(pl.process.map(lambda b: b, batches(), ipc=transport))

    # The batch files are gone but returned batches remain readable.
    assert dir_contents(transport) == []
    for b in result:
        assert isinstance(b, pa.RecordBatch)
        b.to_pydict()

    transport.close()


def test_cleanup_after_error():
    transport = pl.process.ArrowIPCTransport()

    def fail(b):
        raise ValueError("boom")

    with pytest.raises(ValueError):
        list(pl.process.map(fail, batches(), ipc=transport))

    gc.collect()
    assert dir_contents(transport) == []
    transport.close()


def test_cleanup_after_generator_close():
    transport = pl.process.ArrowIPCTransport()
    iterator = iter(
        pl.process.to_iterable(pl.process.map(lambda b: b, batches(), ipc=transport))
    )

    next(iterator)
    iterator.close()
    gc.collect()

    assert dir_contents(transport) == []
    transport.close()


def test_cleanup_after_explicit_close():
    transport = pl.process.ArrowIPCTransport()
    list(pl.process.map(lambda b: b, batches(), ipc=transport))
    run_dir = transport.run_dir

    transport.close()

    assert not os.path.exists(run_dir)


def test_explicit_close_during_execution_defers_deletion():
    transport = pl.process.ArrowIPCTransport()
    iterator = iter(
        pl.process.to_iterable(pl.process.map(lambda b: b, batches(), ipc=transport))
    )

    first = next(iterator)
    run_dir = transport.run_dir

    transport.close()
    assert os.path.exists(run_dir)  # must not yank data from a live consumer

    rest = list(iterator)
    gc.collect()

    assert not os.path.exists(run_dir)
    assert isinstance(first, pa.RecordBatch)


def test_batches_readable_after_files_deleted():
    transport = pl.process.ArrowIPCTransport()
    result = list(pl.process.map(lambda b: b, batches(), ipc=transport))
    run_dir = transport.run_dir

    transport.close()

    assert not os.path.exists(run_dir)
    assert [b.num_rows for b in result] == [3, 3, 0, 1]


def test_fan_out_shares_files_and_keeps_them_until_execution_end():
    transport = pl.process.ArrowIPCTransport()
    source = pl.process.map(lambda b: b, batches(), ipc=transport)

    first = list(pl.process.to_iterable(source))
    second = list(pl.process.to_iterable(source))

    assert len(first) == len(batches())
    assert len(second) == len(batches())
    assert [b.to_pydict() for b in first] == [b.to_pydict() for b in second]

    transport.close()


# ----------------------------------------------------------------
# configuration entry points
# ----------------------------------------------------------------


def test_use_ipc_flag_and_ambient_context():
    result = list(pl.process.map(lambda b: b, batches(), use_ipc=True))
    assert len(result) == len(batches())
    assert all(isinstance(b, pa.RecordBatch) for b in result)

    with pl.process.arrow_ipc() as transport:
        result = list(pl.process.map(lambda b: b, batches()))
        assert len(result) == len(batches())
        run_dir = transport.run_dir
        assert run_dir is not None

    assert not os.path.exists(run_dir)


def test_global_enable_disable(monkeypatch):
    pl.process.enable_ipc()
    try:
        result = list(pl.process.map(lambda b: b, batches()))
        assert len(result) == len(batches())
    finally:
        pl.process.disable_ipc()


def test_env_configuration(monkeypatch, tmp_path):
    monkeypatch.setenv("PYPELN_IPC", "1")
    monkeypatch.setenv("PYPELN_IPC_DIR", str(tmp_path))
    pl.process.close_ipc()

    result = list(pl.process.map(lambda b: b, batches()))

    assert len(result) == len(batches())
    assert all(isinstance(b, pa.RecordBatch) for b in result)

    pl.process.close_ipc()


def test_to_iterable_override(monkeypatch):
    monkeypatch.delenv("PYPELN_IPC", raising=False)
    monkeypatch.delenv("PYPELN_ARROW_IPC", raising=False)

    stage = pl.process.map(lambda b: b, batches())
    result = list(pl.process.to_iterable(stage, use_ipc=True))

    assert len(result) == len(batches())


def test_is_available():
    assert pl.process.is_available() is True


def test_explicit_local_store(tmp_path):
    store = pl.process.LocalArrowIPCStore(str(tmp_path))
    transport = pl.process.ArrowIPCTransport(store=store)

    result = list(pl.process.map(lambda b: b, batches(), ipc=transport))

    assert len(result) == len(batches())
    for b in result:
        assert isinstance(b, pa.RecordBatch)
        b.to_pydict()

    # Per-execution prefixes are cleaned up but the user's store root stays.
    assert os.path.isdir(tmp_path)
    assert os.listdir(tmp_path) == []

    transport.close()
    assert os.path.isdir(tmp_path)


def test_store_passed_directly_as_ipc_setting(tmp_path):
    store = pl.process.LocalArrowIPCStore(str(tmp_path))

    result = list(pl.process.map(lambda b: b, batches(), ipc=store))

    assert len(result) == len(batches())
    assert os.listdir(tmp_path) == []


def test_filesystem_store_roundtrip(tmp_path):
    store = pl.process.FileSystemArrowIPCStore(tmp_path.as_uri())
    transport = pl.process.ArrowIPCTransport(store=store)

    result = list(pl.process.map(lambda b: b, batches(), ipc=transport))

    assert len(result) == len(batches())
    assert [b.num_rows for b in result] == [3, 3, 0, 1]

    transport.close()


def test_closed_transport_refuses_new_executions():
    transport = pl.process.ArrowIPCTransport()
    transport.close()

    with pytest.raises(pl.process.ArrowIPCError):
        list(pl.process.map(lambda b: b, batches(), ipc=transport))


# ----------------------------------------------------------------
# interoperability with other pypeln modules
# ----------------------------------------------------------------


def test_thread_pipeline_after_process_ipc():
    result = pl.process.map(lambda b: b, batches(), ipc=pl.process.ArrowIPCTransport())
    result = pl.thread.ordered(pl.thread.map(lambda b: b, result))

    assert len(list(result)) == len(batches())
    assert all(
        isinstance(b, pa.RecordBatch)
        for b in list(pl.process.map(lambda b: b, batches(), ipc=True))
    )


def test_ipc_off_leaves_batch_intact():
    data = batches()
    result = list(pl.process.map(lambda b: b, data))

    assert len(result) == len(data)
    assert all(isinstance(b, pa.RecordBatch) for b in result)
    for x, y in zip(result, data):
        assert_batch_equal(x, y)
