"""Optional Arrow IPC file transport for ``pypeln.process`` pipelines.

By default :mod:`pypeln.process` serializes every value that crosses a stage
boundary with ``pickle``.  For large :class:`pyarrow.RecordBatch` objects that
means the whole batch body travels through the queue for every stage (and for
every consumer of a fan-out).

This module implements an *optional* alternative: the batch body is written to
a temporary `Arrow IPC file
<https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format>`_ that is
private to the current execution, and only a small :class:`BatchDescriptor`
travels through the queue.  Queues decode descriptors back into real
``RecordBatch`` objects before handing them to worker callbacks
(``map``/``filter``/``flat_map``/``each``) or to the terminal iterable, so user
code never sees the difference.

The feature is opt-in and can be enabled in several equivalent ways::

    import pypeln as pl

    # per stage
    stage = pl.process.map(f, batches, use_ipc=True)

    # per call / per execution
    list(pl.process.to_iterable(stage, use_ipc=True))

    # an explicit transport (closed by the caller, "explicit close")
    transport = pl.process.ArrowIPCTransport()
    stage = pl.process.map(f, batches, ipc=transport)
    ...
    transport.close()

    # ambient configuration for a block of code
    with pl.process.arrow_ipc() as transport:
        stage = pl.process.map(f, batches)
        list(stage)

    # process-wide
    pl.process.enable_ipc()
    ...
    pl.process.close_ipc()

The environment variables ``PYPELN_IPC`` / ``PYPELN_ARROW_IPC`` (truthy) and
``PYPELN_IPC_DIR`` / ``PYPELN_ARROW_IPC_DIR`` can also be used to enable the
transport and choose the directory.

Only ``pyarrow.RecordBatch`` objects (bare or wrapped in an
:class:`pypeln.utils.Element`) are affected; any other value keeps the regular
pickle path, and calls that do not enable the feature are completely unchanged.

Safety rules implemented here:

* a batch file is fully written, flushed and moved into place *before* its
  descriptor is handed to a queue;
* all consumers of one fan-out share a single file, and files are only removed
  when the owning execution finishes (normal completion, exception, generator
  close) or when its transport is explicitly closed -- never while another
  consumer may still need them;
* decoded batches are buffered in memory, so batches already returned to user
  code stay readable after the temporary files are removed.

``pyarrow`` is an optional dependency: importing :mod:`pypeln` and running
pipelines without this feature never imports it.
"""

import contextlib
import dataclasses
import os
import shutil
import tempfile
import threading
import typing as tp
import uuid

from pypeln import utils as pypeln_utils

__all__ = [
    "ArrowIPCError",
    "IPCDependencyError",
    "BatchDescriptor",
    "ArrowIPCStore",
    "IPCStore",
    "LocalArrowIPCStore",
    "FileSystemArrowIPCStore",
    "ArrowIPCTransport",
    "IPCTransport",
    "ArrowIPCExecution",
    "arrow_ipc",
    "enable_ipc",
    "disable_ipc",
    "close_ipc",
    "current_ipc_transport",
    "is_available",
]


class ArrowIPCError(RuntimeError):
    """Raised when the Arrow IPC transport cannot do its job."""


class IPCDependencyError(ArrowIPCError, ImportError):
    """Raised when the Arrow IPC transport is requested without ``pyarrow``."""


_PA = None


def _get_pa():
    """Import :mod:`pyarrow` lazily and cache it.

    The import is deliberately deferred: the default (no IPC) code path must
    keep working in environments where pyarrow is not installed.
    """

    global _PA

    if _PA is None:
        try:
            import pyarrow as pa  # noqa: F401
            import pyarrow.fs  # noqa: F401  (not imported by `import pyarrow`)
            import pyarrow.ipc  # noqa: F401
        except ImportError as e:  # pragma: no cover - exercised via monkeypatch
            raise IPCDependencyError(
                "The Arrow IPC transport requires pyarrow. Install it with "
                "`pip install pyarrow` (or add the optional `ipc` extra)."
            ) from e

        _PA = pa

    return _PA


def is_available() -> bool:
    """Return ``True`` if ``pyarrow`` can be imported."""

    try:
        _get_pa()
        return True
    except IPCDependencyError:
        return False


# ----------------------------------------------------------------
# locators / descriptor
# ----------------------------------------------------------------


def _join(base: str, key: str) -> str:
    if not base:
        return key.lstrip("/")
    return base.rstrip("/") + "/" + key.lstrip("/")


def _read_bytes(locator: tp.Dict[str, tp.Any]) -> bytes:
    kind = locator.get("kind")

    if kind == "local":
        with open(_join(locator["root"], locator["key"]), "rb") as f:
            return f.read()

    if kind == "fs":
        pa = _get_pa()
        fs, base = pa.fs.FileSystem.from_uri(locator["uri"])
        stream = fs.open_input_stream(_join(base, locator["key"]))
        try:
            return stream.readall()
        finally:
            try:
                stream.close()
            except Exception:  # pragma: no cover - best effort
                pass

    raise ArrowIPCError(f"Unknown Arrow IPC locator: {locator!r}")


def _read_batch(locator: tp.Dict[str, tp.Any]):
    pa = _get_pa()

    try:
        data = _read_bytes(locator)
    except ArrowIPCError:
        raise
    except Exception as e:
        raise ArrowIPCError(f"Could not read Arrow IPC batch at {locator!r}") from e

    try:
        reader = pa.ipc.open_file(pa.BufferReader(data))
        return reader.get_batch(0)
    except Exception as e:
        raise ArrowIPCError(f"Could not decode Arrow IPC batch at {locator!r}") from e


@dataclasses.dataclass(frozen=True)
class BatchDescriptor:
    """Tiny picklable stand-in for a :class:`pyarrow.RecordBatch`.

    Instances travel through the queues instead of the batch body.  They are
    self-describing, so any process that receives one can turn it back into a
    real ``RecordBatch`` through :meth:`read`, even after the producing
    process (or the temporary file) is gone.
    """

    locator: tp.Dict[str, tp.Any]
    num_rows: int = 0
    num_columns: int = 0
    nbytes: int = 0

    def read(self):
        """Return the real ``RecordBatch`` (fully buffered in memory)."""

        return _read_batch(self.locator)


def _is_batch(value) -> bool:
    pa = _get_pa()
    return isinstance(value, pa.RecordBatch)


# ----------------------------------------------------------------
# stores
# ----------------------------------------------------------------


class ArrowIPCStore:
    """Backend interface used to persist batch files.

    Implementations are responsible for creating the per-execution prefix,
    writing one finalized IPC file per batch, reading it back and deleting an
    execution's prefix.  The built-in implementations cover the local
    filesystem and any backend understood by ``pyarrow.fs.FileSystem``
    (``s3://``, ``gs://``, ``hdfs://`` ...).
    """

    def create_prefix(self, prefix: str) -> None:
        raise NotImplementedError

    def write(self, prefix: str, batch) -> tp.Dict[str, tp.Any]:
        raise NotImplementedError

    def read(self, locator: tp.Dict[str, tp.Any]):
        return _read_batch(locator)

    def delete_prefix(self, prefix: str) -> None:
        raise NotImplementedError

    def close(self) -> None:
        pass


class LocalArrowIPCStore(ArrowIPCStore):
    """Store batch files under a local directory."""

    def __init__(self, root: str):
        self.root = os.path.abspath(os.fspath(root))

    def _full(self, key: str) -> str:
        return os.path.join(self.root, key)

    def create_prefix(self, prefix: str) -> None:
        os.makedirs(self._full(prefix), exist_ok=True)

    def write(self, prefix: str, batch) -> tp.Dict[str, tp.Any]:
        pa = _get_pa()

        key = _join(prefix, "batch-%s.arrow" % uuid.uuid4().hex)
        full = self._full(key)
        tmp = full + ".partial"

        try:
            with open(tmp, "wb") as sink:
                with pa.ipc.new_file(sink, batch.schema) as writer:
                    writer.write_batch(batch)

            os.replace(tmp, full)
        except BaseException:
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise

        return {"kind": "local", "root": self.root, "key": key}

    def delete_prefix(self, prefix: str) -> None:
        shutil.rmtree(self._full(prefix), ignore_errors=True)


class FileSystemArrowIPCStore(ArrowIPCStore):
    """Store batch files through ``pyarrow.fs`` (local or object storage).

    ``uri`` may be anything understood by ``pyarrow.fs.FileSystem.from_uri``,
    for example ``"file:///tmp/pypeln"`` or ``"s3://bucket/prefix"``.
    """

    def __init__(self, uri: str):
        pa = _get_pa()
        self.uri = os.fspath(uri)
        self._fs, self._base = pa.fs.FileSystem.from_uri(self.uri)

    # The pickled execution carries its store to the workers; rebuild the
    # filesystem from the URI there so we never depend on a particular
    # `pyarrow.fs` implementation being picklable.
    def __getstate__(self):
        return {"uri": self.uri}

    def __setstate__(self, state):
        pa = _get_pa()
        self.uri = state["uri"]
        self._fs, self._base = pa.fs.FileSystem.from_uri(self.uri)

    def _full(self, key: str) -> str:
        return _join(self._base, key)

    def create_prefix(self, prefix: str) -> None:
        self._fs.create_dir(self._full(prefix), recursive=True)

    def write(self, prefix: str, batch) -> tp.Dict[str, tp.Any]:
        pa = _get_pa()

        key = _join(prefix, "batch-%s.arrow" % uuid.uuid4().hex)
        full = self._full(key)
        tmp = full + ".partial"

        try:
            sink = self._fs.open_output_stream(tmp)
            try:
                with pa.ipc.new_file(sink, batch.schema) as writer:
                    writer.write_batch(batch)
            finally:
                try:
                    sink.close()
                except Exception:  # pragma: no cover - best effort
                    pass

            self._fs.move(tmp, full)
        except BaseException:
            try:
                self._fs.delete_file(tmp)
            except Exception:
                pass
            raise

        return {"kind": "fs", "uri": self.uri, "key": key}

    def delete_prefix(self, prefix: str) -> None:
        try:
            self._fs.delete_dir(self._full(prefix))
        except Exception:  # pragma: no cover - best effort
            pass


# ----------------------------------------------------------------
# execution scope
# ----------------------------------------------------------------


class ArrowIPCExecution:
    """Per-execution view of a store.

    One of these is created by :meth:`ArrowIPCTransport.new_execution` before
    any worker starts and is shipped to every queue of that execution.  It
    owns a private ``prefix`` (a directory) inside the store.  Workers write
    their batch files under that prefix; the process that created the
    execution deletes the whole prefix when the execution finishes.

    Instances are picklable and intentionally do **not** hold a reference to
    the owning transport (which contains locks and bookkeeping).
    """

    def __init__(
        self,
        store: ArrowIPCStore,
        prefix: str,
        cleanup: bool = True,
        owner_pid: tp.Optional[int] = None,
    ):
        self.store = store
        self.prefix = prefix
        self.cleanup = bool(cleanup)
        self.owner_pid = os.getpid() if owner_pid is None else owner_pid
        self._finished = False

    def write(self, batch) -> BatchDescriptor:
        locator = self.store.write(self.prefix, batch)
        return BatchDescriptor(
            locator=locator,
            num_rows=batch.num_rows,
            num_columns=batch.num_columns,
            nbytes=batch.nbytes,
        )

    def encode(self, value):
        """Write batches and return descriptors; leave everything else alone."""

        if isinstance(value, BatchDescriptor):
            return value

        if isinstance(value, pypeln_utils.Element):
            inner = value.value
            if isinstance(inner, BatchDescriptor):
                return value
            if _is_batch(inner):
                return value.set(self.write(inner))
            return value

        if _is_batch(value):
            return self.write(value)

        return value

    def decode(self, value):
        """Turn a descriptor (or ``Element`` of a descriptor) back into a batch."""

        if isinstance(value, BatchDescriptor):
            return value.read()

        if isinstance(value, pypeln_utils.Element) and isinstance(
            value.value, BatchDescriptor
        ):
            return value.set(value.value.read())

        return value

    def finish(self) -> None:
        """Delete this execution's files (only in the owning process)."""

        if self._finished:
            return

        self._finished = True

        if not self.cleanup:
            return

        if os.getpid() != self.owner_pid:
            # Pickled copies living in workers must never clean up early.
            return

        try:
            self.store.delete_prefix(self.prefix)
        except Exception:  # pragma: no cover - best effort
            pass

    def __repr__(self) -> str:
        return f"ArrowIPCExecution(prefix={self.prefix!r}, cleanup={self.cleanup})"


# ----------------------------------------------------------------
# transport
# ----------------------------------------------------------------


class ArrowIPCTransport:
    """Factory of per-execution Arrow IPC scopes.

    Arguments:
        root: Directory used to host execution directories. When omitted a
            private temporary directory is created and removed when the
            transport is closed.
        store: An explicit :class:`ArrowIPCStore` (for example
            :class:`FileSystemArrowIPCStore` for object storage) instead of
            ``root``.
        cleanup: When ``True`` (default) each execution's files are removed
            when it finishes and the transport's own root is removed on close.
    """

    def __init__(
        self,
        root: tp.Optional[str] = None,
        store: tp.Optional[ArrowIPCStore] = None,
        cleanup: bool = True,
    ):
        _get_pa()  # fail fast when pyarrow is missing

        if root is not None and store is not None:
            raise ValueError("Pass either `root` or `store`, not both")

        self._explicit_store = store
        self._root = os.fspath(root) if root is not None else None
        self.cleanup = bool(cleanup)

        self._lock = threading.RLock()
        self._executions = set()
        self._closed = False

        self._owns_run_dir = store is None and self._root is None
        self._run_dir = None
        self._store = None

        self._transient = False

    # -- properties --------------------------------------------------

    @property
    def run_dir(self) -> tp.Optional[str]:
        return self._run_dir

    @property
    def store(self) -> ArrowIPCStore:
        with self._lock:
            return self._ensure_store()

    @property
    def closed(self) -> bool:
        return self._closed

    # -- internals ---------------------------------------------------

    def _ensure_store(self) -> ArrowIPCStore:
        if self._store is not None:
            return self._store

        if self._explicit_store is not None:
            self._store = self._explicit_store
            return self._store

        root = self._root
        if root is None:
            root = tempfile.mkdtemp(prefix="pypeln-ipc-")
            self._owns_run_dir = True
        else:
            os.makedirs(root, exist_ok=True)
            self._owns_run_dir = False

        self._run_dir = root
        self._store = LocalArrowIPCStore(root)
        return self._store

    def _delete_run_dir(self) -> None:
        if self._explicit_store is not None:
            return

        root = self._run_dir
        if root is None:
            return

        if self.cleanup and self._owns_run_dir:
            shutil.rmtree(root, ignore_errors=True)

        self._run_dir = None
        self._store = None

    # -- lifecycle ---------------------------------------------------

    def new_execution(self) -> ArrowIPCExecution:
        """Create an execution scope, making its private directory first."""

        with self._lock:
            if self._closed:
                raise ArrowIPCError("Arrow IPC transport is already closed")

            store = self._ensure_store()
            prefix = "exec-%s" % uuid.uuid4().hex
            store.create_prefix(prefix)

            execution = ArrowIPCExecution(
                store=store,
                prefix=prefix,
                cleanup=self.cleanup,
            )
            self._executions.add(execution)
            return execution

    def finish_execution(self, execution: ArrowIPCExecution) -> None:
        """Finish an execution and delete its files when appropriate."""

        execution.finish()

        with self._lock:
            self._executions.discard(execution)
            delete_now = self._closed and not self._executions

        if delete_now:
            self._delete_run_dir()

    def close(self) -> None:
        """Explicitly close the transport.

        New executions are refused.  Files of executions that are still live
        are kept until they finish (so they are never yanked from a consumer
        that still needs them); the transport's own root is deleted once the
        last execution ends.
        """

        with self._lock:
            self._closed = True
            pending = bool(self._executions)

        if not pending:
            self._delete_run_dir()

    def __enter__(self) -> "ArrowIPCTransport":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"ArrowIPCTransport(run_dir={self.run_dir!r}, "
            f"cleanup={self.cleanup}, closed={self._closed})"
        )


# Public aliases: make the API easy to discover.
IPCStore = ArrowIPCStore
IPCTransport = ArrowIPCTransport


# ----------------------------------------------------------------
# ambient configuration
# ----------------------------------------------------------------


_AMBIENT: tp.List[ArrowIPCTransport] = []
_GLOBAL: tp.Optional[ArrowIPCTransport] = None

_ENV_LOCK = threading.RLock()
_ENV_TRANSPORT: tp.Optional[ArrowIPCTransport] = None
_ENV_SIGNATURE = None

_TRUTHY = {"1", "true", "yes", "on", "y", "t"}


def _env_value(*names: str) -> tp.Optional[str]:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def _env_enabled() -> bool:
    value = _env_value("PYPELN_IPC", "PYPELN_ARROW_IPC")
    return value is not None and value.strip().lower() in _TRUTHY


def _env_dir() -> tp.Optional[str]:
    return _env_value("PYPELN_IPC_DIR", "PYPELN_ARROW_IPC_DIR")


def _env_transport() -> tp.Optional[ArrowIPCTransport]:
    global _ENV_TRANSPORT, _ENV_SIGNATURE

    if not _env_enabled():
        return None

    root = _env_dir()

    with _ENV_LOCK:
        if _ENV_SIGNATURE != root:
            if _ENV_TRANSPORT is not None:
                _ENV_TRANSPORT.close()
            _ENV_TRANSPORT = ArrowIPCTransport(root=root)
            _ENV_SIGNATURE = root

        return _ENV_TRANSPORT


def current_ipc_transport() -> tp.Optional[ArrowIPCTransport]:
    """Return the transport configured by context, global state or env."""

    if _AMBIENT:
        return _AMBIENT[-1]

    if _GLOBAL is not None:
        return _GLOBAL

    return _env_transport()


@contextlib.contextmanager
def arrow_ipc(
    root: tp.Optional[str] = None,
    store: tp.Optional[ArrowIPCStore] = None,
    cleanup: bool = True,
):
    """Enable the Arrow IPC transport for the duration of a ``with`` block."""

    transport = ArrowIPCTransport(root=root, store=store, cleanup=cleanup)
    _AMBIENT.append(transport)
    try:
        yield transport
    finally:
        _AMBIENT.pop()
        transport.close()


def enable_ipc(
    root: tp.Optional[str] = None,
    store: tp.Optional[ArrowIPCStore] = None,
    cleanup: bool = True,
) -> ArrowIPCTransport:
    """Enable the Arrow IPC transport process-wide."""

    global _GLOBAL

    if _GLOBAL is not None:
        _GLOBAL.close()

    _GLOBAL = ArrowIPCTransport(root=root, store=store, cleanup=cleanup)
    return _GLOBAL


def disable_ipc() -> None:
    """Stop using the process-wide transport (closing it)."""

    global _GLOBAL

    transport = _GLOBAL
    _GLOBAL = None

    if transport is not None:
        transport.close()


def close_ipc() -> None:
    """Disable and close all ambient Arrow IPC transports."""

    global _ENV_TRANSPORT, _ENV_SIGNATURE

    disable_ipc()

    with _ENV_LOCK:
        transport = _ENV_TRANSPORT
        _ENV_TRANSPORT = None
        _ENV_SIGNATURE = None

    if transport is not None:
        transport.close()


# ----------------------------------------------------------------
# configuration resolution
# ----------------------------------------------------------------


def setting_from_args(use_ipc: bool, ipc) -> tp.Any:
    """Normalize ``use_ipc`` / ``ipc`` API arguments into a stage setting."""

    if ipc is not None:
        return ipc

    if use_ipc:
        return True

    return pypeln_utils.UNDEFINED


def find_setting(stage) -> tp.Any:
    """Find the nearest ``ipc`` setting in a stage dependency graph."""

    seen = set()
    queue = [stage]

    while queue:
        current = queue.pop(0)

        if id(current) in seen:
            continue
        seen.add(id(current))

        setting = getattr(current, "ipc", pypeln_utils.UNDEFINED)
        if setting is not pypeln_utils.UNDEFINED and setting is not None:
            return setting

        for dependency in getattr(current, "dependencies", None) or []:
            queue.append(dependency)

    return pypeln_utils.UNDEFINED


def resolve_transport(setting) -> tp.Tuple[tp.Optional[ArrowIPCTransport], bool]:
    """Resolve a stage setting into ``(transport, owns_transport)``."""

    if setting is pypeln_utils.UNDEFINED or setting is None or setting is False:
        return None, False

    if setting is True:
        transport = current_ipc_transport()
        if transport is not None:
            return transport, False

        transport = ArrowIPCTransport(cleanup=True)
        transport._transient = True
        return transport, True

    if isinstance(setting, ArrowIPCTransport):
        return setting, False

    if isinstance(setting, ArrowIPCStore):
        # Wrapping an explicit store creates a transport owned by this call, so
        # it is closed when the execution ends (the store itself is untouched).
        return ArrowIPCTransport(store=setting), True

    raise TypeError(
        "Invalid `ipc` setting: expected a bool, ArrowIPCTransport or "
        f"ArrowIPCStore, got {type(setting)!r}"
    )


def resolve_stage_transport(
    stage,
    use_ipc: tp.Optional[bool] = None,
    ipc=None,
) -> tp.Tuple[tp.Optional[ArrowIPCTransport], bool]:
    """Resolve configuration for ``Stage.to_iterable``.

    Precedence: explicit ``ipc`` argument, explicit ``use_ipc`` argument, the
    setting on the stage (or its nearest dependency), then ambient
    context/global/environment configuration.
    """

    if ipc is not None:
        return resolve_transport(ipc)

    if use_ipc is True:
        return resolve_transport(True)

    if use_ipc is False:
        return None, False

    setting = find_setting(stage)

    if setting is pypeln_utils.UNDEFINED or setting is None:
        # Nothing configured on the stage: fall back to ambient/global/env.
        return current_ipc_transport(), False

    return resolve_transport(setting)
