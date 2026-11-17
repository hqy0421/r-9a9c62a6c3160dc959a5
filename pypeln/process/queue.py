import multiprocessing
import sys
import traceback
import typing as tp
from multiprocessing.queues import Empty, Queue

from pypeln import utils as pypeln_utils

from . import utils
from .ipc import BatchDescriptor

T = tp.TypeVar("T")


PipelineException = pypeln_utils.PipelineException


class IterableQueue(Queue, tp.Generic[T], tp.Iterable[T]):
    def __init__(
        self,
        maxsize: int = 0,
        total_sources: int = 1,
        transport=None,
    ):
        super().__init__(maxsize=maxsize, ctx=multiprocessing.get_context())

        # Optional Arrow IPC execution scope (see `pypeln.process.ipc`). When
        # set, RecordBatch values are written to files and only descriptors
        # travel through the queue.
        self.transport = transport

        self.namespace = utils.Namespace(
            remaining=total_sources, exception=False, force_stop=False
        )
        self.exception_queue: Queue[PipelineException] = Queue(
            ctx=multiprocessing.get_context()
        )

    def put(self, x: T, block: bool = True, timeout: tp.Optional[float] = None):
        if self.transport is not None and not isinstance(x, pypeln_utils.Done):
            # Write the batch file first; only then publish the descriptor.
            x = self.transport.encode(x)

        return super().put(x, block=block, timeout=timeout)

    def get(self, block: bool = True, timeout: tp.Optional[float] = None) -> T:
        while True:
            if self.namespace.exception:
                exception, trace = self.exception_queue.get()

                try:
                    exception = exception(f"\n\n{trace}")
                except:
                    exception = Exception(f"\n\nOriginal: {exception}\n\n{trace}")

                raise exception

            x = super().get(block=block, timeout=timeout)

            if isinstance(x, pypeln_utils.Done):
                with self.namespace:
                    self.namespace.remaining -= 1

                continue

            if isinstance(x, BatchDescriptor) or (
                isinstance(x, pypeln_utils.Element)
                and isinstance(x.value, BatchDescriptor)
            ):
                # Turn the descriptor back into a real RecordBatch so worker
                # callbacks and terminal consumers only ever see batches.
                x = self._decode(x)

            return x

    @staticmethod
    def _decode(x):
        if isinstance(x, BatchDescriptor):
            return x.read()

        return x.set(x.value.read())

    def clear(self):
        # Use the raw multiprocessing getter so cleanup never reads batch
        # files (``Queue.get_nowait`` dispatches back to our ``get``).
        try:
            while True:
                super().get(block=False)
        except Empty:
            pass

    def __iter__(self) -> tp.Iterator[T]:
        while not self.is_done():
            try:
                x = self.get(timeout=pypeln_utils.TIMEOUT)
            except Empty:
                continue

            yield x

    def is_done(self):
        force_stop = self.namespace.force_stop
        remaining = self.namespace.remaining

        return force_stop or (remaining <= 0 and self.empty())

    def worker_done(self):
        # with self.namespace:
        #     self.namespace.remaining -= 1
        self.put(pypeln_utils.DONE)

    def stop(self):
        with self.namespace:
            self.namespace.remaining = 0
        self.clear()

    def kill(self):
        with self.namespace:
            self.namespace.remaining = 0
            self.namespace.force_stop = True
        self.clear()

    def raise_exception(self, exception: BaseException):
        pipeline_exception = self.get_pipeline_exception(exception)
        self.namespace.exception = True
        self.exception_queue.put(pipeline_exception)

    def get_pipeline_exception(self, exception: BaseException) -> PipelineException:
        if isinstance(exception, PipelineException):
            return exception

        exception_type, _exception, _traceback = sys.exc_info()
        trace = "".join(
            traceback.format_exception(exception_type, _exception, _traceback)
        )

        trace = f"{exception.args}\n\n{trace}"

        return PipelineException(exception_type, trace)

    def __getstate__(self):
        return super().__getstate__() + (
            self.namespace,
            self.exception_queue,
            self.transport,
        )

    def __setstate__(self, state):
        super().__setstate__(state[:-3])
        self.namespace, self.exception_queue, self.transport = state[-3:]


class OutputQueues(tp.List[IterableQueue[T]], tp.Generic[T]):
    def __init__(self, iterable=(), transport=None):
        super().__init__(iterable)
        self.transport = transport

    def put(self, x: T):
        if self.transport is not None and not isinstance(x, pypeln_utils.Done):
            # Encode once so every fan-out consumer shares a single file.
            x = self.transport.encode(x)

        for queue in self:
            queue.put(x)

    def worker_done(self):
        for queue in self:
            queue.worker_done()

    def stop(self):
        for queue in self:
            queue.stop()

    def kill(self):
        for queue in self:
            queue.kill()
