import typing as tp
from dataclasses import dataclass

from pypeln import utils as pypeln_utils
from pypeln.utils import Kwargs, T

from . import ipc as ipc_module
from . import utils
from .queue import IterableQueue, OutputQueues
from .supervisor import Supervisor
from .worker import ProcessFn, StageParams, Worker


@dataclass
class Stage(pypeln_utils.BaseStage[T], tp.Iterable[T]):
    process_fn: ProcessFn
    workers: int
    maxsize: int
    total_sources: int
    timeout: float
    dependencies: tp.List["Stage"]
    on_start: tp.Optional[tp.Callable[..., Kwargs]]
    on_done: tp.Optional[tp.Callable[..., Kwargs]]
    use_threads: bool
    f_args: tp.List[str]
    # Optional Arrow IPC setting: `True`, an `ArrowIPCTransport`, an
    # `ArrowIPCStore` or `None`. `UNDEFINED` means "inherit".
    ipc: tp.Any = pypeln_utils.UNDEFINED

    def __hash__(self):
        return id(self)

    def build(
        self,
        built: tp.Dict["Stage", OutputQueues],
        output_queue: IterableQueue,
        main_queue: IterableQueue,
        transport=None,
    ) -> tp.Iterable[Worker]:
        if self in built:
            built[self].append(output_queue)
            return
        else:
            built[self] = OutputQueues([output_queue], transport=transport)

        input_queue = IterableQueue(
            maxsize=self.maxsize,
            total_sources=self.total_sources,
            transport=transport,
        )

        stage_params = StageParams.create(
            input_queue=input_queue,
            output_queues=built[self],
            total_workers=self.workers,
        )

        for i in range(self.workers):
            worker = Worker(
                process_fn=self.process_fn,
                index=i,
                timeout=self.timeout,
                stage_params=stage_params,
                main_queue=main_queue,
                on_start=self.on_start,
                on_done=self.on_done,
                use_threads=self.use_threads,
                f_args=self.f_args,
            )

            yield worker

        for dependency in self.dependencies:
            yield from dependency.build(
                built, input_queue, main_queue, transport=transport
            )

    def to_iterable(
        self,
        maxsize: int,
        return_index: bool,
        use_ipc: tp.Optional[bool] = None,
        ipc=None,
    ) -> tp.Iterable[T]:
        transport, owns_transport = ipc_module.resolve_stage_transport(
            self, use_ipc=use_ipc, ipc=ipc
        )

        execution = None

        try:
            execution = transport.new_execution() if transport is not None else None

            # build stages first to verify reuse
            main_queue: IterableQueue[pypeln_utils.Element] = IterableQueue(
                maxsize=maxsize,
                total_sources=self.workers,
                transport=execution,
            )

            built = {}

            workers: tp.List[Worker] = list(
                self.build(built, main_queue, main_queue, transport=execution)
            )
            supervisor = Supervisor(workers=workers, main_queue=main_queue)

            with supervisor:
                for elem in main_queue:
                    if return_index:
                        yield elem
                    else:
                        yield elem.value
        finally:
            # Cleanup runs on normal completion, error and generator close.
            if execution is not None:
                transport.finish_execution(execution)

            if owns_transport:
                transport.close()

    def __iter__(self):
        return self.to_iterable(maxsize=self.maxsize, return_index=False)
