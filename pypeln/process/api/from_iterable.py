import typing as tp
from copy import copy

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, T

from ..ipc import setting_from_args
from ..stage import Stage
from ..worker import ProcessFn, Worker


class FromIterable(tp.NamedTuple):
    iterable: tp.Iterable
    maxsize: int

    def __call__(self, worker: Worker, **kwargs):
        iterable = self.iterable

        if isinstance(iterable, pypeln_utils.BaseStage):
            for x in iterable.to_iterable(maxsize=self.maxsize, return_index=True):
                worker.stage_params.output_queues.put(x)
        else:
            for i, x in enumerate(iterable):
                if isinstance(x, pypeln_utils.Element):
                    worker.stage_params.output_queues.put(x)
                else:
                    worker.stage_params.output_queues.put(
                        pypeln_utils.Element(index=(i,), value=x)
                    )


@tp.overload
def from_iterable(
    iterable: tp.Iterable[T],
    use_thread: bool = True,
    maxsize: int = 0,
    use_ipc: bool = False,
    ipc=None,
) -> Stage[T]:
    ...


@tp.overload
def from_iterable(
    use_thread: bool = True,
    maxsize: int = 0,
    use_ipc: bool = False,
    ipc=None,
) -> pypeln_utils.Partial[Stage[T]]:
    ...


def from_iterable(
    iterable: tp.Union[tp.Iterable[T], pypeln_utils.Undefined] = pypeln_utils.UNDEFINED,
    use_thread: bool = True,
    maxsize: int = 0,
    use_ipc: bool = False,
    ipc=None,
) -> tp.Union[Stage[T], pypeln_utils.Partial[Stage[T]]]:
    """
    Creates a stage from an iterable.

    Arguments:
        iterable: A source Iterable.
        use_thread: If set to `True` (default) it will use a thread instead of a process to consume the iterable. Threads start faster and use thread memory to the iterable is not serialized, however, if the iterable is going to perform slow computations it better to use a process.
        use_ipc: If `True`, `RecordBatch` values of this pipeline travel through the queues as Arrow IPC files instead of pickled batch bodies. Requires `pyarrow`, see [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#arrow-ipc-file-transport-process-module).
        ipc: An explicit `ArrowIPCTransport` or `ArrowIPCStore` used for this stage. Takes precedence over `use_ipc`.

    Returns:
        Returns a `Stage` if the `iterable` parameters is given, else it returns a `Partial`.
    """

    if isinstance(iterable, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda iterable: from_iterable(
                iterable,
                use_thread=use_thread,
                maxsize=maxsize,
                use_ipc=use_ipc,
                ipc=ipc,
            )
        )

    return Stage(
        process_fn=FromIterable(iterable, maxsize=maxsize),
        workers=1,
        maxsize=maxsize,
        timeout=0,
        total_sources=1,
        dependencies=[],
        on_start=None,
        on_done=None,
        use_threads=use_thread,
        f_args=[],
        ipc=setting_from_args(use_ipc, ipc),
    )
