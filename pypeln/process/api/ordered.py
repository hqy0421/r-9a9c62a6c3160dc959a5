import bisect
import typing as tp

from pypeln import utils as pypeln_utils
from pypeln.utils import A, B, T

from ..ipc import setting_from_args
from ..stage import Stage
from ..worker import ProcessFn, Worker
from .to_stage import to_stage


class Ordered(tp.NamedTuple):
    def __call__(self, worker: Worker, **kwargs):
        elems = []

        for elem in worker.stage_params.input_queue:
            bisect.insort(elems, elem)

        for _ in range(len(elems)):
            worker.stage_params.output_queues.put(elems.pop(0))


@tp.overload
def ordered(
    stage: Stage[A],
    maxsize: int = 0,
    use_ipc: bool = False,
    ipc=None,
) -> Stage[A]:
    ...


@tp.overload
def ordered(
    maxsize: int = 0, use_ipc: bool = False, ipc=None
) -> pypeln_utils.Partial[Stage[A]]:
    ...


def ordered(
    stage: tp.Union[
        Stage[A], tp.Iterable[A], pypeln_utils.Undefined
    ] = pypeln_utils.UNDEFINED,
    maxsize: int = 0,
    use_ipc: bool = False,
    ipc=None,
) -> tp.Union[Stage[A], pypeln_utils.Partial[Stage[A]]]:
    """
    Creates a stage that sorts its elements based on their order of creation on the source iterable(s) of the pipeline.

    ```python
    import pypeln as pl
    import random
    import time

    def slow_squared(x):
        time.sleep(random.random())

        return x ** 2

    stage = range(5)
    stage = pl.process.map(slow_squared, stage, workers = 2)
    stage = pl.process.ordered(stage)

    print(list(stage)) # [0, 1, 4, 9, 16]
    ```

    !!! note
        `ordered` will work even if the previous stages are from different `pypeln` modules, but it may not work if you introduce an itermediate external iterable stage.

    !!! warning
        This stage will not yield util it accumulates all of the elements from the previous stage, use this only if all elements fit in memory.

    Arguments:
        stage: A Stage or Iterable.
        maxsize: The maximum number of objects the stage can hold simultaneously, if set to `0` (default) then the stage can grow unbounded.
        use_ipc: If `True`, `RecordBatch` values of this pipeline travel through the queues as Arrow IPC files instead of pickled batch bodies. Requires `pyarrow`, see [Advanced Usage](https://cgarciae.github.io/pypeln/advanced/#arrow-ipc-file-transport-process-module).
        ipc: An explicit `ArrowIPCTransport` or `ArrowIPCStore` used for this stage. Takes precedence over `use_ipc`.

    Returns:
        If the `stage` parameters is given then this function returns an iterable, else it returns a `Partial`.
    """

    if isinstance(stage, pypeln_utils.Undefined):
        return pypeln_utils.Partial(
            lambda stage: ordered(stage, maxsize=maxsize, use_ipc=use_ipc, ipc=ipc)
        )

    stage = to_stage(stage, maxsize=maxsize, ipc=setting_from_args(use_ipc, ipc))

    return Stage(
        process_fn=Ordered(),
        workers=1,
        maxsize=maxsize,
        timeout=0,
        total_sources=stage.workers,
        dependencies=[stage],
        on_start=None,
        on_done=None,
        use_threads=False,
        f_args=[],
        ipc=setting_from_args(use_ipc, ipc),
    )
