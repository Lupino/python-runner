import asyncio
from importlib import import_module
import os.path
import argparse
from multiprocessing import Process
import sys
import logging
import signal
from time import time
import math
from typing import List, Any, Optional, Callable, Coroutine

logger = logging.getLogger(__name__)

before_start_events: List[
    Callable[[Any], None]
    | Callable[[Any], Coroutine[Any, Any, None]],
] = []
after_stop_events: List[
    Callable[[Any], None]
    | Callable[[Any], Coroutine[Any, Any, None]],
] = []

stop_event: Optional[asyncio.Event] = None
global_task: Optional[asyncio.Task[Any]] = None


def mod60(t: int) -> tuple[int, int]:
    return math.floor(t / 60), t % 60


def pretty_time(t: int) -> str:
    out = []
    for i in range(2):
        t, v = mod60(t)

        v_s = f'0{v}'
        out.append(v_s[-2:])

        if t == 0:
            break

    if t < 10:
        out.append('0' + str(t))
    else:
        out.append(str(t))
    out.reverse()
    return ':'.join(out)


def sigint_handler(signal: int, frame: Any) -> None:
    logger.error('KeyboardInterrupt Error')
    if stop_event:
        if stop_event.is_set():
            sys.exit(1)

        stop_event.set()

    if global_task:
        global_task.cancel()


def fixed_module_name(module_name: str) -> str:
    if os.path.isfile(module_name):
        if module_name.endswith('.py'):
            module_name = module_name[:-3]

        if module_name.startswith('./'):
            module_name = module_name[2:]

        return module_name.replace('/', '.')

    return module_name


def before_start(
    evt: Callable[[Any], None] | Callable[[Any], Coroutine[Any, Any, None]]
) -> None:
    before_start_events.append(evt)


def after_stop(
    evt: Callable[[Any], None] | Callable[[Any], Coroutine[Any, Any, None]]
) -> None:
    after_stop_events.append(evt)


async def aio_run(module: Any, *argv: str) -> None:
    global stop_event
    global global_task

    signal.signal(signal.SIGINT, sigint_handler)

    for evt in before_start_events:
        if asyncio.iscoroutinefunction(evt):
            await evt(module)
        else:
            evt(module)

    stop_event = asyncio.Event()

    async def main_task() -> None:
        try:
            await module.main(*argv)
        finally:
            stop_event.set()

    try:
        global_task = asyncio.create_task(main_task())
        if stop_event:
            await stop_event.wait()
    finally:
        for evt in after_stop_events:
            if asyncio.iscoroutinefunction(evt):
                await evt(module)
            else:
                evt(module)


def run(module: Any, *argv: str) -> None:
    for evt in before_start_events:
        if asyncio.iscoroutinefunction(evt):
            asyncio.run(evt(module))
        else:
            evt(module)

    try:
        module.main(*argv)
    finally:
        for evt in after_stop_events:
            if asyncio.iscoroutinefunction(evt):
                asyncio.run(evt(module))
            else:
                evt(module)


def start(
    module_name: str,
    argv: List[str],
    processes: Optional[int] = None,
    process_id: Optional[int] = None,
) -> None:
    formatter = "[%(asctime)s] %(name)s:%(lineno)d %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    module_log = f'running module {module_name} {" ".join(argv)}'
    logger.info(f'Start {module_log}')
    start_time = time()
    module = import_module(fixed_module_name(module_name))

    if process_id is not None:
        os.environ['PROCESS_ID'] = str(process_id)
        os.environ['PROCESSES'] = str(processes)

    if hasattr(module, 'parse_args'):
        argv = [module.parse_args(argv)]

    if asyncio.iscoroutinefunction(module.main):
        asyncio.run(aio_run(module, *argv))
    else:
        run(module, *argv)

    logger.info(f'Finish {module_log}')

    t = round(time() - start_time, 4)
    logger.info(f'Spent: {t}s')
    t_s = pretty_time(int(t))
    logger.info(f'Spent: {t_s}')


def split_argv(argv: List[str]) -> tuple[List[str], List[str]]:
    script_argv = []
    is_module_argv = False
    module_argv = []

    argv = list(argv)
    argv.reverse()

    while True:
        if len(argv) == 0:
            break

        arg = argv.pop()

        if is_module_argv:
            module_argv.append(arg)
        else:
            script_argv.append(arg)
            if arg.startswith('-'):
                if arg.find('=') == -1:
                    if len(argv) > 0:
                        script_argv.append(argv.pop())

            else:
                is_module_argv = True

    return script_argv, module_argv


def main(script: str, *argv: str) -> None:
    script_argv, module_argv = split_argv(list(argv))
    parser = argparse.ArgumentParser(description='Prepare and Run command.')
    parser.add_argument(
        '-p',
        '--processes',
        dest='processes',
        default=1,
        type=int,
        help='process size. default is 1',
    )
    parser.add_argument(
        'module_name',
        type=str,
        help='module name or module file',
    )
    parser.add_argument('argv', nargs='*', help='module arguments')

    args = parser.parse_args(script_argv)

    if args.processes > 1:
        processes = []
        for i in range(args.processes):
            p = Process(
                target=start,
                args=(args.module_name, module_argv, args.processes, i + 1),
            )
            p.start()
            processes.append(p)

        running = True
        while running:
            for p in processes:
                p.join(10)
                if not p.is_alive():
                    running = False
                    break
        for p in processes:
            p.terminate()
            p.join(10)
            if p.is_alive():
                p.kill()
    else:
        start(args.module_name, module_argv)


if __name__ == '__main__':
    main(*sys.argv)
