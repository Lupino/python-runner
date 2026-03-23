import argparse
import asyncio
import inspect
import logging
import os
import signal
import sys
from hashlib import sha1
from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location
from multiprocessing import Process
from time import time
from typing import Any, Awaitable, Callable, Coroutine, List, Optional, Union

# Type definitions for better readability
HookHandler = Union[
    Callable[[Any], None],
    Callable[[Any], Coroutine[Any, Any, None]]
]

logger = logging.getLogger(__name__)

# Global hooks
before_start_events: List[HookHandler] = []
after_stop_events: List[HookHandler] = []

# Global state for asyncio tasks/signals
stop_event: Optional[asyncio.Event] = None
global_task: Optional[asyncio.Task[Any]] = None


def pretty_time(seconds: float) -> str:
    """Formats time in seconds to HH:MM:SS or MM:SS."""
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f'{h:02d}:{m:02d}:{s:02d}'
    return f'{m:02d}:{s:02d}'


def sigint_handler(_sig: int, _frame: Any) -> None:
    """Handles KeyboardInterrupt (Ctrl+C)."""
    logger.error('KeyboardInterrupt Error')
    if stop_event:
        if stop_event.is_set():
            sys.exit(1)
        stop_event.set()

    if global_task:
        global_task.cancel()


def fixed_module_name(module_name: str) -> str:
    """Normalizes file paths to dotted module names."""
    if module_name.endswith('.py'):
        module_name = module_name[:-3]
    if module_name.startswith('./'):
        module_name = module_name[2:]
    module_name = module_name.replace('\\', '/').replace('/', '.')
    return module_name.lstrip('.')


def load_module(module_name: str) -> Any:
    """Loads a module by dotted path or python file path."""
    if not os.path.isfile(module_name):
        return import_module(fixed_module_name(module_name))

    file_path = os.path.abspath(module_name)
    module_id = f'_runner_file_{sha1(file_path.encode("utf-8")).hexdigest()}'

    spec = spec_from_file_location(module_id, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f'Cannot load module from file: {file_path}')

    module = module_from_spec(spec)
    # Match python script execution behavior for sibling imports.
    module_dir = os.path.dirname(file_path)
    path_added = False
    if module_dir and module_dir not in sys.path:
        sys.path.insert(0, module_dir)
        path_added = True

    try:
        sys.modules[module_id] = module
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_id, None)
        raise
    finally:
        if path_added and sys.path and sys.path[0] == module_dir:
            sys.path.pop(0)
    return module


def before_start(evt: HookHandler) -> None:
    """Registers a hook to run before the module starts."""
    before_start_events.append(evt)


def after_stop(evt: HookHandler) -> None:
    """Registers a hook to run after the module stops."""
    after_stop_events.append(evt)


async def _run_async_hooks(module: Any, hooks: List[HookHandler]) -> None:
    """Helper to run hooks asynchronously."""
    for evt in hooks:
        if asyncio.iscoroutinefunction(evt):
            await evt(module)
        else:
            evt(module)  # type: ignore


def _run_sync_hooks(module: Any, hooks: List[HookHandler]) -> None:
    """Helper to run hooks synchronously."""
    for evt in hooks:
        if asyncio.iscoroutinefunction(evt):
            asyncio.run(evt(module))
        else:
            evt(module)  # type: ignore


async def _await_value(awaitable: Awaitable[Any]) -> Any:
    """Normalizes generic awaitables into a coroutine result."""
    return await awaitable


async def aio_run(module: Any, *argv: str) -> None:
    """Runs an asynchronous module main function."""
    global stop_event, global_task

    # Register signal handler
    signal.signal(signal.SIGINT, sigint_handler)

    await _run_async_hooks(module, before_start_events)

    stop_event = asyncio.Event()

    async def main_task() -> None:
        try:
            await module.main(*argv)
        finally:
            if stop_event:
                stop_event.set()

    try:
        global_task = asyncio.create_task(main_task())
        if stop_event:
            await stop_event.wait()
    finally:
        await _run_async_hooks(module, after_stop_events)


def run(module: Any, *argv: str) -> None:
    """Runs a synchronous module main function."""
    _run_sync_hooks(module, before_start_events)

    try:
        module.main(*argv)
    finally:
        _run_sync_hooks(module, after_stop_events)


def start(
    module_name: str,
    argv: List[str],
    processes: Optional[int] = None,
    process_id: Optional[int] = None,
) -> None:
    """Initializes environment and starts the module."""
    log_fmt = "[%(asctime)s] %(name)s:%(lineno)d %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    display_args = " ".join(argv)
    module_log = f'running module {module_name} {display_args}'
    logger.info(f'Start {module_log}')

    start_time = time()
    module = load_module(module_name)

    if process_id is not None:
        os.environ['PROCESS_ID'] = str(process_id)
    if processes is not None:
        os.environ['PROCESSES'] = str(processes)

    # Allow module to parse its own arguments if supported
    run_argv = argv
    parser_func = getattr(module, 'parse_args', None)
    if callable(parser_func):
        parsed_argv = parser_func(argv)
        if inspect.isawaitable(parsed_argv):
            parsed_argv = asyncio.run(_await_value(parsed_argv))
        if isinstance(parsed_argv, (list, tuple)):
            run_argv = list(parsed_argv)
        else:
            run_argv = [parsed_argv]

    if asyncio.iscoroutinefunction(module.main):
        asyncio.run(aio_run(module, *run_argv))
    else:
        run(module, *run_argv)

    logger.info(f'Finish {module_log}')
    duration = time() - start_time
    logger.info(f'Spent: {round(duration, 4)}s ({pretty_time(duration)})')


def main(script: str, *argv: str) -> None:
    """Main entry point for the CLI tool."""
    parser = argparse.ArgumentParser(description='Prepare and Run command.')
    parser.add_argument(
        '-p', '--processes',
        dest='processes', default=1, type=int,
        help='Process pool size. Default is 1.'
    )
    parser.add_argument(
        '-w', '--wait-all-stop',
        dest='wait_all_stop', action='store_true',
        help='Wait for all processes to stop. Default is False.'
    )
    parser.add_argument('module_name', help='Python module path to run.')
    parser.add_argument(
        'module_argv',
        nargs=argparse.REMAINDER,
        help='Arguments passed through to the target module.',
    )

    args = parser.parse_args(list(argv))
    target_module = args.module_name
    target_args = args.module_argv

    # Logic for single process
    if args.processes <= 1:
        start(target_module, target_args)
        return

    # Logic for multi-process
    procs: List[Process] = []
    for i in range(args.processes):
        p = Process(
            target=start,
            args=(target_module, target_args, args.processes, i + 1),
        )
        p.start()
        procs.append(p)

    if args.wait_all_stop:
        for p in procs:
            p.join()
        return

    # Default behavior: If one process dies, kill the rest
    running = True
    while running:
        # Check periodically
        for p in procs:
            p.join(1)  # Short timeout
            if not p.is_alive():
                running = False
                break

    # Terminate remaining processes
    for p in procs:
        if p.is_alive():
            p.terminate()
            p.join(5)
            if p.is_alive():
                p.kill()


if __name__ == '__main__':
    main(*sys.argv)
