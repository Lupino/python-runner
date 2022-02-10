import asyncio
from importlib import import_module
import os.path
import argparse
from multiprocessing import Process
import sys
import logging
import signal

logger = logging.getLogger(__name__)

before_start_events = []
after_stop_events = []

stop_event = None
global_task = None


def sigint_handler(signal, frame):
    logger.error('KeyboardInterrupt Error')
    if stop_event.is_set():
        sys.exit(1)

    if stop_event:
        stop_event.set()

    if global_task:
        global_task.cancel()


def fixed_module_name(module_name):
    if os.path.isfile(module_name):
        if module_name.endswith('.py'):
            module_name = module_name[:-3]

        if module_name.startswith('./'):
            module_name = module_name[2:]

        return module_name.replace('/', '.')

    return module_name


def before_start(evt):
    before_start_events.append(evt)


def after_stop(evt):
    after_stop_events.append(evt)


async def aio_run(module, *argv):
    global stop_event
    global global_task

    signal.signal(signal.SIGINT, sigint_handler)

    for evt in before_start_events:
        if asyncio.iscoroutinefunction(evt):
            await evt(module)
        else:
            evt(module)

    stop_event = asyncio.Event()

    async def main_task():
        try:
            await module.main(*argv)
        finally:
            stop_event.set()

    try:
        global_task = asyncio.create_task(main_task())
        await stop_event.wait()
    finally:
        for evt in after_stop_events:
            if asyncio.iscoroutinefunction(evt):
                await evt(module)
            else:
                evt(module)


def run(module, *argv):
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


def start(module_name, argv, process_id=None):
    formatter = "[%(asctime)s] %(name)s:%(lineno)d %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=formatter)
    module_log = f'running module {module_name} {" ".join(argv)}'
    logger.info(f'Start {module_log}')
    module = import_module(fixed_module_name(module_name))

    if process_id is not None:
        os.environ['PROCESS_ID'] = str(process_id)

    if hasattr(module, 'parse_args'):
        argv = [module.parse_args(argv)]

    if asyncio.iscoroutinefunction(module.main):
        asyncio.run(aio_run(module, *argv))
    else:
        run(module, *argv)

    logger.info(f'Finish {module_log}')


def split_argv(argv):
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


def main(script, *argv):
    script_argv, module_argv = split_argv(argv)
    parser = argparse.ArgumentParser(description='Prepare and Run command.')
    parser.add_argument('-p',
                        '--processes',
                        dest='processes',
                        default=1,
                        type=int,
                        help='process size. default is 1')
    parser.add_argument('module_name',
                        type=str,
                        help='module name or module file')
    parser.add_argument('argv', nargs='*', help='module arguments')

    args = parser.parse_args(script_argv)

    if args.processes > 1:
        processes = []
        for i in range(args.processes):
            p = Process(target=start,
                        args=(args.module_name, module_argv, i + 1))
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
