#! /usr/bin/env python3

'''Run commands concurrently in a pool of subprocesses'''

import asyncio
import os
import shlex
from subprocess import CalledProcessError
import sys
from typing import Callable, NamedTuple


class RunResult(NamedTuple):
    '''Result from a called process'''

    cmd: tuple[str, ...]
    '''command that was run'''

    error: CalledProcessError | OSError | None
    '''error encountered or `None` for successful execution with returncode 0'''


def on_run(cmd: tuple[str, ...]):
    '''Default `on_run` callback, displays the command on `stdout`'''
    sys.stdout.write(f'{shlex.join(cmd)}\n')
    sys.stdout.flush()


def on_ran(result: RunResult):
    '''Default `on_ran` callback, displays errors on `stderr` in a nice way'''
    if e := result.error:
        # format OSError
        if isinstance(e, OSError):
            if not (msg := (e.strerror or '').lower()):
                msg = f'errno {e.errno}' if e.errno else 'unknown error'
            if e.filename:
                msg += f': {shlex.quote(e.filename)}'
                if e.filename2:
                    msg += f', {shlex.quote(e.filename2)}'
        # format CalledProcessError
        elif isinstance(e, CalledProcessError):
            msg = f'returncode {e.returncode}'
        # format arbitrary error
        else:
            msg = 'unknown error'
        # write out error message
        cmd = shlex.join(result.cmd)
        if sys.stderr.isatty():
            sys.stderr.write(
                f'\033[31merror: {msg}  \033[2m# {cmd}\033[22;39m\n'
            )
        else:
            sys.stderr.write(f'error: {msg}  # {cmd}\n')
        sys.stderr.flush()


async def _async_run(cmd: tuple[str, ...]) -> RunResult:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL
        )
    except OSError as e:
        return RunResult(cmd, e)
    if code := await proc.wait():
        return RunResult(cmd, CalledProcessError(code, cmd))
    return RunResult(cmd, None)


async def async_run_many(
    cmds: list[tuple[str, ...]],
    n_proc: int = os.cpu_count() or 0,
    on_run: Callable[[tuple[str, ...]], None] | None = on_run,
    on_ran: Callable[[RunResult], None] | None = on_ran
) -> dict[tuple[str, ...], RunResult]:
    '''Run commands concurrently in a pool of subprocesses asynchronously

    - `cmds` are the commands to run
    - `n_proc` is the max count of concurrent subprocesses, 0 for no limit
    - `on_run` is called with each command when a subprocess is started
    - `on_ran` is called for each completed subprocess (succeed or fail)
    - return value is a map of commands to results
    '''
    # set up tracking containers
    cmds = [tuple(cmd) for cmd in cmds]
    cmds.reverse()  # so we can pop them in order
    tasks: set[asyncio.Task] = set()
    cmd2task: dict[tuple[str, ...], asyncio.Task] = {}
    results = {}
    # run until we've gone through all the commands and processes
    while tasks or cmds:
        # top tasks list off by starting new processes
        while cmds and (len(tasks) < n_proc or not n_proc):
            cmd = cmds.pop()
            # run callback
            if on_run:
                on_run(cmd)
            # start a task to run a command
            task = asyncio.create_task(_async_run(cmd))
            # track the newly created task
            tasks.add(task)
            cmd2task[cmd] = task
        # wait for a process to finish
        result = await next(asyncio.as_completed(tasks))
        # stop tracking the associated task, and store results
        tasks.remove(cmd2task.pop(result.cmd))
        results[result.cmd] = result
        # run callback
        if on_ran:
            on_ran(result)
    return results


def run_many(
    cmds: list[tuple[str, ...]],
    n_proc: int = os.cpu_count() or 0,
    on_run: Callable[[tuple[str, ...]], None] | None = on_run,
    on_ran: Callable[[RunResult], None] | None = on_ran
) -> dict[tuple[str, ...], RunResult]:
    '''Run commands concurrently in a pool of subprocesses

    - `cmds` are the commands to run
    - `n_proc` is the max count of concurrent subprocesses, 0 for no limit
    - `on_run` is called with each command when a subprocess is started
    - `on_ran` is called for each completed subprocess (succeed or fail)
    - return value is a map of commands to
    '''
    return asyncio.run(async_run_many(cmds, n_proc, on_run, on_ran))
