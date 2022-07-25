#! /usr/bin/env python3

'''Run multiple subprocesses at the same time'''

from collections import deque
from os import cpu_count, wait
from shlex import join
from subprocess import Popen, DEVNULL, TimeoutExpired
from sys import stderr
from time import time

def advise(about, error=None, verbose=True, color=None):
    '''Show process or text results'''
    if verbose:
        if isinstance(about, Popen):
            if error := error or about.poll():
                about = f'error {about.returncode}: {join(about.args)}'
            else:
                about = f'done: {join(about.args)}'
        FMT = '%s\n'
        if stderr.isatty() if color is None else color:
            FMT = '\033[31m%s\033[39m\n' if error else '\033[2m%s\033[22m\n'
        stderr.write(FMT % about)

def multisubprocess(
    *cmds, n=None, verbose=True, color=None, kill_wait=0.1,
    out=(DEVNULL, DEVNULL), **kwargs
):
    '''Run multiple subprocesses concurrently

    *cmds  ([str, ...], ...)  argument lists to run as subprocesses
    n  (int)  max subprocesses to run concurrently
    verbose  (bool)  print process start and stop info to stderr
    color  (bool)  use ANSI color codes for verbose output
    kill_wait  (float)  time to wait after termination before killing
    out  ((stdout, stderr))
        stdout  (file|None|DEVNULL)  same as Popen's stdout argument
        stderr  (file|None|DEVNULL)  same as Popen's stderr argument
    out  (function(cmd) -> (stdout, stderr))
        cmd  (str, ...)  tuple of command arguments sent to cmds
    **kwargs  (...)  arguments to send to subprocess.Popen
    '''
    # set up
    n = cpu_count() if n is None else float('inf') if n <= 0 else n
    ready, running = deque(*map(list, cmds)), {}
    try:
        while ready or running:
            while ready and len(running) < n:
                # new child
                try:
                    cmd = tuple(ready.popleft())
                    # get file handles
                    try:
                        so, se = out(cmd)
                    except TypeError:
                        try:
                            so, se = out
                        except TypeError:
                            so, se = (None, None) if out else (DEVNULL, DEVNULL)
                    # spawn process
                    proc = Popen(
                        cmd, stdin=DEVNULL, stdout=so, stderr=se, **kwargs
                    )
                    running[proc.pid] = proc
                    advise(f'run: {join(proc.args)}', 0, verbose, color)
                except OSError as e:
                    msg = f'error {e.errno} {e.strerror}: {join(cmd)}'
                    advise(msg, 1, verbose, color)
            if running:
                # wait for next running child to complete
                while True:
                    pid, status = wait()
                    if pid in running:
                        break
                proc = running.pop(pid)
                proc.poll()  # this will result in 0 because of wait()
                proc.returncode = status // 256
                # show results
                advise(proc, None, verbose, color)
    except KeyboardInterrupt:
        stderr.write('\n')
    finally:
        # clean up any still-running processes
        for proc in running.values():
            advise(f'terminate: {join(proc.args)}', 0, verbose, color)
            proc.terminate()
        tmax = time() + kill_wait
        for proc in running.values():
            try:
                proc.wait(max(0, tmax - time()))
                advise(proc, None, verbose, color)
            except TimeoutExpired:
                advise(f'kill: {join(proc.args)}', 0, verbose, color)
                proc.kill()
                proc.wait()
                advise(proc, None, verbose, color)
