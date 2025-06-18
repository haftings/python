#! /usr/bin/env python3

'''Manage jobs across multiple processes and hosts'''

import errno
import os
import re
from datetime import timedelta
from io import BytesIO, BufferedIOBase, TextIOWrapper
from random import choices
from subprocess import DEVNULL, PIPE, STDOUT
from subprocess import Popen as _Popen, run as _run
from subprocess import CompletedProcess, CalledProcessError
from tempfile import TemporaryDirectory as _TemporaryDirectory
from typing import NamedTuple as _NamedTuple, Literal
from collections.abc import Callable, Generator, Iterable
import sys
import weakref

# TODO Call rsync, open, etc from SSH instead of reproducing
# TODO do something to unify SSH and SSHShell

MODE_STR = Literal[
    'r', 'rt', 'tr', 'rb', 'br',
    'w', 'wt', 'tw', 'wb', 'bw',
    'a', 'at', 'ta', 'ab', 'ba'
]
_ID_CHARS = b'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz@_'
_ID_LENGTH = 22
_RE_CODE = re.compile(rb'\n(\d+) [^\n]+\n$')
_RE_ERROR = re.compile(rb'([^\n]+)\n?$')
_RE_RSYNC_ESCAPE = re.compile(rb'\\#([0-7][0-7][0-7])')
_RE_RSYNC = re.compile(rb'''
    # file_name\n
    #       52461568  50%   19.53MB/s   00:00:02\r
    #      104857600 100%   30.86MB/s   00:00:03 (xfer#2, to-check=2/5)\n
    \s* ( \d+ )  # bytes downloaded
    \s+ ( \d+\.?\d*|\.\d+) %  # % complete
    (?: \s+ ( \d+\.?\d*|\.\d+) (?: \s? ( [KMGTPEZY]? ) B?[/p]s ) )?  # speed
    \s+ ( \d+ ) : ( \d+ ) (?: : (\d+) )?  # time
    \s* (?:
            \r  # mid-file update (no summary)
        | \(  # (xfer#1, to-check=2/5)
            \s* xfer \s* \#? \s* (\d+),?  # xfer#1,
            \s+ to[\ -]check \s*=?\s* (\d+) /? (\d*) \s*  # to-check=2/5
        \) \s* \n
    )
    | ( [^\r\n]* ) ([\r\n])  # generic line (possibly file name)
''', re.X)
_UNITS = {
    b'K': 1024, b'M': 1024 ** 2, b'G': 1024 ** 3, b'T': 1024 ** 4,
    b'P': 1024 ** 5, b'E': 1024 ** 6, b'Z': 1024 ** 7, b'Y': 1024 ** 8
}
_SEARCH_UNSAFE = re.compile(r'[^\w@%+=:,./\n-]', re.ASCII).search
_MULTIPLEX_OPTS: dict[str, str] = {
    'loglevel': 'error',
    'controlmaster': 'auto',
    'controlpath': '~/.ssh/.%u@%h:%p.control',
    'controlpersist': 'no'
}

def noop(*args, **kwargs) -> None:
    '''"no operation" function that does nothing'''

class RsyncEvent(_NamedTuple):
    '''an event from a running rsync command'''
    event: str
    name: str | None = None
    bytes_sent: int | None = None
    percent_complete: float | None = None
    bps: float | None = None
    eta: timedelta | None = None
    transfer_number: int | None = None
    n_checked: int | None = None
    n_total: int | None = None
    raw: bytes = b''

def rsync_events(file: BufferedIOBase | bytes) -> Generator[RsyncEvent]:
    '''read rsync events from an rsync process stdout open in binary mode'''
    if not isinstance(file, BufferedIOBase):
        file = BytesIO(file)
    name: str | None = None
    buff: list[bytes] = []
    while data := file.read1():
        buff.append(data)
        if b'\n' in data or b'\r' in data:
            if len(buff) > 1:
                data = b''.join(buff)
            cursor = 0
            for r in _RE_RSYNC.finditer(data):
                cursor = r.end()
                if r[11]:
                    event = 'file' if r[12] == b'\n' else 'unknown'
                    name = rsync_decode(r[11])
                    yield RsyncEvent(event, name=name, raw=r[0])
                else:
                    if r[7]:
                        h, m, s = int(r[5]), int(r[6]), int(r[7])
                    else:
                        h, m, s = 0, int(r[5]), int(r[6])
                    bps = (float(r[3]) * _UNITS.get(r[4], 1)) if r[3] else None
                    yield RsyncEvent(
                        'update',
                        name=name,
                        bytes_sent=int(r[1]),
                        percent_complete=float(r[2]),
                        bps=bps,
                        eta=timedelta(hours=h, minutes=m, seconds=s),
                        transfer_number=(int(r[8]) if r[8] else None),
                        n_checked=(int(r[9]) if r[9] else None),
                        n_total=(int(r[10]) if r[10] else None),
                        raw=r[0]
                    )
            buff = [data[cursor:]] if cursor < len(data) else []
    if buff:
        name = rsync_decode(b''.join(buff))
        yield RsyncEvent('unknown', name=name)

def _rsync_decode(r: re.Match) -> bytes:
    '''convert rsync escape match to a bytes character'''
    return bytes([int(r[1], 8)])

def rsync_decode(text: bytes) -> str:
    '''decode rsync text with escapes'''
    return _RE_RSYNC_ESCAPE.sub(_rsync_decode, text).decode(errors='replace')


class RunResult(_NamedTuple):
    '''Result from an SSH `run()` remote command call'''

    out: bytes
    '''STDOUT and STDERR'''

    code: int
    '''return code, or -1 for N/A'''


class SSH:
    '''Class to handle SSH connection(s) to a host

    `SSH(...)`
    - `host`: hostname and optional username, e.g. `someuser@somehost`
    - `opts`: `ssh_config` options, which are case-insensitive, e.g. `port=22`

    SSH sessions/connections are formed either once for each SSH operation:
    ```python
    ssh_host = SSH('someuser@somehost')  # no connection/session yet
    ssh_host.run(['echo', 'hello'])      # one full ssh session during run #1
    ssh_hsot.run(['echo', 'goodbye'])    # second full ssh session during run #2
    ```

    Or you can form a connection and reuse it using `with`:
    ```python
    with SSH('someuser@somehost') as ssh_host:  # session started
        ssh_host.run(['echo', 'hello'])         # uses existing session
        ssh_host.run(['echo', 'goodbye'])       # uses existing session again
    # session closes at the end of the `with` block
    ```
    '''
    def __init__(self, host: str, **opts: str) -> None:
        self.host = str(host) or 'localhost'
        self.opts: dict[str, str | int | float | bool] = dict(opts)
        self._proc: _Popen | None = None
        self._entries: int = 0

    def __enter__(self):
        if not self._entries:
            self.connect()
        self._entries += 1
        return self

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        if self._entries == 1:
            self.disconnect()
        self._entries -= 1

    def connect(self):
        if not self._proc:
            # cmd needs to make frequent, small, flushed outputs indefinitely,
            # and needs to work in all major shells (bash, sh, ksh, zsh)
            remote_cmd = 'echo; while :; do echo >&2; sleep 1; done'
            # start the ssh process
            opts = ssh_opt_args(_MULTIPLEX_OPTS | self.opts)
            cmd = ['ssh', self.host, *opts, remote_cmd]
            self._proc = _Popen(
                cmd, stderr=DEVNULL, stdout=PIPE, stdin=DEVNULL
            )
            # read the single newline from stdout to ensure multiplex is set up
            assert isinstance(self._proc.stdout, BufferedIOBase)
            if (ack := self._proc.stdout.read(1)) != b'\n':
                self._proc.terminate()
                self._proc.wait()
                self._proc = None
                raise OSError(1, f'bad ack from remote host: {ack=}')

    def disconnect(self):
        if self._proc:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None

    def run(
        self, cmd: Iterable[str] | str, *,
        shell: bool = False, cwd: str = '', **kwargs
    ) -> CompletedProcess:
        '''run a command on the remote host, similar to `subprocess.run`

        - most arguments work the same as for `subprocess.run`
        - `cwd` will change the remote directory before execution
        '''
        # reproduce eccentric way that subprocess handles cmd and shell
        if shell:
            if not isinstance(cmd, str):
                cmd = next(iter(cmd))
        else:
            cmd = _quote(cmd) if isinstance(cmd, str) else _join(cmd)
        # add cd for cwd
        if cwd:
            cmd = f'cd {_quote(cwd)} || exit $?; ' + cmd
        # run the command
        opts = ssh_opt_args(_MULTIPLEX_OPTS | self.opts)
        return _run(['ssh', self.host, *opts, '--', cmd], **kwargs)

    def Popen(
        self, cmd: Iterable[str] | str, *,
        shell: bool = False, cwd: str = '', **kwargs
    ) -> _Popen:
        '''start a command on the remote host, similar to `subprocess.Popen`

        - most arguments work the same as for `subprocess.Popen`
        - `cwd` will change the remote directory before execution
        '''
        # reproduce eccentric way that subprocess handles cmd and shell
        if shell:
            if not isinstance(cmd, str):
                cmd = next(iter(cmd))
        else:
            cmd = _quote(cmd) if isinstance(cmd, str) else _join(cmd)
        # add cd for cwd
        if cwd:
            cmd = f'cd {_quote(cwd)} || exit $?; ' + cmd
        # start the command
        opts = ssh_opt_args(_MULTIPLEX_OPTS | self.opts)
        full_cmd = ['ssh', self.host, *opts, '--', cmd]
        return _Popen(full_cmd, **kwargs)

    def upload(
        self, local_path: str | Iterable[str], rem_path: str = '.',
        arg: str = '-ac', *args: str,
        verbose: bool = False, callback: Callable[[RsyncEvent], None] = noop
    ):
        '''same as `rsync()`, but `rem_path` knows to uses this connection'''
        rsync(
            local_path, f'{self.host}:{rem_path}', arg, *args,
            callback=callback, verbose=verbose, **(_MULTIPLEX_OPTS | self.opts)
        )

    def download(
        self, rem_path: str | Iterable[str], local_path: str = '.',
        arg: str = '-ac', *args: str,
        verbose: bool = False, callback: Callable[[RsyncEvent], None] = noop
    ):
        '''same as `rsync()`, but `rem_path` knows to uses this connection'''
        paths = [rem_path] if isinstance(rem_path, str) else rem_path
        rsync(
            [f'{self.host}:{path}' for path in paths], local_path, arg, *args,
            callback=callback, verbose=verbose, **(_MULTIPLEX_OPTS | self.opts)
        )

    def write(
        self, rem_path: str, content: bytes | str, *,
        append: bool = False, perms: int | None = None
    ):
        '''write content to a remote file

        - `append` appends to the file instead of overwriting
        - `perms` also sets the permissions of the resulting file
        '''
        rpath = _quote(rem_path)
        redir = '>>' if append else '>'
        perms_cmd = f' && chmod {perms:03o} {rpath}' if perms else ''
        rcmd = f'cat {redir} {rpath}{perms_cmd}'
        cmd = ['bash', '-c', rcmd]
        with self.Popen(cmd, stdin=PIPE) as proc:
            if isinstance(content, str):
                content = content.encode()
            if not proc.stdin:
                raise OSError(1, 'ssh missing stdin')
            proc.stdin.write(content)
        if proc.returncode:
            raise CalledProcessError(proc.returncode, cmd)

    def read(
        self, rem_path: str, mode: Literal['b', 't'] = 't',
        encoding='utf-8', errors='replace'
    ) -> str | bytes:
        '''read text from a remote file
        
        - `mode` is `'t'` for text (the default) or `'b'` for bytes
        '''
        data = self.run(['cat', '--', rem_path], stdout=PIPE, check=True).stdout
        binary = 'b' in (mode or '').lower()
        return data if binary else data.decode(encoding, errors)

    def open(
        self, rem_path: str, mode: str = 'r',
        encoding='utf-8', errors='replace'
    ) -> BufferedIOBase | TextIOWrapper:
        ...

class SSHShell:
    '''SSH connection to a host, may be used as a context manager

    `SSHShell(...)`
    - `host` = hostname, with optional username, e.g. "someuser@somehost"
    - `multiplex` = multiplex operations over a single SSH connection
      - `True` turn multiplexing on only while connection is open (the default)
      - `False` turn multiplexing totally off
      - an `int` number keeps the SSH multiplex running in the background
        for that many seconds after closing the most recent connection
    - additional keyword `opts` set `ssh_config` options with `-o` flags

    You may safely run a single command like this:
    ```
    SSHShell('someuser@somehost').run(['echo', 'hello world'])
    ```
    (The SSH connection will open for `run()` and close before `run()` returns.)

    Or you can safely run several commands in a row like:
    ```
    with SSHShell('someuser@somehost') as connection:
        connection.run(['echo', 'hello'])
        connection.run(['echo', 'goodbye'])
    ```
    (The SSH connection will open at the start of the `with` block,
    and close when the `with` block ends.)
    '''

    def __init__(
        self, host: str = '', *, multiplex: bool | int = True, **opts: str
    ):
        self.host = str(host or '')
        self.proc: _Popen | None = None
        self.id = bytes(choices(_ID_CHARS, k=_ID_LENGTH))
        self.multiplex = multiplex
        self.opts = dict(opts)
        self._pwd: str | None = None
        self._entries: int = 0

    def connect(self):
        '''connect to host'''
        if not self.proc:
            # start an ssh process
            self.proc = _Popen(['ssh', self.host, *self._get_args(), (
                # remote bash instance runs a do loop
                b'while :; do'
                # it reads each command
                b' read -rd "" cmd;'
                # exits if asked (without invoking a sub-shell)
                b' [[ $cmd == exit ]] && exit;'
                # invokes a sub-shell for each non-exit command
                b' bash -c -- "$cmd";'
                # remembers the result
                b' code=$?;'
                # sends the return code and the unique ID
                b' echo; echo $code ' + self.id + b'; '
                # rinse and repeat
                b'done'
            )], stderr=STDOUT, stdout=PIPE, stdin=PIPE)

    def disconnect(self):
        '''disconnect from host'''
        if self.proc:
            # tell remote bash instance to exit
            if self.proc.stdin:
                # there's a chance that the process already closed
                try:
                    self.proc.stdin.write(b'exit\n')
                    self.proc.stdin.flush()
                    self.proc.stdin.close()
                # so ignore broken pipe errors
                except OSError as e:
                    if e.errno != errno.EPIPE:
                        raise
            # wait for the process to die
            self.proc.wait()
            # clear current process and working directory references
            self.proc = self._pwd = None

    def __enter__(self):
        # connect if this is the first entry
        if not self._entries:
            self.connect()
        # increment entries
        self._entries += 1
        # use SSHShell connection object as itself with context manager
        return self

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        # decrement entries
        self._entries = max(0, self._entries - 1)
        # disconnect if this is the last entry
        if not self._entries:
            self.disconnect()

    def run(self, cmd: Iterable[str] | str) -> RunResult:
        '''run a remote command

        - `Iterable[str]` is the preferred, safer method
        - `str` will run a verbatim `bash` shell command,
          including any bash-isms and expansions
        '''
        # connect and disconnect if needed, and assert that it worked
        with self:
            assert (
                self.proc and isinstance(self.proc.stdout, BufferedIOBase)
            ), 'ssh process must be running piped stdout'
            # clear cached pwd, in case the command changes it
            self._pwd = None
            # need at least STDIN and STDOUT
            if not (self.proc.stdin and self.proc.stdout):
                raise OSError('connection pipe missing')
            # convert str to command
            if isinstance(cmd, str):
                cmd = 'bash', '-c', cmd
            # escape tokens and join into a command line
            cmd_line = ' '.join(map(_quote, cmd)).encode() + b'\0'
            # send command to remote host
            self.proc.stdin.write(cmd_line)
            self.proc.stdin.flush()
            # read output one block at a time
            output = b''
            while not (output.endswith(self.id + b'\n') or self.proc.poll()):
                # TODO convert this to a list with something clever to add up \
                # TODO \ and blocks less than 26 + _ID_LENGTH
                new_output = self.proc.stdout.read1()
                output += new_output
            # extract return code
            if not (r := _RE_CODE.search(output[(-26 - _ID_LENGTH):])):
                return RunResult(output, -1)
            # return results
            return RunResult(output[:(-3 - len(r[1]) - _ID_LENGTH)], int(r[1]))

    def pwd(self) -> str:
        '''get the present working directory on the remote host'''
        # refresh cached pwd if needed
        if not self._pwd:
            # ask remote host for pwd
            pwd_bytes, code = self.run(['pwd'])
            if code or not pwd_bytes.endswith(b'\n'):
                raise OSError(code or 1, 'ssh pwd failed')
            # convert pwd from bytes to text, stripping the trailing '\n'
            self._pwd = pwd_bytes[:-1].decode()
        return self._pwd

    def read_b(self, rem_path: str) -> bytes:
        '''read a remote binary file'''
        # conveniently, the cat command does exactly what we want
        data, code = self.run(['cat', rem_path])
        # raise error if needed
        if code:
            # try to parse error from process output
            msg = ''
            if r := _RE_ERROR.search(data[-1024:]):
                msg = r[1].decode(errors='replace')
            # raise with parsed error if possible, otherwise a generic message
            raise OSError(code, msg or 'read error')
        return data

    def read(self, rem_path: str, encoding='utf-8', errors='replace') -> str:
        '''read a remote text file'''
        return self.read_b(rem_path).decode(encoding, errors)

    def _get_args(self) -> list[str]:
        '''get the control opts to SSH, and make .ssh dir if missing'''
        # add SSH control options to enable multiplexing
        opts = {}
        if self.multiplex or isinstance(self.multiplex, int):
            # ensure the user has an ~/.ssh directory
            ssh_dir = os.path.expanduser('~/.ssh')
            if not os.path.exists(ssh_dir):
                os.mkdir(ssh_dir, 0o700)
            # tell SSH to use ~/.ssh/<socket>
            opts['controlmaster'] = 'auto'
            opts['controlpath'] = '~/.ssh/.%u@%h:%p.control'
            # optionally set persist time
            if not isinstance(self.multiplex, bool):
                opts['controlpersist'] = f'{int(self.multiplex)}'
        opts.update(self.opts)
        return [f'-o{k} {v}' for k, v in opts.items()]

    def _format_rem_path(self, rem_path: str, quote: bool = True) -> str:
        '''format a remote path using remote PWD'''
        if not (rem_path and rem_path.startswith('/')):
            rem_path = os.path.join(self.pwd(), rem_path)
        return _quote(rem_path) if quote else rem_path


def _quote(token: str) -> str:
    '''quote a token for safe shell use, including newlines'''
    if '\n' in token:
        if not _SEARCH_UNSAFE(token):
            return token.replace('\n', "$'\\n'")
        token = token.replace("'", "'\"'\"'").replace('\n', "'$'\\n''")
        return f"'{token}'"
    elif not _SEARCH_UNSAFE(token):
        return token or "''"
    token = token.replace("'", "'\"'\"'")
    return f"'{token}'"


def _join(tokens: Iterable[str]) -> str:
    '''convert verbatim argument list to safe command string'''
    return ' '.join(map(_quote, tokens))


def rsync(
    source_path: str | Iterable[str],
    dest_path: str = '.',
    arg: str = '-ac', *args: str,
    verbose: bool = False,
    callback: Callable[[RsyncEvent], None] = noop,
    **opts: str | int | float | bool
):
    '''rsync files from `source_path` to `dest_path`

    - `arg` and optionally additional `args` set `rsync` arguments like flags
      - `--rsh`, `--verbose`, `--quiet`, and `--progress` may be ignored
    - `verbose` prints the `rsync` command and shows `rsync`'s STDERR
    - `callback` is called once for each update output from `rsync`
    - `opts` are `ssh_config` options to set with `--rsh`
    - raises a `CalledProcessError` if `rsync` fails
    '''
    src = [source_path] if isinstance(source_path, str) else list(source_path)
    all_args = [arg, *args]
    if opts:
        all_args.append(f'-essh {ssh_opt_args(opts)}')
    cmd = 'rsync', *all_args, '-vq', '--progress', *src, (dest_path or '.')
    if verbose:
        sys.stdout.write(f'{_join(cmd)}\n')
    stderr = None if verbose else STDOUT
    with _Popen(cmd, stdin=DEVNULL, stdout=PIPE, stderr=stderr) as proc:
        assert isinstance(proc.stdout, BufferedIOBase), 'rsync needs stdout'
        for event in rsync_events(proc.stdout):
            callback(event)
    if proc.returncode:
        raise CalledProcessError(proc.returncode, cmd)


def ssh_opt_args(opts: dict[str, str | int | float | bool] = {}) -> list[str]:
    '''convert `ssh_config` options to a list of `-o` arguments'''
    results = []
    for k, v in opts.items():
        if isinstance(v, bool):
            v = 'yes' if v else 'no'
        results.append(f'-o{k} {v}')
    return results


def open(
    host: str, path: str, mode: MODE_STR = 'r', *, verbose: bool = False,
    encoding: str = 'UTF-8', errors='replace', **opts: str | int | float | bool,
    
) -> TextIOWrapper | BufferedIOBase:
    err = None if verbose else DEVNULL
    if mode in {'r', 'rt', 'tr', 'rb', 'br'}:
        cmd = ['ssh', *ssh_opt_args(opts), host, f'cat {_quote(path)}']
        if 'b' in mode:
            proc = _Popen(cmd, stdin=DEVNULL, stdout=PIPE, stderr=err)
            assert isinstance(proc.stdout, BufferedIOBase), \
                'ssh subprocess stdout should be a BufferedIOBase'
            return BinaryFile(proc, proc.stdout)
        else:
            proc = _Popen(
                cmd, encoding=encoding, errors=errors,
                stdin=DEVNULL, stdout=PIPE, stderr=err
            )
            assert isinstance(proc.stdout, TextIOWrapper), \
                'ssh subprocess stdout should be a TextIOWrapper'
            return TextFile(proc, proc.stdout)
    if mode in {'w', 'wt', 'tw', 'wb', 'bw'}:
        cmd = ['ssh', *ssh_opt_args(opts), host, f'cat > {_quote(path)}']
    elif mode in {'a', 'at', 'ta', 'ab', 'ba'}:
        cmd = ['ssh', *ssh_opt_args(opts), host, f'cat >> {_quote(path)}']
    else:
        raise ValueError('SSH file mode must be r, rb, w, wb, a, or ab')
    if 'b' in mode:
        proc = _Popen(cmd, stdin=PIPE, stdout=DEVNULL, stderr=err)
        assert isinstance(proc.stdin, BufferedIOBase), \
            'ssh subprocess stdout should be a BufferedIOBase'
        return BinaryFile(proc, proc.stdin)
    else:
        proc = _Popen(
            cmd, encoding=encoding, errors=errors,
            stdin=PIPE, stdout=DEVNULL, stderr=err
        )
        assert isinstance(proc.stdin, TextIOWrapper), \
            'ssh subprocess stdout should be a TextIOWrapper'
        return TextFile(proc, proc.stdin)


def _close_ssh_file(proc: _Popen, file: BufferedIOBase | TextIOWrapper):
    '''close an ssh file and wait on the parent process'''
    file.close()
    if returncode := proc.wait():
        raise CalledProcessError(returncode, proc.args)


class TextFile(TextIOWrapper):
    '''text file opened over SSH'''

    def __init__(self, proc: _Popen, file: TextIOWrapper):
        self.proc = proc
        self.file = file
        # use weakref so that open(..).write(...) immediately flushes & closes
        self._finalize = weakref.finalize(
            self, _close_ssh_file, self.proc, self.file
        )

    def remove(self):
        self._finalize()

    @property
    def removed(self) -> bool:
        return not self._finalize.alive

    def close(self):
        _close_ssh_file(self.proc, self.file)    

    @property
    def closed(self) -> bool:
        return self.file.closed

    def fileno(self) -> int:
        return self.file.fileno()

    def flush(self):
        self.file.flush()

    @property
    def encoding(self) -> str:
        return self.file.encoding

    @property
    def errors(self) -> str | None:
        return self.file.errors

    @property
    def newlines(self) -> str | tuple[str, ...] | None:
        return self.file.newlines

    def isatty(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self.file.tell()

    def readable(self) -> bool:
        return self.file.readable()

    def read(self, size=-1) -> str:
        return self.file.read(size)

    def readline(self, size=-1) -> str:
        return self.file.readline(size)

    def readlines(self, hint: int = -1) -> list[str]:
        return self.file.readlines()

    def writable(self) -> bool:
        return self.file.writable()

    def write(self, text: str) -> int:
        return self.file.write(text)

    def __next__(self) -> str:
        if line := self.file.readline():
            return line
        raise StopIteration

    def __repr__(self) -> str:
        name = self.__class__.__name__
        return  f'{name}(proc={self.proc!r}, file={self.file!r})'


class BinaryFile(BufferedIOBase):
    '''binary file opened over SSH'''

    def __init__(self, proc: _Popen, file: BufferedIOBase):
        self.proc = proc
        self.file = file
        # use weakref so that open(..).write(...) immediately flushes & closes
        self._finalize = weakref.finalize(
            self, _close_ssh_file, self.proc, self.file
        )

    def remove(self):
        self._finalize()

    @property
    def removed(self) -> bool:
        return not self._finalize.alive

    def close(self):
        _close_ssh_file(self.proc, self.file)    

    @property
    def closed(self) -> bool:
        return self.file.closed

    def fileno(self) -> int:
        return self.file.fileno()

    def flush(self):
        self.file.flush()

    def isatty(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self.file.tell()

    def readable(self) -> bool:
        return self.file.readable()

    def read(self, size=-1) -> bytes:
        return self.file.read(size)

    def read1(self, size: int = -1) -> bytes:
        return self.file.read1(size)

    def readinto(self, buffer: bytearray | memoryview) -> int:
        return self.file.readinto(buffer)

    def readinto1(self, buffer: bytearray | memoryview) -> int:
        return super().readinto1(buffer)

    def readline(self, size=-1) -> bytes:
        return self.file.readline(size)

    def readlines(self, hint: int = -1) -> list[bytes]:
        return self.file.readlines()

    def writable(self) -> bool:
        return self.file.writable()

    def write(self, data: bytes) -> int:
        return self.file.write(data)

    def __next__(self) -> bytes:
        if line := self.file.readline():
            return line
        raise StopIteration

    def __repr__(self) -> str:
        name = self.__class__.__name__
        return  f'{name}(proc={self.proc!r}, file={self.file!r})'
