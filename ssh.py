#! /usr/bin/env python3

'''Manage jobs across multiple processes and hosts'''


from subprocess import CompletedProcess, CalledProcessError
from subprocess import DEVNULL, PIPE, STDOUT, TimeoutExpired
from subprocess import run as _run, Popen as _Popen
from io import BytesIO, BufferedIOBase, BufferedReader, TextIOWrapper, IOBase
from typing import cast, Any, IO, ItemsView, KeysView, Literal
from collections.abc import Callable, Generator, Iterable, Iterator
from collections.abc import MutableMapping
import datetime
import os
import random
import re
import sys
import weakref

# TODO csh / tcsh environment variable parsing and `Shell` support

_DIGITS = frozenset(b'0123456789')
'''decimal digits `[0-9]`'''

_ID_CHARS = b'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz@_'
'''characters used for IDs, `[A-Z0-9a-z@_]`'''

_IS_VALID_ENV_VAR_NAME = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*$').match
'''highly restrictive test for a valid environment variable name'''

_ERRNO_STR = (
    (1, 'operation not permitted'),
    (2, 'no such file'), # omit ' or directory' suffix for dash
    (3, 'no such process'),
    (4, 'interrupted system call'),
    (10, 'no child processes'),
    (13, 'permission denied'),
    (17, 'file exists'),
    (20, 'not a directory'),
    (21, 'is a directory'),
    (32, 'broken pipe'),
)
'''sequence of `(errno, strerror)` pairs to look for in `stderr`'''

_MODE_STR = Literal[
    'r', 'rt', 'tr', 'rb', 'br',
    'w', 'wt', 'tw', 'wb', 'bw', 'a', 'at', 'ta', 'ab', 'ba'
]
'''allowed modes such as `r`, `wb`, etc.'''

_MULTIPLEX_OPTS: dict[str, str] = {
    'loglevel': 'error',
    'controlmaster': 'auto',
    'controlpath': '~/.ssh/.%u@%h:%p.control',
    'controlpersist': 'no'
}
'''options required for multiplexing'''

_RE_RSYNC_ESCAPE = re.compile(rb'\\#([0-7][0-7][0-7])')
'''matches an rsync escape code'''

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
'''parses rsync output line ending in `\r` or `\n`'''

_UNITS = {
    b'K': 1024, b'M': 1024 ** 2, b'G': 1024 ** 3, b'T': 1024 ** 4,
    b'P': 1024 ** 5, b'E': 1024 ** 6, b'Z': 1024 ** 7, b'Y': 1024 ** 8
}
'''units used by rsync'''

_NO_QUOTE_NEEDED = re.compile(r'^[\w./-]+$').match
'''matches for str with only shell-legal unquoted token chars'''

_SINGLE_QUOTABLE = re.compile(r'^[^\n\r\']*$').match
'''matches for str without `'` or newlines'''

_DOUBLE_QUOTABLE = re.compile(r"^[\w\t\ #&()*+,./:;<=>?@\[\]^_|~'-]*$").match
'''matches for str without chars with special meaning inside `""`'''

_DITHER_QUOTABLE = re.compile(r'^[^\n\r]*$').match
'''matches for str expressible with a combo of `''` and `""`'''

_RE_DITHER_QUOTE = re.compile(r'''
    (?:
        # single quotes
        ([^\\'"]*?[^\w\t\ #&()*+,./:;<=>?@\[\]^_|~'-][^']*)
    |
        # double quotes are much more complicated
        [\w\t\ #&()*+,./:;<=>?@\[\]^_|~-]*'['\w\t\ #&()*+,./:;<=>?@\[\]^_|~-]*
    )
''', re.X)
'''match a single sequence of ''-quotable or ""-quotable chars'''

_SUB_PRINTF_ESC = re.compile(r'''[^\w\t\ #&()*+,./:;<=>?@\[\]^_|~-]''').sub
'''`re.sub` function for chars that need quoting in quoted `printf` style'''


def _stderr2oserror(stderr: str, name: str | None = None) -> OSError:
    '''convert common stderr text to errno, return 255 if unrecognized'''
    if name:
        stderr = stderr.replace(name, ' ', 1)
    stderr = ' '.join(stderr[-128:].split()).lower()
    for errno, msg in _ERRNO_STR:
        if msg in stderr:
            return OSError(errno, os.strerror(errno), name or None)
    return OSError(255, 'Unknown error', name or None)


def NOOP(*args, **kwargs) -> None:
    '"no operation" function that does nothing'


class RsyncEvent:
    '''an event from a running rsync command'''

    def __init__(
        self, 
        event: Literal['file', 'update', 'unknown'] | str,
        name: str | None = None,
        bytes_sent: int | None = None,
        percent_complete: float | None = None,
        bps: float | None = None,
        eta: datetime.timedelta | None = None,
        transfer_number: int | None = None,
        n_checked: int | None = None,
        n_total: int | None = None,
        raw: bytes = b''
    ):

        self.event = event
        '''type of event

        - `file` for the start of a file transfer
        - `update` for a progress update
        - `unknown` for an unrecognized event
        '''

        self.name = name
        '''file name or path'''

        self.bytes_sent = bytes_sent
        '''bytes sent so far'''

        self.percent_complete = percent_complete
        '''completion percent, rounded, can be NaN e.g. for 0-length files'''

        self.bps = bps
        '''transfer rate in bits per second'''

        self.eta = eta
        '''estimated time of arrival (upload or download)'''

        self.transfer_number = transfer_number
        '''rsync transfer number, starts with 1'''

        self.n_checked = n_checked
        '''
        count of files checked so far,
        starts with 0, doesn't include current file
        '''

        self.n_total = n_total
        '''count of files to check in all, includes all files and directories'''

        self.raw = raw
        '''the raw binary output from rsync used to infer this event'''

    _att_names = (
        'event',
        'name',
        'bytes_sent',
        'percent_complete',
        'bps',
        'eta',
        'transfer_number',
        'n_checked',
        'n_total',
        'raw'
    )

    def __repr__(self) -> str:
        args = []
        for name in self._att_names:
            if (v := getattr(self, name)) is not None:
                args.append(f'{name}={v!r}')
        return f'{self.__class__.__name__}(' + ', '.join(args) + ')'


class Host:
    '''Class to handle SSH connection(s) to a host

    `Host(...)`
    - `host` is the hostname and optional username, e.g. `user@host`
    - `opts` keywords set `ssh_config` options, e.g. `port=22`

    SSH sessions/connections are formed either once for each SSH operation:
    ```python
    host = Host('user@host')   # no connection/session yet
    host(['echo', 'hello'])    # one full ssh session during run #1
    host(['echo', 'goodbye'])  # second full ssh session during run #2
    ```

    Or you can form a connection and reuse it using `with`:
    ```python
    with Host('user@host') as host:  # session started
        host(['echo', 'hello'])      # uses existing session
        host(['echo', 'goodbye'])    # uses existing session again
    # session closes at the end of the `with` block
    ```
    '''
    def __init__(self, host: str, **opts: str | int | float | bool) -> None:
        self._host = str(host) or 'localhost'
        self.opts: dict[str, str | int | float | bool] = dict(opts)
        self._proc: _Popen | None = None
        self._entries: int = 0

    @property
    def host(self) -> str:
        '''hostname and optionally username, e.g. "user@host"'''
        return self._host

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
            remote_cmd = '\\echo; while \\:; do \\echo >&2; \\sleep 1; done'
            # start the ssh process
            opts = _ssh_opt_args(_MULTIPLEX_OPTS | self.opts)
            cmd = ['ssh', self._host, *opts, remote_cmd]
            self._proc = _Popen(
                cmd, stderr=DEVNULL, stdout=PIPE, stdin=DEVNULL
            )
            # read the single newline from stdout to ensure multiplex is set up
            ack = cast(BufferedIOBase, self._proc.stdout).read(1)
            if ack != b'\n':
                self._proc.terminate()
                self._proc.wait()
                self._proc = None
                raise OSError(1, f'bad ack from remote host: {ack=}')

    def disconnect(self):
        if self._proc:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None

    # alias for shell terminology
    exit = disconnect

    def rsync_upload(
        self,
        local_path: str | Iterable[str],
        remote_path: str = '',
        arg: str = '-ac', *args: str,
        verbose: bool = False,
        callback: Callable[[RsyncEvent], None] = NOOP
    ):
        '''rsync files from `local_path` to `remote_path`
        
        - see `rsync` for more details
        '''
        return rsync(
            local_path, f'{self._host}:{remote_path}', arg, *args,
            verbose=verbose, callback=callback, **(_MULTIPLEX_OPTS | self.opts)
        )

    def rsync_download(
        self,
        remote_path: str | Iterable[str],
        local_path: str = '.',
        arg: str = '-ac', *args: str,
        verbose: bool = False,
        callback: Callable[[RsyncEvent], None] = NOOP
    ):
        '''rsync files from `remote_path` to `local_path`
        
        - see `rsync` for more details
        '''
        if isinstance(remote_path, str):
            source_path = f'{self._host}:{remote_path}'
        else:
            source_path = [f'{self._host}:{path}' for path in remote_path]
        return rsync(
            source_path, local_path, arg, *args,
            verbose=verbose, callback=callback, **(_MULTIPLEX_OPTS | self.opts)
        )

    def open(
        self, path: str, mode: _MODE_STR = 'r', *,
        encoding: str = 'UTF-8', errors='replace', verbose: bool = False
    ) -> TextIOWrapper | BufferedIOBase:
        '''open a file-like object for a remote file over SSH

        - any valid combination of read, write, append, text,
          and binary is allowed
        - seeking and `+` modes are not allowed
        - `opts` keywords set `ssh_config` options, e.g. `port=22`
        - `verbose` sends and remote STDERR error output to the python STDOUT
        '''
        return open(
            self._host, path, mode, encoding=encoding, errors=errors,
            verbose=verbose, **(_MULTIPLEX_OPTS | self.opts)
        )

    def Popen(
        self, cmd: Iterable[str] | str, *,
        text: bool | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        stdin: int | IO | None = None,
        stdout: int | IO | None = None,
        stderr: int | IO | None = None,
        bufsize: int = -1,
        pipesize: int = -1,
        cwd: str | None = None,
        shell: bool = False,
    ) -> _Popen:
        '''start a command on the remote host, similar to `subprocess.Popen`

        - `cwd` changes the remote directory before execution
        - other arguments work the same as for `subprocess.Popen`
        '''
        return Popen(
            self._host, cmd, shell=shell, cwd=cwd,
            text=text, encoding=encoding, errors=errors,
            stdin=stdin, stdout=stdout, stderr=stderr,
            bufsize=bufsize, pipesize=pipesize, **(_MULTIPLEX_OPTS | self.opts)
        )

    def __call__(
        self, cmd: Iterable[str] | str, *,
        check: bool = False,
        text: bool | None = None,
        encoding: str | None = None,
        errors: str | None = None,
        stdin: int | IO | None = None,
        stdout: int | IO | None = None,
        stderr: int | IO | None = None,
        input: str | bytes | None = None,
        capture_output: bool = False,
        bufsize: int = -1,
        pipesize: int = -1,
        cwd: str | None = None,
        shell: bool = False
    ) -> CompletedProcess:
        '''run a command on the remote host, similar to `subprocess.run`

        - `cwd` changes the remote directory before execution
        - other arguments work the same as for `subprocess.run`
        '''
        return run(
            self._host, cmd, shell=shell, cwd=cwd,
            check=check, text=text, encoding=encoding, errors=errors,
            stdin=stdin, stdout=stdout, stderr=stderr,
            input=input, capture_output=capture_output,
            bufsize=bufsize, pipesize=pipesize, **(_MULTIPLEX_OPTS | self.opts)
        )


class TextFile(TextIOWrapper):
    '''text file opened over SSH'''

    def __init__(
        self, host: str | Host, path: str, mode: _MODE_STR = 'r',
        encoding: str = 'UTF-8', errors='replace',
        **opts: str | int | float | bool
    ):
        # no binary allowed
        if 'b' in mode:
            raise ValueError('TextFile cannot be opened in binary mode')
        # store arguments
        if isinstance(host, Host):
            self._host, host_opts = host.host, host.opts
            self._opts = _MULTIPLEX_OPTS | host_opts | opts
        else:
            self._host = host
            self._opts = dict(opts)
        self._path = path
        self._mode = mode
        self._encoding = encoding
        self._errors = errors
        self._proc = None
        # open the connection and file
        self.open()

    def open(self) -> 'TextFile':
        if self._proc:
            return self
        args = _ssh_opt_args(self._opts)
        # this ID is used to ensure stderr has actually finished transmitting
        id_b = bytes(random.choices(_ID_CHARS, k=22))
        id = id_b.decode()
        # set up & run a `cat` command such that:
        # - avoid acknowledgement on file error: `(...)<file`:
        # - csh/tcsh compatibility: `> /dev/stderr` instead of `>&2`
        # - csh/tcsh compatibility: `()` instead of `{}`
        # - prevent aliasing: `\` prefixes for `echo` and `cat`
        # - spaces between certain tokens for compatibility w/ some shells
        # read mode
        if self._mode in {'r', 'rt', 'tr'}:
            cmd = ['ssh', *args, self._host, '--', (
                rf'( \echo {id} > /dev/stderr; \cat; ) < {quote(self._path)}'
                rf' || \echo error {id} > /dev/stderr'
            )]
            self._proc = _Popen(
                cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE,
                encoding=self._encoding, errors=self._errors
            )
        # write mode
        elif self._mode in {'w', 'wt', 'tw'}:
            cmd = ['ssh', *args, self._host, '--', (
                rf'( \echo {id} > /dev/stderr; \cat; ) > {quote(self._path)}'
                rf' || \echo error {id} > /dev/stderr'
            )]
            self._proc = _Popen(
                cmd, stdin=PIPE, stdout=DEVNULL, stderr=PIPE,
                encoding=self._encoding, errors=self._errors
            )
        # append mode
        elif self._mode in {'a', 'at', 'ta'}:
            cmd = ['ssh', *args, self._host, '--', (
                rf'( \echo {id} > /dev/stderr; \cat; ) >> {quote(self._path)}'
                rf' || \echo error {id} > /dev/stderr'
            )]
            self._proc = _Popen(
                cmd, stdin=PIPE, stdout=DEVNULL, stderr=PIPE,
                encoding=self._encoding, errors=self._errors
            )
        # unknown file mode
        else:
            raise ValueError('SSH text file mode must be r, w, or a')
        # read stderr until id is found
        stderr = b''
        while id_b not in stderr:
            # read the next readable block of bytes from the stderr buffer
            stderr_fd = cast(TextIOWrapper, self._proc.stderr)
            if data := cast(BufferedReader, stderr_fd.buffer).read1():
                stderr += data
            # EOF before id?
            else:
                self.close()
                msg = 'Unexpected stderr from ssh'
                raise OSError(32, msg, f'{self._host}:{self._path}')
        # other stderr (actual error) text before/with id?
        if stderr.rstrip(b'\n') != id_b:
            self.close()
            msg = stderr.decode('UTF-8', 'replace')
            raise _stderr2oserror(msg, f'{self._host}:{self._path}')
        return self

    def close(self):
        if self._proc:
            if self._proc.stdin:
                self._proc.stdin.close()
            if self._proc.stdout:
                self._proc.stdout.close()
            if self._proc.stderr:
                self._proc.stderr.close()
            if self._proc.wait(10) is None:
                self._proc.terminate()
                if self._proc.wait(1) is None:
                    self._proc.kill()
                    self._proc.wait()
            self._proc.__del__()
            self._proc = None

    @property
    def closed(self) -> bool:
        return self._proc is None

    def __enter__(self) -> 'TextFile':
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self):
        self.close()

    def _get_file(self) -> TextIOWrapper:
        if self._proc:
            if file := self._proc.stdout:
                return cast(TextIOWrapper, file)
            if file := self._proc.stdin:
                return cast(TextIOWrapper, file)
        raise ValueError('I/O operation on closed file')

    def fileno(self) -> int:
        return self._get_file().fileno()

    def flush(self):
        self._get_file().flush()

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def errors(self) -> str | None:
        return self._errors

    @property
    def newlines(self) -> str | tuple[str, ...] | None:
        return self._get_file().newlines

    def isatty(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._get_file().tell()

    def readable(self) -> bool:
        ans = self._proc and self._proc.stdout and self._proc.stdout.readable()
        return bool(ans)

    def read(self, size=-1) -> str:
        return self._get_file().read(size)

    def readline(self, size=-1) -> str:
        return self._get_file().readline(size)

    def readlines(self, hint: int = -1) -> list[str]:
        return self._get_file().readlines(hint)

    def writable(self) -> bool:
        ans = self._proc and self._proc.stdin and self._proc.stdin.writable()
        return bool(ans)

    def write(self, text: str) -> int:
        return self._get_file().write(text)

    def __next__(self) -> str:
        if line := self.readline():
            return line
        raise StopIteration

    @property
    def host(self) -> str:
        '''hostname'''
        return self._host

    @property
    def path(self) -> str:
        '''remote path (not including hostname)'''
        return self._path

    @property
    def name(self) -> str:
        '''remote path including hostname, e.g. "user@host:path/to/file.txt"'''
        return f'{self._host}:{self._path}'

    def __repr__(self) -> str:
        name = self.__class__.__name__
        return  f'{name}({self._host!r}, {self._path!r}, {self._mode!r}'


class BinaryFile(BufferedIOBase):
    '''binary file opened over SSH'''

    def __init__(
        self, host: str | Host, path: str, mode: _MODE_STR = 'rb',
        **opts: str | int | float | bool
    ):
        # binary required
        if 'b' not in mode:
            raise ValueError('BinaryFile must be opened in binary mode')
        # store arguments
        if isinstance(host, Host):
            self._host, host_opts = host.host, host.opts
            self._opts = _MULTIPLEX_OPTS | host_opts | opts
        else:
            self._host = host
            self._opts = dict(opts)
        self._path = path
        self._mode = mode
        self._proc = None
        # open the connection and file
        self.open()

    def open(self) -> 'BinaryFile':
        if self._proc:
            return self
        args = _ssh_opt_args(self._opts)
        # this ID is used to ensure stderr has actually finished transmitting
        id_b = bytes(random.choices(_ID_CHARS, k=22))
        id = id_b.decode()
        # set up & run a `cat` command such that:
        # - avoid acknowledgement on file error: `(...)<file`:
        # - csh/tcsh compatibility: `> /dev/stderr` instead of `>&2`
        # - csh/tcsh compatibility: `()` instead of `{}`
        # - prevent aliasing: `\` prefixes for `echo` and `cat`
        # - spaces between certain tokens for compatibility w/ some shells
        # read mode
        if self._mode in {'rb', 'br'}:
            cmd = ['ssh', *args, self._host, '--', (
                rf'( \echo {id} > /dev/stderr; \cat; ) < {quote(self._path)}'
                rf' || \echo error {id} > /dev/stderr'
            )]
            self._proc = _Popen(cmd, stdin=DEVNULL, stdout=PIPE, stderr=PIPE)
        # write mode
        elif self._mode in {'wb', 'bw'}:
            cmd = ['ssh', *args, self._host, '--', (
                rf'( \echo {id} > /dev/stderr; \cat; ) > {quote(self._path)}'
                rf' || \echo error {id} > /dev/stderr'
            )]
            self._proc = _Popen(cmd, stdin=PIPE, stdout=DEVNULL, stderr=PIPE)
        # append mode
        elif self._mode in {'ab', 'ba'}:
            cmd = ['ssh', *args, self._host, '--', (
                rf'( \echo {id} > /dev/stderr; \cat; ) >> {quote(self._path)}'
                rf' || \echo error {id} > /dev/stderr'
            )]
            self._proc = _Popen(cmd, stdin=PIPE, stdout=DEVNULL, stderr=PIPE)
        # unknown file mode
        else:
            raise ValueError('SSH binary file mode must be rb, wb, or ab')
        # read stderr until id is found
        stderr = b''
        while id_b not in stderr:
            # read the next readable block of bytes from the stderr buffer
            if data := cast(BytesIO, self._proc.stderr).read1():
                stderr += data
            # EOF before id?
            else:
                self.close()
                msg = 'Unexpected stderr from ssh'
                raise OSError(32, msg, f'{self._host}:{self._path}')
        # other stderr (actual error) text before/with id?
        if stderr.rstrip(b'\n') != id_b:
            self.close()
            msg = stderr.decode('UTF-8', 'replace')
            raise _stderr2oserror(msg, f'{self._host}:{self._path}')
        return self

    def close(self):
        if self._proc:
            if self._proc.stdin:
                self._proc.stdin.close()
            if self._proc.stdout:
                self._proc.stdout.close()
            if self._proc.stderr:
                self._proc.stderr.close()
            if self._proc.wait(10) is None:
                self._proc.terminate()
                if self._proc.wait(1) is None:
                    self._proc.kill()
                    self._proc.wait()
            self._proc.__del__()
            self._proc = None

    @property
    def closed(self) -> bool:
        return self._proc is None

    def __enter__(self) -> 'BinaryFile':
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __del__(self):
        self.close()

    def _get_file(self) -> BytesIO:
        if self._proc:
            if file := self._proc.stdout:
                return cast(BytesIO, file)
            if file := self._proc.stdin:
                return cast(BytesIO, file)
        raise ValueError('I/O operation on closed file')

    def fileno(self) -> int:
        return self._get_file().fileno()

    def flush(self):
        self._get_file().flush()

    def isatty(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return self._get_file().tell()

    def readable(self) -> bool:
        ans = self._proc and self._proc.stdout and self._proc.stdout.readable()
        return bool(ans)

    def read(self, size=-1) -> bytes:
        return self._get_file().read(size)

    def read1(self, size: int = -1) -> bytes:
        return self._get_file().read1(size)

    def readinto(self, buffer: bytearray | memoryview) -> int:
        return self._get_file().readinto(buffer)

    def readinto1(self, buffer: bytearray | memoryview) -> int:
        return self._get_file().readinto1(buffer)

    def readline(self, size=-1) -> bytes:
        return self._get_file().readline(size)

    def readlines(self, hint: int = -1) -> list[bytes]:
        return self._get_file().readlines(hint)

    def writable(self) -> bool:
        ans = self._proc and self._proc.stdin and self._proc.stdin.writable()
        return bool(ans)

    def write(self, data: bytes) -> int:
        return self._get_file().write(data)

    def __next__(self) -> bytes:
        if line := self.readline():
            return line
        raise StopIteration

    @property
    def host(self) -> str:
        '''hostname'''
        return self._host

    @property
    def path(self) -> str:
        '''remote path (not including hostname)'''
        return self._path

    @property
    def name(self) -> str:
        '''remote path including hostname, e.g. "user@host:path/to/file.txt"'''
        return f'{self._host}:{self._path}'

    def __repr__(self) -> str:
        name = self.__class__.__name__
        return  f'{name}({self._host!r}, {self._path!r}, {self._mode!r}'


class PrematureExit(CalledProcessError):
    '''`CalledProcessError` subclass raised when Shell exits uncleanly'''


class Shell(MutableMapping):
    '''SSH connection to host emulating the experience of the remote shell

    - Call a shell object like a function to run a command on the remote shell
    - Use it like a dictionary to access remote environment variables

    starting args:
    - `host` hostname and optionally username, e.g. `user@host`
    - `text` set `True` for `str` output, `False` for `bytes` output
    - `tee` sends remote process output to a local file or function callback
      *in addition* to the result's `stdout` attribute, which is always filled
        - set `True` to send to STDOUT (default)
        - set `False` to send *only* to the result's `stdout` attribute
        - file-like object to send to that object's `write()` function
        - function to send to the function as callbacks
        - Note: input is typically in <= 4 KiB chunks
    - `capture` includes output data/text in response `stdout`
        - Note: response `stderr` is not used

    basic usage:
    ```
    with Shell('hostname') as sh:        # start the shell
        sh('echo hello world')           # run a command
        if 'FOO_BAR' not in sh:          # check for environment variables
            sh['FOO_BAR'] = 'baz qux'    # and get/set environment variables
        remote_environ = dict(sh)        # get the full environment dict
    ```

    caveats:
    - tested only on modern versions of `sh` (POSIX shell), `bash`, `dash`
      - intended to work as generally as possible, but your milage may vary
    - assumes that shell flushes after `printf` newlines, may break otherwise
    '''
    def __init__(
        self,
        host: str | Host,
        text: bool = True,
        tee: (
            bool | IOBase | Callable[[str], Any] | Callable[[bytes], Any]
        ) = True,
        capture: bool = True
    ):

        self.text = bool(text)
        '''set `True` for `str` output, `False` for `bytes` output'''

        self.tee = tee
        '''send remote process output to a local file or function callback

        - `True` to send to STDOUT (default)
        - `False` / `None` (any falsey value) don't send anywhere
        - file-like object to send to that object's `write()` function
        - function to send to the function as callbacks
        - Note: input is typically in <= 4 KiB chunks
        '''

        self.capture = capture
        '''include output data/text in response `stdout`'''

        self.shell = ''
        '''apparent shell being used on remote host
        
        - determined by `$0`, typically sh, bash, dash, ksh, zsh
        - currently determined only at shell launch
          (may become dynamic in a later release)
        '''

        self.shell_version = ''
        '''apparent shell version being used on remote host

        - determined by `$0` and the respective `_VERSION` env variable
        - currently determined only at shell launch
          (may become dynamic in a later release)
        '''

        self._shopts: ShellOptions = ShellOptions(self)
        self._host: str | Host = host
        self._data: bytes = b''
        self._id: bytes = b''
        self._proc: _Popen | None = None
        self._cache: dict = {}

    def connect(self):
        '''connect to the shell with a call to `ssh`'''
        if self._proc:
            raise CalledProcessError(1, 'ssh', 'shell already connected')
        self._cache.clear()
        self._data = b''
        # make command loop
        self._id: bytes = bytes(random.choices(_ID_CHARS, k=22))
        id = self._id.decode('utf-8')
        cmd = (
            # trap exit signals
            rf'''trap 'printf "\377%03d{id}\377" "$?"' EXIT && '''
            # read null-separated inputs
            'while \\read -r -d \'\' CMD; do'
            # run each input command
            ' \\eval "$CMD";'
            # report (returncode, ID)
            f' \\printf \'\\377%03d{id}\\377\' "$?";'
            # redirect stderr remote-side to avoid race conditions
            ' done 2>&1'
        )
        # start SSH
        if isinstance(self._host, Host):
            self._proc = self._host.Popen(
                cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT
            )
        else:
            self._proc = Popen(
                self._host,
                cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT
            )
        # test new connection to ensure that everything is working
        out = self._soft_call('printf "%s" "$0"')
        self.shell = out.rpartition('/')[2].lstrip('-')
        self.shell_version = ''
        if _IS_VALID_ENV_VAR_NAME(self.shell):
            cmd = f'printf "%s" "${{{self.shell.upper()}_VERSION}}"'
            self.shell_version = self._soft_call(cmd)


    def disconnect(self, timeout: int | float = 10):
        '''exit the shell, SIGTERM until `timeout`, then KILL if needed'''
        if self._proc:
            self._cache.clear()
            self._data = b''
            self._proc.terminate()
            try:
                self._proc.wait(timeout)
            except TimeoutExpired:
                self._proc.kill()
                self._proc.wait()
            self._proc = None

    # alias for shell terminology
    exit = disconnect

    @property
    def closed(self) -> bool:
        return not (self._proc and self._proc.poll() is None)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        self.disconnect()

    def __call__(
        self,
        cmd: Iterable[str] | str = ':',
        check: bool = False,
        *,
        text: bool | None = None,
        encoding: str = 'UTF-8',
        errors: str = 'replace',
        tee: (
            None | bool | IOBase | Callable[[str], Any] | Callable[[bytes], Any]
        ) = None,
        capture: bool = True
    ) -> CompletedProcess:
        '''run a remote command

        - `cmd` is the command to run on the shell
            - `[str, ...]` is a single command executed verbatim
            - a single `str` is sent as-is and interpreted by the shell
        - `check` raises a `CalledProcessError` for a non-zero returncode
        - `text` defaults to the value from `Shell(..., text=...)`
        - `tee` defaults to the value from `Shell(..., tee=...)`
        - `capture` saves process output to the result's `stdout` attribute
        '''
        # note: a core assumption is that the full return code and ID string
        #       is always sent in a single read1 block, this *should* be true
        #       for shells because they buffer on lines and code+ID << 4 KiB
        text = self.text if text is None else text
        if tee := (self.tee if tee is None else tee):
            if tee_func := getattr(tee, 'write', None):
                tee = tee_func
            elif not isinstance(tee, Callable):
                tee = _write_flush_stdout if text else _write_flush_stdout_b
        # require process to be well-connected
        if not (
            self._proc and self._proc.poll() is None
            and isinstance(self._proc.stdin, BufferedIOBase)
            and isinstance(self._proc.stdout, BufferedIOBase)
        ):
            raise ValueError('Shell not ready (may need to be connected first)')
        # convert command to shell token string
        if not isinstance(cmd, str):
            cmd = ' '.join(map(quote, cmd))
        # clear cached values
        self._cache.clear()
        # send the command to remote shell loop
        self._proc.stdin.write(f'{cmd}\0'.encode('utf-8'))
        self._proc.stdin.flush()
        # loop over output
        data = []
        for msg in self._iter_output(capture):
            if isinstance(msg, int):
                output = ('' if text else b'').join(data) if capture else None
                if check and msg:
                    raise CalledProcessError(msg, cmd, output)
                return CompletedProcess(cmd, msg, output)
            elif tee or capture:
                msg = msg.decode(encoding, errors) if text else msg
                if tee:
                    (sys.stdout if text else sys.stdout.buffer).write(msg)
                    sys.stdout.flush()
                if capture:
                    data.append(msg)
        # EOF before trigger msg
        output = ('' if text else b'').join(data) if capture else None
        raise PrematureExit(255, cmd, output)

    def _iter_output(self, capture: bool) -> Iterator:
        '''yield blocks of data from stream, or returncode'''
        # msg format is: \377 {code:03d} {id:22} \377
        if not (
            self._proc and self._proc.poll() is None
            and isinstance(self._proc.stdout, BufferedIOBase)
        ):
            raise ValueError('Shell not ready (may need to be connected first)')
        # loop through read1()
        prev_data = b''
        while data := self._proc.stdout.read1():
            # bring in any possible partial msg from previous loop(s)
            if prev_data:
                data = prev_data + data
                prev_data = b''
            # loop through and \xFF instances inside string
            n = len(data)
            i = 0
            while 0 <= (i := data.find(b'\377', i)):
                # check for returncode and ID character match
                if (
                    # first three chars are digits
                    all(char in _DIGITS for char in data[(i + 1):(i + 4)])
                    # next 22 chars match `id`
                    and (id := data[(i + 4):(i + 26)]) == self._id[:len(id)]
                    # last char is \xFF
                    and data[(i + 26):(i + 27)] == b'\377'
                ):
                    # yield data before possible msg
                    if i and capture:
                        yield data[:i]
                    # possible msg intersects with end of block
                    if i + 26 >= n:
                        # remember possible msg for next read1() cycle
                        prev_data = data[i:]
                        break
                    # found a returncode msg
                    else:
                        # yield returncode (int)
                        yield int(data[(i + 1):(i + 4)])
                        # remember any remaining data for next read1() cycle
                        prev_data = data[(i + 26):]
                        break
                i += 1
            # no possible msg found
            else:
                if capture:
                    # check for incomplete UTF-8 sequence
                    u = data[-3:].rjust(3)
                    # 1 of 2-char sequence
                    if u[2] & 0b11100000 == 0b11000000:
                        prev_data = data[-1:]
                        if n > 1:
                            yield data[:-1]
                    # 2 of 3-char sequence
                    elif (
                        u[1] & 0b11110000 == 0b11100000 and
                        u[2] & 0b11000000 == 0b10000000
                    ):
                        prev_data = data[-2:]
                        if n > 2:
                            yield data[:-2]
                    # 3 of 4-char sequence
                    elif (
                        u[0] & 0b11111000 == 0b11110000 and
                        u[1] & 0b11000000 == 0b10000000 and
                        u[2] & 0b11000000 == 0b10000000
                    ):
                        prev_data = data[-3:]
                        if n > 3:
                            yield data[:-3]
                    # no chance of breaking a valid UTF-8 sequence otherwise
                    else:
                        yield data

    def _soft_call(self, cmd: Iterable[str] | str) -> str:
        '''same as __call__ but with some defaults and saving the cache'''
        saved_cache = self._cache
        output = self(
            cmd, check=True, tee=False, capture=True,
            text=True, encoding='UTF-8', errors='replace'
        ).stdout
        self._cache = saved_cache
        return output

    def source(
        self,
        path: str,
        args: Iterable[str] = (),
        check: bool = False,
        *,
        text: bool | None = None,
        encoding: str = 'UTF-8',
        errors: str = 'replace',
        tee: (
            None | bool | IOBase | Callable[[str], Any] | Callable[[bytes], Any]
        ) = None,
        capture: bool = True

    ) -> CompletedProcess:
        '''equivalent to `run(['source', path, *args], ...)`'''
        return self(
            ['source', path, *args], check, tee=tee, capture=capture,
            text=text, encoding=encoding, errors=errors
        )

    def ls(self, path: str = '') -> tuple[str, ...]:
        '''list remote directory'''
        # construct ls-like command with machine-readable output
        if path and path not in '.':
            qpath = quote(path)
            cmd = (
                # test if path is a non-directory file
                f'\\[ -e {qpath} -a \\! -d {qpath} ]'
                # if so, then print the file alone
                f' && printf "%s\\0" {qpath}'
                # otherwise, cd to the directory and run find
                f' || (\\cd {quote(path)} && \\find . -maxdepth 1 -print0)'
            )
        else:
            # a find without cd-ing works for the current (.) directory
            cmd = '\\find . -maxdepth 1 -print0'
        # run cmd and convert to sorted tuple, and cut off leading ./ from find
        return tuple(sorted({
            name[2:] if name[:2] == './' else name
            for name in self._soft_call(cmd).split('\0') if name not in '..'
        }))

    def cd(self, path: str = '') -> str:
        '''change remote present working directory, returns the new `pwd`'''
        cmd = f'\\cd {quote(path)} && \\pwd' if path else '\\cd && \\pwd'
        if not (out := self._soft_call(cmd)).endswith('\n'):
            raise ValueError
        pwd = self._cache['pwd'] = out[:-1]
        return pwd

    @property
    def pwd(self) -> str:
        '''remote present working directory

        - setting `pwd` is equivalent to `cd(pwd, expand=False)`
        '''
        try:
            return self._cache['pwd']
        except KeyError:
            if not (out := self._soft_call('\\pwd')).endswith('\n'):
                raise ValueError('invalid pwd output')
            pwd = self._cache['pwd'] = out[:-1]
            return pwd
    @pwd.setter
    def pwd(self, path: str):
        self.cd(path)

    @property
    def path(self) -> tuple[str, ...]:
        '''tuple of the remote shell's executable search paths

        - may be set with a similar tuple or a scalar string
        '''
        return tuple(self._get_env().get('PATH', '').split(':'))
    @path.setter
    def path(self, path: str | tuple[str, ...]):
        self['PATH'] = ':'.join([path] if isinstance(path, str) else path)

    @property
    def shopts(self) -> 'ShellOptions':
        '''the `set -o` and `shopt` options for the shell'''
        return self._shopts

    def _get_env(self) -> dict[str, str]:
        '''unsafe env access'''
        try:
            return self._cache['env']
        except KeyError:
            env = self._cache['env'] = {k: v for k, _, v in (
                line.partition('=')
                for line in self._soft_call('\\env -0').split('\0') if line
            )}
            return env

    def __setitem__(self, key: str, value: str):
        # ensure valid key and value
        if not _IS_VALID_ENV_VAR_NAME(key):
            raise ValueError(f'invalid environment variable name: {key}')
        if not isinstance(value, str):
            if isinstance(value, bytes):
                value = value.decode('utf-8', 'replace')
            elif isinstance(value, (int, float)):
                value = str(value)
            else:
                raise ValueError(f'invalid environment value: {value!r}')
        # set on remote shell
        self._soft_call(['export', f'{key}={value}'])
        # set in local cache
        self._cache.get('env', {})[key] = value

    def __delitem__(self, key: str):
        # ensure valid key
        if not _IS_VALID_ENV_VAR_NAME(key):
            raise ValueError(f'invalid environment variable name: {key}')
        # set on remote shell
        self._soft_call(['unset', key])
        # set in local cache
        try:
            del self._cache['env'][key]
        except KeyError:
            pass

    def __len__(self) -> int:
        '''environment variable count'''
        return len(self._get_env())
    def __getitem__(self, key: str) -> str:
        '''get environment variable'''
        return self._get_env()[key]
    def __iter__(self) -> Iterator[str]:
        return iter(self._get_env())
    def __contains__(self, key: str) -> bool:
        return key in self._get_env()
    def keys(self) -> Iterable[str]:
        return self._get_env().keys()
    def items(self) -> Iterable[tuple[str, str]]:
        return self._get_env().items()
    def values(self) -> Iterable[str]:
        return self._get_env().values()
    def get(self, key: str, default: Any = None) -> str | Any:
        return self._get_env().get(key, default)
    def __eq__(self, other):
        return self._get_env() == other
    def __ne__(self, other):
        return self._get_env() != other


class ShellOptions(MutableMapping):
    '''expose and write to shell options with `shopt` and `set -o`'''
    def __init__(self, shell: Shell) -> None:
        self._shell_ref = weakref.ref(shell)
    def _get_opts(self) -> dict[str, bool]:
        # ensure shell is still around, and dereference it for use in this func
        if (shell := self._shell_ref()) is None:
            raise ValueError('shell no longer active')
        # try just returning from the shell's cache
        try:
            return shell._cache['opts']
        except KeyError:
            # since cache wasn't there, ask the remote shell instead
            opts = {}
            out = shell._soft_call(r'\shopt 2>/dev/null; \set -o 2>/dev/null')
            # parse the `shopt` and `set -o` output lines
            for line in out.splitlines():
                if len(tokens := line.split()) == 2:
                    if tokens[1] in ('on', 'off'):
                        opts[tokens[0]] = tokens[1] == 'on'
            shell._cache['opts'] = opts
            return opts
    def __contains__(self, key: str) -> bool:
        return key in self._get_opts()
    def __delitem__(self, key: str) -> None:
        raise NotImplementedError('shell options cannot be removed')
    def __getitem__(self, key: str) -> bool:
        return self._get_opts()[key]
    def __iter__(self) -> Iterator[str]:
        return iter(self._get_opts())
    def __len__(self) -> int:
        return len(self._get_opts())
    def __setitem__(self, key: str, value: bool) -> None:
        # ensure shell is still around, and dereference it for use in this func
        if (shell := self._shell_ref()) is None:
            raise ValueError('shell no longer active')
        # convert value to bool/set/shopt args
        value = bool(value)
        set_sign = '-' if value else '+'
        shopt_arg = '-s' if value else '-u'
        # compose and call the cmd to set option
        cmd = f'set {set_sign}o {quote(key)} 2>/dev/null'
        cmd += f' || shopt {shopt_arg} {quote(key)}'
        shell._soft_call(cmd)
        # remember in the cache
        if (opts := shell._cache.get('opts')) is not None:
            opts[key] = value
    def items(self) -> ItemsView[str, bool]:
        return self._get_opts().items()
    def keys(self) -> KeysView:
        return self._get_opts().keys()
    def values(self) -> Iterable[bool]:
        return self._get_opts().values()


def _rsync_events(file: BufferedIOBase | bytes) -> Generator[RsyncEvent]:
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
                        eta=datetime.timedelta(hours=h, minutes=m, seconds=s),
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


def rsync(
    source_path: str | Iterable[str],
    dest_path: str = '.',
    arg: str = '-ac', *args: str,
    verbose: bool = False,
    callback: Callable[[RsyncEvent], None] = NOOP,
    **opts: str | int | float | bool
):
    '''rsync files from `source_path` to `dest_path`

    - `arg` and optionally additional `args` set `rsync` arguments like flags
      - `--rsh`, `--verbose`, `--quiet`, and `--progress` may be ignored
    - `verbose` prints the `rsync` command and shows `rsync`'s STDERR
    - `callback` is called once for each update output from `rsync`
    - `opts` keywords set `ssh_config` options, e.g. `port=22`
    - raises a `CalledProcessError` if `rsync` fails
    '''
    src = [source_path] if isinstance(source_path, str) else list(source_path)
    all_args = [arg, *args]
    if opts:
        all_args.append(f'-essh {_ssh_opt_args(opts)}')
    cmd = 'rsync', *all_args, '-vq', '--progress', *src, (dest_path or '.')
    if verbose:
        sys.stdout.write(f'{join(cmd)}\n')
    stderr = None if verbose else STDOUT
    with _Popen(cmd, stdin=DEVNULL, stdout=PIPE, stderr=stderr) as proc:
        for event in _rsync_events(cast(BufferedIOBase, proc.stdout)):
            callback(event)
    if proc.returncode:
        raise CalledProcessError(proc.returncode, cmd)


def open(
    host: str | Host, path: str, mode: _MODE_STR = 'r',
    encoding: str = 'UTF-8', errors='replace', **opts: str | int | float | bool
) -> TextIOWrapper | BufferedIOBase:
    '''open a file-like object for a remote file over SSH

    - any valid combination of read, write, append, text, and binary is allowed
    - seeking and `+` modes are not allowed
    - `opts` keywords set `ssh_config` options, e.g. `port=22`
    '''
    if mode in {'r', 'rt', 'tr', 'w', 'wt', 'tw', 'a', 'at', 'ta'}:
        return TextFile(host, path, mode, encoding, errors, **opts)
    elif mode in {'rb', 'br', 'wb', 'bw', 'ab', 'ba'}:
        return BinaryFile(host, path, mode, **opts)
    else:
        raise ValueError(f'SSH file cannot be opened in mode {mode}')

def Popen(
    host: str | Host,
    cmd: Iterable[str] | str,
    *,
    text: bool | None = None,
    encoding: str | None = None,
    errors: str | None = None,
    stdin: int | IO | None = None,
    stdout: int | IO | None = None,
    stderr: int | IO | None = None,
    bufsize: int = -1,
    pipesize: int = -1,
    cwd: str | None = None,
    shell: bool = False,
    **opts: str | int | float | bool
) -> _Popen:
    '''start a command on the remote host, similar to `subprocess.Popen`

    - `host` is the hostname and optionally username, e.g. `user@host`
    - `opts` keywords set `ssh_config` options, e.g. `port=22`
    - `cwd` changes the remote directory before execution
    - other arguments work the same as for `subprocess.Popen`
    '''
    # reproduce eccentric way that subprocess handles cmd and shell
    if shell:
        if not isinstance(cmd, str):
            cmd = next(iter(cmd))
    else:
        cmd = quote(cmd) if isinstance(cmd, str) else join(cmd)
    # add cd for cwd
    if cwd:
        cmd = f'cd {quote(cwd)} || exit $?; ' + cmd
    # dereference host info
    if isinstance(host, Host):
        host, host_opts = host.host, host.opts
        opts = _MULTIPLEX_OPTS | host_opts | opts
    # run the command
    return _Popen(
        ['ssh', host, *_ssh_opt_args(opts), '--', cmd],
        text=text, encoding=encoding, errors=errors,
        stdin=stdin, stdout=stdout, stderr=stderr,
        bufsize=bufsize, pipesize=pipesize
    )


def run(
    host: str | Host,
    cmd: Iterable[str] | str,
    *,
    check: bool = False,
    text: bool | None = None,
    encoding: str | None = None,
    errors: str | None = None,
    stdin: int | IO | None = None,
    stdout: int | IO | None = None,
    stderr: int | IO | None = None,
    input: str | bytes | None = None,
    capture_output: bool = False,
    bufsize: int = -1,
    pipesize: int = -1,
    cwd: str | None = None,
    shell: bool = False,
    **opts: str | int | float | bool
) -> CompletedProcess:
    '''run a command on the remote host, similar to `subprocess.run`

    - `host` is the hostname and optionally username, e.g. `user@host`
    - `opts` keywords set `ssh_config` options, e.g. `port=22`
    - `cwd` changes the remote directory before execution
    - other arguments work the same as for `subprocess.run`
    '''
    # reproduce eccentric way that subprocess handles cmd and shell
    if shell:
        if not isinstance(cmd, str):
            cmd = next(iter(cmd))
    else:
        cmd = quote(cmd) if isinstance(cmd, str) else join(cmd)
    # add cd for cwd
    if cwd:
        cmd = f'cd {quote(cwd)} || exit $?; ' + cmd
    # dereference host info
    if isinstance(host, Host):
        host, host_opts = host.host, host.opts
        opts = _MULTIPLEX_OPTS | host_opts | opts
    # run the command
    return _run(
        ['ssh', host, *_ssh_opt_args(opts), '--', cmd],
        check=check, text=text, encoding=encoding, errors=errors,
        stdin=stdin, stdout=stdout, stderr=stderr,
        input=input, capture_output=capture_output,
        bufsize=bufsize, pipesize=pipesize
    )


def _write_flush_stdout(text: str) -> None:
    '''write text to stdout, flush stdout'''
    sys.stdout.write(text)
    sys.stdout.flush()


def _write_flush_stdout_b(data: bytes) -> None:
    '''write data to stdout, flush stdout'''
    sys.stdout.buffer.write(data)
    sys.stdout.flush()


def _ssh_opt_args(opts: dict[str, str | int | float | bool] = {}) -> list[str]:
    '''convert `ssh_config` options to a list of `-o` arguments'''
    results = []
    for k, v in opts.items():
        if isinstance(v, bool):
            v = 'yes' if v else 'no'
        results.append(f'-o{k} {v}')
    return results


def _printf_esc(r: re.Match[str]) -> str:
    '''escape a character for `printf`'''
    return f'\\{ord(r[0]):03o}'


def quote(token: str) -> str:
    '''quote a sting token in the most cross-shell compatible way possible'''
    # simple_text_with_no_spaces_can_be_a_single_token
    if _NO_QUOTE_NEEDED(token):
        return token
    # 'works in single quotes, e.g. with "double quotes" and/or spaces inside'
    if _SINGLE_QUOTABLE(token):
        return f"'{token}'"
    # "double quotes are much more limited, but good for 'single quotes' inside"
    if _DOUBLE_QUOTABLE(token) :
        return f'"{token}"'
    # "dithered 'single quotes' and "'"double quotes" are complicated but short'
    if _DITHER_QUOTABLE(token):
        parts = []
        pos = 0
        while pos < len(token):
            if r := _RE_DITHER_QUOTE.match(token, pos):
                parts.append (f"'{r[0]}'" if r[1] else f'"{r[0]}"')
                pos = r.end()
            else:
                raise ValueError('what do I do?')
        return ''.join(parts)
    # "`printf 'Why?\012Why on Earth would you put a newline in a token???'`"
    return f'''"`printf '{_SUB_PRINTF_ESC(_printf_esc, token)}'`"'''


def join(tokens: Iterable[str]) -> str:
    '''convert verbatim argument list to safe command string'''
    return ' '.join(map(quote, tokens))


def __dir__() -> tuple[str, ...]:
    '''define items exported for use outside, e.g. for tab-completion'''
    return _EXPORTS


_EXPORTS = tuple(export.__name__ for export in (
    run,
    Host,
    Shell,
    ShellOptions,
    Popen,
    open,
    TextFile,
    BinaryFile,
    PrematureExit,
    RsyncEvent,
    rsync,
    rsync_decode,
    quote,
    join,
    NOOP
))
