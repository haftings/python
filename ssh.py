#! /usr/bin/env python3

'''Manage jobs across multiple processes and hosts'''

from collections.abc import Callable, Generator, Iterable, Iterator
from collections.abc import MutableMapping
from datetime import timedelta
from io import BytesIO, BufferedIOBase, TextIOWrapper, IOBase
from random import choices
import re
from subprocess import CompletedProcess, CalledProcessError
from subprocess import DEVNULL, PIPE, STDOUT, TimeoutExpired
from subprocess import Popen as _Popen, run as _run
import sys
from typing import NamedTuple, Literal, IO, Any
import weakref

_DIGITS = frozenset(b'0123456789')
_ID_CHARS = b'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz@_'
_IS_VALID_ENV_VAR_NAME = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*$').match
_MODE_STR = Literal[
    'r', 'rt', 'tr', 'rb', 'br',
    'w', 'wt', 'tw', 'wb', 'bw',
    'a', 'at', 'ta', 'ab', 'ba'
]
_MULTIPLEX_OPTS: dict[str, str] = {
    'loglevel': 'error',
    'controlmaster': 'auto',
    'controlpath': '~/.ssh/.%u@%h:%p.control',
    'controlpersist': 'no'
}
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


def NOOP(*args, **kwargs) -> None: '"no operation" function that does nothing'


class RsyncEvent(NamedTuple):
    '''an event from a running rsync command'''

    event: Literal['file', 'update', 'unknown'] | str
    '''type of event

    - `file` for the start of a file transfer
    - `update` for a progress update
    - `unknown` for an unrecognized event
    '''

    name: str | None = None
    '''file name or path'''

    bytes_sent: int | None = None
    '''bytes sent so far'''

    percent_complete: float | None = None
    '''percent complete so far, rounded, can be NaN e.g. for 0-length files'''

    bps: float | None = None
    '''transfer rate in bits per second'''

    eta: timedelta | None = None
    '''estimated time of arrival (upload or download)'''

    transfer_number: int | None = None
    '''rsync transfer number, starts with 1'''

    n_checked: int | None = None
    '''number of files checked, starts with 0, does not include current file'''

    n_total: int | None = None
    '''number of files to check in all, includes all files and directories'''

    raw: bytes = b''
    '''the raw binary output from rsync used to infer this event'''


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
        encoding: str = 'UTF-8', errors='replace',
        verbose: bool = False, **opts: str | int | float | bool,
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
        **opts
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

    def __init__(self, proc: _Popen, file: TextIOWrapper):
        self.proc = proc
        self.file = file
        # use weakref so that open(..).write(...) immediately flushes & closes
        self._finalize = weakref.finalize(
            self, self._close, self.file, self.proc, CalledProcessError
        )

    @staticmethod
    def _close(file: TextIOWrapper, proc: _Popen, CalledProcessError: type):
        file.close()
        if returncode := proc.wait():
            raise CalledProcessError(returncode, proc.args)

    def remove(self):
        self._finalize()

    @property
    def removed(self) -> bool:
        return not self._finalize.alive

    def close(self):
        self._close(self.file, self.proc, CalledProcessError)

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
        return self.file.readlines(hint)

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
            self, self._close, self.file, self.proc, CalledProcessError
        )

    @staticmethod
    def _close(file: BufferedIOBase, proc: _Popen, CalledProcessError: type):
        file.close()
        if returncode := proc.wait():
            raise CalledProcessError(returncode, proc.args)

    def remove(self):
        self._finalize()

    @property
    def removed(self) -> bool:
        return not self._finalize.alive

    def close(self):
        self._close(self.file, self.proc, CalledProcessError)

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
        return self.file.readlines(hint)

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

        self._host: str | Host = host
        self._data: bytes = b''
        self._pwd: str | None = None
        self._env: dict[str, str] | None = None
        self._id: bytes = b''
        self._proc: _Popen | None = None

    def connect(self):
        '''connect to the shell with a call to `ssh`'''
        if self._proc:
            raise CalledProcessError(1, 'ssh', 'shell already connected')
        # make command loop
        self._id: bytes = bytes(choices(_ID_CHARS, k=22))
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
        out = self('printf "%s" "$0"', check=True, text=True, tee=False).stdout
        self.shell = out.rpartition('/')[2].lstrip('-')
        self.shell_version = ''
        if _IS_VALID_ENV_VAR_NAME(self.shell):
            cmd = f'printf "%s" "${{{self.shell.upper()}_VERSION}}"'
            v = self(cmd, check=True, text=True, tee=False).stdout
            self.shell_version = v


    def close(self, timeout: int | float = 10):
        '''exit the shell, SIGTERM until `timeout`, then KILL if needed'''
        if self._proc:
            self._proc.terminate()
            try:
                self._proc.wait(timeout)
            except TimeoutExpired:
                self._proc.kill()
                self._proc.wait()
            self._proc = None

    # alias for shell terminology
    exit = close

    @property
    def closed(self) -> bool:
        return not (self._proc and self._proc.poll() is None)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        self.close()

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
            cmd = ' '.join(map(_quote, cmd))
        # clear cached environment and pwd
        self._env = self._pwd = None
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
            qpath = _quote(path)
            cmd = (
                # test if path is a non-directory file
                f'\\[ -e {qpath} -a \\! -d {qpath} ]'
                # if so, then print the file alone
                f' && printf "%s\\0" {qpath}'
                # otherwise, cd to the directory and run find
                f' || (\\cd {_quote(path)} && \\find . -maxdepth 1 -print0)'
            )
        else:
            # a find without cd-ing works for the current (.) directory
            cmd = '\\find . -maxdepth 1 -print0'
        # run the command
        output = self(cmd, check=True, text=True, tee=False).stdout
        # convert to sorted tuple, and cut off leading ./ from find
        return tuple(sorted({
            name[2:] if name[:2] == './' else name
            for name in output.split('\0') if name not in '..'
        }))

    def cd(self, path: str = '') -> str:
        '''change remote present working directory, returns the new `pwd`'''
        cmd = f'\\cd {_quote(path)} && \\pwd' if path else '\\cd && \\pwd'
        out: str = self(cmd, check=True, text=True, tee=False).stdout
        if not out.endswith('\n'):
            raise ValueError
        self._pwd = out[:-1]
        return self._pwd

    @property
    def pwd(self) -> str:
        '''remote present working directory

        - setting `pwd` is equivalent to `cd(pwd, expand=False)`
        '''
        if self._pwd is None:
            out: str = self('\\pwd', check=True, text=True, tee=False).stdout
            if not out.endswith('\n'):
                raise ValueError('invalid pwd output')
            self._pwd = out[:-1]
        return self._pwd
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

    def _get_env(self) -> dict[str, str]:
        '''unsafe env access'''
        if self._env is None:
            self._env = {
                k: v for k, _, v in (
                    line.partition('=') for line in self(
                        '\\env -0', check=True, text=True, tee=False
                    ).stdout.split('\0') if line
                )
            }
        return self._env

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
        self(['export', f'{key}={value}'], check=True, tee=False)
        # set in local cache
        if self._env is not None:
            self._env[key] = value

    def __delitem__(self, key: str):
        # ensure valid key
        if not _IS_VALID_ENV_VAR_NAME(key):
            raise ValueError(f'invalid environment variable name: {key}')
        # set on remote shell
        self(f'unset {key}', check=True, tee=False)
        # set in local cache
        if self._env is not None:
            try:
                del self._env[key]
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
        sys.stdout.write(f'{_join(cmd)}\n')
    stderr = None if verbose else STDOUT
    with _Popen(cmd, stdin=DEVNULL, stdout=PIPE, stderr=stderr) as proc:
        assert isinstance(proc.stdout, BufferedIOBase), 'rsync needs stdout'
        for event in _rsync_events(proc.stdout):
            callback(event)
    if proc.returncode:
        raise CalledProcessError(proc.returncode, cmd)


def open(
    host: str,
    path: str,
    mode: _MODE_STR = 'r',
    *,
    verbose: bool = False,
    encoding: str = 'UTF-8',
    errors='replace',
    **opts: str | int | float | bool
) -> TextIOWrapper | BufferedIOBase:
    '''open a file-like object for a remote file over SSH

    - any valid combination of read, write, append, text, and binary is allowed
    - seeking and `+` modes are not allowed
    - `opts` keywords set `ssh_config` options, e.g. `port=22`
    - `verbose` sends and remote STDERR error output to the python STDOUT
    '''
    err = None if verbose else DEVNULL
    if mode in {'r', 'rt', 'tr', 'rb', 'br'}:
        cmd = ['ssh', *_ssh_opt_args(opts), host, f'cat {_quote(path)}']
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
        cmd = ['ssh', *_ssh_opt_args(opts), host, f'cat > {_quote(path)}']
    elif mode in {'a', 'at', 'ta', 'ab', 'ba'}:
        cmd = ['ssh', *_ssh_opt_args(opts), host, f'cat >> {_quote(path)}']
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


def Popen(
    host: str, cmd: Iterable[str] | str, *,
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
    **opts
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
        cmd = _quote(cmd) if isinstance(cmd, str) else _join(cmd)
    # add cd for cwd
    if cwd:
        cmd = f'cd {_quote(cwd)} || exit $?; ' + cmd
    # run the command
    return _Popen(
        ['ssh', host, *_ssh_opt_args(opts), '--', cmd],
        text=text, encoding=encoding, errors=errors,
        stdin=stdin, stdout=stdout, stderr=stderr,
        bufsize=bufsize, pipesize=pipesize
    )


def run(
    host: str, cmd: Iterable[str] | str, *,
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
    **opts
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
        cmd = _quote(cmd) if isinstance(cmd, str) else _join(cmd)
    # add cd for cwd
    if cwd:
        cmd = f'cd {_quote(cwd)} || exit $?; ' + cmd
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


def __dir__() -> tuple[str, ...]:
    '''define items exported for use outside, e.g. for tab-completion'''
    return _EXPORTS


_EXPORTS = tuple(export.__name__ for export in (
    Host,
    Shell,
    open,
    TextFile,
    BinaryFile,
    PrematureExit,
    rsync,
    rsync_decode,
    RsyncEvent,
    run,
    Popen,
    NOOP,
))
