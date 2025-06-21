#! /usr/bin/env python3

'''Manage jobs across multiple processes and hosts'''

import re
from datetime import timedelta
from io import BytesIO, BufferedIOBase, TextIOWrapper
from random import choices
from subprocess import DEVNULL, PIPE, STDOUT, TimeoutExpired
from subprocess import Popen as _Popen, run as _run
from subprocess import CompletedProcess, CalledProcessError
from typing import NamedTuple, Literal, IO, Any
from collections.abc import Callable, Generator, Iterable, Iterator
from collections.abc import MutableMapping
import sys
import weakref

MODE_STR = Literal[
    'r', 'rt', 'tr', 'rb', 'br',
    'w', 'wt', 'tw', 'wb', 'bw',
    'a', 'at', 'ta', 'ab', 'ba'
]
_ID_CHARS = b'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz@_'
_ID_LENGTH = 22
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
_IS_VALID_ENV_VAR_NAME = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*$').match

def noop(*args, **kwargs) -> None:
    '''"no operation" function that does nothing'''

class RsyncEvent(NamedTuple):
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


class SSH:
    '''Class to handle SSH connection(s) to a host

    `SSH(...)`
    - `host` is the hostname and optional username, e.g. `user@host`
    - `opts` keywords set `ssh_config` options, e.g. `port=22`

    SSH sessions/connections are formed either once for each SSH operation:
    ```python
    ssh_host = SSH('user@host')    # no connection/session yet
    ssh_host(['echo', 'hello'])    # one full ssh session during run #1
    ssh_hsot(['echo', 'goodbye'])  # second full ssh session during run #2
    ```

    Or you can form a connection and reuse it using `with`:
    ```python
    with SSH('user@host') as ssh_host:  # session started
        ssh_host(['echo', 'hello'])     # uses existing session
        ssh_host(['echo', 'goodbye'])   # uses existing session again
    # session closes at the end of the `with` block
    ```
    '''
    def __init__(self, host: str, **opts: str) -> None:
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
            remote_cmd = 'echo; while :; do echo >&2; sleep 1; done'
            # start the ssh process
            opts = ssh_opt_args(_MULTIPLEX_OPTS | self.opts)
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

    def open(
        self, path: str, mode: MODE_STR = 'r', *,
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

class PrematureExit(CalledProcessError):
    '''`CalledProcessError` subclass raised when Shell exits uncleanly'''

class Shell(MutableMapping):
    '''SSH connection to host emulating the experience of the remote shell

    - Call a shell object like a function to run a command on the remote shell
    - Use it like a dictionary to access remote environment variables

    basic usage:
    ```
    with Shell('hostname') as sh:        # start the shell
        sh('echo hello world')           # run a command
        if 'FOO_BAR' not in sh:          # check for environment variables
            sh['FOO_BAR'] = 'baz qux'    # and get/set environment variables
        remote_environ = dict(sh)        # get the full environment dict
    ```
    '''
    # TODO Shell shopt wrapper
    # TODO shell.ls() -> list[str]
    # TODO make `Shell` a context manager
    # TODO use `weakref` for more robust cleanup
    # TODO add `Shell(tee=True)` to forward stdout to local stdout (after run)

    def __init__(self, host: str | SSH, text: bool = True):
        # settings
        self.text = bool(text)
        self._host: str | None = None
        self._data: bytes = b''
        self._pwd: str | None = None
        self._env: dict[str, str] | None = None
        self._id: bytes = bytes(choices(_ID_CHARS, k=_ID_LENGTH))
        self._re_msg = re.compile(b'\n([0-9][0-9][0-9])' + self._id + b'\n')
        # make command loop
        id = self._id.decode('utf-8')
        cmd = (
            'while \\read -r -d \'\' CMD; do'    # read null-separated inputs
            ' eval "$CMD";'                      # run each input command
            f' printf \'\\n%03d{id}\\n\' "$?";'  # report (returncode, ID)
            ' done 2>&1'  # redirect stderr remote-side to avoid race condition
        )
        # start SSH
        if isinstance(host, SSH):
            self._proc = host.Popen(
                cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT
            )
        else:
            self._proc = Popen(
                host, cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT
            )
        # test new connection to ensure that everything is working
        cmd = rf'''trap 'printf "\n%03d{id}\n" "$?"' EXIT'''
        if (out := self(cmd, check=True).stdout) != '':
            self.close()
            raise CalledProcessError(1, cmd, f'unexpected output: {out!r}')

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

    def __call__(
        self, cmd: Iterable[str] | str = ':', check: bool = False, *,
        text: bool | None = None,
        encoding: str = 'UTF-8', errors: str = 'replace'
    ) -> CompletedProcess:
        '''run a remote command

        - list of strings is a single command executed verbatim (preferred)
        - plain string is sent as-is and interpreted by shell (may be unsafe)
        - `text` defaults to the value from `Shell(..., text=...)`
        '''
        text = self.text if text is None else text
        # require process to be initialized
        if not (
            self._proc and self._proc.poll() is None
            and isinstance(self._proc.stdin, BufferedIOBase)
            and isinstance(self._proc.stdout, BufferedIOBase)
        ):
            raise ValueError('Shell not initialized')
        # convert command to shell token string
        if not isinstance(cmd, str):
            cmd = ' '.join(map(_quote, cmd))
        # clear cached environment and pwd
        self._env = self._pwd = None
        # send the command to remote shell loop
        self._proc.stdin.write(f'{cmd}\0'.encode('utf-8'))
        self._proc.stdin.flush()
        # initialize variables for loop
        find_msg = self._re_msg.search
        msg_len = _ID_LENGTH + 5
        stored_data: list[bytes] = []
        old_data: bytes = b''
        new_data: bytes = self._data or b''
        self._data = b''
        # reading loop
        while True:
            # combine the last two blocks to search for the next set of data
            data = old_data + new_data if old_data else new_data
            # search for the trigger msg
            if r := find_msg(data, max(0, len(old_data) - msg_len)):
                # parse returncode
                code = int(r[1])
                # remember any extra data after trigger msg
                self._data = data[r.end():]
                # store data before trigger msg with the rest of the data
                stored_data.append(data[:r.start()])
                # join and decode output
                output = b''.join(stored_data)
                if text:
                    output = output.decode(encoding, errors)
                # return result
                if code and check:
                    raise CalledProcessError(code, cmd, output)
                return CompletedProcess(cmd, code, output)
            # bump old_data into the storage list
            if old_data:
                stored_data.append(old_data)
            old_data = new_data
            # read the next new_data block
            if not (new_data := self._proc.stdout.read1()):
                # EOF before trigger msg
                if old_data:
                    stored_data.append(old_data)
                output = b''.join(stored_data)
                if text:
                    output = output.decode(encoding, errors)
                raise PrematureExit(255, cmd, output)

    def source(self, path: str, args: Iterable[str] = ()) -> CompletedProcess:
        '''equivalent to `run(['source', path, **args])`'''
        return self(['source', path, *args])

    def cd(self, path: str = '') -> str:
        '''change remote present working directory, returns the new `pwd`'''
        cmd = f'cd {_quote(path)} && pwd' if path else 'cd && pwd'
        out: str = self(cmd, check=True, text=True).stdout
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
            out: str = self('pwd', check=True, text=True).stdout
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
            lines = self('env -0', check=True, text=True).stdout.split('\0')
            tokenized_lines = (line.partition('=') for line in lines if line)
            self._env = {k: v for k, _, v in tokenized_lines}
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
        self(['export', f'{key}={value}'], check=True)
        # set in local cache
        if self._env is not None:
            self._env[key] = value
    
    def __delitem__(self, key: str):
        # ensure valid key
        if not _IS_VALID_ENV_VAR_NAME(key):
            raise ValueError(f'invalid environment variable name: {key}')
        # set on remote shell
        self(f'unset {key}', check=True)
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
        ['ssh', host, *ssh_opt_args(opts), '--', cmd],
        check=check, text=text, encoding=encoding, errors=errors,
        stdin=stdin, stdout=stdout, stderr=stderr,
        input=input, capture_output=capture_output,
        bufsize=bufsize, pipesize=pipesize
    )


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
        ['ssh', host, *ssh_opt_args(opts), '--', cmd],
        text=text, encoding=encoding, errors=errors,
        stdin=stdin, stdout=stdout, stderr=stderr,
        bufsize=bufsize, pipesize=pipesize
    )


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
    - `opts` keywords set `ssh_config` options, e.g. `port=22`
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
    host: str,
    path: str,
    mode: MODE_STR = 'r',
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
