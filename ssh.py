#! /usr/bin/env python3

'''Manage jobs across multiple processes and hosts'''

import errno as _errno
import os as _os
import re as _re
from io import BufferedReader as _BufferedReader
from random import choices as _choices
from subprocess import DEVNULL as _DEVNULL, PIPE as _PIPE, STDOUT as _STDOUT
from subprocess import Popen as _Popen, run as _run
from tempfile import TemporaryDirectory as _TemporaryDirectory
from typing import Iterable as _Iterable, NamedTuple as _NamedTuple

_ID_CHARS = b'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz@_'
_ID_LENGTH = 22
_RE_CODE = _re.compile(rb'\n(\d+) [^\n]+\n$')
_RE_ERROR = _re.compile(rb'([^\n]+)\n?$')


class RunResult(_NamedTuple):
    '''Result from an SSH `run()` remote command call'''

    out: bytes
    '''STDOUT and STDERR'''

    code: int
    '''return code, or -1 for N/A'''


class SSH:
    '''SSH connection to a host, may be used as a context manager

    `SSH(...)`
    - `host` = hostname, with optional username, e.g. "someuser@somehost"
    - `multiplex` = multiplex operations over a single SSH connection
      - `True` turn multiplexing on only while connection is open (the default)
      - `False` turn multiplexing totally off
      - an `int` number keeps the SSH multiplex running in the background
        for that many seconds after closing the most recent connection
    - additional keyword `opts` set `ssh_config` options with `-o` flags

    You may safely run a single command like this:
    ```
    SSH('someuser@somehost').run(['echo', 'hello world'])
    ```
    (The SSH connection will open for `run()` and close before `run()` returns.)

    Or you can safely run several commands in a row like:
    ```
    with SSH('someuser@somehost') as connection:
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
        self.id = bytes(_choices(_ID_CHARS, k=_ID_LENGTH))
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
            )], stderr=_STDOUT, stdout=_PIPE, stdin=_PIPE)

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
                    if e.errno != _errno.EPIPE:
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
        # use SSH connection object as itself with context manager
        return self

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        # decrement entries
        self._entries = max(0, self._entries - 1)
        # disconnect if this is the last entry
        if not self._entries:
            self.disconnect()

    def run(self, cmd: _Iterable[str] | str) -> RunResult:
        '''run a remote command

        - `Iterable[str]` is the preferred, safer method
        - `str` will run a verbatim `bash` shell command,
          including any bash-isms and expansions
        '''
        # connect and disconnect if needed, and assert that it worked
        with self:
            assert (
                self.proc and isinstance(self.proc.stdout, _BufferedReader)
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

    def upload(self, local_path: str, rem_path: str = ''):
        '''upload a local file to the remote host using an SCP command,
        file is uploaded to the CWD of this connection by default
        '''
        rem_path = f'{self.host}:{self._format_rem_path(rem_path)}'
        cmd = 'scp', '-p', *self._get_args(), local_path, rem_path
        _run(cmd, stdin=_DEVNULL, stderr=_DEVNULL, stdout=_DEVNULL)

    def download(self, rem_path: str, local_path: str = ''):
        '''download a remote file to the local host using an SCP command,
        file is downloaded from the CWD of this connection by default
        '''
        rem_path = f'{self.host}:{self._format_rem_path(rem_path)}'
        cmd = 'scp', '-p', *self._get_args(), rem_path, local_path
        _run(cmd, stdin=_DEVNULL, stderr=_DEVNULL, stdout=_DEVNULL)

    def write(self, content: bytes | str, rem_path: str, perms: int = 0o664):
        '''upload data or text to a remote file'''
        # ensure that remote path isn't obviously a directory
        if not (basename := _os.path.basename(rem_path)):
            raise ValueError('rem_path for write must be a file')
        # use a temporary directory to store working file
        with _TemporaryDirectory() as dir:
            path = _os.path.join(dir, basename)
            # save working file for transfer
            with open(path, 'w' if isinstance(content, str) else 'wb') as file:
                _os.fchmod(file.fileno(), perms)
                file.write(content)
            # upload working file
            self.upload(file.name, rem_path)

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
            ssh_dir = _os.path.expanduser('~/.ssh')
            if not _os.path.exists(ssh_dir):
                _os.mkdir(ssh_dir, 0o700)
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
            rem_path = _os.path.join(self.pwd(), rem_path)
        return _quote(rem_path) if quote else rem_path

_SEARCH_UNSAFE = _re.compile(r'[^\w@%+=:,./\n-]', _re.ASCII).search

def _quote(token: str) -> str:
    '''quote a token for shell use, including newlines'''
    if '\n' in token:
        if not _SEARCH_UNSAFE(token):
            return token.replace('\n', "$'\\n'")
        token = token.replace("'", "'\"'\"'").replace('\n', "'$'\\n''")
        return f"'{token}'"
    elif not _SEARCH_UNSAFE(token):
        return token or "''"
    token = token.replace("'", "'\"'\"'")
    return f"'{token}'"
