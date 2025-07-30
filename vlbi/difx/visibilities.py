'''Provides the `Visibilities` class to represent a DiFX visibilities file'''

from dataclasses import dataclass, field
from io import BufferedIOBase, RawIOBase, BytesIO
from struct import Struct, pack as _pack, unpack as _unpack
from typing import Generator

J = complex(0, 1)
BINARY_FILE = BufferedIOBase | RawIOBase
STRUCT_RECORD = Struct('<IIdIII2sIdddd')


def _format_num(num: float | int) -> str:
    '''Format a number for DiFX visibility ASCII output'''
    return str(num if num % 1 else int(num))


@dataclass
class Record:
    '''DiFX visibility header and data'''

    format: int = 1
    '''format version, 1 for binary, 0 for legacy mixed ASCII/binary'''

    baseline: tuple[int, int] = (0, 0)
    '''1-based station indexes'''

    mjd: int = 0
    '''modified Julian date (MJD)'''

    seconds: float = 0.0
    '''fractional seconds of MJD'''

    config: int = 0
    '''configuration index (from input file)'''

    source: int = 0
    '''source index (from calc file)'''

    freq: int = 0
    '''frequency index (from input file)'''

    pols: str = 'RR'
    '''polarizations pair'''

    pulsar: int = 0
    '''pulsar bin index (from pulsar file)'''

    flagged: int = 0
    '''legacy flagging indicator, only present in mixed ASCII/binary files'''

    weight: float = 0.0
    '''data weighting factor'''

    u: float = 0.0
    '''U in meters'''

    v: float = 0.0
    '''V in meters'''

    w: float = 0.0
    '''W in meters'''

    visibilities: list[complex] = field(default_factory=list)
    '''complex visibilities for each channel'''


    def pack(self, format: int | None = None) -> bytes:
        '''Serialize record into bytes for output to DiFX file

        `format` is 0 for mixed ASCII/binary, 1 for binary, None for auto
        '''
        format = self.format if format is None else format
        baseline = self.baseline[0] * 256 + self.baseline[1]
        visibilities = _pack('<' + 2 * len(self.visibilities) * 'f', *(
            i for x in self.visibilities for i in (x.real, x.imag)
        ))
        # binary version
        if format == 1:
            return b'\x00\xff\x00\xff\x01\x00\x00\x00' + STRUCT_RECORD.pack(
                baseline, self.mjd, self.seconds,
                self.config, self.source, self.freq,
                self.pols.encode('utf-8').ljust(2),
                self.pulsar, self.weight, self.u, self.v, self.w
            ) + visibilities
        # mixed ASCII version
        elif format == 0:
            return (
                f'BASELINE NUM:       {baseline}\n'
                f'MJD:                {self.mjd}\n'
                f'SECONDS:            {_format_num(self.seconds)}\n'
                f'CONFIG INDEX:       {self.config}\n'
                f'SOURCE INDEX:       {self.source}\n'
                f'FREQ INDEX:         {self.freq}\n'
                f'POLARISATION PAIR:  {self.pols}\n'
                f'PULSAR BIN:         {self.pulsar}\n'
                f'FLAGGED:            {self.flagged}\n'
                f'DATA WEIGHT:        {_format_num(self.weight)}\n'
                f'U (METRES):         {_format_num(self.u)}\n'
                f'V (METRES):         {_format_num(self.v)}\n'
                f'W (METRES):         {_format_num(self.w)}\n'
            ).encode('utf-8') + visibilities
        # unsupported version
        else:
            msg = f'Unsupported DiFX visibilities format version: {format}'
            raise ValueError(msg)

    def write(self, file: BINARY_FILE, version: int = 1) -> int:
        '''Write record to file, version is 0 for mixed ASCII, 1 for binary'''
        return file.write(self.pack(version))


def records(
    source: BINARY_FILE | bytes | str,
    n_chan: int
) -> Generator[Record, None, None]:
    # TODO infer n_chan from matching input file for str path
    unpacker = Struct('<' + 2 * n_chan * 'f').unpack
    unpacker_indexes = [(i, i + 1) for i in range(0, 2 * n_chan, 2)]
    # read file from path
    if isinstance(source, str):
        with open(source, 'rb') as file:
            yield from records(file, n_chan)
    # accept verbatim input data
    elif isinstance(source, (bytes, bytearray)):
        yield from records(BytesIO(source), n_chan)
    # don't accept weird things
    elif not isinstance(source, (BufferedIOBase, RawIOBase)):
        msg = 'Visibility source must be binary file, bytes, or str'
        raise ValueError(msg)
    # read open binary file
    else:
        while True:
            if not (magic := source.read(8)):
                break
            elif magic == b'\x00\xff\x00\xff\x01\x00\x00\x00':
                if n_chan is None:
                    msg = 'n_chan required to parse binary DiFX visibilities'
                    raise ValueError(msg)
                (
                    baseline, mjd, sec, config, src, freq, pols, pulsar,
                    weight, u, v, w
                ) = STRUCT_RECORD.unpack(source.read(STRUCT_RECORD.size))
                x = unpacker(source.read(8 * n_chan))
                x = [J * x[j] + x[i] for i, j in unpacker_indexes]
                # note: J * imag + real is the fastest way to create a complex
                bl = baseline // 256, baseline % 256
                try:
                    yield Record(
                        1, bl, mjd, sec, config, src, freq,
                        pols.decode('utf-8'), pulsar, 0, weight, u, v, w, x
                    )
                except ValueError:
                    raise ValueError('Unrecognized data in DiFX visibilities')
            elif magic == b'BASELINE':
                values = []
                for key in (
                    b'NUM',  # truncated because 'BASELINE' already read
                    b'MJD', b'SECONDS',
                    b'CONFIG INDEX', b'SOURCE INDEX', b'FREQ INDEX',
                    b'POLARISATION PAIR', b'PULSAR BIN', b'FLAGGED',
                    b'DATA WEIGHT', b'U (METRES)', b'V (METRES)', b'W (METRES)'
                ):
                    k, _, value = source.readline().partition(b':')
                    if k.strip() != key:
                        if key == b' NUM':
                            k, key = b'BASELINE' + k, b'BASELINE' + key
                        msg = 'Unrecognized data in DiFX visibilities: '
                        msg += f'expected {key!r}, got {k!r}'
                        raise ValueError(msg)
                    values.append(value)
                (
                    baseline, mjd, sec, config, src, freq, pols, pulsar,
                    flagged, weight, u, v, w
                ) = values
                x = unpacker(source.read(8 * n_chan))
                x = [J * x[j] + x[i] for i, j in unpacker_indexes]
                # note: J * imag + real is the fastest way to create a complex
                try:
                    baseline = int(baseline)
                    bl = baseline // 256, baseline % 256
                    yield Record(
                        0, bl, int(mjd), float(sec), int(config), int(src),
                        int(freq), pols.strip().decode('utf-8'),
                        int(pulsar), int(flagged), float(weight),
                        float(u), float(v), float(w), x
                    )
                except ValueError:
                    raise ValueError('Unrecognized data in DiFX visibilities')
            elif len(magic) == 8 and magic.startswith(b'\x00\xff\x00\xff'):
                ver = _unpack('<I', magic[4:8])[0]
                msg = f'DiFX visibilities format version {ver} not supported'
                raise ValueError(msg)
            else:
                sync = ''.join(f'{i:02x}' for i in reversed(magic[:4]))
                msg = 'Unrecognized sync word in DiFX visibilities: '
                msg += f'expected 0xFF00FF00, got 0x{sync}'
                raise ValueError(f'{msg}: Are you sure {n_chan = }?')
