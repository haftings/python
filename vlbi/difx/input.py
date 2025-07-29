'''Provides the `Input` class to represent a DiFX `.input` file'''

from collections.abc import Mapping, MutableMapping
from io import IOBase
import re as _re
from typing import Any, Generator, Iterator, TypeVar

T = TypeVar('T')
RE_INPUT_SECTION = _re.compile(
    r'^#+(?P<name>[^#\n]*)#+!\n(?P<content>(?:^(?!#).*\n)*)', _re.M
)
RE_INPUT_LINE = _re.compile(
    r'^(?P<name>[^#@:\n][^:\n]*): *(?P<value>.*)', _re.M
)
RE_UNITS = _re.compile(
    r'\((?P<units>.*)\)'
)
RE_INDEX = _re.compile(
    r'\b([0-9]+)\b(?:/([0-9]+)\b)?'
)
SPECIAL_INDEX_FORMATS = {
    'datastream index': 'datastream %d index',
    'baseline index': 'baseline %d index',
    'rule config name': 'rule %d config name',
    'phase cals out': 'phase cals %d out',
    'phase cal index': 'phase cal %d/%d index',
    'clk offset': 'clk offset %d',
    'freq offset': 'freq offset %d',
    'rec band pol': 'rec band %d pol',
    'rec band index': 'rec band %d index',
    'd/stream files': 'd/stream %d files'
}
SEPARATE_ARRAYS = {
    'baseline index',
    'num freqs',
    'pol products',
    'd/stream a band',
    'd/stream b band'
}


def key(name: str) -> str:
    '''convert arbitrary text to a standardized lowercase key name'''
    return ' '.join(RE_INDEX.sub('', RE_UNITS.sub('', name)).lower().split())


def copy(value: Any) -> Any:
    if isinstance(value, list):
        return list(map(copy, value))
    if isinstance(value, dict):
        return {k: copy(v) for k, v in value.items()}
    return value


def _array_into_dict(dict: dict, index: tuple[int, ...], value: Any):
    if len(index) == 1:
        dict[index[0]] = value
    else:
        _array_into_dict(dict.setdefault(index[0], {}), index[1:], value)


def _dict2array(d: dict[int, Any]) -> list:
    indexes = sorted(d)
    expected = list(range(len(indexes)))
    if indexes != expected:
        raise ValueError(f'bad indexes: got ({indexes}) expected ({expected})')
    return [
        _dict2array(d[i]) if isinstance(d[i], Mapping) else d[i]
        for i in indexes
    ]


def enumerate_nested(
    array: list[T] | tuple[T, ...]
) -> Generator[tuple[tuple[int, ...], T], None, None]:
    '''Enumerate through a nested list'''
    for i, v in enumerate(array):
        if isinstance(v, (list, tuple)):
            for jj, v in enumerate_nested(v):
                yield (i, *jj), v
        else:
            yield (i,), v


def format_value(value: Any, key: str = '') -> str:
    '''Format a value for printing in a DiFX `.input` file'''
    # format the value
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if key == 'clock coeff':
        return f'{value:.15e}'
    return str(value)


class Section(MutableMapping):
    '''DiFX `.input` file section
    
    Initialize with one of:
    - `Section(text)`
      - text of section, not including header
    - `Section({name: value, ...}, units={name: units, ...})`
      - create from scratch (preferred)
    '''

    def __init__(
        self,
        content: str | Mapping[str, Any] = '',
        units: Mapping[str, str] | None = None
    ) -> None:
        self.map = {}
        self.units = {}
        if isinstance(content, str):
            self.ingest(content)
        else:
            for name, value in content.items():
                self.map[key(name)] = copy(value)
        self.units.update(units or ())

    def __getitem__(self, name: str) -> Any:
        return self.map[key(name)]

    def __setitem__(self, name: Any, value: Any) -> None:
        self.map[key(name)] = copy(value)

    def __delitem__(self, name: str) -> None:
        del self.map[key(name)]

    def __iter__(self) -> Iterator[str]:
        return iter(self.map)

    def __len__(self) -> int:
        return len(self.map)

    def ingest(self, text: str):
        # first set up storage variables
        index_prefix: tuple[int, ...] = ()
        arrays: dict[str, dict[int, Any]] = {}
        # now go through each line
        for line in RE_INPUT_LINE.finditer(text):
            # store units
            if r := RE_UNITS.search(name := line['name']):
                name = key(RE_UNITS.sub('', name))
                self.units[name] = r['units']
            else:
                name = key(name)
            # convert the value to int, float, bool, or leave as str
            v = line['value']
            try:
                v = int(v)
            except ValueError:
                try:
                    v = float(v)
                except ValueError:
                    if v in ['TRUE', 'FALSE']:
                        v = v == 'TRUE'
            # new telescope index? (set as value, used as hidden index prefix)
            if name == 'telescope index':
                # make sure the value is an integer
                if not isinstance(v, int) or isinstance(v, bool):
                    msg = repr(line['value']) + ' (expected integer)'
                    raise ValueError(f'bad telescope index: {msg}')
                # make sure the value is the next positive integer
                next_index = index_prefix[0] + 1 if index_prefix else 0
                if v != next_index:
                    msg = f'{v} (expected {next_index})'
                    raise ValueError(f'wrong telescope index: {msg}')
                # store the new index prefix and continue
                index_prefix = (v,)
                _array_into_dict(arrays.setdefault(name, {}), (v,), v)
            # new pol products? (set as index, used as hidden index prefix)
            elif name == 'pol products':
                index_strs = RE_INDEX.findall(line['name'])
                index_prefix = tuple(
                    int(i) for ii in index_strs for i in ii if i != ''
                )
                _array_into_dict(arrays.setdefault(name, {}), index_prefix, v)
            # inside of an array? (if there's an index)
            elif (index_strs := RE_INDEX.findall(line['name'])) or index_prefix:
                index = index_prefix + tuple(
                    int(i) for ii in index_strs for i in ii if i != ''
                )
                _array_into_dict(arrays.setdefault(name, {}), index, v)
            # scalar value? (no index right now)
            else:
                self.map[key(line['name'])] = v
        # now it's time to unroll those arrays
        # TODO check that array indexes are positive integers (0, 1, 2, ...)
        for name, array_dict in arrays.items():
            self.map[name] = _dict2array(array_dict)

    def __repr__(self) -> str:
        units = f', units={self.units!r}' if self.units else ''
        return f'{self.__class__.__name__}({self.map!r}{units})'

    def __str__(self) -> str:
        # converting from data to `.input` file is surprisingly complicated,
        # because there are numerous edge cases, sorting, and formatting rules
        cls0 = ''  # array, separate (array), scalar, or none
        output = []
        arrays: dict[tuple[int, ...], list[tuple[str, Any]]] = {}
        hide_indexes = 0
        units: str = ''
        for key, val in [*self.items(), (None, None)]:
            # determine class
            if isinstance(val, (list, tuple)):
                cls = 'separate' if key in SEPARATE_ARRAYS else 'array'
            else:
                cls = 'none' if key is None else 'scalar'
            # output array or separate array block
            if cls != cls0 and cls0 in ('array', 'separate'):
                # sort output lines
                ii_k_v = [
                    (ii, k, v)
                    for ii, items in sorted(arrays.items())
                    for k, v in items
                ]
                # handle semi-interlaced ordering of *band* and *zoom* lines
                if any(k == 'telescope index' for _, k, v in ii_k_v):
                    # this complicated spell sorts in order of (first to last):
                    # - telescope
                    # - normal line(s), then *band* line(s), then *zoom* line(s)
                    # - normal default sorting order
                    new = sorted((ii[0], (
                        2 if 'zoom' in k else 1 if 'band' in k else 0
                    ), i, ii, k, v) for i, (ii, k, v) in enumerate(ii_k_v))
                    ii_k_v = [(ii, k, v) for _, _, _, ii, k, v in new]
                # loop over output lines
                for ii0, k, v in ii_k_v:
                    # hide first indexes after telescope line
                    if k == 'telescope index':
                        hide_indexes = 1
                    # or hide first pair of indexes after pol products line
                    elif k == 'pol products':
                        hide_indexes = 2
                    # except for the pol products line itself
                    ii = ii0[hide_indexes:] if k != 'pol products' else ii0
                    # fetch units
                    units = self.units.get(k, '')
                    units = f' ({units})' if units else ''
                    # apply any a-priori special formats
                    if k0 := SPECIAL_INDEX_FORMATS.get(k):
                        # count index spots in special format
                        n = k0.count('%d')
                        # insert the first n indexes into the format slots
                        k0 = (k0 % ii[:n]).upper()
                        # append the remaining indexes at the end
                        if i_str := '/'.join(map(str, ii[n:])):
                            k_str = f'{k0}{units} {i_str}:'
                        else:
                            k_str = f'{k0}{units}:'
                    # apply end-of-key index(es)
                    elif ii:
                        i_str = '/'.join(map(str, ii))
                        k_str = f'{k.upper()}{units} {i_str}:'
                    # sometimes a single index is entirely hidden
                    else:
                        k_str = f'{k.upper()}{units}:'
                    # format into line
                    output.append(f'{k_str:20}{format_value(v, k)}\n')
                    # add clock comment
                    if k == 'clock poly order':
                        output.append(
                            '@ ***** Clock poly coeff N: has units '
                            'microsec / sec^N ***** @\n'
                        )
                # clear cache
                arrays = {}
            # output scalar immediately
            if cls == 'scalar':
                # fetch units
                units = self.units.get(key, '')
                units = f' ({units})' if units else ''
                # format into line
                k_str = f'{key.upper()}{units}:'
                output.append(f'{k_str:20}{format_value(val, key)}\n')
            # store arrays for later
            elif cls in ('array', 'separate'):
                for ii, v in enumerate_nested(val):
                    arrays.setdefault(ii, []).append((key, v))
            # remember the last class
            cls0 = cls
        return ''.join(output) + '\n'


class Input(MutableMapping):
    '''DiFX `.input` file

    Initialize with any of:
    - `Input(file)`
      - open file (preferred)
    - `Input({name: section, ...})`
      - create from scratch (preferred)
    - `Input(path)`
      - file on disk, `path` name must not contain any newlines
    - `Input(text)`
      - pre-read or generated file content

    Access input file data like:
    ```python
    with open('myfile.input') as file:
        input = Input(file)
    stations = input['telescope table']['telescope name']
    ```

    Write input file data back out like:
    ```python
    with open('myfile.input', 'w') as file:
        file.write(str(input))
    ```

    You can also create a web-readable json like this:
    ```python
    json.dumps(input, default=dict)
    ```

    Notes:
    - keys are case insensitive and do not include indexes or units
    - A and B (a.k.a. target and reference stations) are considered indexes
      and are equivalent to index 0 and 1 respectively
    '''

    def __init__(self, input: IOBase | str | Mapping[str, Section]) -> None:
        self.map = {}
        # open and read from file path
        if isinstance(input, str):
            if '\n' not in input:
                with open(input) as file:
                    input = file.read()
        # read open file and convert to str
        elif isinstance(input, IOBase):
            input = input.read()
            if isinstance(input, bytes):
                input = input.decode('utf-8', 'replace')
            elif not isinstance(input, str):
                input = str(input)
        # parse string
        if isinstance(input, str):
            self.update(RE_INPUT_SECTION.findall(input))
        # accept dict
        else:
            self.update(input)

    def __getitem__(self, name: str) -> Section:
        return self.map[key(name)]

    def __setitem__(self, name: str, section: Section | str) -> None:
        section = section if isinstance(section, Section) else Section(section)
        self.map[key(name)] = section

    def __delitem__(self, name: str) -> None:
        del self.map[key(name)]

    def __iter__(self) -> Iterator[str]:
        return iter(self.map)

    def __len__(self) -> int:
        return len(self.map)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.map!r})'

    def __str__(self) -> str:
        out = []
        for k, v in self.map.items():
            out.append(f'# {k.upper()} #'.ljust(20, '#') + '!\n')
            out.append(str(v))
        return ''.join(out)
