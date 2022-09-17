#! /usr/bin/env python3

'''Progress bar

example:
>>> import progress, time
... with progress.Bar(range(150)) as pb:
...     for _ in pb:
...         time.sleep(0.01)
'''

import typing as _t
import collections.abc as _abc
import sys as _sys

FILL_NONE = '', '', ''
'''renders bar without a fill'''

FILL_FADE = '', '\033[2m', '\033[22m'
'''renders bar fill as un-fading text (artistic, but watch out for spaces)'''

FILL_INVERT = '\033[7m', '\033[27m', ''
'''renders bar fill as highlighted (traditional bar look)'''

FILL_UNDERLINE = '\033[4m', '\033[24m', ''
'''renders bar fill as an underline (clear and non-distracting)'''

def FMT_DEFAULT(pb: 'Bar') -> str:
    '''Format as `f'processing {pb.i:{pb.w}} / {pb.n} ({pb.p:7.2%})'`'''
    return f'processing {pb.i:{pb.w}} / {pb.n} ({pb.p:7.2%})'

class Bar(_abc.Iterator):
    '''Iterating progress bar

    The progress bar renders once when entering or exiting a `with` block,
    and also after each `next()` call or `for` loop iteration.

    * `items` is the items through which to iterate, `range(...)` works well
    * `color` turns ANSI coloring on (`True`), off (`False`), or auto (`None`)
    * `display` is set to `False` to hide the bar (no output)
    * `fill` sets ANSI codes for before, between, and after the bar segmets
    * `fmt` is the bar text or a formatter function to generate it
    * `keep` to leave the bar on-screen when done
    '''

    def __init__(
        self, items: _t.Iterable[_t.Any], *,
        color: bool = None,
        display: bool = True,
        fill: _t.Tuple[str, str, str] = FILL_UNDERLINE,
        fmt: _t.Union[_t.Callable[['Bar'], str], str] = FMT_DEFAULT,
        keep: bool = False
    ):
        self._i = 0
        self._items = 0
        self._iter = iter(items)
        try:
            self._n = len(items)
        except TypeError:
            self._n = 0

        self.color  = color
        '''ANSI coloring on (`True`), off (`False`), or auto (`None`)'''

        self.display = display
        '''`False` to hide the bar'''

        self.fill: _t.Tuple[str, str, str] = fill
        '''color codes for before, between, and after the bar segmets'''

        self.fmt: _t.Union[_t.Callable[['Bar'], str], str] = fmt
        '''Bar text or a formatter to generate it'''

        self.keep = keep
        '''`True` to leave bar on-screen when done'''

    @property
    def i(self) -> int:
        '''Current item, 0 - `n`'''
        return self._i

    @property
    def n(self) -> int:
        '''Length of items, defaults to `i` if not specified'''
        return self._n

    @property
    def p(self) -> float:
        '''Progress fraction 0.0 - 1.0'''
        return self._i / self._n if self._n else 1.0

    @property
    def w(self) -> int:
        '''Width of `n` as a string'''
        return len(str(self._n))

    @property
    def text(self) -> str:
        '''Text shown on the progress bar'''
        return self.fmt if isinstance(self.fmt, str) else self.fmt(self)

    @property
    def do_color(self) -> bool:
        '''`True` if and only if ANSI color should be used'''
        return _sys.stderr.isatty() if self.color is None else self.color

    def show(self, fill: bool = True):
        '''Show progress bar (usually called automatically)

        * `fill=False` to avoid showing the fill bar
        '''
        s = self.text
        if not fill:
            _sys.stderr.write(f'\r{s}\033[K' if self.do_color else s + '\n')
        elif self.do_color:
            n = round(len(s) * self.p)
            s = self.fill[0] + s[:n] + self.fill[1] + s[n:] + self.fill[2]
            _sys.stderr.write(f'\r{s}\033[K')
        else:
            _sys.stderr.write(s + '\n')
        _sys.stderr.flush()

    def hide(self):
        '''Hide progress bar (or current line)'''
        if self.do_color:
            _sys.stderr.write('\r\033[K')
            _sys.stderr.flush()

    def __enter__(self) -> 'Bar':
        self.show()
        return self

    def __exit__(self, etype, e, trace):
        if self.do_color:
            if self.keep:
                self.show(False)
                _sys.stderr.write('\n')
                _sys.stderr.flush()
            else:
                self.hide()

    def __iter__(self) -> _t.Iterator:
        return self

    def __len__(self) -> int:
        return self._n

    def __next__(self) -> _t.Any:
        item = next(self._iter)
        self._i += 1
        if self._n < self._i:
            self._n = self._i
        self.show()
        return item
