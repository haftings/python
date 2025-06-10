#! /usr/bin/env python3

'''Convert to/from Modified Julian Dates (MJDs) and other datetime formats'''

import argparse as _argparse
import os as _os
import re as _re
import shlex as _shlex
import sys as _sys
from datetime import datetime as _datetime, timedelta as _timedelta
from datetime import timezone as _timezone, UTC as _UTC
from typing import Iterable as _Iterable, NamedTuple as _NamedTuple

MJD_EPOCH = _datetime(1858, 11, 17, tzinfo=_UTC)
_ONE_DAY = _timedelta(1)
_MON2MO = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}
_RE_ISO_PLUS = _re.compile(
    r'''
        ^ (?:
            (?P<yyyy>\d\d\d\d) [^t\d]?
            (?:
                (?P<doy>\d\d\d)
                |
                (?:
                    (?P<mo>\d\d)
                    |
                    (?P<mon>jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)
                )
                [^t\d]? (?P<dd>\d\d)
            )
            (?:
                [^\d]? (?P<hh>\d\d)
                [^t\d]? (?P<mm>\d\d)
                (?:
                    [^t\d]? (?P<ss>\d\d(?:\.\d*)?)
                )?
            )?
            (?:
                [^:\d+-]? (?P<sign>[+-]) [^:\d+-]?
                (?:
                    (?P<tz_hh>\d\d?) [^\d+-]?
                    (?P<tz_mm>\d\d)? [^\d+-]?
                    (?P<tz_ss>\d\d)?
                )?
            )?
            |
            (?P<scalar>\s*[+-]?\d+\.?\d*|[+-]?\.\d+)
        )
        [^:\d+-a-z]? (?:z|utc)? $
    ''',
    _re.X | _re.I
)


class Info(_NamedTuple):
    '''Detailed information about a UTC datetime'''

    yyyy: int
    '''4-digit year'''

    mm: int
    '''month of year'''

    dd: int
    '''day of month'''

    HH: int
    '''hour of day (24-hr scale)'''

    MM: int
    '''minute of hour'''

    SS: float
    '''second of minute'''

    doy: float
    '''day of year'''

    woy: int
    '''week of year'''

    dow: int
    '''day of week (0 = Sun ... 6 = Sat)'''

    yy: int
    '''2-digit year'''

    dt: _datetime
    '''`datetime` representation of date and time'''

    mjd: float
    '''Modified Julian Datetime (MJD)'''

    jd: float
    '''Julian Datetime (JD)'''


def info(
    value: str | float | int | _datetime | Info | _Iterable | None = None
) -> Info:
    '''Get calendar `Info` about a date and time'''
    t = datetime(value)
    m = mjd(t if value is None else value)
    cal = t.isocalendar()
    return Info(
        yyyy=t.year, mm=t.month, dd=t.day, HH=t.hour, MM=t.minute,
        SS=((t.second * 1000000 + t.microsecond) / 1000000),
        doy=((t - _datetime(t.year, 1, 1, tzinfo=_UTC) + _ONE_DAY) / _ONE_DAY),
        woy=cal.week, dow=(cal.weekday - 1) % 7, yy=(t.year % 100),
        dt=t, mjd=m, jd=(m + 2400000.5)
    )


def doy(
    value: str | float | int | _datetime | Info | _Iterable | None = None
) -> float:
    '''Get day of year for a date and time'''
    t = datetime(value)
    return (t - _datetime(t.year, 1, 1, tzinfo=_UTC) + _ONE_DAY) / _ONE_DAY


def datetime(
    value: str | float | int | _datetime | Info | _Iterable | None = None
) -> _datetime:
    '''Convert various types to a UTC `datetime`, input `value` can be:

    - `str` with an ISO format datetime
      - alternative characters for "-", "T", and ":" allowed
      - en-us 3-char month names are allowed
      - week/day-of-week are not allowed
    - `float` or `int` for an MJD or JD
    - `tuple` with one of the formats:
      - `(yyyy, doy)`
      - `(yyyy, mo, dd)`
      - `(yyyy, doy, hh, mm, ss)`
      - `(yyyy, mo, dd, hh, mm, ss)`
    - `datetime` assumed to be UTC if timezone isn't explicitly set
    '''
    if value is None:
        return _datetime.now(_UTC)
    if isinstance(value, _datetime):
        if value.tzinfo:
            return value if value.tzinfo == _UTC else value.astimezone(_UTC)
        return value.replace(tzinfo=_UTC)
    if isinstance(value, (int, float)):
        if value >= 2973119:
            return MJD_EPOCH + _timedelta(days=(value - 2400000.5))
        return MJD_EPOCH + _timedelta(days=value)
    if isinstance(value, bytes):
        value = value.decode(errors='replace')
    if isinstance(value, str):
        return text2datetime(value)
    if isinstance(value, Info):
        return value.dt
    if isinstance(value, _Iterable):
        value = tuple(value)
        n = len(value)
        if n == 2:
            yyyy, doy = value
            return _datetime(yyyy, 1, 1, tzinfo=_UTC) + _timedelta(doy - 1)
        if n == 3:
            yyyy, mo, dd = value
            return _datetime(yyyy, mo, dd, tzinfo=_UTC)
        if n == 5:
            yyyy, doy, hh, mm, ss = value
            t = _datetime(yyyy, 1, 1, hh, mm, tzinfo=_UTC)
            return t + _timedelta(doy - 1, ss)
        if n == 6:
            yyyy, mo, dd, hh, mm, ss = value
            t = _datetime(yyyy, mo, dd, hh, mm, tzinfo=_UTC)
            return t + _timedelta(seconds=ss)
        raise TypeError(f'unsupported date & time tuple length: {n}')
    raise TypeError(f'unsupported date & time type: {type(value)}')


def mjd(
    value: str | float | int | _datetime | Info | _Iterable | None = None
) -> float:
    '''Convert various types to a Modified Julian Date (MJD),
    input `value` can be:

    - `str` with an ISO format datetime
      - alternative characters for "-", "T", and ":" allowed
      - en-us 3-char month names are allowed
      - week/day-of-week are not allowed
    - `float` or `int` for an MJD or JD
    - `tuple` with one of the formats:
      - `(yyyy, doy)`
      - `(yyyy, mo, dd)`
      - `(yyyy, doy, hh, mm, ss)`
      - `(yyyy, mo, dd, hh, mm, ss)`
    - `datetime` assumed to be UTC if timezone isn't explicitly set
    '''
    if value is None:
        return (_datetime.now(_UTC) - MJD_EPOCH) / _ONE_DAY
    if isinstance(value, _datetime):
        try:
            return (value - MJD_EPOCH) / _ONE_DAY
        except TypeError:
            value = value.replace(tzinfo=_UTC)
            return (value - MJD_EPOCH) / _ONE_DAY
    if isinstance(value, (int, float)):
        if value >= 2973119:
            return value - 2400000.5
        return float(value)
    if isinstance(value, bytes):
        value = value.decode(errors='replace')
    if isinstance(value, str):
        return text2mjd(value)
    if isinstance(value, Info):
        return value.mjd
    if isinstance(value, _Iterable):
        value = tuple(value)
        n = len(value)
        if n == 2:
            yyyy, doy = value
            value = _datetime(yyyy, 1, 1, tzinfo=_UTC) + _timedelta(doy - 1)
        elif n == 3:
            yyyy, mo, dd = value
            value = _datetime(yyyy, mo, dd, tzinfo=_UTC)
        elif n == 5:
            yyyy, doy, hh, mm, ss = value
            t = _datetime(yyyy, 1, 1, hh, mm, tzinfo=_UTC)
            value = t + _timedelta(doy - 1, ss)
        elif n == 6:
            yyyy, mo, dd, hh, mm, ss = value
            t = _datetime(yyyy, mo, dd, hh, mm, tzinfo=_UTC)
            value = t + _timedelta(seconds=ss)
        else:
            raise TypeError(f'unsupported date & time tuple length: {n}')
        return (value - MJD_EPOCH) / _ONE_DAY
    raise TypeError(f'unsupported date & time type: {type(value)}')


def text2datetime(text: str) -> _datetime:
    '''Parse an ISO-datetime or decimal MJD/JD string to a `datetime`

    - Standard ISO are supported, including both yyyy-mm-dd and yyyy-ddd format
    - Accepts any characters besides a digit between the date and time
    - Accepts any characters besides a digit or the letter "T"/"t"
      between time and date parts
    - Accepts 1- or 2-digit hour for time zone offsets
    - Accepts "Z"/"z" or "UTC"/"utc" extension with an optional space
    - -678940 <= MJD < 1000000 (0001-01-01 <= datetime < 4596-10-12)
    - 1721060.5 <= JD < 5373119.5 (0001-01-01 <= datetime < 10000-01-01)
    '''
    # match with regex, extract groups
    if not (m := _RE_ISO_PLUS.match(text)):
        raise ValueError(f'unrecognized datetime format: {text}')
    yyyy, doy, mo, mon, dd, hh, mm, ss, sign, tz_h, tz_m, tz_s, v = m.groups()
    try:
        # JD or MJD
        if v:
            v = float(v)
            if v > 1721060.5:
                return MJD_EPOCH + _timedelta(v - 2400000.5)  # JD
            return MJD_EPOCH + _timedelta(v)  # MJD
        # convert MON (e.g. "DEC") to MO (e.g. 12)
        mo = _MON2MO[mon.lower()] if mon else int(mo or 1)
        # calculate time zone using offset (default 00:00 for UTC)
        tz_s = 3600 * int(tz_h or 0) + 60 * int(tz_m or 0) + int(tz_s or 0)
        tz = _timezone(_timedelta(seconds=(-tz_s if sign == '-' else tz_s)))
        # calculate date with timezone
        return _datetime(
            int(yyyy), mo, int(dd or 1), int(hh or 0), int(mm or 0), tzinfo=tz
        ) + _timedelta(int(doy or 1) - 1, float(ss or 0))
    # give a friendlier error for bad input
    except OSError:
        raise ValueError(f'invalid datetime value: {text}')


def text2mjd(text: str) -> float:
    '''Parse an ISO-datetime or decimal MJD/JD string to an MJD

    - Standard ISO are supported, including both yyyy-mm-dd and yyyy-ddd format
    - Accepts any characters besides a digit between the date and time
    - Accepts any characters besides a digit or the letter "T"/"t"
      between time and date parts
    - Accepts 1- or 2-digit hour for time zone offsets
    - Accepts "Z"/"z" or "UTC"/"utc" extension with an optional space
    - MJD < 1000000 (datetime < 4596-10-12)
    - JD >= 1000000
    '''
    # match with regex, extract groups
    if not (m := _RE_ISO_PLUS.match(text)):
        raise ValueError(f'unrecognized datetime format: {text}')
    yyyy, doy, mo, mon, dd, hh, mm, ss, sign, tz_h, tz_m, tz_s, v = m.groups()
    try:
        # JD or MJD
        if v:
            v = float(v)
            return v - 2400000.5 if v >= 1000000 else v
        # convert MON (e.g. "DEC") to MO (e.g. 12)
        mo = _MON2MO[mon.lower()] if mon else int(mo or 1)
        # calculate time zone using offset (default 00:00 for UTC)
        tz_s = 3600 * int(tz_h or 0) + 60 * int(tz_m or 0) + int(tz_s or 0)
        tz = _timezone(_timedelta(seconds=(-tz_s if sign == '-' else tz_s)))
        # calculate date with timezone
        dt = _datetime(
            int(yyyy), mo, int(dd or 1), int(hh or 0), int(mm or 0), tzinfo=tz
        ) + _timedelta(int(doy or 1) - 1, float(ss or 0))
        return (dt - MJD_EPOCH) / _ONE_DAY
    # give a friendlier error for bad input
    except OSError:
        raise ValueError(f'invalid datetime value: {text}')

_help_text = (
    '\narguments:\n'
    '    -                 current MJD (default with no other options)\n'
    '    iso               ISO datetime (week/day-of-week not supported)\n'
    '    yyyy              4-digit year (0-9999) or 2-digit year (1900-1999)\n'
    '    mm                month of year\n'
    '    dd                day of month\n'
    '    doy               day of year (a decimal value is allowed)\n'
    '    HH                hour of day (24-hour clock)\n'
    '    MM                minute of hour\n'
    '    SS                second of minute (a decimal value is allowed)\n'
    '    mjd               Modified Julian Day (MJD) - converted to calendar\n'
    '    jd                Julian Day (JD) - converted to calendar\n'
    '\noptions:\n'
    '    -m, --mjd         show the modified Julian date (MJD) (default)\n'
    '    -j, --jd          show the Julian date (JD)\n'
    '    -d, --doy         show the day of year (DOY)\n'
    '    -c, --calendar    show the ISO calendar format\n'
    '    -i, --integer     truncate to day (show MJD/JD as integers)\n'
    '    -f, --fractional  include time (show MJD/JD with decimal)\n'
    '    -h, --help        show this help message and exit\n'
)
def main():
    '''run as script'''
    # get command name
    cmd = _sys.argv[0] or 'mjd'
    cmd = _shlex.quote(_os.path.basename(cmd) if cmd.startswith('/') else cmd)
    # set up argument parser (non-POSIX args make these parameters annoying)
    p = _argparse.ArgumentParser(add_help=False, usage=(
        f'\n    {cmd} [-ifmjdc] [-]                    # current mjd'
        f'\n    {cmd} [-ifmjdc] iso                    # ISO datetime'
        f'\n    {cmd} [-ifmjdc] yyyy mm dd [HH MM SS]  # calendar datetime'
        f'\n    {cmd} [-ifmjdc] yyyy doy [HH MM SS]    # calendar datetime'
        f'\n    {cmd} [-ifmjdc] mjd                    # modified Julian date'
        f'\n    {cmd} [-ifmjdc] jd                     # Julian date'
    ))
    p.add_argument(
        'args', nargs='*'
    )
    p.add_argument(
        '-m', '--mjd', action='store_const', const='mjd', dest='fmt',
        default='mjd'
    )
    p.add_argument(
        '-j', '--jd', action='store_const', const='jd', dest='fmt'
    )
    p.add_argument(
        '-d', '--doy', action='store_const', const='doy', dest='fmt'
    )
    p.add_argument(
        '-c', '--calendar', action='store_const', const='calendar', dest='fmt'
    )
    p.add_argument(
        '-i', '--integer', action='store_true'
    )
    p.add_argument(
        '-f', '--fractional', action='store_false', dest='integer'
    )
    p.add_argument(
        '-h', '--help', action='store_true'
    )
    a = p.parse_args()
    if a.help:
        # write help message
        _sys.stdout.write(f'{__doc__}\n\n')
        p.print_usage()
        _sys.stdout.write(_help_text)
        _sys.exit()
    n = len(a.args)
    try:
        # current datetime
        if n == 0 or n == 1 and a.args[0] == '-':
            t = _datetime.now(_UTC)
        # mjd or jd
        elif n == 1:
            t = datetime(a.args[0])
        # calendar
        elif n in (2, 3, 5, 6):
            t = datetime(map(int, a.args))
        # wat?
        else:
            p.error('incorrect argument count')
    except ValueError:
        p.error(f'invalid input date')
    except OverflowError:
        p.error(f'invalid input date (out of max range)')
    try:
        # output
        if a.fmt in 'mjd':
            v = mjd(t) + 2400000.5 if a.fmt == 'jd' else mjd(t)
            _sys.stdout.write(f'{int(v)}\n' if a.integer else f'{v:0.6f}\n')
        elif a.fmt == 'calendar':
            if a.integer:
                _sys.stdout.write(f'{t:%Y-%m-%d}\n')
            else:
                _sys.stdout.write(f'{t:%Y-%m-%dT%H:%M:%S.%f}\n')
        else:
            v = doy(t)
            _sys.stdout.write(f'{int(v)}\n' if a.integer else f'{v:0.6f}\n')
    except ValueError:
        p.error(f'could not convert to {a.fmt}')

if __name__ == '__main__':
    main()
