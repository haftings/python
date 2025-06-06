#! /usr/bin/env python3

'''Convert to/from Modified Julian Dates (MJDs) and other datetime formats'''

import os as _os
import re as _re
import shlex as _shlex
import sys as _sys
from datetime import datetime as _datetime, timedelta as _timedelta
from datetime import timezone as _timezone, UTC as _UTC
from typing import Iterable as _Iterable, NamedTuple as _NamedTuple

MJD_EPOCH = _datetime(1858, 11, 17, tzinfo=_UTC)
_A_DAY = _timedelta(1)
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


def info(value: float | int | _datetime | Info | tuple) -> Info:
    '''Get calendar `Info` about a date and time'''
    t = datetime(value)
    m = mjd(value)
    cal = t.isocalendar()
    return Info(
        yyyy=t.year, mm=t.month, dd=t.day, HH=t.hour, MM=t.minute,
        SS=((t.second * 1000000 + t.microsecond) / 1000000),
        doy=((t - _datetime(t.year, 1, 1, tzinfo=_UTC) + _A_DAY) / _A_DAY),
        woy=cal.week, dow=(cal.weekday - 1) % 7, yy=(t.year % 100),
        dt=t, mjd=m, jd=(m + 2400000.5)
    )


def datetime(value: float | int | _datetime | Info | tuple) -> _datetime:
    '''Convert various types to a UTC `datetime`, input `value` can be:

    - `float` or `int` for an MJD or JD
    - `tuple` with one of the formats:
      - `(yyyy, doy)`
      - `(yyyy, mo, dd)`
      - `(yyyy, doy, hh, mm, ss)`
      - `(yyyy, mo, dd, hh, mm, ss)`
    - `datetime` assumed to be UTC if timezone isn't explicitly set
    '''
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
        try:
            n = len(value)
        except TypeError:
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


def mjd(value: float | int | _datetime | Info | tuple) -> float:
    '''Convert various types to a Modified Julian Date (MJD),
    input `value` can be:

    - `float` or `int` for an MJD or JD
    - `tuple` with one of the formats:
      - `(yyyy, doy)`
      - `(yyyy, mo, dd)`
      - `(yyyy, doy, hh, mm, ss)`
      - `(yyyy, mo, dd, hh, mm, ss)`
    - `datetime` assumed to be UTC if timezone isn't explicitly set
    '''
    if isinstance(value, _datetime):
        try:
            return (value - MJD_EPOCH) / _A_DAY
        except TypeError:
            value = value.replace(tzinfo=_UTC)
            return (value - MJD_EPOCH) / _A_DAY
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
        try:
            n = len(value)
        except TypeError:
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
        return (value - MJD_EPOCH) / _A_DAY
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
        return (dt - MJD_EPOCH) / _A_DAY
    # give a friendlier error for bad input
    except OSError:
        raise ValueError(f'invalid datetime value: {text}')


def main():
    '''run as script'''
    # non-POSIX syntax means `argparse` won't work well U_U
    # get command name
    cmd = _sys.argv[0] or 'mjd'
    cmd = _shlex.quote(_os.path.basename(cmd) if cmd.startswith('/') else cmd)
    n = len(_sys.argv) - 1
    # check for -h, --help
    if any(map(_re.compile(r'^--help$|^-(?!-).*h').match, _sys.argv)):
        n = -1
    # current datetime
    if n == 0 or n == 1 and _sys.argv[1] in '-':
        print(f' {mjd(_datetime.now(tz=_UTC)):0.6f}')
    elif n == 1:
        # mjd or jd -> calendar
        if _re.match(r'^\s*(?:\.\d+|\d+\.?\d*)\s*$', _sys.argv[1]):
            print(f' {datetime(_sys.argv[1]):%Y %m %d %H %M %S}')
        # calendar -> mjd or jd
        else:
            print(f' {mjd(_datetime.now(tz=_UTC)):0.6f}')
    # calendar -> mjd or jd
    elif n in (2, 3, 5, 6):
        print(f' {mjd(map(int, _sys.argv[1:])):0.6f}')
    # help message
    else:
        if n != -1:
            print(f'error: invalid input\n')
        print(
            f'{cmd} - Convert to/from Modified Julian Date (MJD)\n'
            '\nUsage:\n'
            f'    {cmd} [-]\n'
            f'    {cmd} iso\n'
            f'    {cmd} year month day [hour minute second]\n'
            f'    {cmd} year doy [hour minute second]\n'
            f'    {cmd} mjd\n'
            f'    {cmd} jd\n'
            '\nArguments:\n'
            '    -       current MJD (default with no other options)\n'
            '    iso     ISO datetime (week/day-of-week format not supported)\n'
            '    year    4-digit or 2-digit year, 2-digit years are 1900-1999\n'
            '    month   month of year\n'
            '    day     day of month\n'
            '    hour    hour of day (24-hour clock)\n'
            '    minute  minute of hour\n'
            '    second  second of minute (a decimal value is allowed)\n'
            '    doy     day of year (a decimal value is allowed)\n'
            '    mjd     Modified Julian Day (MJD) - converted to calendar\n'
            '    jd      Julian Day (JD) - converted to calendar'
        )
        if n != -1:
            _sys.exit(1)


if __name__ == '__main__':
    main()
