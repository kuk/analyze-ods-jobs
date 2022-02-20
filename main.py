
import re
import json
import zipfile
from typing import Any
from fnmatch import fnmatch
from pathlib import Path
from dataclasses import dataclass
from collections import Counter
from datetime import datetime as Datetime
from random import (
    seed,
    sample
)

from tqdm.auto import tqdm as log_progress

import pandas as pd

import markdown
import markdown.extensions.nl2br

import html_text as html_text_

import langdetect

from yargy import (
    Parser,
    rule,
    or_, and_
)
from yargy.pipelines import (
    caseless_pipeline,
    morph_pipeline
)
from yargy.predicates import (
    caseless,
    eq, in_,
    lte, length_eq
)
from yargy.interpretation import (
    fact,
)
from yargy import interpretation

from ipymarkup import (
    show_span_box_markup as show_markup
)


DATA_DIR = Path('data')
DUMPS_DIR = DATA_DIR / 'dumps'
DUMP = DUMPS_DIR / '2022-01-17.zip'


#####
#
#   ZIP
#
#####


def zip_list(path, pattern):
    with zipfile.ZipFile(path) as zip:
        for name in  zip.namelist():
            if fnmatch(name, pattern):
                yield name


def zip_read_texts(path, names, encoding='utf8'):
    with zipfile.ZipFile(path) as zip:
        for name in names:
            bytes = zip.read(name)
            yield bytes.decode(encoding)


#######
#
#   USER
#
######


@dataclass
class User:
    id: str
    name: str
    real_name: str
    image_url: str
    is_bot: bool


# {'id': 'U02USE4C85N',
#  'team_id': 'T040HKJE3',
#  'name': 'korzhov.work2019',
#  'deleted': False,
#  'color': '2b6836',
#  'real_name': 'Дмитрий Коржов',
#  'tz': 'Europe/Moscow',
#  'tz_label': 'Moscow Time',
#  'tz_offset': 10800,
#  'profile': {'title': '',
#   'phone': '',
#   'skype': '',
#   'real_name': 'Дмитрий Коржов',
#   'real_name_normalized': 'Dmitrij Korzhov',
#   'display_name': '',
#   'display_name_normalized': '',
#   'fields': None,
#   'status_text': '',
#   'status_emoji': '',
#   'status_emoji_display_info': [],
#   'status_expiration': 0,
#   'avatar_hash': '5d9292f158b2',
#   'image_original': 'https://avatars.slac_5d9292f158b234638114_original.png',
#   'is_custom_image': True,
#   'first_name': 'Дмитрий',
#   'last_name': 'Коржов',
#   'image_24': 'https://avatars.slack-edge_5d9292f158b234638114_24.png',
#   'image_32': 'https://avatars.slack-edge_32.png',
#   'image_48': 'https://avatars.slack-edge_5d9292f158b234638114_48.png',
#   'image_72': 'https://avatars.slack-edge_5d9292f158b234638114_72.png',
#   'image_192': 'https://avatars.slack-edg/2948086481686_5d9292f158b234638114_192.png',
#   'image_512': 'https://avatars.slack-edg/2948086481686_5d9292f158b234638114_512.png',
#   'image_1024': 'https://avatars.slack-ed_5d9292f158b234638114_1024.png',
#   'status_text_canonical': '',
#   'team': 'T040HKJE3'},
#  'is_admin': False,
#  'is_owner': False,
#  'is_primary_owner': False,
#  'is_restricted': False,
#  'is_ultra_restricted': False,
#  'is_bot': False,
#  'is_app_user': False,
#  'updated': 1642169967,
#  'is_email_confirmed': True,
#  'who_can_share_contact_card': 'EVERYONE'}


def parse_users(items):
    for item in items:
        yield User(
            id=item['id'],
            name=item['name'],
            real_name=item.get('real_name'),
            image_url=item['profile']['image_192'],
            is_bot=item['is_bot'],
        )


########
#
#   MESSAGE
#
#######


@dataclass
class Message:
    user_name: str
    datetime: Datetime
    mrkdwn: str
    html: str = None
    text: str = None
    lang: str = None


# {'type': 'message',
#   'subtype': 'channel_join',
#   'ts': '1495803329.968075',
#   'user': 'U5JHDFLP6',
#   'text': '<@U5JHDFLP6> has joined the channel'}


def is_subtype_item(item):
    return 'subtype' in item


def is_thread_item(item):
    return (
        'thread_ts' in item
        and item['thread_ts'] != item['ts']
    )


def parse_ts(value):
    value = float(value)
    return Datetime.fromtimestamp(value)


def parse_messages(items):
    for item in items:
        if is_subtype_item(item) or is_thread_item(item):
            continue

        profile = item.get('user_profile')
        if not profile:
            # {'type': 'message', 'text': 'A file was commented on',
            # 'ts': '1526210711.000045'}
            continue

        user_name = profile['name']
        datetime = parse_ts(item['ts'])
        mrkdwn = item['text']

        yield Message(
            user_name=user_name,
            datetime=datetime,
            mrkdwn=mrkdwn
        )


########
#
#   VACANCY MESSAGE
#
######


THREADS_DATE = Datetime.fromisoformat('2017-04-01')


def is_before_threads(record):
    # Slack introduce threads 2017-01-17. ODS #jobs transition during
    # 2017-01..2017-04
    return record.datetime < THREADS_DATE


def count_words(text):
    matches = re.findall(r'\w+', text)
    return len(matches)


def is_vacancy_message(record):
    # ~90% vacancy messages after threads intro. Before hard to
    # detect. Just drop them
    return (
        not is_before_threads(record)
        and count_words(record.mrkdwn) > 50
    )


def vacancy_message_query(record):
    return 'в:#_jobs когда: {date} от:@{user_name}'.format(
        date=record.datetime.date(),
        user_name=record.user_name,
    )

########
#
#   MRKDWN
#
######


def mrkdwn_html(source):
    # Slack own Markdown flaivour
    # https://api.slack.com/reference/surfaces/formatting
    # https://stackoverflow.com/questions/55816333/does-slack-support-markdown-tables

    # DONE Treat new lines as <br>
    # TODO User mention <@U04DXFZ2G>
    # TODO Named links http://jobs.dbrain.io|jobs.dbrain.io
    # TODO Treat *...* as bold
    # TODO Support ~...~, ```...```

    return markdown.markdown(
        source,
        extensions=[
            markdown.extensions.nl2br.Nl2BrExtension()
        ]
    )


#######
#
#   HTML
#
########


def show_html(html):
    display(HTML(html))


def html_text(html):
    return html_text_.extract_text(html)


#######
#
#  LANG
#
######


RU = 'ru'
EN = 'en'


def text_lang(text):
    for result in langdetect.detect_langs(text):
        if result.prob > 0.95 and result.lang in (RU, EN):
            return result.lang


#######
#
#   NORM TEXT
#
######


def trans_table(source, target):
    return {
        ord(a): ord(b)
        for a, b in zip(source, target)
    }


DASH_TRANS = trans_table(
    '‑–—−',
    '----'
)


def norm_text(text):
    return text.translate(DASH_TRANS)


########
#
#   VILKA
#
######


Bound = fact(
    'Bound',
    ['amount', 'currency', 'scale', 'tax']
)
Vilka = fact(
    'Vilka',
    ['min', 'max']
)


@dataclass
class NormVilka:
    min: int
    max: int
    currency: str
    tax: str


def norm_vilka(record):
    min, max = record
    currency = min.currency or max.currency or RUB
    tax = min.tax or max.tax or NET
    scale = min.scale or max.scale

    min, max = min.amount, max.amount
    if scale:
        min *= scale
        max *= scale
    else:
        if 20 <= min <= 600:
            min *= K
            max *= K

    return NormVilka(min, max, currency, tax)


def is_ok_vilka(record):
    if record.min >= record.max:
        return

    if record.currency == RUB:
        min, max = 20_000, 600_000
    elif record.currency in (USD, EUR, GBP):
        min, max = 1000, 10000

    return record.min >= min and record.max <= max


NET = 'net'
GROSS = 'gross'

RUB = 'rub'
USD = 'usd'
EUR = 'eur'
GBP = 'gbp'

K = 1000
M = 1_000_000

TAXES = {
    'gross': GROSS,
    'net': NET,
    'гросс': GROSS,
    'грязными': GROSS,
    'до НДФЛ': GROSS,
    'до вычета НДФЛ': GROSS,
    'до налогов': GROSS,
    'на руки': NET,
    'нетто': NET,
    'после НДФЛ': NET,
    'после вычета НДФЛ': NET,
    'после налогов': NET,
    'чистыми': NET,
}

TAX = caseless_pipeline(TAXES).interpretation(
    interpretation.normalized().custom(TAXES.get)
)

PER = caseless_pipeline([
    '/mo',
    '/мес',
    '/месяц',
    'per month',
    'в мес',
    'в месяц',
    '/month',
])

SCALES = {
    'k': K,
    'к': K,
    'т': K,
    'т.': K,
    'тыс': K,
    'тыс.': K,
    'тысяч': K,

    'm': M,
    'м': M,
    'млн': M,
    'млн.': M,
}

SCALE = caseless_pipeline(SCALES).interpretation(
    interpretation.normalized().custom(SCALES.get)
)

CURRENCIES = {
    'р': RUB,
    'р.': RUB,
    'руб': RUB,
    'руб.': RUB,
    'рубл': RUB,
    'рублей': RUB,
    'рос. руб.': RUB,
    '₽': RUB,
    'RUR': RUB,

    '$': USD,
    '$$': USD,
    'USD': USD,
    'долларов': USD,

    '€': EUR,
    'eur': EUR,
    'euro': EUR,
    'евро': EUR,

    '£': GBP,
}

CURRENCY = caseless_pipeline(CURRENCIES).interpretation(
    interpretation.normalized().custom(CURRENCIES.get)
)

SEP = in_('.,')


def norm_int(value):
    value = re.sub(r'[\s,\.]', '', value)
    return int(value)


INT = or_(
    rule(lte(1_000_000)),
    rule(
        lte(999),
        SEP.optional(),
        and_(
            lte(999),
            length_eq(3)
        )
    )
).interpretation(
    interpretation.custom(norm_int)
)


def norm_float(value):
    value = value.replace(' ', '')
    value = value.replace(',', '.')
    return float(value)


FLOAT = rule(
    lte(10),
    SEP,
    lte(100)
).interpretation(
    interpretation.custom(norm_float)
)

APPROX = eq('~')
PLUS = eq('+')

NUM = or_(
    INT,
    FLOAT
)

ATTR = or_(
    SCALE.interpretation(Bound.scale),
    CURRENCY.interpretation(Bound.currency),
    PER,
    TAX.interpretation(Bound.tax),
)

AMOUNT = rule(
    APPROX.optional(),
    NUM.interpretation(Bound.amount),
    PLUS.optional()
)

BOUND = rule(
    CURRENCY.optional().interpretation(Bound.currency),
    AMOUNT,
    ATTR.repeatable(max=4).optional()
).interpretation(
    Bound
)

OT = caseless('от')

DO = caseless('до')

DASH = eq('-')

MIN = BOUND.interpretation(Vilka.min)

MAX = BOUND.interpretation(Vilka.max)

VILKA = or_(
    rule(
        OT, MIN,
        DO, MAX
    ),
    rule(
        MIN,
        DASH,
        MAX
    )
).interpretation(
    Vilka
)

VILKA_TESTS = [
    # AndreyKolomiets/ods_jobs_analytics
    'от 60К до 300К грязными',
    'от 60к до 300к gross',
    '120т.р. - 160 т.р. чистыми',
    '$5k-$8k',
    '150-250 т.р.',
    '2.5-4.5k USD',
    '2.5-4.5k $',
    '2.5-4.5k$',
    '1K - 2K EUR нетто ',
    '1K - 2K € нетто ',
    '1K - 2K€ нетто ',
    '€1K - €2K EUR нетто ',
    '1K - 2K € нетто ',
    '1000 - 2000 € нетто ',
    'от 150 до 250 гросс',
    '130-200к руб.',
    'от 200К до 1М рублей',
    '60 000 - 120 000 т.р. net',
    'от 3,4 до 4,8 млн.рублей',
    '280-400+ тысяч рублей',
    '$$1000-5000',
    '1000-2500k USD',
    'от $ 800 до 1100 net',

    # Вилка:
    '100-160к',
    '40 - 100',
    '180-450 т. руб.',
    '90 000 ₽ - 120 000 ₽ net',

    # TODO Shilo
    # TODO Per hour

    # 'от 150к Net',
    # 'junior 75k руб.- senior 200к руб',
    # 'До 200K USD/год net',
    # 'Стартовая ЗП 2,300€',
    # 'от 80 тыс. руб. на руки, верхнюю границу не указываю',
    # 'до 100 тыс.рублей',

    # '1000-1500р/ч на руки',

    # '100-150 тр gross',
    # '5-6 килобаксов',
    # '80-130кк.',
    # '100-200круб/мес',
]


######
#
#   LOCATION
#
#####


Location = fact(
    'Location',
    ['city', 'metro', 'remote']
)


MSK = 'Москва'
SPB = 'Санкт-Петербург'

CITIES = {
    'Москва': MSK,
    'default-city': MSK,
    'Moscow': MSK,

    'Санкт-Петербург': SPB,
    'Петербург': SPB,
    'СПб': SPB,

    'Новосибирск': 'Новосибирск',
    'Уфа': 'Уфа',
    'Иннополис': 'Иннополис',

    'Минск': 'Минск',
    'Minsk': 'Минск',

    'Киев': 'Киев',
    'Kiev': 'Киев',

    'Амстердам': 'Амстердам',
    'Amsterdam': 'Амстердам',

    'Лондон': 'Лондон',
    'London': 'Лондон',

    'New York': 'Нью-Йорк'
}

CITY = morph_pipeline(CITIES).interpretation(
    interpretation.normalized().custom(CITIES.get)
)

METROS = [
    'Павелецкая',
    'Октябрьская',
    'Шаболовская',
    'Водный Стадион',
    'Выставочная',
    'Кутузовская',
    'Белорусская',
    'Аэропорт',
    'Динамо',
    'Таганская',
]

METRO = morph_pipeline(METROS).interpretation(
    interpretation.normalized()
)

REMOTE = morph_pipeline([
    'Удаленно',
    'Удаленка',
    'Remote',

    'удаленный',
])

LOCATION = or_(
    CITY.interpretation(Location.city),
    METRO.interpretation(Location.metro),
    REMOTE.interpretation(Location.remote.const(True))
).interpretation(Location)


#######
#
#   POSITION
#
#####


Position = fact(
    'Position',
    ['grade', 'title']
)


INTERN = 'intern'
JUNIOR = 'junior'
MIDDLE = 'middle'
SENIOR = 'senior'
LEAD = 'lead'

DS = 'DS'
DA = 'DA'
DE = 'DE'
RESEARCHER = 'researcher'
ANALYST = 'analyst'
DEV = 'dev'

GRADES = {
    'Стажер': INTERN,

    'Юниор': JUNIOR,
    'Младший': JUNIOR,
    'Джун': JUNIOR,
    'Jun': JUNIOR,
    'Junior': JUNIOR,

    'Мидл': MIDDLE,
    'Mid': MIDDLE,
    'Middle': MIDDLE,

    'Старший': SENIOR,
    'Сеньор': SENIOR,
    'Синьёр': SENIOR,
    'Senior': SENIOR,

    'Лид': LEAD,
    'Chief': LEAD,
    'Head': LEAD,
    'Lead': LEAD,
    'Team Lead': LEAD,
    'Tech Lead': LEAD,
}

GRADE = morph_pipeline(GRADES).interpretation(
    interpretation.normalized().custom(GRADES.get)
)

TITLES = {
    'DS': DS,
    'Data Scientist': DS,
    'Data Science': DS,

    'Data Analyst': DA,
    'Аналитик данных': DA,

    'Analyst': ANALYST,
    # 'Аналитик': ANALYST,
    'бизнес-аналитик': ANALYST,
    'Product Analytics': ANALYST,
    'Бизнес аналитик': ANALYST,
    'Product Analyst': ANALYST,

    'Big Data Engineer': DE,
    'DS-инженер': DE,
    'Data Engineer': DE,
    'Data-Engineer': DE,
    'Deep Learning Engineer': DE,
    'ML Engineer': DE,
    'ML-Engineer': DE,
    'ML-инженер': DE,
    'Machine Learning Engineer': DE,
    'СV Engineer': DE,
    'CV Engineer': DE,
    'NLP Engineer': DE,
    'дата-инженер': DE,
    'DL инженер': DE,
    'DataOps Engineer': DE,
    'ML разработчик': DE,
    'ML-разработчик': DE,
    'DL Engineer': DE,
    'MLE': DE,
    'AI Engineer': DE,
    'Computer Vision Engineer': DE,

    'Quantitative Researcher': RESEARCHER,
    'ML Researcher': RESEARCHER,
    'Researcher': RESEARCHER,

    'Software Engineer': DEV,
    'DevOps': DEV,
    'MLOps': DEV,
    'Python Developer': DEV,
    'Software Developer': DEV,
    'backend developer': DEV,
    'python-разработчик': DEV,
}

TITLE = morph_pipeline(TITLES).interpretation(
    interpretation.normalized().custom(TITLES.get)
)

POSITION = or_(
    GRADE.interpretation(Position.grade),
    TITLE.interpretation(Position.title)
).interpretation(
    Position
)


#######
#
#   EMAIL
#
####


def find_emails(text):
    matches = re.finditer(r'[a-z0-9\.]+@[a-z0-9.-]+\.[a-z]{2,7}', text, re.I)
    for match in matches:
        yield match.start(), match.end()


def email_domain(email):
    name, domain = email.split('@', 1)
    return domain


GENERIC_EMAIL_DOMAINS = {
    'gmail.com',
    'mail.ru',
    'yandex.ru',
    'mac.com',
}


def norm_domain(value):
    value = value.lower()
    if value.startswith('www.'):
        value = value[4:]
    return value


#####
#
#   COMPANY
#
#######


COMPANIES = {
    'Yandex Data Factory': 'yandex-team.ru',
    'Фабрику данных Яндекса': 'yandex-team.ru',
    'Яндекс': 'yandex-team.ru',
    'Яндекс.Go': 'yandex-team.ru',
    'Яндекс.Алиса': 'yandex-team.ru',
    'Яндекс.Вертикали': 'yandex-team.ru',
    'Яндекс.Дзен': 'yandex-team.ru',
    'Яндекс.Маркет': 'yandex-team.ru',
    'Яндекс.Погода': 'yandex-team.ru',
    'Яндекс.Поиск': 'yandex-team.ru',
    'Яндекс.Такси': 'yandex-team.ru',
    
    'Авито': 'avito.ru',
    'Joom': 'joom.ru',
    'Ozon': 'ozon.ru',
    'OzonExpress': 'ozon.ru',

    'SberCloud': 'sberbank.ru',
    'SberDevices': 'sberbank.ru',
    'SberGames': 'sberbank.ru',
    'Сбер': 'sberbank.ru',
    'СберТех': 'sberbank.ru',
    'Сбербанк': 'sberbank.ru',

    'HeadHunter': 'hh.ru',
    'МТС': 'mts.ru',

    'X5': 'x5.ru',
    'ЛЕНТА': 'lenta.ru',
    'Leroy Merlin': 'leroymerlin.ru',

    'Тинькофф': 'tinkoff.ru',
    'Tinkoff': 'tinkoff.ru',
    'Райффайзенбанк': 'raiffeisen.ru',
    'Альфа-Банк': 'alfabank.ru',
    'банке Открытие': 'open.ru',
}

COMPANY = morph_pipeline(COMPANIES).interpretation(
    interpretation.normalized().custom(COMPANIES.get)
)


#######
#
#   EXTRACTOR
#
#####


@dataclass
class Match:
    start: int
    stop: int
    type: str
    value: Any


class const:
    VILKA = 'vilka'
    LOCATION = 'location'
    POSITION = 'position'
    COMPANY = 'company'


class VilkaExtractor:
    def __init__(self):
        self.parser = Parser(VILKA)

    def __call__(self, text):
        matches = self.parser.findall(text)
        for match in matches:
            fact = norm_vilka(match.fact)
            if is_ok_vilka(fact):
                start, stop = match.span
                yield Match(start, stop, const.VILKA, fact)


class LocationExtractor:
    def __init__(self):
        self.parser = Parser(LOCATION)

    def __call__(self, text):
        matches = self.parser.findall(text)
        for match in matches:
            start, stop = match.span
            yield Match(
                start, stop,
                const.LOCATION,
                match.fact
            )


class PositionExtractor:
    def __init__(self):
        self.parser = Parser(POSITION)

    def __call__(self, text):
        matches = self.parser.findall(text)
        for match in matches:
            start, stop = match.span
            yield Match(
                start, stop,
                const.POSITION,
                match.fact
            )


class CompanyExtractor:
    def __init__(self):
        self.parser = Parser(COMPANY)

    def __call__(self, text):
        for start, stop in find_emails(text):
            value = text[start:stop]
            value = email_domain(value)
            value = norm_domain(value)
            if value not in GENERIC_EMAIL_DOMAINS:
                yield Match(start, stop, const.COMPANY, value)

        matches = self.parser.findall(text)
        for match in matches:
            start, stop = match.span
            yield Match(
                start, stop,
                const.COMPANY,
                match.fact
            )


class Extractor:
    def __init__(self):
        self.extractors = [
            VilkaExtractor(),
            LocationExtractor(),
            PositionExtractor(),
            CompanyExtractor(),
        ]

    def __call__(self, text):
        for extractor in self.extractors:
            for match in extractor(text):
                yield match

    )
