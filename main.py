
import re
import json
import zipfile
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
        and count_words(record.text) > 50
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
    )
