import asyncio
import logging
import os
import random
import re
import sys
import time
import traceback
import typing
from datetime import datetime
from functools import cached_property
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
import telegram
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, stream=sys.stdout)


def wait(n: float):
    def decorator(call):
        def wrapper(*args, **kwargs):
            time.sleep(n)
            return call(*args, **kwargs)
        return wrapper
    return decorator


def no_exception(v: typing.Any):
    def decorator(call):
        def wrapper(*args, **kwargs):
            try:
                return call(*args, **kwargs)
            except Exception as e:
                logging.warning('{function}() => {exception}'.
                                format(function=call.__name__, exception=e))
                return v
        return wrapper
    return decorator


class Helper1P3A:

    def __init__(self, token: str, device_id: str):
        self.authorization = token
        self.device_id = device_id
        self.session = requests.Session()
        self.useragent = '{UA}/0 CFNetwork/1404.0.5 Darwin/22.3.0' \
            .format(UA=quote("一亩三分地"))

    @cached_property
    def headers(self) -> dict:
        return {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'device-id': self.device_id,
            'user-agent': self.useragent,
            'authorization': self.authorization,
        }

    @cached_property
    def options(self) -> dict:
        """
        curl 'https://api.1point3acres.com/api/types/164/options'
            -H ':authority: api.1point3acres.com'
            -H 'accept: */*'
            -H 'content-type: application/json'
            -H 'user-agent: %E4%B8%80%E4%BA%A9%E4%B8%89%E5%88%86%E5%9C%B0/0 CFNetwork/1404.0.5 Darwin/22.3.0'
            -H 'device-id: 00000000-0000-0000-0000-000000000000'
            -H 'accept-language: en-US,en;q=0.9'
            -H 'authorization: eyJhbG...'
            -H 'accept-encoding: gzip, deflate, br' --compressed
        """

        with self.session.get(
                url='https://api.1point3acres.com/api/types/164/options',
                headers=self.headers) as r:
            r.raise_for_status()
            data = r.json()

        assert data['errno'] == 0

        # pre-defined options
        return data['options']

    def __find_option_by_id(self, option_id: int) -> dict:
        for option in self.options:
            if option['optionid'] == option_id:
                return option
        return {}

    def get_gradapp_threads(self, last_tid: int = 0) -> typing.Iterable[dict]:

        @wait(random.uniform(1, 3))
        def inline_get_gradapp_threads(pg: int = 1, depth: int = 1) -> typing.Iterable[dict]:
            """
            curl 'https://api.1point3acres.com/api/forums/82/threads?ps=20&order=time_desc&includes=images,topic_tag&pg=1'
                -H ':authority: api.1point3acres.com'
                -H 'accept: application/json, text/plain, */*'
                -H 'accept-encoding: gzip, deflate, br'
                -H 'device-id: 00000000-0000-0000-0000-000000000000'
                -H 'user-agent: %E4%B8%80%E4%BA%A9%E4%B8%89%E5%88%86%E5%9C%B0/0 CFNetwork/1404.0.5 Darwin/22.3.0'
                -H 'authorization: eyJhbGciOiJIUzUx...'
                -H 'accept-language: en-US,en;q=0.9' --compressed
            """

            with self.session.get(
                    url='https://api.1point3acres.com/api/forums/82/threads',
                    headers=self.headers,
                    params={
                        'ps': 20,
                        'order': 'time_desc',
                        'includes': 'images,topic_tag',
                        'pg': pg,
                    },
            ) as r:
                r.raise_for_status()
                data = r.json()

            assert data['errno'] == 0
            assert len(data['threads']) > 0

            threads = data['threads']

            # return all current threads
            if last_tid <= 0 or depth >= 5:
                return threads

            # this list contains all unpushed threads
            if threads[-1]['tid'] <= last_tid:
                return [t for t in threads if t['tid'] > last_tid]

            # need to fetch more threads
            return threads + inline_get_gradapp_threads(pg=pg + 1, depth=depth + 1)

        return reversed(inline_get_gradapp_threads())

    def get_gradapp_threads_with_details(self, last_tid: int = 0) -> typing.Iterable[dict]:
        return (dict(**thread, details=self.get_thread_details(thread['tid']))
                for thread in self.get_gradapp_threads(last_tid=last_tid))

    @no_exception(v={})
    @wait(random.uniform(0.5, 2.0))
    def get_thread_details(self, tid: int) -> dict:
        """
        curl 'https://api.1point3acres.com/api/threads/968936/options'
            -H ':authority: api.1point3acres.com'
            -H 'accept: */*'
            -H 'content-type: application/json'
            -H 'user-agent: %E4%B8%80%E4%BA%A9%E4%B8%89%E5%88%86%E5%9C%B0/0 CFNetwork/1404.0.5 Darwin/22.3.0'
            -H 'device-id: 00000000-0000-0000-0000-000000000000'
            -H 'accept-language: en-US,en;q=0.9'
            -H 'authorization: eyJhbG...'
            -H 'accept-encoding: gzip, deflate, br' --compressed
        """

        with self.session.get(
                url='https://api.1point3acres.com/api/threads/{tid}/options'.format(tid=tid),
                headers=self.headers
        ) as r:
            r.raise_for_status()
            data = r.json()

        assert data['errno'] == 0

        # translate options to details
        details = {}
        for option in data['options']:
            value = str(option['value']).strip(' |')
            table = self.__find_option_by_id(option['optionid'])
            if not table or not value:
                continue
            details[table['title']] = \
                dict(table['choices']).get(value) if table.get('choices') else value
        return details

    @no_exception(v={})
    @wait(random.uniform(0.5, 2.0))
    def get_thread_details_legacy(self, tid: int) -> dict:
        # NOTE: this method can be usually blocked by Cloudflare
        with self.session.get(
                url='https://www.1point3acres.com/bbs/thread-{tid}-1-1.html'.format(tid=tid)) as r:
            r.raise_for_status()

            return dict((k, v) for k, v in
                        ((str(row.find('th').text).rstrip(':'), str(row.find('td').text).strip())
                         for row in BeautifulSoup(r.content, 'html.parser')
                        .find('table', attrs={'summary': '分类信息'})
                        .find('tbody')
                        .find_all('tr'))
                        if not any(s in v for s in ('隐藏内容', '积分不足', '解锁阅读')))


class GradAppBot:

    def __init__(self, bot_token: str, chat_id: str, helper: Helper1P3A):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.helper = helper
        self.chat_description = ''
        self.bot = telegram.Bot(self.bot_token)

    async def get_last_tid(self) -> int:
        chat = await self.bot.get_chat(chat_id=self.chat_id)
        self.chat_description = chat.description
        tids = re.findall(r'last-tid=(\d+)', chat.description)
        return int(tids[0]) if len(tids) > 0 else -1

    @wait(random.uniform(2, 3))
    async def set_last_tid(self, tid: int) -> bool:
        if not self.chat_description:
            return False

        self.chat_description = re.sub(
            r'last-tid=(\d+)',
            f'last-tid={tid}',
            self.chat_description)
        return await self.bot.set_chat_description(
            chat_id=self.chat_id,
            description=self.chat_description)

    @staticmethod
    def format_message(thread: dict):
        post_date = datetime.fromtimestamp(thread['dateline'],
                                           tz=ZoneInfo("Asia/Shanghai")).strftime('%Y-%m-%d')

        logo = {
            'Offer': '🎉',
            'AD小奖': '✅',
            'AD无奖': '✅',
            'Reject': '🚫',
            'Waiting': '⏳',
        }.get(thread['details'].get('申请结果'), '📖')

        return '\n'.join([
            '{logo} {subject}'.format(logo=logo, subject=thread['subject']),
            *(f'* {k}: {v}' for k, v in dict(thread['details']).items()),
            '#{author} {date}'.format(author=thread['author'], date=post_date),
            *('#' + str(dict(i)['tagname']).replace(' ', '_')
              for i in thread['topic_tag'] if isinstance(i, dict)),
            'https://www.1point3acres.com/bbs/thread-{tid}-1-1.html'.format(tid=thread['tid']),
        ])

    @wait(random.uniform(1, 3))
    async def broadcast(self, message: str):
        await self.bot.send_message(
            chat_id=self.chat_id,
            text=message,
            disable_web_page_preview=False,
            disable_notification=False)

    async def check_and_push(self):
        last_tid = await self.get_last_tid()
        threads = self.helper.get_gradapp_threads_with_details(last_tid=last_tid)

        count = 0
        # iterate threads in ascending order
        for thread in threads:
            # build broadcast message from thread
            message = self.format_message(thread=thread)

            # break if update last tid failed
            if not await self.set_last_tid(thread['tid']):
                break

            logging.info('tid={tid}\tsubject={subject}'.format(
                tid=thread['tid'], subject=thread['subject']))
            # broadcast to channel
            await self.broadcast(message=message)
            count += 1

        logging.info('Found and broadcast {0} threads.'.format(count))

    def async_check_and_push(self):
        asyncio.run(self.check_and_push())


def main():
    bot_token = os.getenv('TG_BOT_TOKEN')
    chat_id = os.getenv('TG_CHAT_ID')
    api_token_1p3a = os.getenv('API_TOKEN_1P3A')
    device_id_1p3a = os.getenv('DEVICE_ID_1P3A')

    if not bot_token \
            or not chat_id \
            or not api_token_1p3a \
            or not device_id_1p3a:
        logging.info('missing key environment variables.')
        return

    try:
        helper = Helper1P3A(token=api_token_1p3a, device_id=device_id_1p3a)
        bot = GradAppBot(bot_token=bot_token, chat_id=chat_id, helper=helper)
        bot.async_check_and_push()
    except Exception as e:
        traceback.print_exception(e)


if __name__ == '__main__':
    main()
