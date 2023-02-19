import asyncio
import os
import re
from datetime import datetime
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
import telegram


def get_gradapp_threads(last_tid: int = 0) -> list[dict]:
    # HTTP session
    with requests.Session() as session:

        def inline_get_gradapp_threads(pg: int = 1, depth: int = 1) -> list[dict]:
            """
            cURL Example:
            curl 'https://api.1point3acres.com/api/forums/82/threads?ps=20&order=time_desc&includes=images,topic_tag&pg=1'
                -H ':authority: api.1point3acres.com'
                -H 'accept: application/json, text/plain, */*'
                -H 'accept-encoding: gzip, deflate, br'
                -H 'device-id: 00000000-0000-0000-0000-000000000000'
                -H 'user-agent: %E4%B8%80%E4%BA%A9%E4%B8%89%E5%88%86%E5%9C%B0/0 CFNetwork/1404.0.5 Darwin/22.3.0'
                -H 'authorization: eyJhbGciOiJIUzUx...'
                -H 'accept-language: en-US,en;q=0.9' --compressed
            """

            with session.get(
                    url='https://api.1point3acres.com/api/forums/82/threads',
                    params={
                        'ps': 20,
                        'order': 'time_desc',
                        'includes': 'images,topic_tag',
                        'pg': pg,
                    },
                    headers={
                        'accept': 'application/json, text/plain, */*',
                        'accept-encoding': 'gzip, deflate, br',
                        'accept-language': 'en-US,en;q=0.9',
                        'user-agent': '{UA}/0 CFNetwork/1404.0.5 Darwin/22.3.0'.format(UA=quote("一亩三分地")),
                    }
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

        return inline_get_gradapp_threads()


class GradAppBot:

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.chat_description = ''
        self.bot = telegram.Bot(self.bot_token)

    async def get_last_tid(self) -> int:
        async with self.bot:
            chat = await self.bot.get_chat(chat_id=self.chat_id)
            self.chat_description = chat.description

        tids = re.findall(r'last-tid=(\d+)', chat.description)
        return int(tids[0]) if len(tids) > 0 else -1

    async def set_last_tid(self, tid: int) -> bool:
        if not self.chat_description:
            return False

        async with self.bot:
            self.chat_description = re.sub(
                r'last-tid=(\d+)',
                f'last-tid={tid}',
                self.chat_description)
            return await self.bot.set_chat_description(
                chat_id=self.chat_id,
                description=self.chat_description)

    async def broadcast(self, thread: dict):
        message = '{subject}\nhttps://www.1point3acres.com/bbs/thread-{tid}-1-1.html\n' \
            .format(subject=thread['subject'], tid=thread['tid'])

        message += ' '.join(
            ['#' + thread['author'], '#' +
             datetime.fromtimestamp(thread['dateline'], tz=ZoneInfo("Asia/Shanghai")).strftime('%Y-%m-%d')] +
            ['#' + dict(i)['tagname'] for i in thread['topic_tag'] if isinstance(i, dict)])

        async with self.bot:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                disable_web_page_preview=False,
                disable_notification=False)

    async def check_and_push(self):
        last_tid = await self.get_last_tid()
        threads = get_gradapp_threads(last_tid=last_tid)

        # skip if no threads
        if len(threads) <= 0:
            print('No new threads found since last tid: {0}.'.format(last_tid))
            return

        # broadcast to channel if update last tid succeeded
        if await self.set_last_tid(threads[0]['tid']):
            print('Found {0} threads since last tid: {0}.'.format(len(threads), last_tid))

            # iterate threads
            for thread in threads[::-1]:
                print('tid={tid}\tsubject={subject}'.format(
                    tid=thread['tid'], subject=thread['subject']))
                await self.broadcast(thread)

    def async_check_and_push(self):
        asyncio.run(self.check_and_push())


def main():
    bot_token = os.getenv('TG_BOT_TOKEN')
    chat_id = os.getenv('TG_CHAT_ID')

    if not bot_token \
            or not chat_id:
        print('missing key environment variables.')
        return

    bot = GradAppBot(bot_token=bot_token, chat_id=chat_id)
    bot.async_check_and_push()


if __name__ == '__main__':
    main()
