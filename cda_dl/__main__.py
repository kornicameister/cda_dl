import asyncio
import re
import time
from pathlib import Path
import typing as t

import aiohttp
from bs4 import BeautifulSoup as BS
from bs4 import SoupStrainer as SS
from loguru import logger
import yarl


class VideoPage(t.NamedTuple):
    url: yarl.URL
    title: str


class VideoURL(t.NamedTuple):
    url: yarl.URL
    title: str
    ext: t.Optional[str]


async def main(args: t.Sequence[str]) -> None:
    logger.info(
        'Received arguments args={args}',
        args=args,
    )

    if len(args) != 2:
        raise Exception('python -m cda_dl [url] [download_dir]')

    root_url = yarl.URL(args[0])
    download_dir = Path(args[1]).resolve()

    video_dl_workers_count = 100

    gallery_queue: asyncio.Queue[VideoPage] = asyncio.LifoQueue()
    video_queue: asyncio.Queue[VideoURL] = asyncio.LifoQueue()
    traverse_queue: asyncio.Queue[yarl.URL] = asyncio.LifoQueue()

    logger.info(
        'Will download from url={url}',
        url=root_url,
    )

    async with aiohttp.ClientSession(headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)',
    }) as session:
        # set downloading workers
        workers = [
            asyncio.create_task(
                _download(
                    worker_id=i,
                    video_queue=video_queue,
                    session=session,
                    download_dir=download_dir,
                ),
            ) for i in range(video_dl_workers_count)
        ]

        # worker to traverse gallery page
        workers.append(
            asyncio.create_task(
                _traverse_page(
                    gallery_queue=gallery_queue,
                    traverse_queue=traverse_queue,
                    session=session,
                ),
            ),
        )

        # worker to load video page and find URL to download
        # a file from
        workers.append(
            asyncio.create_task(
                _find_video_link(
                    gallery_queue=gallery_queue,
                    video_queue=video_queue,
                    session=session,
                ),
            ),
        )

        # simple worker that just tracks things
        workers.append(asyncio.create_task(_remaining_downloads(video_queue)))

        # put first url to queue
        await traverse_queue.put(root_url)


async def _download(
        worker_id: int,
        video_queue: asyncio.Queue[VideoURL],
        session: aiohttp.ClientSession,
        download_dir: Path,
) -> None:
    while True:
        video = await video_queue.get()
        try:
            logger.info(
                'worker {worker_id} >> downloading {video}',
                worker_id=worker_id,
                video=video,
            )
            started_at = time.perf_counter()
            resp = await session.get(video.url)

            if resp.status == 200 and resp.content_type == 'application/video':
                filename = download_dir / video.title / (video.ext or '.mp4')
                filename.touch()

                with filename.open('wb') as f_handle:
                    read_size = pow(2, 24)
                    chunk = await resp.content.read(read_size)
                    while chunk:
                        f_handle.write(chunk)
                        chunk = await resp.content.read(read_size)
                await resp.release()

                finished_at = time.perf_counter() - started_at
                logger.info(
                    'worker {worker_id} >> downloaded {video} in {finished_at} seconds',
                    worker_id=worker_id,
                    video=video,
                    finished_at=f'{finished_at:.2f}',
                )
            else:
                print(
                    f'worker {worker_id} >> received ({resp.status}, {resp.content_type}) for {video}, skipping...'
                )
        except Exception:
            print(f'worker {worker_id} >> downloading {video} has just failed...')
            pass

        queue.task_done()


async def _remaining_downloads(queue: asyncio.Queue[t.Any]) -> None:
    await asyncio.sleep(1)
    while True:
        logger.opt(lazy=True).debug(
            'There are {all_task_count} tasks in total',
            all_task_count=lambda: len(asyncio.all_tasks()),
        )
        await asyncio.sleep(5)


async def _find_video_link(
        session: aiohttp.ClientSession,
        gallery_queue: asyncio.Queue[VideoPage],
        video_queue: asyncio.Queue[VideoURL],
) -> None:
    while True:
        video_page_url = await gallery_queue.get(e
        logger.info('Examining video url={u}', u=video_page_url)

        content = await (await session.get(video_page_url)).text()

        soup = BS(content, 'html.parser')
        video_anchors = map(
            lambda h: VideoPage(title=h.img.get('alt').strip(), url=h.get('href')),
            soup.find_all(
                'a',
                class_=re.compile('-?link-?'),
                href=re.compile('video'),
            ),
        )
        for va in video_anchors:
            await gallery_queue.put(va)

        next_anchor = soup.find('a', rel='next')
        if next_anchor:
            _, page_number = next_anchor['href'].replace('?', '').split('=')
            await traverse_queue.put(video_page_url.with_query(page=int(page_number)))


async def _traverse_page(
        session: aiohttp.ClientSession,
        traverse_queue: asyncio.Queue[yarl.URL],
        gallery_queue: asyncio.Queue[VideoPage],
) -> None:
    while True:
        root_url = await traverse_queue.get()
        domain_url = yarl.URL(f'{root_url.scheme}://{root_url.parent.host}')

        logger.info(f'Looking for downloadable video files at {root_url}')

        content = await (await session.get(root_url)).text()

        soup = BS(content, 'html.parser', parse_only=SS('a'))
        video_anchors = map(
            lambda h: VideoPage(
                title=h.img.get('alt').strip(),
                url=domain_url.with_path(h.get('href')),
            ),
            soup.find_all(
                'a',
                class_=re.compile('-?link-?'),
                href=re.compile('video'),
            ),
        )
        for va in video_anchors:
            await gallery_queue.put(va)

        next_anchor = soup.find('a', rel='next')
        if next_anchor:
            _, page_number = next_anchor['href'].replace('?', '').split('=')
            await traverse_queue.put(root_url.with_query(page=int(page_number)))


if __name__ == '__main__':
    import sys

    asyncio.run(main(sys.argv[1:]))
