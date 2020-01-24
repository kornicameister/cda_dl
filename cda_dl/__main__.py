import asyncio
import operator
from pathlib import Path
import re
import time
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
    quality: t.Optional[str]


if t.TYPE_CHECKING:
    VideoDownloadQueue = asyncio.Queue[VideoURL]
    GalleryQueue = asyncio.Queue[VideoPage]
    URLQueue = asyncio.Queue[yarl.URL]
    AnyQueue = asyncio.Queue[t.Any]
else:
    URLQueue = GalleryQueue = VideoDownloadQueue = AnyQueue = None


async def main(args: t.Sequence[str]) -> None:
    logger.info(
        'Received arguments args={args}',
        args=args,
    )

    if len(args) != 2:
        raise Exception('python -m cda_dl [url] [download_dir]')

    root_url = yarl.URL(args[0])
    download_dir = Path(args[1]).expanduser()
    download_dir.mkdir(exist_ok=True)

    video_dl_workers_count = 5
    traverse_page_workers = 1
    find_video_link_workers = 2

    gallery_queue: GalleryQueue = asyncio.LifoQueue()
    video_dl_queue: VideoDownloadQueue = asyncio.LifoQueue()
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
            *[
                asyncio.create_task(
                    _download(
                        worker_id=i,
                        video_dl_queue=video_dl_queue,
                        session=session,
                        download_dir=download_dir,
                    ),
                ) for i in range(video_dl_workers_count)
            ],
            *[
                asyncio.create_task(
                    _traverse_gallery(
                        worker_id=i,
                        gallery_queue=gallery_queue,
                        traverse_queue=traverse_queue,
                        session=session,
                    ),
                ) for i in range(traverse_page_workers)
            ],
            *[
                asyncio.create_task(
                    _find_video_link(
                        worker_id=i,
                        gallery_queue=gallery_queue,
                        video_dl_queue=video_dl_queue,
                        session=session,
                    ),
                ) for i in range(find_video_link_workers)
            ],
        ]

        workers.append(asyncio.create_task(_remaining_downloads()))

        started_at = time.perf_counter()

        # put first url to download
        await traverse_queue.put(root_url)

        # wait for all queues to be empty
        await traverse_queue.join()
        await gallery_queue.join()
        await video_dl_queue.join()

        # kill workers
        [dw.cancel() for dw in workers]
        await asyncio.gather(*workers, return_exceptions=True)

        finished_at = time.perf_counter() - started_at

        logger.info('Downloaded everything in {t}s', t=f'{finished_at:.2f}')


async def _download(
        worker_id: int,
        video_dl_queue: VideoDownloadQueue,
        session: aiohttp.ClientSession,
        download_dir: Path,
) -> None:
    with logger.contextualize(
            worker_id=worker_id,
            task='download',
    ):
        while True:
            video = await video_dl_queue.get()
            try:
                logger.info(
                    'Downloading {video}',
                    video=video,
                )
                started_at = time.perf_counter()
                resp = await session.get(video.url)

                filename = download_dir / f'{video.title}.{video.ext or "mp4"}'
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
                    'Downloaded {video} in {finished_at} seconds',
                    video=video,
                    finished_at=f'{finished_at:.2f}',
                )
            except Exception:
                logger.exception(
                    'Download {video} just failed...',
                    video=video.title,
                )

            video_dl_queue.task_done()


async def _remaining_downloads() -> None:
    await asyncio.sleep(1)
    while True:
        logger.opt(lazy=True).debug(
            'There are {all_task_count} tasks in total',
            all_task_count=lambda: len(asyncio.all_tasks()),
        )
        await asyncio.sleep(10)


async def _find_video_link(
        worker_id: int,
        session: aiohttp.ClientSession,
        gallery_queue: GalleryQueue,
        video_dl_queue: VideoDownloadQueue,
) -> None:
    from selenium import webdriver

    options = webdriver.FirefoxOptions()
    options.headless = True

    driver = webdriver.Firefox(options=options)

    with logger.contextualize(
            worker_id=worker_id,
            task='find_video_dl_link',
    ):
        while True:
            # in here we will deal with selenium and firefox driver
            video_page_url = await gallery_queue.get()
            logger.info('Examining video url={u}', u=video_page_url)

            soup = BS(
                await (await session.get(video_page_url.url)).text(),
                'html.parser',
                parse_only=SS('a'),
            )
            logger.info(
                'Successfully downloaded video page {u}',
                u=video_page_url,
            )

            highest_quality = sorted(
                map(
                    operator.attrgetter('text'),
                    soup.find_all('a', class_='quality-btn'),
                ),
                key=lambda q: int(q.replace('p', '')),
            )[-1]
            logger.info(
                'Highest quality is {hq}',
                hq=highest_quality,
            )

            driver.get(str(video_page_url.url.with_query(wersja=highest_quality)))
            await asyncio.sleep(5)

            video_src = driver.find_element_by_tag_name('video').get_attribute('src')
            video_dl_url = yarl.URL(video_src)
            logger.info(
                'Obtained downloadable video URL={u}',
                u=video_dl_url,
            )

            await video_dl_queue.put(
                VideoURL(
                    title=video_page_url.title,
                    quality=highest_quality,
                    url=video_dl_url,
                    ext=video_dl_url.path.split('/')[-1].rsplit('.', 1)[-1],
                ),
            )
            gallery_queue.task_done()


async def _traverse_gallery(
        worker_id: int,
        session: aiohttp.ClientSession,
        traverse_queue: URLQueue,
        gallery_queue: GalleryQueue,
) -> None:
    with logger.contextualize(
            worker_id=worker_id,
            task='traverse_gallery',
    ):
        while True:
            root_url = await traverse_queue.get()
            domain_url = yarl.URL(f'{root_url.scheme}://{root_url.parent.host}')

            logger.info(
                'Looking for downloadable video files at {root_url}',
                root_url=root_url,
            )

            soup = BS(
                await (await session.get(root_url)).text(),
                'html.parser',
                parse_only=SS('a'),
            )

            video_anchors = list(
                map(
                    lambda h: VideoPage(
                        title=h.img.get('alt').strip(),
                        url=domain_url.with_path(h.get('href')),
                    ),
                    soup.find_all(
                        'a',
                        class_=re.compile('-?link-?'),
                        href=re.compile('video'),
                    ),
                ),
            )
            logger.info(
                'Located {c} videos to download via {u}',
                c=len(video_anchors),
                u=root_url,
            )

            for va in video_anchors:
                await gallery_queue.put(va)
            traverse_queue.task_done()

            next_anchor = soup.find('a', rel='next')
            if next_anchor:
                _, page_number = next_anchor['href'].replace('?', '').split('=')
                await traverse_queue.put(root_url.with_query(page=int(page_number)))


if __name__ == '__main__':
    import sys
    import uvloop

    uvloop.install()
    asyncio.run(main(sys.argv[1:]))
