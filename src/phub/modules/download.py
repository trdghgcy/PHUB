from __future__ import annotations
import time
import logging
from pathlib import Path
from ffmpeg_progress_yield import FfmpegProgress
from typing import TYPE_CHECKING, Callable, Union
from concurrent.futures import ThreadPoolExecutor as Pool, as_completed
from .. import consts
if TYPE_CHECKING:
    from .. import Client
    from ..objects import Video
    from ..utils import Quality
logger = logging.getLogger(__name__)
CallbackType = Callable[[int, int], None]


def default(video, quality, callback, path, start=0):
    """
    Dummy downloader. Fetch a segment after the other.

    Args:
        video       (Video): The video object to download.
        quality   (Quality): The video quality.
        callback (Callable): Download progress callback.
        path          (str): The video download path.
        start         (int): Where to start the download from. Used for download retries.
    """
    logger.info('Downloading using default downloader')
    buffer = b''
    segments = list(video.get_segments(quality))[start:]
    length = len(segments)
    for i, url in enumerate(segments):
        for _ in range(consts.DOWNLOAD_SEGMENT_MAX_ATTEMPS):
            try:
                segment = video.client.call(
                    url, throw=False, timeout=4, silent=True)
                if segment.is_success:
                    buffer += segment.content
                    callback(i + 1, length)
                    break
            except Exception as err:
                logger.error('Error while downloading: %s', err)
            logger.warning('Segment %s failed. Retrying.', i)
            time.sleep(consts.DOWNLOAD_SEGMENT_ERROR_DELAY)
        else:
            logger.error('Maximum attempts reached. Refreshing M3U...')
            return default(video, quality, callback, i - 1)
    logger.info('Concatenating buffer to %s', path)
    with open(path, 'wb') as file:
        file.write(buffer)
    logger.info('Downloading successful.')


def FFMPEG(video, quality, callback, path, start=0):
    """
    Download using FFMPEG with real-time progress reporting.
    FFMPEG must be installed on your system.
    You can override FFMPEG access with consts.FFMPEG_COMMAND.

    Args:
        video       (Video): The video object to download.
        quality   (Quality): The video quality.
        callback (Callable): Download progress callback.
        path          (str): The video download path.
        start         (int): Where to start the download from. Used for download retries.
    """
    logger.info('Downloading using FFMPEG')
    M3U = video.get_M3U_URL(quality)
    command = [f'{consts.FFMPEG_EXECUTABLE}', '-i', M3U,
               '-bsf:a', 'aac_adtstoasc', '-y', '-c', 'copy', str(path)]
    logger.info('Executing `%s`', ' '.join(map(str, command)))
    try:
        ff = FfmpegProgress(command)
        for progress in ff.run_command_with_progress():
            callback(round(progress), 100)
            if progress == 100:
                logger.info('Download successful')
    except Exception as err:
        logger.error('Error while downloading: %s', err)


def _thread(client, url, timeout):
    """
    Download a single segment using the client's call method.
    This function is intended to be used within a ThreadPoolExecutor.
    """
    try:
        response = client.call(url, timeout=timeout, silent=True)
        response.raise_for_status()
        return (url, response.content, True)
    except Exception as e:
        logging.warning(f'Failed to download segment {url}: {e}')
        return (url, b'', False)


def _base_threaded(client, segments, callback, max_workers=20, timeout=10):
    """
    Base threaded downloader using ThreadPoolExecutor.
    """
    logging.info('Threaded download initiated')
    buffer = {}
    length = len(segments)
    with Pool(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(
            _thread, client, url, timeout): url for url in segments}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                url, data, success = future.result()
                if success:
                    buffer[url] = data
                callback(len(buffer), length)
            except Exception as e:
                logging.warning(f'Error processing segment {url}: {e}')
    return buffer


def threaded(max_workers=20, timeout=10):
    """
    Simple threaded downloader.

    Args:
        max_workers (int): How many downloads can take place simultaneously.
        timeout (int): Maximum time before considering a download failed.

    Returns:
        Callable: A download wrapper.
    """

    def wrapper(video, quality, callback, path):
        """
        Wrapper.
        """
        segments = list(video.get_segments(quality))
        buffer = _base_threaded(client=video.client, segments=segments,
                                callback=callback, max_workers=max_workers, timeout=timeout)
        logger.info('Writing buffer to file')
        with open(path, 'wb') as file:
            for url in segments:
                file.write(buffer.get(url, b''))
        logger.info('Successfully wrote file to %s', path)
    return wrapper
