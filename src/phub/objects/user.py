from __future__ import annotations
import logging
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Literal, Union
from .. import utils
from .. import consts
from .. import errors
if TYPE_CHECKING:
    from ..core import Client
    from . import Video, Image
    from . import queries
logger = logging.getLogger(__name__)


@dataclass
class _QuerySupportIndex:
    """
    Represents indexes about supported queries for a user and their built urls.
    """
    videos: object = None
    upload: object = None


class User:
    """
    Represents a Pornhub user.
    """

    def __init__(self, client, name, url, type=None):
        """
        Initialize a new user object.

        Args:
            client (Client): The client parent.
            name (str): The username.
            url (str): The user page URL.
        """
        self.client = client
        self.name = name
        self.url = consts.re.remove_host(url)
        self.type = type or consts.re.get_user_type(url)
        self.loaded_keys = list(self.__dict__.keys()) + ['loaded_keys']
        logger.debug('Initialized new user object %s', self)
        self._cached_avatar_url: str = None

    def __repr__(self):
        return f'phub.{self.type.capitalize()}({self.name})'

    def refresh(self):
        """
        Refresh this instance cache.
        """
        logger.info('Refreshing %s object', self)
        for key in list(self.__dict__.keys()):
            if not key in self.loaded_keys:
                logger.debug('Deleting key %s', key)
                delattr(self, key)

    def dictify(self, keys='all', recursive=False):
        """
            Convert the object to a dictionary.

            Args:
                keys (str): The data keys to include.
                recursive (bool): Whether to allow other PHUB objects to dictify.

            Returns:
                dict: A dict version of the object.
            """
        return utils.dictify(self, keys, ['name', 'url', 'type', 'bio', 'info', 'avatar'], recursive)

    @classmethod
    def from_video(cls, video):
        """
        Find the author of a video.

        Args:
            video (Video): A video object.
        """
        if video.page is None:
            video.fetch('page@')
        guess = consts.re.video_model(
            video.page, throw=False) or consts.re.video_channel(video.page, throw=False)
        if not guess:
            logger.error('Author of %s not found', video)
            raise errors.RegexError('Could not find user for video', video)
        return cls(client=video.client, name=guess[1], url=utils.concat(consts.HOST, guess[0]))

    @classmethod
    def get(cls, client, user):
        """
        Fetch a user knowing its name or URL.
        Note - Using only a username makes the fetch between
        1 and 3 times slower, you might prefer to use a direct
        URL.

        Args:
            client (Client): The parent client.
            user (str): Username or URL.
        """
        if consts.re.is_url(user):
            url = user
            path = url.split('/')
            user_type = path[-2]
            name = path[-1]
        else:
            name = '-'.join(user.split())
            for type_ in ('model', 'pornstar', 'channels'):
                guess = utils.concat(type_, name)
                response = utils.head(client, guess)
                if response:
                    logger.info('Guessing type of %s is %s', user, type_)
                    url = response
                    user_type = type_
                    break
            else:
                logger.error('Could not guess type of %s', user)
                raise errors.UserNotFound(f'User {user} not found.')
        return cls(client=client, name=name, type=user_type, url=url)

    @cached_property
    def _supports_queries(self):
        """
        Checks query support.
        """
        index = _QuerySupportIndex()
        videos_url = utils.concat(self.url, 'videos')
        if utils.head(self.client, videos_url):
            index.videos = videos_url
        upload_url = utils.concat(videos_url, 'upload')
        if self.type == 'pornstar' and utils.head(self.client, upload_url):
            index.upload = upload_url
        return index

    @cached_property
    def videos(self):
        """
        Get the list of videos published by this user.
        """
        from .query import queries
        url = self._supports_queries.videos or self.url
        hint = (lambda raw: raw.split('id="mostRecentVideosSection')
                [1]) if self._supports_queries.upload else None
        return queries.VideoQuery(client=self.client, func=url, container_hint=hint)

    @cached_property
    def uploads(self):
        """
        Attempt to get the list of videos uploaded by this user.
        """
        from .query import queries
        url = self._supports_queries.upload
        query = queries.VideoQuery
        if not url:
            logger.info('User %s does not support uploads', self)
            query = queries.EmptyQuery
        return query(self.client, func=url)

    @cached_property
    def _page(self):
        """
        The user page.
        """
        return self.client.call(self.url).text

    @cached_property
    def bio(self):
        """
        The user bio.
        """
        return consts.re.user_bio(self._page, throw=False)

    @cached_property
    def info(self):
        """
        The user detailed infos.

        [Experimental]

        Warning: keys depend on the language.
        """
        li = consts.re.user_infos(self._page)
        return {k: v for k, v in li}

    @cached_property
    def avatar(self):
        """
        The user avatar.
        """
        from . import Image
        url = getattr(self, '_cached_avatar_url') or consts.re.user_avatar(
            self._page)
        return Image(client=self.client, url=url, name=f'{self.name}-avatar')
