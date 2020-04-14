"""
Copyright 2019 Cognitive Scale, Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json
import logging
from requests.exceptions import HTTPError
from pathlib import Path


def log_message(msg: str, log: logging.Logger, level=logging.INFO, *args, **kwargs):
    """
    Logs a message.

    :param msg: Message to log
    :param log: logger where message should be logged
    :param level: optional log level, defaults to INFO
    :param args: a tuple of arguments passed to the logger
    :param kwargs: a dictionary of keyword arguments passed to the logger
    """
    log.log(level, msg, *args, **kwargs)


def get_cortex_profile(profile_name=None):
    """
    Gets the current cortex profile or the profile that matches the optionaly given name.
    """
    cortex_config_path = Path.home() / '.cortex/config'

    if cortex_config_path.exists():
        with cortex_config_path.open() as f:
            cortex_config = json.load(f)

        if profile_name is None:
            profile_name = cortex_config.get('currentProfile')

        return cortex_config.get('profiles', {}).get(profile_name, {})
    return {}


def get_logger(name):
    """
    Gets a logger with the given name.
    """
    log = logging.getLogger(name)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s/%(module)s: %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    return log


def raise_for_status_with_detail(resp):
    """
    wrap raise_for_status and attempt give detailed reason for api failure
    re-raise HTTPError for normal flow
    :param resp: python request resp
    :return:
    """
    try:
        resp.raise_for_status()
    except HTTPError as http_exception:
        try:
            log_message(msg=resp.json(), log=get_logger('http_status'), level=logging.ERROR)
        except Exception as e:
            pass # resp.json() failed
        finally:
            raise http_exception

