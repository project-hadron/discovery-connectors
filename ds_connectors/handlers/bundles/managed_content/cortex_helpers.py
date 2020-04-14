import os
import json
import sys

from .env import CortexEnv

TOKEN_ENV_VAR_NAME_FOR_DAEMONS = "startup.token"
API_ENDPOINT_FOR_DAEMONS = "http://cortex-kong.cortex.svc.cluster.local:8000"


# -------------------------------------- Token Loaders --------------------------------------
def load_token_from_job_env():
    try:
        return json.loads(sys.argv[1]).get("token", None)
    except Exception:
        return None


def load_token_from_daemon_env():
    return os.getenv(TOKEN_ENV_VAR_NAME_FOR_DAEMONS, None)


def load_token_from_local_env():
    return CortexEnv().token


def load_token(env_resolution_order=["args","local-cli","local-env","job","daemon"], token=None):
    """
    Returns the first token it can find based on the resolution order
    """
    token_finders_per_env = {
        "args": (lambda: token),
        "daemon": load_token_from_daemon_env,
        "job": load_token_from_job_env,
        "local-env": CortexEnv.get_cortex_token,
        "local-cli": (lambda: CortexEnv.get_cortex_profile().get("token", None))
    }
    for env in env_resolution_order:
        if not env in token_finders_per_env:
            raise Exception(f"Invalid Env: {env}")
        else:
            _token = token_finders_per_env[env]()
        if _token is not None:
            return _token
    # raise Exception(f"Could not find token in envs: {env_resolution_order}")
    return None

# ------------------------------------ Endpoint Loaders ------------------------------------


def load_endpoint_from_daemon_env():
    return API_ENDPOINT_FOR_DAEMONS


def load_endpoint_from_job_env():
    try:
        return json.loads(sys.argv[1]).get("apiEndpoint", None)
    except Exception:
        return None


def load_api_endpoint(env_resolution_order=["args","local-cli","local-env","job","daemon"], endpoint=None):
    """
    Returns the first api endpoint it can extract from the enviroment.
    The order of in which the environments are searched is dictated by the env_resolution_order
    """
    endpoint_finders_per_env = {
        "args": (lambda: endpoint),
        "daemon": load_endpoint_from_daemon_env,
        "job": load_endpoint_from_job_env,
        "local-env": (lambda: os.getenv('CORTEX_URI', None)),
        "local-cli": (lambda: CortexEnv.get_cortex_profile().get("url", None))
    }
    for env in env_resolution_order:
        if not env in endpoint_finders_per_env:
            raise Exception(f"Invalid Env: {env}")
        else:
            _endpoint = endpoint_finders_per_env[env]()
        if _endpoint is not None:
            return _endpoint
    # raise Exception(f"Could not find endpoint in envs: {env_resolution_order}")
    return None