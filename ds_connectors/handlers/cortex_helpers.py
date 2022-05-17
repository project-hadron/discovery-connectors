import os

TOKEN_ENV_VAR_NAME = "CORTEX_TOKEN"
API_ENPOINT_ENV_VAR_NAME = "CORTEX_API_ENDPOINT"


# -------------------------------------- Token Loaders --------------------------------------
def load_token_from_env():
    return os.getenv(TOKEN_ENV_VAR_NAME, None)


def load_token(env_resolution_order=["args","env"], token=None):
    """
    Returns the first token it can find based on the resolution order
    """
    token_finders_per_env = {
        "args": (lambda: token),
        "env": load_token_from_env
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


def load_endpoint_from_env():
    return API_ENPOINT_ENV_VAR_NAME


def load_api_endpoint(env_resolution_order=["args","env"], endpoint=None):
    """
    Returns the first api endpoint it can extract from the enviroment.
    The order of in which the environments are searched is dictated by the env_resolution_order
    """
    endpoint_finders_per_env = {
        "args": (lambda: endpoint),
        "env": load_endpoint_from_env
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