import os


def get_optional_env_variable(var_name):
    try:
        return os.environ[var_name]
    except KeyError:
        return None


def enable_debugger():
    if get_optional_env_variable("DEBUG") == "True":
        import ptvsd
        secret = get_optional_env_variable("REMOTE_DEBUG_SECRET")
        ptvsd.enable_attach(secret=secret, address=('0.0.0.0', 3000))

        if get_optional_env_variable("REMOTE_DEBUG") == "True":
            print "Waiting for debugger to attach..."
            ptvsd.wait_for_attach()
