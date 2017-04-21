import os


def get_optional_env_variable(var_name):
    try:
        return os.environ[var_name]
    except KeyError:
        return None


def enable_debugger():
    if get_optional_env_variable("DEBUG") == "True":
        import ptvsd
        ptvsd.enable_attach("my_secret", address=('0.0.0.0', 3000))
        print "Waiting for debugger to attach..."
        ptvsd.wait_for_attach()

