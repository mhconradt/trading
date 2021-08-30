import time


def ttl_cache(ttl: float):
    def decorator_function(user_function):
        last_t, last_v, last_args = 0, None, ()

        def decorated_function(*args):
            t = time.time()
            nonlocal last_t, last_v, last_args
            if last_t:
                if args == last_args:
                    if t - last_t < ttl:
                        return last_v
            last_t = t
            last_v = user_function(*args)
            last_args = args
            return last_v

        return decorated_function

    return decorator_function
