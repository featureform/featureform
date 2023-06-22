from tenacity import retry, stop_after_attempt, wait_exponential

# Adds an exponential retry to any function/method.
def add_exponential_retry(f, max_attempts=10, multiplier=1, max_wait=60):
    @retry(stop=stop_after_attempt(max_attempts),
           wait=wait_exponential(multiplier=multiplier, max=max_wait))
    def retried_f(*args, **kwargs):
        return f(*args, **kwargs)

    return retried_f
