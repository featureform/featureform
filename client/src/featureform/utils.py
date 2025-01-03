from urllib.parse import urlparse


def is_valid_url(url: str, requires_port: bool = True) -> bool:
    try:
        parsed = urlparse(url)

        return all(
            [
                parsed.scheme,
                parsed.hostname,
                parsed.port is not None if requires_port else True,
            ]
        )
    except ValueError:
        # Raised if the URL is invalid
        return False
