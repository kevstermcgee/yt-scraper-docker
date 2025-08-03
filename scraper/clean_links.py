import re
from urllib.parse import urlparse, parse_qs

def extract_youtube_id(url: str) -> str:
    parsed_url = urlparse(url)
    if parsed_url.hostname in ["www.youtube.com", "youtube.com"]:
        if parsed_url.path == "/watch":
            return parse_qs(parsed_url.query).get("v", [None])[0]
        elif parsed_url.path.startswith("/embed/"):
            return parsed_url.path.split("/embed/")[1]
        elif parsed_url.path.startswith("/v/"):
            return parsed_url.path.split("/v/")[1]

    if parsed_url.hostname in ["youtu.be"]:
        return parsed_url.path.lstrip("/")

    match = re.search(r"(?:v=|\/)([0-9A-Za-z_-]{11})(?:\?|&|$)", url)
    return match.group(1) if match else None
