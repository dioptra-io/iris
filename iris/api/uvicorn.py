from uvicorn.workers import UvicornWorker


class Worker(UvicornWorker):
    CONFIG_KWARGS = {"forwarded_allow_ips": "*", "proxy_headers": True}
