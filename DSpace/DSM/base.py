from concurrent import futures


class AdminBaseHandler(object):
    def __init__(self):
        self.executor = futures.ThreadPoolExecutor(max_workers=10)
