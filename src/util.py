"""util"""

import tornado


class PostHandler(tornado.web.RequestHandler):
    def initialize(self, ctx, fn) -> None:
        self.ctx = ctx
        self.fn = fn

    def post(self) -> None:
        res = self.fn(self.ctx, self.request)
        self.write(str(res))
