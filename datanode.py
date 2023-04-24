import asyncio
import os
import sys
from aiohttp import web
import base64
def parse_message(message):
    splitted_message = message.split(" ")
    return splitted_message[0], splitted_message[1:]


class DataNode:
    def __init__(self):
        self.id = sys.argv[1]
        self.local_storage_base_path = f"~/Project-edfs/{self.id}"
        self.info = ('127.0.0.1', sys.argv[2])

    def launch_server(self):
        app = web.Application()
        app.add_routes([web.post('/write', self.write_block),
                        web.get('/read', self.read_block)])
        web.run_app(app, host=self.info[0], port=self.info[1])



    async def write_block(self, req):
        data = await req.json()
        block_id = data["block_id"]
        replica = data["replica"]
        block_content = base64.b64decode(data["block_content"].encode('ascii'))
        if not os.path.exists(f'./{self.id}'):
            os.makedirs(f'./{self.id}')
        with open(f"./{self.id}/{block_id}-r{replica}", 'wb') as block_writer:
            block_writer.write(block_content)
        return web.Response(status=200, text=f"block {block_id} replica {replica} written succesfully")


    async def read_block(self, req):
        pass
    async def block_report(self):
        pass


if __name__ == "__main__":
    dn = DataNode()
    dn.launch_server()