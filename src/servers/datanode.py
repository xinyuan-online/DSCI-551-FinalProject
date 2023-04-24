import asyncio
import os
import sys
from aiohttp import web
import base64
def parse_message(message):
    splitted_message = message.split(" ")
    return splitted_message[0], splitted_message[1:]


class DataNode:
    def __init__(self, id, hostname, port, local_storage_path, home_path):
        self.id = id
        self.local_storage_base_path = f"{local_storage_path}/{self.id}"
        print(self.local_storage_base_path)
        self.info = (hostname, port)
        self.home_path = home_path

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
        if not os.path.exists(f'{self.local_storage_base_path}'):
            os.makedirs(f'{self.local_storage_base_path}')
        with open(f"{self.local_storage_base_path}/{block_id}-r{replica}", 'wb') as block_writer:
            block_writer.write(block_content)
        return web.Response(status=200, text=f"block {block_id} replica {replica} written succesfully")


    async def read_block(self, req):
        pass
    async def block_report(self):
        pass

def run_datanode(*args):
    try:
        dn = DataNode(*args)
        dn.launch_server()
    except KeyboardInterrupt:
        print("Interrupted")

    