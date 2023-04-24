import asyncio
from aiohttp import web
import random
import json
import logging
from FSTree import FSTree, Inode, INodeError
import FSTree as fst

routes = web.RouteTableDef()

class NameNode():
    def __init__(self):
        self.fstree = FSTree()
        self.info = ('127.0.0.1', 10001)
        self.datanodes_avaliable = ['datanode_1', 'datanode_2']
        self.client_info = ('127.0.0.1', 10000)
        self.replication_factor = 3
        self.block_size = 64 * 1024 * 1024

        # {id: [datanodes that hold the replica]}
        self.block_mapping = {}
        self.request_methods = {
            "ls": self.ls,
            "put": self.put,
        }
        logging.basicConfig(filename='namenode.log', encoding='utf-8', level=logging.DEBUG)


    def launch_server(self):
        logging.info(f'Listening on port {self.info[1]}')
        app = web.Application()
        app.add_routes([web.get('/ls', self.ls),
                        web.put('/put', self.put),
                        web.put('/allocate', self.allocate_block),
                        web.put('/mkdir', self.mkdir)])
        web.run_app(app, host=self.info[0], port=self.info[1])


    def choose_datanodes(self, count):
        datanodes = self.datanodes_avaliable
        if count > len(datanodes):
            return datanodes, len(datanodes)
        choice = random.choices(datanodes, k=count)
        return choice, count
        

    def initialize(self):
        self.fstree.load_xml("fsimage_test.xml")


    async def ls(self, req):
        '''
            route: /ls?path={path_to_ls(urlencoded)}
            return: metadata as text
        '''
        try:
            path_to_ls = req.query['path']
            path_lst = fst.parse_path(path_to_ls)
            fount_node = None
            found_node = self.fstree.find(path_lst)
            if found_node.node_type != "DIRECTORY":
                return web.Response(status=405, text="Target is not a directory")
        except INodeError as e:
            return web.Response(status=e.error_code, text=str(e))
        else:
            return web.Response(text=found_node.display_child())


    async def mkdir(self, req):
        try:
            req_body = await req.json()
            
            dest_path = fst.parse_path(req_body["path"])
            if len(dest_path) == 0:
                raise INodeError("Given path is invalid", fst.INVALID_PATH_ERROR)
            dir_name = dest_path[-1]
            parent_path = dest_path[:-1]
            new_dir_node = Inode(dir_name, "DIRECTORY")
            self.fstree.insert(new_dir_node, parent_path)
        except INodeError as e:
            return web.Response(status=e.error_code, text=str(e))
        else:
            return web.Response(text="Successfully created directory")


    async def allocate_block(self, req):
        '''
            route: /allocate
            request body: {
                "name": str,
                "size": int,
                "path": str
            }
            return: 
            response object: 
            {
                "block_count": int
                "full_block_size": int
                "block_info": [{"block_id": str
                                "datanode_id": [str of datanode ids that will hold replica]
                                }] 
            }
        '''
        
        try:
            req_body = await req.json()
            item_name = req_body["name"]
            item_size = req_body["size"]
            path_to_put = req_body["path"]
            block_count = (item_size // self.block_size) + 1
            
            parent_path = fst.parse_path(path_to_put)
            new_node = Inode(item_name, "FILE")
            self.fstree.insert(new_node, parent_path, attempt=True)
        except INodeError as e:
            return web.Response(status=e.error_code, text=str(e))
        else:
            response = {
                "block_count": block_count,
                "full_block_size": self.block_size,
                "block_info": []
            }
            remaining_size = item_size
            for i in range(block_count):
                block_id = self.fstree.currBlockID
                self.fstree.currBlockID += 1
                curr_block_size = min(remaining_size, self.block_size)
                remaining_size -= self.block_size
                choosed_datanodes, actual_replica_count = self.choose_datanodes(self.replication_factor)

                response["block_info"].append({"block_id": block_id, "datanode_id": choosed_datanodes, "block_size": curr_block_size})

            return web.Response(status=200, text=json.dumps(response))



    async def put(self, req):
        '''
            route: /put
            request body: {
                "name": str,
                "size": int,
                "path": str,
                "allocation: json response from allocate request
            }
            return: Success
        '''
        req_body = await req.json()
        item_name = req_body["name"]
        item_size = req_body["size"]
        path_to_put = req_body["path"]
        allocation = req_body["allocation"]
        block_count = allocation["block_count"]
        block_info = allocation["block_info"]
        
        parent_path = fst.parse_path(path_to_put)
        new_node = Inode(item_name, "FILE")

        for block in block_info:
            block_id = block["block_id"]
            datanode_id = block["datanode_id"]
            block_size = block["block_size"]
            self.block_mapping[block_id] = self.block_mapping.get(block_id, []) + datanode_id

            new_node.blocks.append((block_id, block_size))
        new_node.set_replication(block_count)

        self.fstree.insert(new_node, parent_path)
       
        return web.Response(status=200, text="Successfully put file")
            
            
    def close_server(self):
        self.fstree.save_fs_to_fsimage("fsimage_test.xml")

    '''
    async def handle_client_request(self, reader, writer):
        addr = writer.get_extra_info('peername')
        req = await receive_request(reader)
        logging.info(f"Recieved [{req.command}] request from {addr}")
        code, response = 0, "no response"
        try:
            code, response = await self.request_methods[req.command](*req.arguments, content=req.content)
        except TypeError:
            code = 405
            response = "Incorrect argument"
        except Exception as e:
            code = -1
            response = str(e)
        finally:
            await send_response(writer, code, response)
            writer.close()
            await writer.wait_closed()
            logging.info('Connection with client terminated.')
    '''
if __name__ == "__main__":
    namenode_instance = NameNode()
    namenode_instance.initialize()

    try:
        namenode_instance.launch_server()
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        namenode_instance.close_server()
        logging.info('Namenode metadata saved to fsimage_test.xml')
