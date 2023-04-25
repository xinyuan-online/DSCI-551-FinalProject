#!/usr/bin/python3
import asyncio
import argparse
import aiohttp
import os
import json
import base64
from urllib.parse import quote_plus as urlencode
import xml.etree.ElementTree as ET

def get_config(homepath):
    def get_namenode_info(xmlnode):
        return (xmlnode.find('hostname').text, int(xmlnode.find('port').text), 
        int(xmlnode.find('blockSize').text),int(xmlnode.find('replicationFactor').text))

    def get_datanode_info(xmlnode):
        return xmlnode.find('id').text, xmlnode.find('hostname').text, int(xmlnode.find('port').text)
    configuration = ET.parse(f'{homepath}/conf/configuration.xml').getroot()
    namenode = configuration.find('namenode')
    namenode_info = get_namenode_info(namenode)
    datanodes = configuration.find('datanodes')
    datanodes_info = {
        'storage': datanodes.find('localStorage'),
        'nodes': [get_datanode_info(datanode) for datanode in datanodes.findall('datanode')]
    }
    return namenode_info, datanodes_info


class EdfsClient:

    def __init__(self):
        homepath = os.path.dirname(os.path.realpath(__file__))
        namenode_info, datanode_info = get_config(homepath)
        self.namenode_session = None
        self.datanode_sessions = None
        self.handle_request_methods = {
            "ls": self.handle_ls,
            "put": self.handle_put,
            "mkdir": self.handle_mkdir,
            "rm": self.handle_rm,
            "rmdir": self.handle_rmdir
        }
        self.namenode_info = (namenode_info[0], namenode_info[1])
        self.datanodes_info = [(nodeid, (hostname, port))
                                for nodeid, hostname, port 
                                in datanode_info['nodes']]


    async def initialize(self):
        self.namenode_session = \
            aiohttp.ClientSession(f'http://{self.namenode_info[0]}:{self.namenode_info[1]}')
        self.datanode_sessions = \
            {d[0]: aiohttp.ClientSession(f'http://{d[1][0]}:{d[1][1]}') 
            for d in self.datanodes_info}
    async def close(self):
        if not self.namenode_session.closed:
            await self.namenode_session.close()
        for d in self.datanode_sessions.values():
            await d.close()


    async def handle_ls(self, path_to_ls):
        print(path_to_ls)
        #encoded_path_to_ls = urlencode(path_to_ls)
        async with self.namenode_session.get(f'/ls', params={"path": path_to_ls}) as resp:
            return resp.status, await resp.text()


    async def handle_put(self, src_path, dest_path):
        return await self.put_single_file(src_path, dest_path)
        
    async def handle_mkdir(self, path):
        mkdir_request = {
            'path': path
        }
        async with self.namenode_session.put('/mkdir', json=mkdir_request) as resp:
            return resp.status, await resp.text()

    async def handle_rm(self, path):
        rm_request = {
            'path': path
        }
        async with self.namenode_session.delete('/rm', json=rm_request) as resp:
            return resp.status, await resp.text()

    async def handle_rmdir(self, path):
        rmdir_request = {
            'path': path
        }
        async with self.namenode_session.delete('/rmdir', json=rmdir_request) as resp:
            return resp.status, await resp.text()

    async def put_single_file(self, src_path, dest_path):
        abs_path = os.path.abspath(src_path)
        file_name = os.path.basename(abs_path)
        file_size = os.path.getsize(abs_path)
        allocation_request = {
            'name': file_name,
            'size': file_size,
            'path': dest_path
        }

        #allocate blocks
        allocation_response = None
        
        async with self.namenode_session.put('/allocate', json=allocation_request) as a_resp:
            if a_resp.status != 200:
                return a_resp.status, await a_resp.text()
            allocation_response = json.loads(await a_resp.text())

        block_count = allocation_response["block_count"]
        full_block_size = allocation_response["full_block_size"]
        block_info = allocation_response["block_info"]

        #contact DataNodes to writeblocks
        with open(src_path, 'rb') as fr:
            for block in block_info:
                chunk = fr.read(full_block_size)
                block_id = block["block_id"]
                datanode_ids = block["datanode_id"]
                for replica, datanode_id in enumerate(datanode_ids):
                    writeblock_body = {
                        "block_id": block_id,
                        "replica": replica,
                        "block_content": base64.b64encode(chunk).decode("ascii")
                    }
                    async with self.datanode_sessions[datanode_id].post(f'/write', json=writeblock_body) as d_resp:
                        if d_resp.status != 200:
                            return d_resp.status, await d_resp.text()

        #upon succesfully datanode writes, update metadata in namenodes   
        put_request = allocation_request.copy()

        #copy the last request and response for namenode 
        put_request["allocation"] = allocation_response
        async with self.namenode_session.put('/put', json=put_request) as p_resp:
            return p_resp.status, await p_resp.text()

            
        

    async def parse_user_input(self):
        parser = argparse.ArgumentParser(
                    prog='edfs',
                    description='Shell for controlling edfs file system',
                    epilog='Text at the bottom of help')
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-ls', nargs=1)
        group.add_argument('-mkdir', nargs=1)
        group.add_argument('-put', nargs=2)
        group.add_argument('-rm', nargs=1)
        group.add_argument('-rmdir', nargs=1)
        args = parser.parse_args().__dict__
        for k, v in args.items():
            if v == None:
                continue
            return k, v
            
    async def handle_user_request(self, command, arguments):
        resp_code, resp_content = await self.handle_request_methods[command](*arguments)
        return resp_code, resp_content


async def run_client():
    client = EdfsClient()
    try:
        await client.initialize()
        command, arguments = await client.parse_user_input()
        resp_code, resp_content = await client.handle_user_request(command, arguments)
        if resp_code != 200:
            print(f"Error {resp_code}: {resp_content}")
        else:
            print(resp_content)
    except aiohttp.client_exceptions.ClientConnectorError as e:
        print(e)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(run_client())