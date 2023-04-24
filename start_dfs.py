#!/usr/bin/python3
import asyncio
import xml.etree.ElementTree as ET
import sys
import os
import src.servers.namenode as nn
import src.servers.datanode as dn
from multiprocessing import Process

def get_namenode_info(xmlnode):
    return (xmlnode.find('hostname').text, int(xmlnode.find('port').text), 
        int(xmlnode.find('blockSize').text),int(xmlnode.find('replicationFactor').text))

def get_datanode_info(xmlnode):
    return xmlnode.find('id').text, xmlnode.find('hostname').text, int(xmlnode.find('port').text)

def get_config():
    configuration = ET.parse('./conf/configuration.xml').getroot()
    namenode = configuration.find('namenode')
    namenode_info = get_namenode_info(namenode)
    datanodes = configuration.find('datanodes')
    datanodes_info = {
        'storage': datanodes.find('localStorage'),
        'nodes': [get_datanode_info(datanode) for datanode in datanodes.findall('datanode')]
    }
    return namenode_info, datanodes_info



if __name__ == "__main__":
    namenode_info, datanodes_info = get_config()
    homepath = os.path.dirname(os.path.realpath(__file__))
    p = Process(target=nn.run_namenode,
                args=(*namenode_info, datanodes_info, homepath))
    datanodes_proc = []
    for node_info in datanodes_info['nodes']:
        p_d = Process(target=dn.run_datanode, args=(*node_info, datanodes_info['storage'].text, homepath))
        datanodes_proc.append(p_d)
        #dn.run_datanode(*node_info, datanodes_info['storage'], homepath)


    p.start()
    for d in datanodes_proc:
        d.start()
    
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Interrupted")