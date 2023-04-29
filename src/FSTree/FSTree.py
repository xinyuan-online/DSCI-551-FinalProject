from enum import Enum
import xml.etree.ElementTree as ET

FILE = 1
DIRECTORY = 2
OTHER = 3

PATH_NOT_FOUND_ERROR = 404
PATH_DUPLICATE_ERROR = 405
REMOVE_DIRECTORY_NOT_EMPTY_ERROR = 405
DELETE_ROOT_NODE_ERROR = 405
INVALID_PATH_ERROR = 404

NODE_TYPES = {
    FILE: "FILE",
    DIRECTORY: "DIRECTORY",
    OTHER: "OTHER"
}
class INodeError(Exception):
    def __init__(self, message, errors = 400):            
        super().__init__(message)
        self.error_code = errors

class Inode:
    def __init__(self, node_name, node_type):
        self.id = None
        self.parent = None
        self.node_name = node_name
        self.node_type = node_type
        self.replication = 1
        self.childs = [] if node_type=="DIRECTORY" else None
        self.blocks = [] if node_type=="FILE" else None

    def __repr__(self):
        return "{}:{}{}".format(self.id, self.node_name, ('/' if self.node_type=="DIRECTORY" else ""))

    def is_root(self):
        return self.name == "" and self.parent == None

    def set_parent(self, parent):
        self.parent = parent

    def set_id(self, id):
        self.id = id

    def set_replication(self, rep):
        self.replication = rep

    def get_name(self):
        return self.node_name
    
    def set_blocks(self, blocks):
        self.blocks = blocks.copy()

    def display_child(self):
        return '\t'.join([c.get_name() for c in self.childs])

    def attach_to_xmlnode(self, inode_section, directory_section):
        inode = ET.SubElement(inode_section, 'inode')

        ID = ET.SubElement(inode, 'id')
        ID.text = str(self.id)

        node_type = ET.SubElement(inode, 'type')
        node_type.text = self.node_type

        name = ET.SubElement(inode, 'name')
        name.text = self.node_name

        if self.node_type == "DIRECTORY":
            directory = ET.SubElement(directory_section, 'directory')

            parent = ET.SubElement(directory, 'parent')
            parent.text = str(self.id)

            for c in self.childs:
                child_node = ET.SubElement(directory, 'child')
                child_node.text = str(c.id)

        if self.node_type == "FILE":
            replication = ET.SubElement(inode, "replication")
            replication.text = str(self.replication)
            blocks = ET.SubElement(inode, "blocks")
            for b in self.blocks:
                block = ET.SubElement(blocks, "block")
                block_id = ET.SubElement(block, "id")
                block_id.text = str(b[0])
                block_num_bytes = ET.SubElement(block, "numBytes")
                block_num_bytes.text = str(b[1])

        return 

def parse_path(path_str):
    path_list = path_str.split('/')
    if path_list[0] != '':
        raise INodeError("Given path is invalid", INVALID_PATH_ERROR)
    path_list = path_list[1:]
    if path_list[-1] == '':
        path_list = path_list[:-1]
    
    return path_list

def insert_node(node_to_insert, parent_node):
    parent_node.childs.append(node_to_insert)
    node_to_insert.parent = parent_node
    return

class FSTree:
    def __init__(self):
        self.root = None
        self.num_inodes = None
        self.currInodeID = None
        self.currBlockID = None

    def initialize(self, num_inodes, currInodeID, currBlockID):
        self.num_inodes = num_inodes
        self.currInodeID = currInodeID
        self.currBlockID = currBlockID

    def set_root(self, root):
        self.root = root

    def find(self, path):
        def find_rec(curr, path):
            
            if len(path)==0:
                return curr
            if curr.childs == None:
                raise INodeError("Given path is not a valid directory", PATH_NOT_FOUND_ERROR)
            next_directory = None
            for d in curr.childs:
                if d.node_name != path[0]:
                    continue
                target = find_rec(d, path[1:])
                if target == None:
                    continue
                return target
            raise INodeError("Given path not found or is not valid", PATH_NOT_FOUND_ERROR)
        
        return find_rec(self.root, path)

    def insert(self, node_to_insert=None, path=None, attempt=False):
        if self.root == None:
            self.root = node_to_insert
        
        target_directory = self.find(path)
        if target_directory.node_type != "DIRECTORY":
            raise INodeError("Given path not a directory", PATH_NOT_FOUND_ERROR)
        
        if node_to_insert.node_name in [x.node_name for x in target_directory.childs]:
            raise INodeError("Given path already exists", PATH_DUPLICATE_ERROR)

        if attempt:
            return 
        if node_to_insert.id == None:
            node_to_insert.set_id(self.currInodeID)
            self.currInodeID += 1

        self.num_inodes += 1
        insert_node(node_to_insert, target_directory)
        return node_to_insert




    def remove(self, path):
        target = self.find(path)
        if target == self.root:
            raise INodeError("Cannot delete root node", DELETE_ROOT_NODE_ERROR)
        if target == None:
            raise INodeError("Given node to remove of is not found", PATH_NOT_FOUND_ERROR)
        if (target.node_type == "DIRECTORY") and (len(target.childs) != 0):
            raise INodeError("Given directory to remove is not empty", REMOVE_DIRECTORY_NOT_EMPTY_ERROR)
        self.remove_node(target)
        self.num_inodes -= 1
        

    def remove_node(self, node_to_remove):
        parent_d = node_to_remove.parent
        if parent_d != None:  
            parent_d.childs.remove(node_to_remove)
        node_to_remove.parent = None
        node_to_remove.childs = []
        return

    
    def remove_rec(self, path):
        target = self.find(path)
        self.remove_node(target)
        return

    
    def FSTree_to_xml(self):
        def rec_export(node, level):
            indentation = "\t"*level
            self_name = "root" if node.is_root() else node.node_name
            if node.node_type == "FILE":
                return "{}<{}/>\n".format(indentation+"\t", self_name)
            if node.node_type == "DIRECTORY":
                xml = "{}<{}>\n".format(indentation,self_name)
                child_list = node.childs
                for child in child_list:
                    xml += rec_export(child, level+1)
                xml += "{}</{}>\n".format(indentation, self_name)
                return xml
            return ""
        return (rec_export(self.root, 0))


    def get_all_nodes(self):
        def get_all_rec(root):
            if root.childs == None:
                return [root]
            child_nodes = [[root]] + [get_all_rec(x) for x in root.childs]
            child_nodes_flattened = [item for child in child_nodes for item in child]
            return child_nodes_flattened

        return get_all_rec(self.root)

    def save_fs_to_fsimage(self, path_to_save):
        fsimage = ET.Element('fsimage')
        inode_section = ET.SubElement(fsimage, 'INodeSection')
        directory_section = ET.SubElement(fsimage, 'INodeDirectorySection')
        last_inode_id = ET.SubElement(inode_section, 'lastInodeId')
        last_inode_id.text = str(self.currInodeID)

        num_inodes = ET.SubElement(inode_section, 'numInodes')
        num_inodes.text = str(self.num_inodes)

        last_block_id = ET.SubElement(inode_section, 'lastBlockId')
        last_block_id.text = str(self.currBlockID)
        
        all_nodes_in_tree = self.get_all_nodes()
        for node in all_nodes_in_tree:
            node.attach_to_xmlnode(inode_section, directory_section)


        tree = ET.ElementTree(fsimage)
        ET.indent(tree, space="\t", level=0)
        #ET.dump(tree)
        tree.write(path_to_save, short_empty_elements=False)
        
        
    def load_xml(self, path_to_xml):
        fsimage_xml = ET.parse(path_to_xml)
        fsimage = fsimage_xml.getroot()
        inode_section = fsimage.find('INodeSection')
        last_node_id = int(inode_section.find('lastInodeId').text)
        num_inodes = int(inode_section.find('numInodes').text)
        last_block_id = int(inode_section.find('lastBlockId').text)

        self.initialize(num_inodes, last_node_id, last_block_id)

        all_nodes = inode_section.findall('inode')
        
        directory_section = fsimage.find('INodeDirectorySection')
        directories = directory_section.findall('directory')
        inodes = {}
        block_mapping = {}
        for node in all_nodes:

            node_id = int(node[0].text)
            node_type = node[1].text
            node_name = node[2].text
            new_node = Inode(node_name, node_type)
            new_node.set_id(node_id)

            if node_type == "FILE":
                replication = node[3].text
                blocks = node.find('blocks')
                blocks_info = [(int(block[0].text), int(block[1].text)) for block in blocks]
                for block in blocks:
                    block_mapping[int(block[0].text)] = []
                new_node.set_blocks(blocks_info)
                new_node.set_replication(replication)

            
            inodes[node_id] = new_node
            if node_type == "DIRECTORY" and node_name == None:
                new_node.node_name = ""
                self.set_root(new_node)
            
                
        for directory in directories:
            parent_id = int(directory.find('parent').text)
            parent_node = inodes[parent_id]
            
            if parent_node == None:
                continue

            for child in directory.findall('child'):
                child_id = int(child.text)
                child_node = inodes[child_id]
                insert_node(child_node, parent_node)

        return block_mapping 
    def __repr__(self):
        return self.FSTree_to_xml()



