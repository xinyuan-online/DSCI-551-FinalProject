import os
import asyncio
from quart import Quart, render_template, request, redirect, url_for, send_from_directory, jsonify
from io import BytesIO
from quart import Response
import shutil
from edfs import EdfsClient
import logging
import json
from urllib.parse import quote_plus as urlquote
from urllib.parse import unquote_plus as urlunquote


    
app = Quart(__name__)
app.config.from_object(f"conf.Config.Config")



def get_parent(unquoted_url):
    slash_index = unquoted_url.rfind('/')
    parent = unquoted_url[:slash_index] if slash_index != 0 else '/'
    return parent

def join_url(a, b):
    return (a if a != '/' else '') + '/' + b

@app.route('/list_folder/<folder_name>')
async def list_folder(folder_name):
    print(folder_name)
    code, folder_contents = await app.client.handle_user_request("ls", [folder_name])
    if isinstance(folder_contents, str):
        return folder_contents, 404
    return jsonify(folder_contents)


@app.context_processor
def utility_processor():
    def quote(url):
        return urlquote(url)
    def unquote(url):
        return urlunquote(url)
    return dict(quote=quote, unquote=unquote)


@app.route('/create_folder', methods=['POST'])
async def create_folder():
    folder_name = (await request.form).get("folder_name")
    path = (await request.form).get("path", "/")
    unquoted_path = urlunquote(path)
    if folder_name:
        full_folder_name = join_url(unquoted_path, folder_name)
        code, resp = await app.client.handle_user_request("mkdir", [full_folder_name])
        if code != 200:
            return code, resp
        return redirect(url_for('folder_contents', item_path=path))
    return "Invalid folder name"

@app.route("/deletefolder/<item_path>")
async def delete_folder(item_path):
    unquoted_path = urlunquote(item_path)
    code, resp = await app.client.handle_user_request("rmdir", [unquoted_path])
    if code != 200:
        return resp, code
    parent = urlquote(get_parent(unquoted_path))
    return redirect(url_for('folder_contents', item_path=parent))


@app.route("/delete/<item_path>")
async def delete_item(item_path):
    unquoted_path = urlunquote(item_path)
    code, resp = await app.client.handle_user_request("rm", [unquoted_path])
    if code != 200:
        return resp, code
    parent = urlquote(get_parent(unquoted_path))
    return redirect(url_for('folder_contents', item_path=parent))


@app.route('/')
async def index(path=""):
    code, resp = await app.client.handle_user_request("ls", ['/'])
    
    folder_contents = json.loads(resp)
    existing_files = [
        {
            "name": file['name'],
            "type": file['type'],
            "path": urlquote("/" + file['name'])
        }
        for file in folder_contents
    ]
    logging.info(existing_files)
    return await render_template("index_new.html", existing_files=existing_files, folder_name=urlquote("/"))

@app.route('/parent/<item_path>')
async def parent(item_path):
    unquoted_path = urlunquote(item_path)
    parent = urlquote(get_parent(unquoted_path))
    print(f'/folder/{parent}')
    return redirect(url_for('folder_contents', item_path=parent))

@app.route("/folder/<item_path>")
async def folder_contents(item_path):
    unquoted_path = urlunquote(item_path)
    if unquoted_path == '/':
        return redirect('/')
    code, resp = await app.client.handle_user_request("ls", [unquoted_path])
    if code != 200:
        return resp, code
    logging.info(resp)
    folder_contents = json.loads(resp)
    existing_files = [
        {
            "name": file["name"],
            "type": file["type"],
            "path": urlquote(join_url(unquoted_path, file["name"]))  
        }
        for file in folder_contents
    ]
    return await render_template(
        "index_new.html", existing_files=existing_files, folder_name=item_path
    )


@app.route('/upload', methods=['POST'])
async def upload_file():
    
    file = (await request.files).get('file')
    path = (await request.form).get("path", "/")
    unquoted_folder_path = urlunquote(path)
    if file and file.filename:
        code, resp = await app.client.handle_user_request("put", 
                                                          [file.filename, 
                                                           unquoted_folder_path, 
                                                           file])
        if code != 200:
            return resp, code
        return redirect(url_for('folder_contents', item_path=path))


    return "Invalid file or filename", 400


@app.route('/download/<item_path>')
async def download_file(item_path):
    unquoted_path = urlunquote(item_path)
    code, resp = await app.client.handle_user_request("get", 
                                            [unquoted_path])
    if code != 200:
        return code, resp
    filename = unquoted_path.split('/')[-1]
    file_storage = BytesIO(resp)
    response = Response(file_storage, content_type='application/octet-stream')
    response.headers.set('Content-Disposition', 'attachment', filename=filename)

    return response

@app.before_serving
async def startup():
    app.client = EdfsClient()
    await app.client.initialize()


    

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=4000)
