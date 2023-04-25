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
logging.basicConfig(filename=f'./webui.log', 
                            encoding='utf-8', level=logging.DEBUG)
@app.route('/list_folder/<folder_name>')
async def list_folder(folder_name):
    print(folder_name)
    code, folder_contents = await app.client.handle_user_request("ls", [folder_name])
    if isinstance(folder_contents, str):
        return folder_contents, 404
    return jsonify(folder_contents)


@app.route('/create_folder', methods=['POST'])
async def create_folder():
    folder_name = (await request.form).get("folder_name")
    path = (await request.form).get("path", "")

    if folder_name:
        full_folder_name = os.path.join(path, folder_name)
        response = metadata_server.exposed_create_folder(full_folder_name)
        existing_files = [
            {
                "name": file["name"],
                "is_folder": (metadata_server.exposed_get_file_info(file["name"]) or {})
            }
            for file in metadata_server.exposed_ls(path)
        ]
        return await render_template('index_new.html', existing_files=existing_files, folder_name=path)
    return "Invalid folder name"

@app.route("/deletefolder/<item_path>")
async def delete_folder(item_path):
    unquoted_path = urlunquote(item_path)
    code, resp = await app.client.handle_user_request("rmdir", ['/'+ unquoted_path])
    slash_index = unquoted_path.rfind('/')

    parent = urlquote(unquoted_path[:slash_index] if slash_index != -1 else '/')
    return redirect(url_for('folder_contents', item_path=parent))


@app.route("/delete/<item_path>")
async def delete_item(item_path):
    unquoted_path = urlunquote(item_path)
    code, resp = await app.client.handle_user_request("rm", ['/'+ unquoted_path])
    slash_index = unquoted_path.rfind('/')

    parent = urlquote(unquoted_path[:slash_index] if slash_index != -1 else '/')
    return redirect(url_for('folder_contents', item_path=parent))


@app.route('/')
async def index(path=""):
    code, resp = await app.client.handle_user_request("ls", ['/'])
    
    folder_contents = json.loads(resp)
    existing_files = [
        {
            "name": file['name'],
            "type": file['type'],
            "path": file['name']
        }
        for file in folder_contents
    ]
    logging.info(existing_files)
    return await render_template("index_new.html", existing_files=existing_files)


@app.route('/folder/')
async def folder_root():
    return redirect('/')

@app.route("/folder/<item_path>")
async def folder_contents(item_path):
    unquoted_path = urlunquote(item_path)
    code, resp = await app.client.handle_user_request("ls", ['/'+ unquoted_path])
    logging.info(resp)
    folder_contents = json.loads(resp)
    existing_files = [
        {
            "name": file["name"],
            "type": file["type"],
            "path": urlquote(unquoted_path + '/' + file["name"])  
        }
        for file in folder_contents
    ]
    return await render_template(
        "index_new.html", existing_files=existing_files, folder_name=item_path
    )

@app.route('/upload', methods=['POST'])
async def upload_file():
    file = (await request.files).get('file')
    folder_name = (await request.folder_name).get("folder_name", "")
    logging.info(request.__dict__)
    if file and file.filename:
        file_data = file.read()
        file_size = len(file_data)
        file_path = os.path.join(folder_name, file.filename)

        response = metadata_server.exposed_put(file_path, file_size, file_data)
        parent = urlquote(unquoted_path[:slash_index] if slash_index != -1 else '/')
        return redirect(url_for('folder_contents', item_path=parent))

        return await render_template('index_new.html', existing_files=existing_files, folder_name=folder_name if folder_name else None)

    return "Invalid file or filename", 400


@app.route('/download/<item_path>')
async def download_file(item_path):
    file_info = metadata_server.exposed_get_file_info(filename)
    print(file_info)

    if not file_info:
        return "File not found", 404

    if file_info['is_folder']:
        print("detect folder")
        return redirect(url_for('folder_contents', folder_name=filename))
    print(filename)
    file_data = metadata_server.exposed_get(filename)
    if not file_data:
        return "File not found", 404

    file_storage = BytesIO(file_data)
    response = Response(file_storage, content_type='application/octet-stream')
    response.headers.set('Content-Disposition', 'attachment', filename=filename)

    return response

@app.before_serving
async def startup():
    app.client = EdfsClient()
    asyncio.ensure_future(app.client.initialize())
    app.config['UPLOAD_FOLDER'] = "temp"
    

if __name__ == '__main__':
    app.run(debug=True)
