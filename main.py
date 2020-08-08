import sqlite3
import sys
import os
import time
from multiprocessing import Pipe, Process
import requests

if not os.path.exists('downloaded'):
    os.mkdir('downloaded')

def download(pname, conn):
    while 1:
        try:
            msg = conn.recv()
        except EOFError:
            time.sleep(.5)
        command, url, name = msg
        if command == 'exit':
            break
        print(f'[{pname}] download: {url}, name: {name}')
        if not os.path.exists(f'downloaded/{name}.mp3'):
            resp = requests.get(url)
            with open(f'downloaded/{name}.mp3', 'wb+') as f:
                f.write(resp.content)
        conn.send('finish')

if __name__ == '__main__':
    db_path = sys.argv[1]

    def dict_factory(cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d
    db_conn = sqlite3.connect(db_path)
    db_conn.row_factory = dict_factory
    cur = db_conn.cursor()
    cur.execute('SELECT name, dSourceUrl FROM T_Download')
    downloads = cur.fetchall()
    db_conn.close()
 
    processes = []
    connections = []
    for i in range(10):
        download_info = downloads.pop()
        if not download_info:
            continue
        parent_conn, child_conn = Pipe()
        connections.append(parent_conn)
        p = Process(target=download, args=(f'd-{i}', child_conn))
        processes.append(p)
        p.start()
        parent_conn.send(['download', download_info['dSourceUrl'], download_info['name']])

    while 1:
        if len(downloads) <= 0:
            for c in connections:
                c.send(['exit', '', ''])
            break
        for conn in connections:
            try:
                result = conn.recv()
            except EOFError:
                continue
            if result == 'finish':
                try:
                    download = downloads.pop()
                except IndexError:
                    continue
                conn.send(['download', download['dSourceUrl'], download['name']])
        time.sleep(.3)
    
    for p in processes:
        p.join()