import threading
from encodings import idna

import pymysql
# from sshtunnel import SSHTunnelForwarder

from utils.DBcrypt import decode_password
from gui.PopupManager import  PopupManager
from utils.logger import logger

popup_manager = PopupManager()

port_pool = set(range(12345, 22400))
def get_local_bind_port():
    with threading.Lock():
        if not port_pool:
            raise Exception("No available ports")
        port = port_pool.pop()
        return port


def release_local_bind_port(port):
    with threading.Lock():
        port_pool.add(port)


def connect_to_server(server_config):
    """数据库连接"""
    connection = None  # 初始化数据库连接
    try:
        # if server_config['SslMode'] == 'on':
        #     local_bind_port = get_local_bind_port()
        #     mysql_password = decode_password(server_config.get('password', raw=True))
        #     ssh_password = decode_password(server_config['ssh_password'])
        #     server = SSHTunnelForwarder(
        #         ssh_address_or_host=(server_config['ssh_host'], server_config.getint('ssh_port')),
        #         ssh_username=server_config['ssh_user'],  # 跳转机的用户
        #         ssh_password=ssh_password,
        #         local_bind_address=('127.0.0.1', local_bind_port),
        #         remote_bind_address=(server_config['host'], server_config.getint('port'))
        #     )
        #     server.start()
        #     connection = pymysql.connect(
        #         host='127.0.0.1',
        #         port=local_bind_port,
        #         database=server_config['database'],
        #         user=server_config['user'],
        #         password=mysql_password,
        #         autocommit=True
        #     )
        #
        # else:
        mysql_password = decode_password(server_config.get('password', raw=True))
        connection = pymysql.connect(
            host=server_config['host'],
            port=server_config.getint('port'),
            database=server_config['database'],
            user=server_config['user'],
            password=mysql_password,
            autocommit=True
        )
        logger.info(f"开始操作数据库：{server_config}")
        return connection
    except pymysql.err.OperationalError as err:
        logger.error(f"Error connecting to MySQL Platform: {err}")
        popup_manager.message_signal.emit(f"无法连接到数据库：{err}")
        return None
    except Exception as e:
        logger.error(f"其他错误：{e}")
        popup_manager.message_signal.emit(f"连接出现其他错误：{e}")
        return None