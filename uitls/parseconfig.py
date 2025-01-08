import configparser

from uitls.logger import logger


def parse_config(server_name):
    """从配置文件加载数据库配置"""
    config = configparser.ConfigParser()
    config.read(f'server/{server_name}-config.ini', encoding='utf-8')
    logger.info(f"服务器： {server_name}")
    return config
