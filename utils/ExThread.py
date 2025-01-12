import datetime
import queue
import re
import threading

import pandas as pd
import sqlparse
from PyQt5 import QtCore
from PyQt5.QtCore import pyqtSignal, QThread


from utils.DBconnectServer import popup_manager, connect_to_server
from utils.Exdatabases import kill_sql, split_statements, clean_sql, execute_sql
from utils.parseconfig import parse_config

from utils.logger import logger

class Thread_1(QThread):
    signal = pyqtSignal()
    result_ready = pyqtSignal(pd.DataFrame, str)

    def __init__(self, server_name, sql, timeLabel, execute_group, execute_pms, execute_member):
        super(Thread_1, self).__init__()
        self.timeLabel = timeLabel
        self.server_name = server_name
        self.sql = sql
        self.result_queue = queue.Queue()
        self.execute_group = execute_group
        self.execute_pms = execute_pms
        self.execute_member = execute_member


    def run(self):
        try:
            server_name = self.server_name
            sql = self.sql
            result_dict = {}
            config = parse_config(server_name)
            start_time = datetime.datetime.now()  # 记录开始时间

            def execute_query(server, section, sql, type, name, db_name):
                connection = connect_to_server(server)
                if connection is not None:
                    try:
                        logger.info(f"执行SQL语句：{sql}")
                        result = execute_sql(connection, sql, section, type, db_name, name, None)
                        if result is not None:
                            self.result_queue.put((sql, result))
                    except Exception as e:

                        logger.error(f"未完成执行的错误：{section}-{e}")
                        popup_manager.message_signal.emit(f"未完成执行的错误：{section}-{e}")
                    finally:
                        connection.close()
                else:
                    err_df = pd.DataFrame(columns=['信息', '服务器组'])
                    row = {'信息': '发生错误，请检查', '服务器组': section}
                    err_df = err_df.append(row, ignore_index=True)
                    self.result_queue.put((sql,err_df))

            threads = []
            sql_query, db_name = clean_sql(sql)
            statements = split_statements(sql_query)
            for sql_segment in statements:
                for section in config.sections():
                    if section == 'group' and not self.execute_group.isChecked():
                        continue
                    if section == 'member' and not self.execute_member.isChecked():
                        continue
                    server = config[section]
                    # 为每个服务组创建线程
                    if re.search(r"CREATE\s+(?:FUNCTION|PROCEDURE|VIEW|EVENT|TRIGGER)", sql_segment):
                        # 提取
                        create_pattern = r"CREATE\s+(?:/[*!].*?[*]/\s*)?(?:DEFINER\s*=\s*`[^`]+`@`[^`]+`\s*)?(TRIGGER|PROCEDURE|FUNCTION|VIEW|EVENT)\s+`([a-zA-Z0-9_]+)`"
                        match = re.search(create_pattern, sql_segment)
                        if match:
                            _type = match.group(1)  # 类型
                            _name = match.group(2)  # 名称
                            thread = threading.Thread(target=execute_query,
                                                      args=(server, section, sql_segment, _type, _name, db_name),
                                                      name=f"{section}-Thread")
                            thread.start()
                            threads.append((thread, server, section))

                    else:
                        for statement in sqlparse.parse(sql_segment):
                            # 创建线程执行每个 SQL 语句
                            sql_str = str(statement)  # 使用 sql_str 而不是 sql_query
                            # logger.info(f"执行SQL语句：{sql_str}")
                            print(f"执行到：{sql_str}")
                            thread = threading.Thread(target=execute_query,
                                                      args=(server, section, sql_str, 'Sql语句', None, None),
                                                      name=f"{section}-Thread")
                            thread.start()
                            threads.append((thread, server, section))
            for thread, server, section in threads:
                thread.join()
                logger.info(f"{thread.name} 已执行!")

            while not self.result_queue.empty():
                sql_query, result_df = self.result_queue.get()
                print(result_df)
                if not result_df.index.is_unique:
                    print("Warning: Duplicate index in result_df!")
                    result_df = result_df.reset_index(drop=True)
                if sql_query in result_dict:
                    result_dict[sql_query] = pd.concat([result_dict[sql_query], result_df], ignore_index=True)
                else:
                    result_dict[sql_query] = result_df.reset_index(drop=True)

            # 计算总耗时并显示
            elapsed_time = datetime.datetime.now() - start_time
            formatted_time = elapsed_time.total_seconds()
            self.timeLabel.setText(f"{round(formatted_time, 4)} Seconds")
            # 在 UI 上显示结果
            for sql_query, result_df in result_dict.items():
                print(1)
                self.result_ready.emit(result_df, sql_query)

        except Exception as e:
            logger.error(f"执行操作未知错误: {str(e)}")
            popup_manager.message_signal.emit(f"未完成执行的错误:{e}")



class Thread_2(QThread):
    signal = pyqtSignal()  # 括号里填写信号传递的参数
    stop_click = pyqtSignal(pd.DataFrame)

    def __init__(self, server_name, timeLabel, execute_group, execute_pms, execute_mem):
        super(Thread_2, self).__init__()
        self.timeLabel = timeLabel
        self.server_name = server_name
        self.result_queue = queue.Queue()
        self.execute_group = execute_group
        self.execute_pms = execute_pms
        self.execute_mem = execute_mem

    def run(self):
        try:
            server_name = self.server_name
            result_queue = queue.Queue()  # 创建一个队列来存储每个线程的执行结果
            config = parse_config(server_name)
            start_time = datetime.datetime.now()  # 记录开始时间

            def execute_query(server, section, result_queue):
                connection = connect_to_server(server)
                if connection:
                    try:
                        result = kill_sql(connection, section, None)
                        if result is not None:
                            result_queue.put(result)  # 将结果放入队列
                    except Exception as e:
                        logger.error(f"未完成执行的错误：=>>  {server}-{e}")
                        # popup_manager.message_signal.emit(f"未完成执行的错误：=>>  {server}-{e}")
                    finally:
                        connection.close()

            threads = []
            for section in config.sections():
                if section == 'group' and not self.execute_group.isChecked():
                    continue
                if section == 'member' and not self.execute_mem.isChecked():
                    continue
                server = config[section]
                thread = threading.Thread(target=execute_query, args=(server, section, result_queue),
                                          name=f"{section}-Thread")
                thread.start()
                threads.append((thread, server, section))
            for thread, server, section in threads:
                thread.join()
                logger.info(f"{thread.name} 已执行!")
            elapsed_time = datetime.datetime.now() - start_time
            formatted_time = elapsed_time.total_seconds()
            self.timeLabel.setText(f"{round(formatted_time, 4)} Seconds")

            # 在界面上显示结果
            result_df = pd.DataFrame()
            while not result_queue.empty():
                result = result_queue.get()
                if result_df.empty:
                    result_df = result
                else:
                    result_df = result_df.append(result, ignore_index=True)
            self.stop_click.emit(result_df)
        except Exception as e:
            logger.error(f"停止操作未知错误: {str(e)}")