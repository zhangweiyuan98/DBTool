import logging
import queue
import threading

from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QMessageBox, QTableWidgetItem, QPushButton, QTableWidget, QVBoxLayout, QLabel, QDialog

from utils.DBconnectServer import connect_to_server
from utils.logger import logger
from utils.parseconfig import parse_config

# class ProcessDialog(QDialog):
#     """mysql进程列表"""
#     def __init__(self, server_name, execute_mem, execute_group, parent=None):
#         super(ProcessDialog, self).__init__(parent)
#         self.execute_mem = execute_mem
#         self.execute_group = execute_group
#         self.server_name = server_name
#         self.result_queue = queue.Queue()
#         self.setWindowTitle("进程列表")
#         self.resize(1080, 600)
#         layout = QVBoxLayout(self)
#
#         self.label = QLabel("进程：")
#         layout.addWidget(self.label)
#         self.process_table = QTableWidget()
#         layout.addWidget(self.process_table)
#
#         self.refresh_button = QPushButton("刷新")
#         self.refresh_button.clicked.connect(self.load_processes)
#         layout.addWidget(self.refresh_button)
#
#         self.kill_button = QPushButton("杀了它")
#         self.kill_button.clicked.connect(self.kill_selected_process)
#         layout.addWidget(self.kill_button)
#
#         self.load_processes()
#
#         self.timer = QTimer(self)
#         self.timer.timeout.connect(self.load_processes)
#         self.timer.start(1000000)  # 10秒刷新
#
#     def load_processes(self):
#         self.process_table.clear()
#         self.result_queue = queue.Queue()
#         try:
#             config = parse_config(self.server_name)
#
#             def execute_query(server, section, sql):
#                 connection = connect_to_server(server)
#                 cursor = connection.cursor()
#                 if connection:
#                     try:
#                         logging.info(f"执行SQL语句：{sql}")
#                         cursor.execute(sql)
#                         result = cursor.fetchall()
#                         column_names = [i[0] for i in cursor.description]
#                         if result is not None:
#                             self.result_queue.put((section, result, column_names))  # 包含section和列名
#                     except Exception as e:
#                         logging.error(f"未完成执行的错误：=>>  {server}-{e}")
#                     finally:
#                         cursor.close()
#                         connection.close()
#
#             threads = []
#             for section in config.sections():
#                 if section == 'group' and not self.execute_group.isChecked():
#                     continue
#                 if section == 'member' and not self.execute_mem.isChecked():
#                     continue
#                 server = config[section]
#                 sql_str = "SHOW PROCESSLIST"
#                 thread = threading.Thread(target=execute_query, args=(server, section, sql_str),
#                                           name=f"{section}-Thread")
#                 thread.start()
#                 threads.append((thread, section))
#
#             for thread, section in threads:
#                 thread.join()
#                 logging.info(f"{thread.name} 已执行!")
#
#             self.process_table.setRowCount(0)
#             headers_set = False
#             while not self.result_queue.empty():
#                 section, result, column_names = self.result_queue.get()
#                 if not headers_set:
#                     self.process_table.setColumnCount(len(column_names) + 1)
#                     self.process_table.setHorizontalHeaderLabels(["服务器节"] + column_names)
#                     headers_set = True
#
#                 for row in result:
#                     row_count = self.process_table.rowCount()
#                     self.process_table.insertRow(row_count)
#                     self.process_table.setItem(row_count, 0, QTableWidgetItem(section))
#                     for column_index, value in enumerate(row):
#                         self.process_table.setItem(row_count, column_index + 1, QTableWidgetItem(str(value)))
#
#         except Exception as e:
#             logging.error(f"执行操作未知错误: {str(e)}")
#
#     def kill_selected_process(self):
#         selected_items = self.process_table.selectedItems()
#         if not selected_items:
#             QMessageBox.warning(self, "警告", "搞毛，没选到！")
#             return
#
#         process_id = None
#         server_section = None
#         selected_row = selected_items[0].row()
#
#         for col in range(self.process_table.columnCount()):
#             item = self.process_table.item(selected_row, col)
#             if item:
#                 if col == 0:  # 服务器节
#                     server_section = item.text()
#                 elif col == 1:  # 进程ID
#                     process_id = item.text()
#
#         if process_id is None or server_section is None:
#             QMessageBox.warning(self, "警告", "搞毛，没选到！")
#             return
#
#         kill_sql = f"KILL {process_id};"
#         config = parse_config(self.server_name)
#         # 执行杀掉进程的操作
#         for section in config.sections():
#             if section == server_section:
#                 server = config[section]
#                 connection = connect_to_server(server)
#                 cursor = connection.cursor()
#                 try:
#                     logging.info(f"执行 KILL SQL：{kill_sql}")
#                     cursor.execute(kill_sql)
#                     connection.commit()
#                     QMessageBox.information(self, "成功", f"成功杀掉进程 {process_id} 在服务器节 {server_section}")
#                     self.load_processes()
#                 except Exception as e:
#                     logger.error(f"杀掉进程时发生错误：{e}")
#                     QMessageBox.critical(self, "错误", f"无法杀掉进程 {process_id}：{e}")
#                 finally:
#                     cursor.close()
#                     connection.close()
class ProcessDialog(QDialog):
    """mysql进程列表"""

    def __init__(self, server_name, execute_mem, execute_group, parent=None):
        super(ProcessDialog, self).__init__(parent)
        self.execute_mem = execute_mem
        self.execute_group = execute_group
        self.server_name = server_name
        self.result_queue = queue.Queue()
        self.is_closing = False  # 添加关闭标志

        self.setWindowTitle("进程列表")
        self.resize(1080, 600)
        layout = QVBoxLayout(self)

        self.label = QLabel("进程：")
        layout.addWidget(self.label)
        self.process_table = QTableWidget()
        layout.addWidget(self.process_table)

        self.refresh_button = QPushButton("刷新")
        self.refresh_button.clicked.connect(self.load_processes)
        layout.addWidget(self.refresh_button)

        self.kill_button = QPushButton("杀了它")
        self.kill_button.clicked.connect(self.kill_selected_process)
        layout.addWidget(self.kill_button)

        self.load_processes()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.load_processes)
        self.timer.start(10000)  # 修正为10秒（10000毫秒）

    def closeEvent(self, event):
        """重写关闭事件，确保资源正确释放"""
        self.is_closing = True
        # 停止定时器
        if hasattr(self, 'timer') and self.timer.isActive():
            self.timer.stop()
        # 清空队列
        if hasattr(self, 'result_queue'):
            while not self.result_queue.empty():
                try:
                    self.result_queue.get_nowait()
                except:
                    pass
        super(ProcessDialog, self).closeEvent(event)

    def load_processes(self):
        # 如果正在关闭，不再执行新的操作
        if self.is_closing:
            return

        self.process_table.clear()
        self.result_queue = queue.Queue()
        try:
            config = parse_config(self.server_name)

            # def execute_query(server, section, sql):
            #     # 检查是否正在关闭
            #     if self.is_closing:
            #         return
            #
            #     connection = connect_to_server(server)
            #     if connection:
            #         cursor = connection.cursor()
            #         try:
            #             logging.info(f"执行SQL语句：{sql}")
            #             cursor.execute(sql)
            #             result = cursor.fetchall()
            #             column_names = [i[0] for i in cursor.description]
            #             if result is not None and not self.is_closing:
            #                 self.result_queue.put((section, result, column_names))
            #         except Exception as e:
            #             logging.error(f"未完成执行的错误：=>>  {server}-{e}")
            #         finally:
            #             cursor.close()
            #             connection.close()

            def execute_query(server, section, sql):
                # 检查是否正在关闭
                if self.is_closing:
                    return

                connection = connect_to_server(server)
                if not connection:  # 检查连接是否成功
                    logging.error(f"无法连接到服务器: {server}")
                    return

                cursor = None
                try:
                    logging.info(f"执行SQL语句：{sql}")
                    cursor = connection.cursor()
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    column_names = [i[0] for i in cursor.description]
                    if result is not None and not self.is_closing:
                        self.result_queue.put((section, result, column_names))
                except Exception as e:
                    logging.error(f"未完成执行的错误：=>>  {server}-{e}")
                finally:
                    if cursor:
                        cursor.close()
                    if connection:
                        connection.close()

            threads = []
            for section in config.sections():
                if self.is_closing:
                    break

                if section == 'group' and not self.execute_group.isChecked():
                    continue
                if section == 'member' and not self.execute_mem.isChecked():
                    continue

                server = config[section]
                sql_str = "SHOW PROCESSLIST"
                thread = threading.Thread(target=execute_query, args=(server, section, sql_str),
                                          name=f"{section}-Thread")
                thread.start()
                threads.append((thread, section))

            for thread, section in threads:
                if self.is_closing:
                    break
                thread.join()
                logging.info(f"{thread.name} 已执行!")

            # 如果正在关闭，不再更新UI
            if self.is_closing:
                return

            self.process_table.setRowCount(0)
            headers_set = False
            while not self.result_queue.empty():
                section, result, column_names = self.result_queue.get()
                if not headers_set:
                    self.process_table.setColumnCount(len(column_names) + 1)
                    self.process_table.setHorizontalHeaderLabels(["服务器节"] + column_names)
                    headers_set = True

                for row in result:
                    row_count = self.process_table.rowCount()
                    self.process_table.insertRow(row_count)
                    self.process_table.setItem(row_count, 0, QTableWidgetItem(section))
                    for column_index, value in enumerate(row):
                        self.process_table.setItem(row_count, column_index + 1, QTableWidgetItem(str(value)))

        except Exception as e:
            if not self.is_closing:  # 只有在不是关闭过程中才记录错误
                logging.error(f"执行操作未知错误: {str(e)}")

    def kill_selected_process(self):
        # 如果正在关闭，不再执行操作
        if self.is_closing:
            return

        selected_items = self.process_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "警告", "搞毛，没选到！")
            return

        process_id = None
        server_section = None
        selected_row = selected_items[0].row()

        for col in range(self.process_table.columnCount()):
            item = self.process_table.item(selected_row, col)
            if item:
                if col == 0:  # 服务器节
                    server_section = item.text()
                elif col == 1:  # 进程ID
                    process_id = item.text()

        if process_id is None or server_section is None:
            QMessageBox.warning(self, "警告", "搞毛，没选到！")
            return

        kill_sql = f"KILL {process_id};"
        config = parse_config(self.server_name)
        # 执行杀掉进程的操作
        for section in config.sections():
            if section == server_section:
                server = config[section]
                connection = connect_to_server(server)
                cursor = connection.cursor()
                try:
                    logging.info(f"执行 KILL SQL：{kill_sql}")
                    cursor.execute(kill_sql)
                    connection.commit()
                    QMessageBox.information(self, "成功", f"成功杀掉进程 {process_id} 在服务器节 {server_section}")
                    self.load_processes()
                except Exception as e:
                    logging.error(f"杀掉进程时发生错误：{e}")
                    QMessageBox.critical(self, "错误", f"无法杀掉进程 {process_id}：{e}")
                finally:
                    cursor.close()
                    connection.close()