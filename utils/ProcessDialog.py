import logging
import queue
import threading

from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QMessageBox, QTableWidgetItem, QPushButton, QTableWidget, QVBoxLayout, QLabel, QDialog

from utils.logger import logger
from utils.DBconnectServer import connect_to_server
from utils.parseconfig import parse_config


class ProcessDialog(QDialog):
    """mysql进程列表"""

    def __init__(self, server_name, execute_mem, execute_group, parent=None):
        super(ProcessDialog, self).__init__(parent)
        self.execute_mem = execute_mem
        self.execute_group = execute_group
        self.server_name = server_name
        self.result_queue = queue.Queue()
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
        self.timer.start(10000)  # 10秒刷新

    def load_processes(self):
        self.process_table.clear()
        self.result_queue = queue.Queue()
        try:
            config = parse_config(self.server_name)

            def execute_query(server, section, sql):
                connection = connect_to_server(server)
                cursor = connection.cursor()
                if connection:
                    try:
                        logging.info(f"执行SQL语句：{sql}")
                        cursor.execute(sql)
                        result = cursor.fetchall()
                        column_names = [i[0] for i in cursor.description]
                        if result is not None:
                            self.result_queue.put((section, result, column_names))  # 包含section和列名
                    except Exception as e:
                        logging.error(f"未完成执行的错误：=>>  {server}-{e}")
                    finally:
                        cursor.close()
                        connection.close()

            threads = []
            for section in config.sections():
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
                thread.join()
                logging.info(f"{thread.name} 已执行!")

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
            logging.error(f"执行操作未知错误: {str(e)}")

    def kill_selected_process(self):
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
                    logger.error(f"杀掉进程时发生错误：{e}")
                    QMessageBox.critical(self, "错误", f"无法杀掉进程 {process_id}：{e}")
                finally:
                    cursor.close()
                    connection.close()