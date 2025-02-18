import sys
import pymysql
import concurrent.futures
import queue
import threading
import json
import time
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QTextEdit, QGroupBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont


# 全局变量，用于控制任务是否继续执行
is_running = True
active_processes = []
log_lock = threading.Lock()  # 日志锁，确保线程安全


# 从配置文件读取数据库配置
def load_db_configs(config_file):
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"读取配置文件失败: {e}")
        return []


# 处理每个线程的数据
class WorkerThread(QThread):
    log_signal = pyqtSignal(str)  # 用于发送日志信号

    def __init__(self, db_config, task_queue, thread_id, proc_name, first_param):
        super().__init__()
        self.db_config = db_config
        self.task_queue = task_queue
        self.thread_id = thread_id
        self.proc_name = proc_name
        self.first_param = first_param

    def run(self):
        conn = pymysql.connect(host=self.db_config['host'], user=self.db_config['user'],
                               password=self.db_config['password'], database=self.db_config['database'], autocommit=True)
        cursor = conn.cursor()
        process_id = conn.thread_id()
        try:
            if process_id:
                with log_lock:
                    active_processes.append(process_id)  # 保存当前进程ID
                    self.log_signal.emit(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 线程 {self.thread_id} 在数据库 {self.db_config['name']} 上执行，数据库进程ID: {process_id}\n")

            while not self.task_queue.empty() and is_running:  # 从队列中获取任务
                hotel_id = self.task_queue.get()  # 获取一个酒店ID
                try:
                    with log_lock:
                        self.log_signal.emit(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据库进程ID: {process_id} 线程 {self.thread_id} 在数据库 {self.db_config['name']} 正在执行酒店 ID {hotel_id}\n")
                    # 调用存储过程
                    cursor.callproc(self.proc_name, (self.first_param, hotel_id))
                    with log_lock:
                        self.log_signal.emit(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据库进程ID: {process_id} 线程 {self.thread_id} 在数据库 {self.db_config['name']} 执行酒店 ID {hotel_id} 已完成\n")
                except Exception as e:
                    with log_lock:
                        self.log_signal.emit(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据库进程ID: {process_id} 线程 {self.thread_id} 在数据库 {self.db_config['name']} 执行酒店 ID {hotel_id} 失败: {e}\n")
                finally:
                    self.task_queue.task_done()  # 标记任务已完成
        except Exception as e:
            with log_lock:
                self.log_signal.emit(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据库进程ID: {process_id} 线程 {self.thread_id} 在数据库 {self.db_config['name']} 发生错误: {e}\n")
        finally:
            cursor.close()
            conn.close()
            with log_lock:
                self.log_signal.emit(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据库进程ID: {process_id} 线程 {self.thread_id} 在数据库 {self.db_config['name']} 已退出\n")


# 主窗口类
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("数据库多线程处理")
        self.setFixedSize(1024, 768)  # 固定窗口大小为 800x600，不允许调整

        # 设置全局字体
        font = QFont("Arial", 10)
        self.setFont(font)

        # 主布局
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout()
        main_widget.setLayout(layout)

        # 参数配置和任务控制区域（同一行）
        control_group = QGroupBox()
        control_layout = QHBoxLayout()
        control_group.setLayout(control_layout)

        # 参数配置区域（左边）
        param_layout = QHBoxLayout()

        # 存储过程名称
        param_layout.addWidget(QLabel("过程名称:"))
        self.proc_name_input = QLineEdit()
        self.proc_name_input.setPlaceholderText("输入存储过程名称")
        self.proc_name_input.setText("up_ihotel_call_hotelcall")  # 默认值
        self.proc_name_input.setFixedWidth(150)
        param_layout.addWidget(self.proc_name_input)

        # 线程数量
        param_layout.addWidget(QLabel("进程数量:"))
        self.num_threads_input = QLineEdit()
        self.num_threads_input.setPlaceholderText("进程数量")
        self.num_threads_input.setText("8")  # 默认值
        self.num_threads_input.setFixedWidth(80)
        param_layout.addWidget(self.num_threads_input)

        # 首位参数
        param_layout.addWidget(QLabel("集团ID:"))
        self.first_param_input = QLineEdit()
        self.first_param_input.setPlaceholderText("集团ID")
        self.first_param_input.setText("2")  # 默认值
        self.first_param_input.setFixedWidth(80)
        param_layout.addWidget(self.first_param_input)

        control_layout.addLayout(param_layout)

        # 任务控制区域（右边）
        exec_layout = QHBoxLayout()

        self.start_button = QPushButton("开搞")
        self.start_button.setFixedSize(100, 30)
        self.start_button.clicked.connect(self.start_tasks)
        exec_layout.addWidget(self.start_button)

        self.stop_button = QPushButton("不搞了")
        self.stop_button.setFixedSize(100, 30)
        self.stop_button.clicked.connect(self.stop_tasks)
        self.stop_button.setEnabled(False)  # 初始状态下停止按钮不可点击
        exec_layout.addWidget(self.stop_button)

        control_layout.addLayout(exec_layout)

        layout.addWidget(control_group)

        # 日志显示区
        log_group = QGroupBox("日志")
        log_layout = QVBoxLayout()
        log_group.setLayout(log_layout)

        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        log_layout.addWidget(self.log_output)

        layout.addWidget(log_group)

        # 加载数据库配置
        self.db_configs = load_db_configs("db_config.json")
        if not self.db_configs:
            self.log_output.append("未加载到数据库配置，请检查配置文件！\n"
                                   "手工建立数据库配置文件请在本目录下建立！\n"
                                   "文件名称：db_config.json \n"
                                   "格式: \n"
                                   "[ \n"
                                   "    {\n"
                                   "        \"name\": \"数据库1\",\n"
                                   "        \"host\": \"127.0.0.1\",\n"
                                   "        \"user\": \"root\",\n"
                                   "        \"password\": \"123456\",\n"
                                   "        \"database\": \"mysql\",\n"
                                   "    },\n"
                                   "    {\n"
                                   "        \"name\": \"数据库2\",\n"
                                   "        \"host\": \"127.0.0.1\",\n"
                                   "        \"user\": \"root\",\n"
                                   "        \"password\": \"123456\",\n"
                                   "        \"database\": \"mysql\",\n"
                                   "    },\n"
                                   "]\n"
                                   "\n")
        self.log_output.append("====== 同时在所有数据库上多线程执行同一个存储过程！======\n"
                               "1、程序会去查询酒店表所有为I状态的酒店并将酒店ID作为存储过程的入参！\n"
                               "2、存储过程第一入参必须为集团ID，第二入参为程序带入的酒店ID，不允许有第三参数！\n"
                               "3、存储过程需要自行设置日志输出，程序不会反馈存储过程结果输出，程序日志只记录反馈当前执行进程及酒店执行结果（已完成）！\n")
    # 启动任务
    def start_tasks(self):
        global is_running
        is_running = True
        self.log_output.clear()
        self.start_button.setEnabled(False)  # 禁用启动按钮
        self.stop_button.setEnabled(True)  # 启用停止按钮

        proc_name = self.proc_name_input.text()
        num_threads = int(self.num_threads_input.text())
        first_param = int(self.first_param_input.text())

        self.log_output.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 任务启动...\n")

        # 启动线程池来处理多个数据库
        self.threads = []
        for db_config in self.db_configs:
            conn = pymysql.connect(host=db_config['host'], user=db_config['user'],
                                   password=db_config['password'], database=db_config['database'])
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT id FROM hotel WHERE sta = 'I' ORDER BY id")
                hotel_ids = [row[0] for row in cursor.fetchall()]  # 获取所有ID
                self.log_output.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据库 {db_config['name']} 共执行 {len(hotel_ids)} 酒店\n")

                # 创建任务队列并将ID加入队列
                task_queue = queue.Queue()
                for hotel_id in hotel_ids:
                    task_queue.put(hotel_id)

                # 启动线程池来处理任务
                for i in range(num_threads):
                    thread = WorkerThread(db_config, task_queue, i + 1, proc_name, first_param)
                    thread.log_signal.connect(self.log_output.append)
                    thread.finished.connect(self.check_tasks_complete)
                    self.threads.append(thread)
                    thread.start()

            except Exception as e:
                self.log_output.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据库 {db_config['name']} 查询失败: {e}\n")
            finally:
                cursor.close()
                conn.close()

    # 检查任务是否全部完成
    def check_tasks_complete(self):
        if all(not thread.isRunning() for thread in self.threads):
            self.log_output.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 所有任务已完成！\n")
            self.start_button.setEnabled(True)  # 启用启动按钮
            self.stop_button.setEnabled(False)  # 禁用停止按钮

    # 停止任务
    def stop_tasks(self):
        global is_running
        is_running = False
        self.log_output.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 任务已停止，正在终止数据库进程...\n")

        # 终止所有数据库进程
        for process_id in active_processes:
            try:
                for db_config in self.db_configs:
                    conn = pymysql.connect(host=db_config['host'], user=db_config['user'],
                                           password=db_config['password'], database=db_config['database'],autocommit=True)
                    cursor = conn.cursor()
                    cursor.execute(f"KILL {process_id}")  # 终止进程
                    cursor.close()
                    conn.close()
                self.log_output.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 成功终止进程 {process_id}\n")
            except Exception as e:
                self.log_output.append(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 终止进程 {process_id} 失败: {e}\n")

        self.start_button.setEnabled(True)  # 启用启动按钮
        self.stop_button.setEnabled(False)  # 禁用停止按钮


# 运行程序
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())