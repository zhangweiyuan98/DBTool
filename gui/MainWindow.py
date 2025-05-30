import base64
import configparser
import datetime
import os
import queue
import re
import threading
import pandas as pd
import sqlparse
from PyQt5 import QtCore, QtGui
from PyQt5 import QtWidgets
from PyQt5.QtCore import QTimer
from PyQt5.QtCore import Qt, QItemSelection, QItemSelectionModel
from PyQt5.QtGui import QIcon, QClipboard
from PyQt5.QtWidgets import QFileDialog, QMenu, QAction, QLabel, QSplitter, QPushButton, \
    QComboBox, QAbstractItemView, QDialog, QMessageBox, QDesktopWidget
from PyQt5.QtWidgets import QApplication
from utils.AddConnet import ServerDialog
from utils.DBconnectServer import popup_manager, connect_to_server
from utils.DBcrypt import encode_password
from utils.ExThread import Thread_1, Thread_2
from utils.Ex_Threads import ExThreadDialog
from utils.Exdatabases import split_statements, clean_sql, execute_sql, Process_df
from utils.LargeTableModel import LargeTableModel, ExportThreadCsv, ExportThread
from utils.ProcessDialog import ProcessDialog
from utils import logger
from utils.logger import logger
from utils.parseconfig import parse_config
from utils.SqlEdit import SQLTextEdit
from utils.wehotel_interface_log import wehotel_log_info


class Ui_MainWindow(object):
    def __init__(self):
        super().__init__()
        self.statusbar = None
        self.sqlTextEdit = None
        self.selected_rows = None
        self.selected_colums = None
        self.file_path = ""
        self.selected_rows = []
        self.tab_table_map = {}
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.setWindowTitle("DBtool")
        MainWindow.setWindowIcon(QIcon("resources\icon.ico"))
        MainWindow.setEnabled(True)

        screen = QDesktopWidget().screenGeometry()
        MainWindow.setGeometry(0, 0, screen.width() * 0.6, screen.height() * 0.5)  # 设置窗口大小为屏幕大小
        # 获取窗口尺寸
        window_width = MainWindow.width()
        window_height = MainWindow.height()

        x = (screen.width() - window_width) // 2
        y = (screen.height() - window_height) // 2

        # 移动窗口到屏幕中央
        MainWindow.move(x,y)
        # MainWindow.resize(1024, 768)

        menubar = self.menuBar()
        server_menu = menubar.addMenu("菜单")
        self.statusbar = MainWindow.statusBar()
        
        add_server_action = QAction("添加服务器", self)
        add_server_action.triggered.connect(self.open_server_dialog)
        server_menu.addAction(add_server_action)

        view_pross_action = QAction("看进程", self)
        view_pross_action.triggered.connect(self.open_process_dialog)
        server_menu.addAction(view_pross_action)

        ex_threads = QAction("多线程执行任务", self)
        ex_threads.triggered.connect(self.open_ex_threads)
        server_menu.addAction(ex_threads)

        # copy_table = QAction("将表复制到不同的主机/数据库...", self)
        # copy_table.triggered.connect(self.copy_table_to)
        # server_menu.addAction(copy_table)

        self.centralwidget = QtWidgets.QWidget(parent=MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        layout = QtWidgets.QVBoxLayout(self.centralwidget)
        font = QtGui.QFont("Arial")
        font.setPointSize(10)
        self.row1Layout = QtWidgets.QHBoxLayout()
        self.sever_1 = QtWidgets.QGroupBox("版本", parent=self.centralwidget)
        self.sever_1_Layout = QtWidgets.QHBoxLayout(self.sever_1)
        self.serverComboBox = QtWidgets.QComboBox(parent=self.centralwidget)
        self.load_server_names()  # 加载服务器列表
        self.serverComboBox.setFont(font)
        self.serverComboBox.currentIndexChanged.connect(lambda: self.updateWindowTitle(MainWindow))
        self.sever_1_Layout.addWidget(self.serverComboBox)
        self.row1Layout.addWidget(self.sever_1)
        self.fast_query = QtWidgets.QGroupBox("快捷查询", parent=self.centralwidget)
        self.fast_query_Layout = QtWidgets.QHBoxLayout(self.fast_query)
        self.CODE = QtWidgets.QLineEdit(parent=self.centralwidget)
        self.CODE.setPlaceholderText("酒店CODE")
        self.CODE.setFont(font)
        self.fast_query_Layout.addWidget(self.CODE)
        self.desc = QtWidgets.QLineEdit(parent=self.centralwidget)
        self.desc.setPlaceholderText("酒店名称")
        self.desc.setFont(font)
        self.fast_query_Layout.addWidget(self.desc)
        self.id = QtWidgets.QLineEdit(parent=self.centralwidget)
        self.id.setPlaceholderText("酒店ID")
        self.id.setFont(font)
        self.fast_query_Layout.addWidget(self.id)
        self.LIMIT = QtWidgets.QLineEdit(parent=self.centralwidget)
        self.LIMIT.setToolTip("限制查询条数")
        self.LIMIT.setFont(font)
        self.LIMIT.setText("100")
        self.fast_query_Layout.addWidget(self.LIMIT)
        self.executeButton = QtWidgets.QPushButton(parent=self.centralwidget)
        self.executeButton.setText("执行 Ctrl+F9")
        self.executeButton.setFont(font)
        self.fast_query_Layout.addWidget(self.executeButton)
        self.executeButton.setShortcut("Ctrl+F9")  # 设置快捷键
        self.executeButton.clicked.connect(self.Find_Wyn_Hotel)
        self.row1Layout.addWidget(self.fast_query)
        layout.addLayout(self.row1Layout)
        self.row2Layout = QtWidgets.QHBoxLayout()

        self.import_box = QtWidgets.QGroupBox("数据导入", parent=self.centralwidget)
        self.import_box_Layout = QtWidgets.QGridLayout(self.import_box)
        self.checkbox = QtWidgets.QCheckBox(parent=self.centralwidget)
        self.checkbox.setText("启用")
        self.checkbox.setFont(font)
        self.checkbox.move(20, 20)
        self.checkbox.stateChanged.connect(self.toggleButtons)
        self.toggleButtons(0)
        self.import_box_Layout.addWidget(self.checkbox, 0, 0)
        self.dbname = QtWidgets.QLineEdit(parent=self.centralwidget)
        self.dbname.setToolTip("库名")
        self.dbname.setText("migrate_db")
        self.dbname.setFont(font)
        self.dbname.setEnabled(False)
        self.import_box_Layout.addWidget(self.dbname, 0, 1)
        self.crate_table = QtWidgets.QLineEdit(parent=self.centralwidget)
        self.crate_table.setPlaceholderText("创建表名")
        self.crate_table.setToolTip("默认Excel表单名")
        self.crate_table.setFont(font)
        self.crate_table.setEnabled(False)
        self.import_box_Layout.addWidget(self.crate_table, 0, 2)
        self.select_button = QPushButton(parent=self.centralwidget)
        self.select_button.setText("Excel文件")
        self.select_button.clicked.connect(self.select_excel)
        self.select_button.setEnabled(False)
        self.import_box_Layout.addWidget(self.select_button, 1, 0)
        self.sheet_dropdown = QComboBox()
        self.sheet_dropdown.setFont(font)
        self.sheet_dropdown.setEnabled(False)
        self.import_box_Layout.addWidget(self.sheet_dropdown, 1, 1)
        self.import_button = QPushButton(parent=self.centralwidget)
        self.import_button.setText("导入")
        self.import_button.setFont(font)
        self.import_button.clicked.connect(self.import_data)
        self.import_button.setEnabled(False)
        self.import_box_Layout.addWidget(self.import_button, 1, 2)
        self.row2Layout.addWidget(self.import_box)

        self.sql_box = QtWidgets.QGroupBox("搞SQL脚本", parent=self.centralwidget)
        self.sql_box_Layout = QtWidgets.QGridLayout(self.sql_box)
        self.sql_file = QPushButton(parent=self.centralwidget)
        self.sql_file.setText("文件夹")
        self.sql_file.setFont(font)
        self.sql_file.clicked.connect(self.select_file)
        self.sql_file.setEnabled(True)
        self.sql_box_Layout.addWidget(self.sql_file)
        self.sqlButton2 = QtWidgets.QPushButton(parent=self.centralwidget)
        self.sqlButton2.setText("搞脚本")
        # self.sqlButton2.setFixedSize(130, 32)
        self.sqlButton2.setFont(font)
        self.sqlButton2.clicked.connect(self.execute_sql_scripts)
        self.sql_box_Layout.addWidget(self.sqlButton2)
        self.row2Layout.addWidget(self.sql_box)

        self.other_box = QtWidgets.QGroupBox("其他", parent=self.centralwidget)
        self.other_box_Layout = QtWidgets.QGridLayout(self.other_box)

        self.hotel_resg = QPushButton(parent=self.centralwidget)
        self.hotel_resg.setText("生成注册码")
        self.hotel_resg.setFont(font)
        self.hotel_resg.clicked.connect(self.get_regs)
        self.other_box_Layout.addWidget(self.hotel_resg)

        self.wehotellog = QPushButton(parent=self.centralwidget)
        self.wehotellog.setText("wehotel接口信息")
        self.wehotellog.setFont(font)
        self.wehotellog.clicked.connect(self.select_wehotellog)
        self.other_box_Layout.addWidget(self.wehotellog)


        self.row2Layout.addWidget(self.other_box)

        self.timeLabel = QLabel()
        self.timeLabel.setFont(font)
        self.end_box = QtWidgets.QGroupBox("开搞", parent=self.centralwidget)
        self.end_box_Layout = QtWidgets.QGridLayout(self.end_box)
        self.executeButton2 = QtWidgets.QPushButton(parent=self.centralwidget)
        self.executeButton2.setText("开搞 F9")
        # self.executeButton2.setFixedSize(130, 47)
        self.executeButton2.setFont(font)
        self.executeButton2.setShortcut("F9")  # 设置快捷键
        self.executeButton2.clicked.connect(self.execute_button_clicked)
        self.end_box_Layout.addWidget(self.executeButton2, 0, 0)
        self.executeButton3 = QtWidgets.QPushButton(parent=self.centralwidget)
        self.executeButton3.setText("不搞了 F12")
        # self.executeButton3.setFixedSize(130, 47)
        self.executeButton3.setFont(font)
        self.executeButton3.setShortcut("F12")  # 设置快捷键
        self.executeButton3.setEnabled(False)
        self.executeButton3.clicked.connect(self.stop_button_clicked)
        self.end_box_Layout.addWidget(self.executeButton3, 1, 0)

        self.execute_PMS = QtWidgets.QCheckBox(parent=self.centralwidget)
        self.execute_PMS.setText("门店")
        self.execute_PMS.move(20, 20)
        self.execute_PMS.setChecked(True)
        self.execute_PMS.setEnabled(False)
        self.execute_group = QtWidgets.QCheckBox(parent=self.centralwidget)
        self.execute_group.setText("集团")
        self.execute_group.move(20, 20)
        self.execute_member = QtWidgets.QCheckBox(parent=self.centralwidget)
        self.execute_member.setText("会员")
        self.execute_member.move(20, 20)
        self.end_box_Layout.addWidget(self.execute_PMS, 0, 1)
        self.end_box_Layout.addWidget(self.execute_group, 1, 1)
        self.end_box_Layout.addWidget(self.execute_member, 2, 1)
        self.row2Layout.addWidget(self.end_box)

        layout.addLayout(self.row2Layout)
        splitter = QSplitter(Qt.Vertical)

        self.queryInput = SQLTextEdit()
        splitter.addWidget(self.queryInput)

        self.tab_widget = QtWidgets.QTabWidget(self.centralwidget)
        self.tab_widget.tabBar().setTabsClosable(True)
        self.tab_widget.tabBar().tabCloseRequested.connect(self.close_tab)
        self.tab_widget.tabBar().setMouseTracking(True)

        self.tab_widget.currentChanged.connect(self.setup_table_context_menu)
        self.context_menu = QMenu(self.centralwidget)
        self.action_export = QAction("导出为excel", self.centralwidget)
        self.action_export_csv = QAction("导出为CSV", self.centralwidget)
        self.filter = QAction("查找", self.centralwidget)

        self.action_export.triggered.connect(self.export_to_excel)
        self.action_export_csv.triggered.connect(self.export_to_csv)
        self.filter.triggered.connect(self.filterTable)

        self.context_menu.addAction(self.action_export)
        self.context_menu.addAction(self.action_export_csv)
        self.context_menu.addAction(self.filter)

        splitter.addWidget(self.tab_widget)
        layout.addWidget(splitter)

        layout = QtWidgets.QVBoxLayout(self.centralwidget)
        layout.addWidget(self.tab_widget)

        MainWindow.setCentralWidget(self.centralwidget)
        self.retranslateUi(MainWindow)

        self.timer = QTimer(self)
        self.timer.timeout.connect(lambda: self.updateRuntime(MainWindow))

        # 标志变量来判断是否需要更新时间
        self.isRunning = False
        self.elapsed_time = 0  # 记录已经经过的时间（单位为毫秒）
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

        # 设置主窗口样式
        MainWindow.setStyleSheet("""
            /* 全局样式 */
            QWidget {
                font-family: -apple-system, BlinkMacSystemFont, sans-serif;
                font-size: 13px;
                color: #1d1d1f;
            }
            
            /* 主窗口 */
            QMainWindow {
                background-color: #f5f5f7;
            }
            
            /* 分组框 */
            QGroupBox {
                border: 1px solid #d2d2d7;
                border-radius: 6px;
                margin-top: 10px;
                padding-top: 15px;
                font-size: 13px;
                font-weight: 500;
                color: #1d1d1f;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 3px;
            }
            
            /* 按钮 */
            QPushButton {
                border: 1px solid #d2d2d7;
                border-radius: 6px;
                padding: 6px 12px;
                min-width: 80px;
            }
            QPushButton:hover:enabled {
                background-color: #e5e5e7;
                border-color: #a2a2a7;
            }
            QPushButton:pressed:enabled {
                background-color: #d5d5d7;
            }
            QPushButton:disabled {
                background-color: #f5f5f7;
                color: #a2a2a7;
                border-color: #d2d2d7;
            }

            /* 输入框 */
            QLineEdit, QComboBox {
                border: 1px solid #d2d2d7;
                border-radius: 6px;
                padding: 6px;
                background-color: white;
                selection-background-color: #007aff;
                selection-color: white;
            }
            QLineEdit:hover, QComboBox:hover {
                border-color: #a2a2a7;
            }
            QLineEdit:focus, QComboBox:focus {
                border-color: #007aff;
            }
            
            /* 下拉框 */
            QComboBox {
                padding-right: 20px; /* 为箭头留出空间 */
            }
            QComboBox::drop-down {
                subcontrol-origin: padding;
                subcontrol-position: top right;
                width: 20px;
                border-left: 1px solid #d2d2d7;
                border-radius: 0 6px 6px 0;
            }
            QComboBox::down-arrow {
                image: url(resources/down-arrow.png);
                width: 12px;
                height: 12px;
            }
            QComboBox QAbstractItemView {
                border: 1px solid #d2d2d7;
                border-radius: 6px;
                padding: 4px;
                background-color: white;
                selection-background-color: #007aff;
                selection-color: white;
                min-width: 200px; /* 设置最小宽度 */
            }
            QComboBox QAbstractItemView::item {
                padding: 4px 8px;
            }
            
            /* 表格样式 */
            QTableView {
                background-color: white;
                alternate-background-color: #f5f5f7;
                gridline-color: #d2d2d7;
                selection-background-color: #007aff;
                selection-color: white;
            }
            
            QHeaderView::section {
                background-color: #f5f5f7;
                padding: 8px;
                border: 1px solid #d2d2d7;
            }
        """)

        # 给按钮设置对象名称
        self.executeButton.setObjectName("executeButton")
        self.executeButton2.setObjectName("executeButton2")
        self.executeButton3.setObjectName("executeButton3")

        # 设置下拉框的最小宽度
        self.serverComboBox.setMinimumWidth(100)
        self.sheet_dropdown.setMinimumWidth(100)

        # 调整布局间距
        layout.setSpacing(10)
        layout.setContentsMargins(10, 10, 10, 10)

        # 调整表格样式
        self.tab_widget.setStyleSheet("""
            QTableView {
                background-color: white;
                alternate-background-color: #f9f9f9;
                gridline-color: #ddd;
                selection-background-color: #e3f2fd;
                selection-color: #000;
            }
            QHeaderView::section {
                background-color: #f5f5f5;
                padding: 8px;
                border: 1px solid #ddd;
            }
        """)
        
        # 调整菜单栏样式
        menubar.setStyleSheet("""
            QMenuBar {
                background-color: #f5f5f5;
                padding: 4px;
                border-bottom: 1px solid #ddd;
            }
            QMenuBar::item {
                padding: 4px 8px;
            }
            QMenuBar::item:selected {
                background-color: #e0e0e0;
            }
        """)

        # 调整复选框样式
        checkbox_style = """
            QCheckBox {
                spacing: 5px;
            }
            QCheckBox::indicator {
                width: 16px;
                height: 16px;
            }
        """
        self.checkbox.setStyleSheet(checkbox_style)
        self.execute_PMS.setStyleSheet(checkbox_style)
        self.execute_group.setStyleSheet(checkbox_style)
        self.execute_member.setStyleSheet(checkbox_style)

        # 设置标签页关闭按钮可见
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabBar().setStyleSheet("""
            QTabBar::tab {
                padding-right: 32px; /* 为关闭按钮留出空间 */
            }
        """)

    def open_server_dialog(self):
        dialog = ServerDialog()
        if dialog.exec_() == QDialog.Accepted:
            # 获取用户输入的数据
            service_group_name = dialog.service_group_name.text()
            server_data = {
                "host": dialog.host.text(),
                "port": dialog.port.text(),
                "user": dialog.user.text(),
                "password": encode_password(f"{dialog.password.text()}"),
                "database": dialog.database.text(),
                "ssh_host": dialog.ssh_host.text() if dialog.ssh_checkbox.isChecked() else None,
                "ssh_port": dialog.ssh_port.text() if dialog.ssh_checkbox.isChecked() else None,
                "ssh_user": dialog.ssh_user.text() if dialog.ssh_checkbox.isChecked() else None,
                "ssh_password": encode_password(
                    f"{dialog.ssh_password.text()}") if dialog.ssh_checkbox.isChecked() else None,
                "SslMode": "on" if dialog.ssh_checkbox.isChecked() else "no"  # 根据需要设置SslMode
            }
            ini_file_path = f"server/{self.serverComboBox.currentText().strip()}-config.ini"  # 替换为您的INI文件路径
            config = configparser.ConfigParser()

            # 检查是否存在INI文件，如果不存在，则创建一个
            if os.path.exists(ini_file_path):
                config.read(ini_file_path, encoding='utf-8')
            else:
                config['服务器'] = {}
            config[service_group_name] = {k: v for k, v in server_data.items() if v is not None}
            # 写入到INI文件
            with open(ini_file_path, 'w', encoding='utf-8') as configfile:
                config.write(configfile)

            print(f"{service_group_name} 的服务器信息已写入到 {ini_file_path}")

    def open_process_dialog(self):
        server_name = self.serverComboBox.currentText().strip()
        execute_group = self.execute_group
        execute_member = self.execute_member
        dialog = ProcessDialog(server_name, execute_member, execute_group, self)
        dialog.exec_()

    def open_ex_threads(self):
        server_name = self.serverComboBox.currentText().strip()
        execute_group = self.execute_group
        execute_member = self.execute_member
        dialog = ExThreadDialog(server_name, execute_member, execute_group)
        dialog.exec_()

    def select_wehotellog(self):
        server_name = self.serverComboBox.currentText().strip()
        dialog = wehotel_log_info(server_name)
        dialog.exec_()

    # def copy_table_to(self):
    #     server_name = self.serverComboBox.currentText().strip()
    #     execute_group = self.execute_group
    #     execute_member = self.execute_member
    #     dialog = CopyTableto_Dialog(server_name, execute_member, execute_group, self)
    #     dialog.exec_()

    def closeEvent(self, event):
        reply = QtWidgets.QMessageBox.question(self, '确认退出',
                                               '您确定要退出吗？', QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
                                               QtWidgets.QMessageBox.No)

        if reply == QtWidgets.QMessageBox.Yes:
            event.accept()  # Close the application
        else:
            event.ignore()  # Ignore the close event

    def close_tab(self, index):
        self.tab_widget.removeTab(index)

    def setup_table_context_menu(self):
        # 设置每个表格的右键菜单
        for i in range(self.tab_widget.count()):
            table = self.tab_widget.widget(i)
            table.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
            table.customContextMenuRequested.connect(self.show_context_menu)

    def show_context_menu(self, pos):
        self.context_menu.exec_(self.tab_widget.mapToGlobal(pos))

    def execute_button_clicked(self):
        server_name = self.serverComboBox.currentText().strip()
        sql = self.queryInput.toPlainText().strip()
        timeLabel = self.timeLabel
        execute_group = self.execute_group
        execute_pms = self.execute_PMS
        execute_member = self.execute_member
        self.startExecution()
        self.thread = Thread_1(server_name, sql, timeLabel, execute_group, execute_pms, execute_member)
        self.thread.start()
        self.thread.result_ready.connect(self.on_result_ready)
        self.serverComboBox.setEnabled(False)
        self.executeButton2.setEnabled(False)
        self.executeButton3.setEnabled(True)

    def stop_button_clicked(self):
        server_name = self.serverComboBox.currentText().strip()
        timeLabel = self.timeLabel
        execute_group = self.execute_group
        execute_pms = self.execute_PMS
        execute_member = self.execute_member
        self.thread = Thread_2(server_name, timeLabel, execute_group, execute_pms, execute_member)
        self.thread.start()
        self.thread.stop_click.connect(self.stop_click)
        self.executeButton2.setEnabled(True)
        self.executeButton3.setEnabled(False)

    def filterTable(self):
        try:
            current_index = self.tab_widget.currentIndex()  # 获取当前选项卡索引
            current_tab = self.tab_widget.widget(current_index)  # 获取当前选项卡

            text, okPressed = QtWidgets.QInputDialog.getText(current_tab, "查找", "输入查找文本:",
                                                             QtWidgets.QLineEdit.Normal, "")
            if okPressed and text.strip():
                selection_model = current_tab.selectionModel()
                model = current_tab.model()

                selected_indexes = []
                self.matched_rows = []
                for row in range(model.rowCount()):
                    for column in range(model.columnCount()):
                        index = model.index(row, column)
                        if index.isValid() and text.lower() in index.data().lower():  # 模糊搜索，忽略大小写
                            selected_indexes.append(index)
                            if row not in self.matched_rows:
                                self.matched_rows.append(row)
                selection_model.clearSelection()
                if selected_indexes:
                    new_selection = QItemSelection()
                    for index in selected_indexes:
                        new_selection.select(index, index)

                    selection_model.select(new_selection, QItemSelectionModel.ClearAndSelect)
                    view = current_tab  # 假设 current_tab 是 QTableView
                    first_matched_index = selected_indexes[0]
                    view.scrollTo(first_matched_index, QAbstractItemView.PositionAtTop)
                    self.current_matched_row_index = 0
        except Exception as e:
            print(e)

    def deleteTable(self):
        current_index = self.tab_widget.currentIndex()
        if current_index != -1:  # 确保当前选项卡有效
            self.tab_widget.removeTab(current_index)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        server = self.serverComboBox.currentText()
        MainWindow.setWindowTitle(_translate("MainWindow", f"DB_Tool-{server} "))


    def updateWindowTitle(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        server = self.serverComboBox.currentText()
        MainWindow.setWindowTitle(_translate("MainWindow", f"DB_Tool-{server}"))
    def startExecution(self):
        """模拟开始执行的过程"""
        self.isRunning = True
        self.elapsed_time = 0
        self.timer.start(10)

    def finishExecution(self,MainWindow):
        """模拟执行完毕的操作"""
        _translate = QtCore.QCoreApplication.translate
        self.isRunning = False
        self.timer.stop()
        self.elapsed_time += 10
        # 转换成分钟:秒:毫秒 格式
        minutes = self.elapsed_time // 60000
        seconds = (self.elapsed_time % 60000) // 1000
        milliseconds = self.elapsed_time % 1000
        # 格式化时间为 "mm:ss:SSS"
        formatted_time = f"{minutes:02}:{seconds:02}:{milliseconds:03}"
        MainWindow.setWindowTitle(_translate("MainWindow", f"DB_Tool- 耗时:{formatted_time}"))

    def dorp_table(self,MainWindow):
        """模拟执行完毕的操作"""
        _translate = QtCore.QCoreApplication.translate
        self.isRunning = False
        self.timer.stop()
        MainWindow.setWindowTitle(_translate("MainWindow", f"DB_Tool-渲染表格中"))

    def updateRuntime(self,MainWindow):
        """更新窗口标题，显示时间"""
        _translate = QtCore.QCoreApplication.translate
        if self.isRunning:
            self.elapsed_time += 10
            # 转换成分钟:秒:毫秒 格式
            minutes = self.elapsed_time // 60000
            seconds = (self.elapsed_time % 60000) // 1000
            milliseconds = self.elapsed_time % 1000
            # 格式化时间为 "mm:ss:SSS"
            formatted_time = f"{minutes:02}:{seconds:02}:{milliseconds:03}"
            # 更新窗口标题
            MainWindow.setWindowTitle(f"DBTool - 耗时:{formatted_time}")

    def toggleButtons(self, state):
        try:
            if state == 2:  # 2表示选中状态
                self.dbname.setEnabled(True)
                self.crate_table.setEnabled(True)
                self.select_button.setEnabled(True)
                self.sheet_dropdown.setEnabled(True)
                self.import_button.setEnabled(True)
            elif state == 0:
                self.dbname.setEnabled(False)
                self.crate_table.setEnabled(False)
                self.select_button.setEnabled(False)
                self.sheet_dropdown.setEnabled(False)
                self.import_button.setEnabled(False)
        except Exception as e:
            pass

    def load_server_names(self):
        config_files = [f for f in os.listdir('server') if f.endswith('-config.ini')]
        server_names = [f.replace('-config.ini', '') for f in config_files]
        self.serverComboBox.addItems(server_names)

    def Find_Wyn_Hotel(self):
        hotelcode = self.CODE.text()
        hotelid = self.id.text()
        desc = self.desc.text()
        hotellimit = self.LIMIT.text()
        server_name = self.serverComboBox.currentText().strip()

        logger.info(f"当前执行集团: {server_name}")
        config = parse_config(server_name)
        for section in config.sections():
            if section == 'group':
                try:
                    server = config[section]
                    conn = connect_to_server(server)
                    if conn is not None:
                        try:
                            with conn.cursor() as cursor:
                                sql = "SELECT hotel_group_id,id,CODE,sta,audit,descript,descript_en,descript_short,country,city,address1,address2,phone,fax,phone_rsv,website,remark,create_user,create_datetime,modify_user,modify_datetime,province_code,city_code,district_code,brand_code,score,manage_type,client_type,online_check,server_name FROM hotel "
                                conditions = []
                                if hotelcode:
                                    conditions.append(f"CODE = '{hotelcode}'")
                                if hotelid:
                                    conditions.append(f"ID = {hotelid}")
                                if desc:
                                    conditions.append(f"descript LIKE '%{desc}%'")
                                if conditions:
                                    sql += " WHERE " + " AND ".join(conditions)
                                sql += " ORDER BY id"
                                if hotellimit:
                                    sql += f" LIMIT {hotellimit};"
                                logger.info(f"执行的sql: {sql}")
                                print(sql)
                                start_time = datetime.datetime.now()
                                cursor.execute(sql)
                                elapsed_time = datetime.datetime.now() - start_time
                                formatted_time = elapsed_time.total_seconds()
                                print("查询了吗")
                                results = cursor.fetchall()
                                column = [desc[0] for desc in cursor.description]
                                self.dorp_tablenew(column, results, "快捷查询")
                        except Exception as e:
                            popup_manager.message_signal.emit(f"错误内容: {str(e)}")
                            logger.error(f"发生异常: {e}")
                        finally:
                            # 关闭数据库连接
                            conn.close()

                except  Exception as e:
                    QMessageBox.critical(None, "连接错误", f"无法连接到数据库：{e}")
                    logger.error(f"无法连接到数据库：{e}")
                    return

    def get_regs(self):
        hotelcode = self.CODE.text()
        hotelid = self.id.text()
        server_name = self.serverComboBox.currentText().strip()
        self.hotel_resg.setText("生成中...")

        config = parse_config(server_name)
        for section in config.sections():
            if section == 'group':
                try:
                    server = config[section]
                    # 确保连接使用UTF8编码读取数据
                    conn = connect_to_server(server)  # 关键修改1
                    if conn is not None:
                        try:
                            with conn.cursor() as cursor:
                                # 修改SQL移除编码转换
                                sql = (
                                    "SELECT a.code, a.descript, "  # 分开获取字段
                                    "LEFT(b.server_ip, LENGTH(b.server_ip)-LENGTH(SUBSTRING_INDEX(b.server_ip,'/',-1))) AS ip_part "
                                    "FROM hotel a "
                                    "JOIN (SELECT server_name,MAX(server_ip) AS server_ip "
                                    "FROM sync_ip WHERE server_type = 'thef' AND is_local = 'T' GROUP BY server_name) b "
                                    "ON a.server_name = b.server_name")

                                conditions = []
                                params = {}  # 改用参数化查询
                                if hotelcode:
                                    conditions.append("a.code = %(code)s")
                                    params['code'] = hotelcode
                                if hotelid:
                                    conditions.append("a.id = %(id)s")
                                    params['id'] = hotelid

                                if conditions:
                                    sql += " WHERE " + " AND ".join(conditions)

                                cursor.execute(sql, params)
                                result = cursor.fetchall()

                                if result:
                                    # 在Python端处理编码转换
                                    for row in result:
                                        code = row[0]
                                        descript = row[1]
                                        ip_part = row[2]

                                        # 将中文描述转换为GB18030字节
                                        try:
                                            descript_gb = descript.encode('gb18030', errors='replace')  # 关键修改2
                                        except UnicodeEncodeError:
                                            descript_gb = descript.encode('gb18030', errors='ignore')

                                        # 构建字节序列
                                        raw_data = '|'.join([code, descript, '|||', ip_part])
                                        print(raw_data)
                                        raw_bytes = code.encode('utf-8') + b'|' + descript_gb + b'|||' + ip_part.encode(
                                            'utf-8')

                                        # 生成Base64
                                        base64_str = base64.b64encode(raw_bytes).decode('utf-8')
                                        regs = base64_str.replace('\n', '')  # 移除换行符

                                        # 剪贴板操作
                                        if not QApplication.instance():
                                            app = QApplication([])
                                        clipboard = QApplication.clipboard()
                                        clipboard.setText(regs)

                                        popup_manager.message_info.emit("注册码生成成功！已复制到剪贴板。")
                                        break
                                else:
                                    popup_manager.message_info.emit("没有找到符合条件的记录。")
                        except Exception as e:
                            error_msg = f"注册码生成错误: {str(e)}"
                            print(error_msg)
                            popup_manager.message_signal.emit(error_msg)
                            logger.error(error_msg)
                        finally:
                            conn.close()
                            self.hotel_resg.setText("生成注册码")
                except Exception as e:
                    QMessageBox.critical(None, "连接错误", f"无法连接到数据库：{e}")
                    logger.error(f"连接错误: {e}")
                    return

    def select_excel(self):
        try:
            self.sheet_dropdown.clear()
            self.file_path, _ = QFileDialog.getOpenFileName(self.centralwidget, "选择Excel文件", "",
                                                            "Excel files (*.xlsx *.xls)")
            print(f"cd:/{self.file_path}")
            if self.file_path == "":
                self.select_button.setText("Excel文件")
                self.select_button.setToolTip("")

            if self.file_path != "":
                self.select_button.setText("已选择")
                self.select_button.setToolTip(self.file_path)

            if self.file_path:
                xl = pd.ExcelFile(self.file_path)
                sheet_names = xl.sheet_names
                self.sheet_dropdown.addItems(sheet_names)
        except Exception as e:
            print(f"Error occurred: {str(e)}")

    def export_to_excel(self):
        try:
            current_index = self.tab_widget.currentIndex()
            print(current_index)
            # 获取当前标签页中的表格
            current_tab = self.tab_widget.widget(current_index)
            model = current_tab.model()  # 假设每个标签页都有一个模型
            if model is None or model.columnCount() == 0:
                QMessageBox.information(self, "！", "无数据")
                return
            file_dialog = QFileDialog()
            file_dialog.setDefaultSuffix("xlsx")
            file_path, _ = file_dialog.getSaveFileName(None, "保存文件", "", "Excel Files (*.xlsx)")
            if not file_path:
                return
            try:
                headers = [model.headerData(column, Qt.Horizontal) for column in range(model.columnCount())]
                data = []
                for row in range(model.rowCount()):
                    rowData = []
                    for column in range(model.columnCount()):
                        index = model.index(row, column)
                        rowData.append(model.data(index))
                    data.append(rowData)
            except Exception as e:
                print(e)
            # 如果有选中行，则只导出选中行的数据
            selection_model = current_tab.selectionModel()
            selected_indexes = selection_model.selectedRows()
            selected_rows = [index.row() for index in selected_indexes] if selected_indexes else []
            if selected_rows:
                data = [data[row] for row in selected_rows]
            export_thread = ExportThread(data, headers, file_path)
            print(datetime.datetime.now(), "------------开始导出-----------------")
            logger.info(f"{datetime.datetime.now()} ------------开始导出-----------------")
            export_thread.start()
        except Exception as e:
            print(f"Error occurred while exporting to Excel: {str(e)}")

    def export_to_csv(self):
        try:
            current_index = self.tab_widget.currentIndex()
            print(current_index)
            # 获取当前标签页中的表格
            current_tab = self.tab_widget.widget(current_index)
            model = current_tab.model()  # 假设每个标签页都有一个模型
            if model is None or model.columnCount() == 0:
                QMessageBox.information(self, "！", "无数据")
                return

            # 打开文件保存对话框，设置默认文件扩展名为 .csv
            file_dialog = QFileDialog()
            file_dialog.setDefaultSuffix("csv")
            file_path, _ = file_dialog.getSaveFileName(None, "保存文件", "", "CSV Files (*.csv)")
            if not file_path:
                return

            try:
                # 获取表头数据
                headers = [model.headerData(column, Qt.Horizontal) for column in range(model.columnCount())]
                data = []
                for row in range(model.rowCount()):
                    rowData = []
                    for column in range(model.columnCount()):
                        index = model.index(row, column)
                        rowData.append(model.data(index))
                    data.append(rowData)
            except Exception as e:
                print(e)

            # 如果有选中行，则只导出选中行的数据
            selection_model = current_tab.selectionModel()
            selected_indexes = selection_model.selectedRows()
            selected_rows = [index.row() for index in selected_indexes] if selected_indexes else []
            if selected_rows:
                data = [data[row] for row in selected_rows]
            export_thread = ExportThreadCsv(data, headers, file_path)
            print(datetime.datetime.now(), "------------开始导出-----------------")
            logger.info(f"{datetime.datetime.now()} ------------开始导出-----------------")
            export_thread.start()
        except Exception as e:
            print(f"Error occurred while exporting to CSV: {str(e)}")


    def on_export_finished(self):
        QMessageBox.information(self, "完成", "导出完成!")

    def on_result_ready(self,result_df, sql_query):
        # 处理DataFrame并更新UI
        cursor = pd.DataFrame(result_df.columns).reset_index().values.tolist()
        description = [column[1] for column in cursor]
        data = result_df.values.tolist()
        data_tuple = tuple(map(tuple, data))
        # 处理 sql_query
        sql_query = sql_query.replace('\n', ' ').replace('\r', ' ')
        if len(sql_query) > 20:
            sql_query_display = sql_query[:20] + '...'
        else:
            sql_query_display = sql_query
        tab_name = f"{sql_query_display}"
        # self.dorp_table(self)
        self.dorp_tablenew(description, data_tuple, tab_name)
        Process_df.drop(Process_df.index, inplace=True)
        self.executeButton2.setEnabled(True)
        self.executeButton3.setEnabled(False)
        self.serverComboBox.setEnabled(True)
        self.finishExecution(self)

    def stop_click(self, result_df):
        # 处理DataFrame并更新UI
        cursor = pd.DataFrame(result_df.columns).reset_index().values.tolist()
        description = [column[1] for column in cursor]
        data = result_df.values.tolist()
        data_tuple = tuple(map(tuple, data))
        tab_count = self.tab_widget.count()
        tab_name = f"停止结果 {tab_count + 1}"
        Process_df.drop(Process_df.index, inplace=True)
        self.dorp_tablenew(description, data_tuple, tab_name)
        self.executeButton2.setEnabled(True)
        self.serverComboBox.setEnabled(True)
        self.finishExecution(self)

    def select_file(self):
        try:
            self.folder_path = QtWidgets.QFileDialog.getExistingDirectory(self.centralwidget, "选择文件夹")
            print(self.folder_path)
            self.sql_file.setText("已选择")
            self.sql_file.setToolTip(self.folder_path)
        except Exception as e:
            logger.error(f"Error occurred: {str(e)}")
            popup_manager.message_signal.emit(f"Error occurred: {str(e)}")
            print(f"Error occurred: {str(e)}")

    def execute_sql_scripts(self):
        if not hasattr(self, 'folder_path') or not self.folder_path:
            print("请先选择文件夹")
            popup_manager.message_signal.emit("请先选择文件夹")
            return

        result_queue = queue.Queue()  # 创建一个队列来存储每个线程的执行结果
        self.serverComboBox.setEnabled(False)
        try:
            server_name = self.serverComboBox.currentText().strip()
            config = parse_config(server_name)

            def process_server_group(server, section, result_queue):
                connection = connect_to_server(server)
                for filename in os.listdir(self.folder_path):
                    if filename.endswith(".sql"):
                        file_path = os.path.join(self.folder_path, filename)

                        with open(file_path, 'r' ,encoding= 'utf-8' ,errors='ignore' ) as file:
                            concat = file.read()
                            decoded_content = concat.replace('\xa0', ' ')
                            try:
                                sql_query, db_name = clean_sql(decoded_content)
                                print(sql_query)
                                statements = split_statements(sql_query)
                                for sql_segment in statements:
                                    # print(f"SQL execute_sql_scripts 语句: {sql_segment}\n")
                                    if re.search(r"CREATE\s+(?:FUNCTION|PROCEDURE|VIEW|EVENT|TRIGGER)", sql_segment):
                                        # 提取
                                        create_pattern = r"CREATE\s+(?:/[*!].*?[*]/\s*)?(?:DEFINER\s*=\s*`[^`]+`@`[^`]+`\s*)?(TRIGGER|PROCEDURE|FUNCTION|VIEW|EVENT)\s+`([a-zA-Z0-9_]+)`"
                                        match = re.search(create_pattern, sql_segment)
                                        if match:
                                            _type = match.group(1)  # 类型
                                            _name = match.group(2)  # 名称

                                            result = execute_sql(connection, sql_segment, section, _type, db_name, _name,
                                                             None)

                                        if result is not None:
                                            result_queue.put((sql_segment, result))
                                    else:
                                        for statement in sqlparse.parse(sql_segment):
                                            sql_str = str(statement)
                                            logger.info(f"执行SQL语句：{sql_str}")
                                            result = execute_sql(connection, sql_str, section, None, db_name, None,None)
                                            if result is not None:
                                                result_queue.put((sql_str, result))
                            except Exception as e:
                                logger.error(f"未完成执行的错误?：=>>  {server}-{e}")
                                popup_manager.message_signal.emit(f"未完成执行的错误?：=>>  {server}-{e}")

            threads = []

            for section in config.sections():
                if section == 'group' and not self.execute_group.isChecked():
                    continue
                if section == 'member' and not self.execute_member.isChecked():
                    continue
                server = config[section]
                thread = threading.Thread(target=process_server_group, args=(server, section, result_queue),
                                          name=f"{section}-Thread")
                thread.start()
                threads.append((thread, server, section))  # 记录线程，服务器和节的元组

            # 等待所有线程完成

            for thread, server, section in threads:
                thread.join()

                logger.info(f"{thread.name} 已执行!")
                print(f"{thread.name} 已执行！！")

            # 在界面上显示结果
            result_df = pd.DataFrame()
            while not result_queue.empty():
                sql_str, result = result_queue.get()
                if result_df.empty:
                    result_df = result
                else:
                    result_df = result_df.append(result, ignore_index=True)

            cursor_df = pd.DataFrame(result_df.columns).reset_index().values.tolist()
            description = [(column[1]) for column in cursor_df]
            data = result_df.values.tolist()

            data_tuple = tuple(map(tuple, data))
            self.dorp_tablenew(description, data_tuple, "脚本结果")
            self.serverComboBox.setEnabled(True)

        except Exception as e:
            logger.error(f"Error occurred: {str(e)}")
            popup_manager.message_signal.emit(f"Error occurred: {str(e)}")

    def import_data(self):
        cursor_pms = None
        try:
            selected_sheet = self.sheet_dropdown.currentText()
            data = pd.read_excel(self.file_path, sheet_name=selected_sheet, dtype=str)
            server_name = self.serverComboBox.currentText().strip()
            config = parse_config(server_name)
            self.database_name = self.dbname.text()
            self.table_name = self.crate_table.text()
            if self.table_name == '':
                self.table_name = selected_sheet

            for section in config.sections():
                if section == 'group' and not self.execute_group.isChecked():
                    continue
                if section == 'member' and not self.execute_member.isChecked():
                    continue
                server = config[section]
                connection = connect_to_server(server)
                cursor_pms = connection.cursor()
                self.process_data(cursor_pms, data, self.database_name, self.table_name, section)
                connection.commit()
                connection.close()

            QMessageBox.information(None, "成功", f"已成功导入到 {self.database_name} 库，表名：{self.table_name}")
        except Exception as e:
            print(f"导入数据时出现异常: {e}")
            logger.error(f"导入数据异常: {e}")
            QMessageBox.information(None, "错误", f"导入数据异常: {e}")
        finally:
            cursor_pms.close()

    def process_data(self, cursor_pms, data, database_name, table_name, section):
        create_table_query = f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} ("
        for column in data.columns:
            create_table_query += f"`{column}` VARCHAR(255), "
        create_table_query = create_table_query[:-2] + ");"
        cursor_pms.execute(create_table_query)

        data = data.where(pd.notnull(data), None)

        insert_query = f"INSERT INTO {database_name}.{table_name} ({', '.join(data.columns)}) VALUES ({', '.join(['%s' for _ in range(len(data.columns))])})"
        values = [tuple(row) for row in data.values]

        cursor_pms.executemany(insert_query, values)

    def dorp_tablenew(self, column, results, tablename):
        """创建或更新表格"""
        resultTable = QtWidgets.QTableView(self.centralwidget)
        resultTable.setUpdatesEnabled(True)
        resultTable.setSortingEnabled(True)
        resultTable.setStyleSheet("""
            QTableView {
                selection-background-color: #3a3a3a;
                selection-color: #ffffff;
                border: 1px solid #444444;
                border-radius: 2px;
            }
            QTableView::item {
                height: 30px;
            }
        """)
        resultTable.setMinimumHeight(150)
        tab_name = f"{tablename}"

        model = LargeTableModel(results, column)
        proxy_model = QtCore.QSortFilterProxyModel()
        proxy_model.setSourceModel(model)
        resultTable.setModel(proxy_model)

        self.tab_widget.addTab(resultTable, tab_name)
        self.tab_table_map[tablename] = resultTable
        resultTable.resizeColumnsToContents()
        resultTable.resizeRowsToContents()

        last_index = self.tab_widget.count() - 1
        self.tab_widget.setCurrentIndex(last_index)
        self.update_status_bar()
    def update_status_bar(self):
        """更新状态栏显示的行数"""
        current_table = self.tab_widget.currentWidget()
        if current_table:
            model = current_table.model()
            row_count = model.rowCount()
            self.statusbar.showMessage(f"行数: {row_count}")
            
class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    def __init__(self):
        super().__init__()
        self.setupUi(self)

    def closeEvent(self, event):
        # 创建确认退出的对话框
        reply = QtWidgets.QMessageBox.question(
            self,
            '确认退出',
            '您确定要退出吗？',
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No
        )

        if reply == QtWidgets.QMessageBox.Yes:
            event.accept()
        else:
            event.ignore()