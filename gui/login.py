from PyQt5 import QtCore, QtGui
from PyQt5 import QtWidgets
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QDesktopWidget

from gui.MainWindow import MainWindow


class LoginWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('来了老弟')
        self.setWindowIcon(QIcon("resources\icon.ico"))
        # 设置窗口大小和初始位置
        self.setGeometry(0, 0, 600, 200)

        screen = QDesktopWidget().screenGeometry()
        window_width = self.width()
        window_height = self.height()

        x = (screen.width() - window_width) // 2
        y = (screen.height() - window_height) // 2

        # 移动窗口到屏幕中央
        self.move(x,y)

        self.setStyleSheet("background-color: #f5f5f5;")  # 设置背景色

        # 使用 QVBoxLayout 来更好的控制布局
        layout = QtWidgets.QVBoxLayout(self)

        # 创建一个标题标签
        self.title_label = QtWidgets.QLabel('最牛就是你了', self)
        self.title_label.setAlignment(QtCore.Qt.AlignCenter)
        self.title_label.setFont(QtGui.QFont('Arial', 20, QtGui.QFont.Bold))
        layout.addWidget(self.title_label)

        layout.addSpacing(20)

        # 用户名
        self.label_username = QtWidgets.QLabel('用户名:', self)
        self.label_username.setFont(QtGui.QFont('Arial', 12))
        layout.addWidget(self.label_username)

        self.username_input = QtWidgets.QLineEdit(self)
        self.username_input.setPlaceholderText('请输入用户名')
        self.username_input.setStyleSheet("""
            QLineEdit {
                background-color: white;
                border: 1px solid #ccc;
                border-radius: 10px;
                padding: 10px;
                font-size: 12px;
            }
        """)
        layout.addWidget(self.username_input)

        layout.addSpacing(10)

        # 密码
        self.label_password = QtWidgets.QLabel('密码:', self)
        self.label_password.setFont(QtGui.QFont('Arial', 12))
        layout.addWidget(self.label_password)

        self.password_input = QtWidgets.QLineEdit(self)
        self.password_input.setPlaceholderText('请输入密码')
        self.password_input.setEchoMode(QtWidgets.QLineEdit.Password)  # 密码框隐藏输入内容
        self.password_input.setStyleSheet("""
            QLineEdit {
                background-color: white;
                border: 1px solid #ccc;
                border-radius: 10px;
                padding: 10px;
                font-size: 12px;
            }
        """)
        layout.addWidget(self.password_input)

        layout.addSpacing(30)  # 再加些间距

        # 登录按钮
        self.login_button = QtWidgets.QPushButton('登录', self)
        self.login_button.setStyleSheet("""
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 12px 20px;
                text-align: center;
                font-size: 20px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        layout.addWidget(self.login_button)

        layout.addStretch(1)  # 让布局保持一定的对齐

        # 登录次数限制
        self.max_attempts = 3  # 最大尝试次数
        self.attempts = 0      # 当前尝试次数
        
        # 登录按钮点击事件
        self.login_button.clicked.connect(self.handle_login)
        
        # 支持通过回车键登录
        self.username_input.returnPressed.connect(self.handle_login)
        self.password_input.returnPressed.connect(self.handle_login)

    def handle_login(self):
        """处理登录逻辑"""
        # 检查尝试次数
        if self.attempts >= self.max_attempts:
            QtWidgets.QMessageBox.critical(
                self,
                '登录失败',
                '登录次数过多，请稍后再试'
            )
            return
        
        username = self.username_input.text()
        password = self.password_input.text()

        if username == 'admin' and password == '1':
            self.accept_login()
        else:
            self.attempts += 1
            remaining_attempts = self.max_attempts - self.attempts
            if remaining_attempts > 0:
                QtWidgets.QMessageBox.warning(
                    self,
                    '登录失败',
                    f'用户名或密码错误，剩余尝试次数：{remaining_attempts}'
                )
            else:
                QtWidgets.QMessageBox.critical(
                    self,
                    '登录失败',
                    '登录次数已用完，请稍后再试'
                )
                self.login_button.setEnabled(False)  # 禁用登录按钮
                self.username_input.setEnabled(False)  # 禁用用户名输入
                self.password_input.setEnabled(False)  # 禁用密码输入

    def accept_login(self):
        """登录成功处理"""
        # 重置尝试次数
        self.attempts = 0
        # 登录成功，关闭当前窗口，打开主窗口
        self.close()
        self.main_window = MainWindow()
        self.main_window.show()