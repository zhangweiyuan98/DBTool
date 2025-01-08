from PyQt5 import QtCore, QtGui
from PyQt5 import QtWidgets

from gui.MainWindow import MainWindow


class LoginWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('登录界面')

        # 设置窗口大小和初始位置
        self.setGeometry(500, 300, 400, 300)
        self.setStyleSheet("background-color: #f5f5f5;")  # 设置背景色

        # 使用 QVBoxLayout 来更好的控制布局
        layout = QtWidgets.QVBoxLayout(self)

        # 创建一个标题标签
        self.title_label = QtWidgets.QLabel('欢迎登录', self)
        self.title_label.setAlignment(QtCore.Qt.AlignCenter)
        self.title_label.setFont(QtGui.QFont('Arial', 20, QtGui.QFont.Bold))
        layout.addWidget(self.title_label)

        layout.addSpacing(30)  # 加一些间距，使布局更舒服

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
                font-size: 14px;
            }
        """)
        layout.addWidget(self.username_input)

        layout.addSpacing(20)  # 再加些间距

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
                font-size: 14px;
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
                font-size: 16px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
        """)
        layout.addWidget(self.login_button)

        layout.addStretch(1)  # 让布局保持一定的对齐

        # 按钮点击事件
        self.login_button.clicked.connect(self.handle_login)

        # 支持通过回车键登录
        self.username_input.returnPressed.connect(self.handle_login)
        self.password_input.returnPressed.connect(self.handle_login)

    def handle_login(self):
        username = self.username_input.text()
        password = self.password_input.text()

        if username == 'admin' and password == 'password':
            self.accept_login()
        else:
            QtWidgets.QMessageBox.warning(self, '登录失败', '用户名或密码错误')

    def accept_login(self):
        # 登录成功，关闭当前窗口，打开主窗口
        self.close()
        self.main_window = MainWindow()
        self.main_window.show()