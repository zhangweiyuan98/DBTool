import re

from PyQt5.QtWidgets import QLineEdit, QHBoxLayout, QLabel, QPushButton, QCheckBox, QFormLayout, QDialog, QMessageBox

class ServerDialog(QDialog):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("添加服务器")

        # 创建表单布局
        layout = QFormLayout()

        # 添加输入字段
        self.service_group_name = QLineEdit()
        self.host = QLineEdit()
        self.user = QLineEdit()
        self.password = QLineEdit()
        self.port = QLineEdit()
        self.database = QLineEdit()
        self.ssh_checkbox = QCheckBox("SSH")

        # SSH相关字段
        self.ssh_host = QLineEdit()
        self.ssh_user = QLineEdit()
        self.ssh_password = QLineEdit()  # 确保这是一个新的QLineEdit实例
        self.ssh_port = QLineEdit()

        # 密码可见性按钮
        self.password_visible_button = QPushButton("显示")
        self.password_visible_button.setCheckable(True)
        self.password_visible_button.toggled.connect(self.toggle_password_visibility)

        self.ssh_password_visible_button = QPushButton("显示")
        self.ssh_password_visible_button.setCheckable(True)
        self.ssh_password_visible_button.toggled.connect(self.toggle_ssh_password_visibility)

        # 把SSH字段隐藏，初始状态
        self.toggle_ssh_fields(False)  # 初始状态为禁用

        # 连接复选框状态变化信号
        self.ssh_checkbox.toggled.connect(self.toggle_ssh_fields)

        # 添加到布局
        layout.addRow(QLabel("服务组名称:"), self.service_group_name)
        layout.addRow(QLabel("主机:"), self.host)
        layout.addRow(QLabel("用户:"), self.user)

        layout.addRow(QLabel("密码:"), self.create_password_layout())  # 使用创建密码布局的函数
        layout.addRow(QLabel("端口:"), self.port)
        layout.addRow(QLabel("数据库:"), self.database)
        layout.addRow(self.ssh_checkbox)
        layout.addRow(QLabel("SSH主机:"), self.ssh_host)
        layout.addRow(QLabel("SSH用户:"), self.ssh_user)
        layout.addRow(QLabel("SSH密码:"), self.create_ssh_password_layout())  # 使用创建SSH密码布局的函数
        layout.addRow(QLabel("SSH端口:"), self.ssh_port)

        # 添加确认按钮
        self.ok_button = QPushButton("确定")
        self.ok_button.clicked.connect(self.handle_ok_click)
        layout.addWidget(self.ok_button)

        self.setLayout(layout)

    def create_password_layout(self):
        """创建密码输入框和按钮的布局"""
        layout = QHBoxLayout()
        self.password.setEchoMode(QLineEdit.Password)
        layout.addWidget(self.password)
        layout.addWidget(self.password_visible_button)
        return layout

    def create_ssh_password_layout(self):
        """创建SSH密码输入框和按钮的布局"""
        layout = QHBoxLayout()
        self.password.setEchoMode(QLineEdit.Password)
        layout.addWidget(self.ssh_password)
        layout.addWidget(self.ssh_password_visible_button)
        return layout

    def toggle_ssh_fields(self, checked):
        """Toggle the SSH input fields based on checkbox state."""
        state = not checked
        self.ssh_host.setDisabled(state)
        self.ssh_user.setDisabled(state)
        self.ssh_password.setDisabled(state)
        self.ssh_port.setDisabled(state)

    def toggle_password_visibility(self, checked):
        """切换主密码可见性"""
        if checked:
            self.password.setEchoMode(QLineEdit.Normal)
            self.password_visible_button.setText("隐藏")
        else:
            self.password.setEchoMode(QLineEdit.Password)
            self.password_visible_button.setText("显示")

    def toggle_ssh_password_visibility(self, checked):
        """切换SSH密码可见性"""
        if checked:
            self.ssh_password.setEchoMode(QLineEdit.Normal)
            self.ssh_password_visible_button.setText("隐藏")
        else:
            self.ssh_password.setEchoMode(QLineEdit.Password)
            self.ssh_password_visible_button.setText("显示")

    def handle_ok_click(self):
        """处理确定按钮点击事件"""
        if self.validate_inputs():
            self.accept()  # 输入有效，接受对话框

    def validate_inputs(self):
        """验证主机和端口的输入"""
        host_text = self.host.text().strip()
        port_text = self.port.text().strip()

        # 验证主机地址（IP 格式或域名）
        ip_pattern = r'^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
        domain_pattern = r'^[a-zA-Z0-9][-a-zA-Z0-9.]*[a-zA-Z0-9]$'  # 更严格的域名正则

        if not re.match(ip_pattern, host_text) and not re.match(domain_pattern, host_text):
            QMessageBox.warning(self, "输入错误", "请输入有效的主机地址（IP 或 域名）。")
            return False

        # 验证端口号
        if not port_text.isdigit():
            QMessageBox.warning(self, "输入错误", "端口号必须是数字。")
            return False

        port_number = int(port_text)
        if not (1 <= port_number <= 65535):
            QMessageBox.warning(self, "输入错误", "请输入有效的端口号（1-65535）。")
            return False

        return True