# -*- coding: utf-8 -*-

from PyQt5.QtCore import QObject, pyqtSignal, pyqtSlot
from PyQt5.QtWidgets import QMessageBox, QInputDialog

class PopupManager(QObject):
    message_signal = pyqtSignal(str)
    message_info  = pyqtSignal(str)
    input_signal = pyqtSignal(str, str, str)

    def __init__(self):
        super().__init__()

        self.message_signal.connect(self.show_message_box)
        self.input_signal.connect(self.show_input_dialog)

        self.message_info.connect(self.show_info)
    @pyqtSlot(str)
    def show_message_box(self, message):
        QMessageBox.critical(None, "错误", message)

    @pyqtSlot(str)
    def show_info(self, message):
        QMessageBox.information(None, "提示！", message)

    @pyqtSlot(str, str, str)
    def show_input_dialog(self, title, label, default_text):
        text, ok = QInputDialog.getText(None, title, label, text=default_text)
        if ok:
            # 处理输入后的操作，这里可以发射信号到其他槽函数处理
            print("输入的内容为:", text)
        else:
            # 取消输入时的操作
            pass





# if __name__ == "__main__":
#     import sys
#     from PyQt5.QtWidgets import QApplication
#
#     app = QApplication(sys.argv)
#
#     # 创建 PopupManager 实例
#     popup_manager = PopupManager()
#
#     # 发射信号以显示弹窗
#     popup_manager.message_signal.emit("这是一个测试消息")
#
#     # 进入事件循环
#     sys.exit(app.exec_())
