from gui.login import LoginWindow

if __name__ == "__main__":
    import sys
    from PyQt5 import QtWidgets
    app = QtWidgets.QApplication(sys.argv)
    login_window = LoginWindow()
    login_window.show()
    ret = app.exec_()
    for window in app.topLevelWindows():
        window.close()
    sys.exit(ret)