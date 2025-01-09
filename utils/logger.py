# utils/logger.py
import logging
import os
import datetime

class DailyRotatingFileHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=False):
        # 初始化日志目录
        self.logs_dir = os.path.dirname(filename)
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)

        # 初始化当前日期
        self.current_date = datetime.datetime.now().strftime("%Y-%m-%d")

        # 生成完整的日志文件路径
        self.filename_template = os.path.join(self.logs_dir, "{date}.log")
        self.filename = self.filename_template.format(date=self.current_date)

        # 调用父类构造函数
        super().__init__(self.filename, mode, encoding, delay)

    def emit(self, record):
        # 检查当前日期
        new_date = datetime.datetime.now().strftime("%Y-%m-%d")
        if new_date != self.current_date:
            # 日期变化，关闭当前文件并创建新文件
            self.current_date = new_date
            self.filename = self.filename_template.format(date=self.current_date)
            self.close()
            self.stream = self._open()

        # 调用父类的 emit 方法写入日志
        super().emit(record)

# 初始化日志记录器
def setup_logger():
    logger = logging.getLogger("DBtool")
    if not logger.handlers:  # 避免重复添加处理器
        logger.setLevel(logging.INFO)

        # 使用自定义的 DailyRotatingFileHandler
        handler = DailyRotatingFileHandler('logs/app.log')  # 指定具体的文件路径
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # 将处理程序添加到日志记录器
        logger.addHandler(handler)
    return logger

# 初始化并导出 logger 对象
logger = setup_logger()