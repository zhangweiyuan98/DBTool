
from xml.dom.minidom import parseString
import xml.parsers.expat
from PyQt5.QtGui import QIcon, QPainter, QFont, QColor, QTextCharFormat, QSyntaxHighlighter
from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout,
                             QLabel, QLineEdit, QComboBox, QPushButton, QTableWidget,
                             QTableWidgetItem, QDialog, QMessageBox, QDateTimeEdit, QPlainTextEdit
                             )
from PyQt5.QtCore import Qt, QDateTime, QSize, QRect, QRegExp
from utils.DBconnectServer import connect_to_server
from utils.parseconfig import parse_config

class wehotel_log_info(QDialog):
    def __init__(self,server_name):
        super().__init__()
        self.server_name = server_name  # 根据实际配置名称修改
        self.setWindowTitle("wehotel接口信息")
        self.setWindowIcon(QIcon("resources\icon.ico"))
        self.setGeometry(100, 100, 900, 600)
        self.init_ui()

    def init_ui(self):
        # 主布局
        layout = QVBoxLayout(self)

        # 输入区域
        input_layout = QHBoxLayout()

        # 酒店编码
        lbl_code = QLabel("酒店编码:")
        self.txt_code = QLineEdit()
        self.txt_code.setFixedWidth(100)
        input_layout.addWidget(lbl_code)
        input_layout.addWidget(self.txt_code)

        # 时间范围
        lbl_start = QLabel("开始时间:")
        self.dt_start = QDateTimeEdit()
        self.dt_start.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.dt_start.setCalendarPopup(True)  # 启用日历弹窗
        self.dt_start.setDateTime(QDateTime.currentDateTime().addDays(-1))
        input_layout.addWidget(lbl_start)
        input_layout.addWidget(self.dt_start)

        lbl_end = QLabel("结束时间:")
        self.dt_end = QDateTimeEdit()
        self.dt_end.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.dt_end.setCalendarPopup(True)    # 启用日历弹窗
        self.dt_end.setDateTime(QDateTime.currentDateTime())
        input_layout.addWidget(lbl_end)
        input_layout.addWidget(self.dt_end)

        # 中央预定号
        lbl_crs = QLabel("中央预定号:")
        self.txt_crs = QLineEdit()
        self.txt_crs.setFixedWidth(100)
        input_layout.addWidget(lbl_crs)
        input_layout.addWidget(self.txt_crs)

        # 操作类型
        lbl_op = QLabel("操作类型:")
        self.cmb_op = QComboBox()
        self.cmb_op.addItems(["", "UP", "DOWN"])
        input_layout.addWidget(lbl_op)
        input_layout.addWidget(self.cmb_op)

        # 所属模块
        lbl_model = QLabel("所属模块:")
        self.cmb_model = QComboBox()
        self.cmb_model.addItems([
            "", "CRS_RESRV", "MASTER", "PMS_RESRV",
            "RATE_DETAIL", "COMPANY", "ACCOUNT", "RTAV", "RENTAL_RATE"
        ])
        input_layout.addWidget(lbl_model)
        input_layout.addWidget(self.cmb_model)

        # 消息类型
        lbl_msg_type = QLabel("消息类型:")
        self.cmb_msg_type = QComboBox()
        self.cmb_msg_type.addItems([
            "", "crscxlresv", "crsmodresv", "crsnewresv",
            "getfeedetail", "partyresv", "sendalert", "updaterates",
            "updatesalesid", "getagreementinfo", "getunclosedauditstatus",
            "pmsresv", "resvrefund", "updateauditstatus", "updateavail",
            "updatefeedetail", "updateoccupancyrate"
        ])
        input_layout.addWidget(lbl_msg_type)
        input_layout.addWidget(self.cmb_msg_type)

        # 查询按钮
        btn_query = QPushButton("查询")
        btn_query.clicked.connect(self.execute_query)
        input_layout.addWidget(btn_query)

        layout.addLayout(input_layout)

        # 结果表格
        self.table = QTableWidget()
        self.table.setColumnCount(9)
        self.table.setHorizontalHeaderLabels([
            "ID", "所属模块", "消息类型", "状态",
            "CRS订单号", "XML报文", "结果XML",
            "发送时间", "完成时间"
        ])

        # 修改这里：逐个设置列宽
        column_widths = [100, 100, 120, 80, 120, 200, 200, 150, 150]
        for idx, width in enumerate(column_widths):
            self.table.setColumnWidth(idx, width)  # 正确的设置方式

        self.table.doubleClicked.connect(self.show_xml_detail)
        layout.addWidget(self.table)

    def build_query(self, hotel_group_id, hotel_id):
        """构建 SQL 查询语句"""
        base_sql = """
            SELECT id,message_type,message_no,status,crs_no,
                   request_xml,result_xml,send_date,done_date
            FROM hubs1_interface_log 
            WHERE hotel_group_id = %s AND hotel_id = %s
        """
        params = [hotel_group_id, hotel_id]
        conditions = []

        # 时间范围
        start_time = self.dt_start.dateTime().toString("yyyy-MM-dd HH:mm:ss")
        end_time = self.dt_end.dateTime().toString("yyyy-MM-dd HH:mm:ss")
        if start_time and end_time:
            conditions.append("send_date BETWEEN %s AND %s")
            params.extend([start_time, end_time])

        # 中央预定号
        crs_no = self.txt_crs.text().strip()

        if crs_no != '':
            conditions.append("crs_no = %s")
            params.append(crs_no)

        # 操作类型
        op_type = self.cmb_op.currentText()
        if op_type != '':
            conditions.append("log_type = %s")
            params.append(op_type)

        # 所属模块
        model = self.cmb_model.currentText()
        if model != '':
            conditions.append("model = %s")
            params.append(model)

        # 消息类型
        msg_type = self.cmb_msg_type.currentText()
        if msg_type != '':
            conditions.append("message_type = %s")
            params.append(msg_type)

        if conditions:  # 直接判断列表是否非空
            base_sql += " AND " + " AND ".join(conditions)

        return base_sql, params

    def execute_query(self):
        """执行数据库查询"""
        try:
            # 获取酒店ID
            code = self.txt_code.text().strip()
            if not code:
                QMessageBox.warning(self, "警告", "请输入酒店编码")
                return
            # 连接数据库查询酒店信息
            config = parse_config(self.server_name)
            for section in config.sections():
                if section == 'group' :
                    server = config[section]
                    connection = connect_to_server(server)
                    cursor = connection.cursor()
                    try:
                        cursor.execute(
                            "SELECT hotel_group_id, id FROM hotel WHERE code = %s",
                            (code,)
                        )
                        result = cursor.fetchone()
                        if not result:
                            QMessageBox.warning(self, "警告", "无效的酒店编码")
                            return
                        hotel_group_id, hotel_id = result
                        # 执行主查询
                        sql, params = self.build_query(hotel_group_id, hotel_id)
                        cursor.execute(sql, params)
                        results = cursor.fetchall()
                        # 更新表格
                        self.table.setRowCount(len(results))
                        for row_idx, row in enumerate(results):
                            for col_idx, col in enumerate(row):
                                item = QTableWidgetItem(str(col) if col else "")
                                item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsSelectable)
                                self.table.setItem(row_idx, col_idx, item)
                    except Exception as e:
                        QMessageBox.critical(self, "错误", str(e))
                    finally:
                        cursor.close()
        except Exception as e:
            QMessageBox.critical(self, "错误1", str(e))

    def show_xml_detail(self, index):
        """显示 XML """
        dialog = None  # 提前声明变量
        editor = None
        try:
            row = index.row()
            record_id = self.table.item(row, 0).text()

            config = parse_config(self.server_name)
            for section in config.sections():
                if section == 'group':
                    server = config[section]
                    with connect_to_server(server) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                "SELECT request_xml, result_xml FROM hubs1_interface_log WHERE id = %s",
                                (record_id,)
                            )
                            request_xml, result_xml = cursor.fetchone()

                            dialog = QDialog(self)
                            editor = QPlainTextEdit(dialog)
                            # 格式化XML内容
                            def format_xml(xml_str):
                                try:
                                    dom = parseString(xml_str)
                                    raw =  dom.toprettyxml(indent="  ")
                                    return format_xml_content(raw)
                                except xml.parsers.expat.ExpatError:
                                    return xml_str
                                except Exception:
                                    return xml_str

                            formatted_request = format_xml(request_xml)
                            formatted_response = format_xml(result_xml)
                            # 构建内容
                            content = f"/* === Request XML === */\n{formatted_request}\n"
                            content += f"\n/* === Response XML === */\n{formatted_response}"
                            editor.setPlainText(content)

                            # 优化显示设置
                            editor.setLineWrapMode(QPlainTextEdit.NoWrap)  # 禁用自动换行
                            editor.setStyleSheet("""
                                QPlainTextEdit {
                                    background-color: #F5F5F5;
                                    padding: 10px;
                                }
                            """)
                            # 创建带行号的显示窗口
                            dialog = QDialog(self)
                            dialog.setWindowTitle(f"XML详情 - ID: {record_id}")
                            dialog.resize(1000, 800)

                            editor = XmlEditor(dialog)
                            editor.setFont(QFont("Consolas", 10))
                            editor.setPlainText(content)

                            layout = QVBoxLayout(dialog)
                            layout.setContentsMargins(0, 0, 0, 0)
                            layout.addWidget(editor)
                            dialog.setLayout(layout)
                            dialog.exec_()
        except Exception as e:
            print(f"数据显示失败: {str(e)}")
            QMessageBox.critical(self, "错误", f"数据显示失败: {str(e)}")


class XmlEditor(QPlainTextEdit):
    """带行号的简单文本编辑器"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setReadOnly(True)
        self.setFont(QFont("Consolas", 10))
        self.lineNumberArea = LineNumberArea(self)
        self.blockCountChanged.connect(self.updateLineNumberAreaWidth)
        self.updateRequest.connect(self.updateLineNumberArea)
        self.updateLineNumberAreaWidth()

    def lineNumberAreaWidth(self):
        digits = len(str(max(1, self.blockCount())))
        return 20 + self.fontMetrics().width('9') * digits

    def updateLineNumberAreaWidth(self):
        self.setViewportMargins(self.lineNumberAreaWidth(), 0, 0, 0)

    def updateLineNumberArea(self, rect, dy):
        if dy:
            self.lineNumberArea.scroll(0, dy)
        else:
            self.lineNumberArea.update(0, rect.y(),
                                       self.lineNumberArea.width(), rect.height())

    def resizeEvent(self, event):
        super().resizeEvent(event)
        cr = self.contentsRect()
        self.lineNumberArea.setGeometry(
            QRect(cr.left(), cr.top(),
                  self.lineNumberAreaWidth(), cr.height()))

    def paintLineNumbers(self, event):
        painter = QPainter(self.lineNumberArea)
        painter.fillRect(event.rect(), QColor(240, 240, 240))

        block = self.firstVisibleBlock()
        blockNumber = block.blockNumber()
        top = self.blockBoundingGeometry(block).translated(self.contentOffset()).top()
        bottom = top + self.blockBoundingRect(block).height()

        while block.isValid() and top <= event.rect().bottom():
            if block.isVisible() and bottom >= event.rect().top():
                number = str(blockNumber + 1)
                painter.setPen(Qt.darkGray)
                painter.drawText(
                    0, top,
                    self.lineNumberArea.width() - 5,
                    self.fontMetrics().height(),
                    Qt.AlignRight, number)

            block = block.next()
            top = bottom
            bottom = top + self.blockBoundingRect(block).height()
            blockNumber += 1


class LineNumberArea(QWidget):
    """行号显示区域"""
    def __init__(self, editor):
        super().__init__(editor)
        self.editor = editor

    def sizeHint(self):
        return QSize(self.editor.lineNumberAreaWidth(), 0)

    def paintEvent(self, event):
        self.editor.paintLineNumbers(event)


class XmlHighlighter(QSyntaxHighlighter):
    """XML语法高亮器"""

    def __init__(self, parent=None):
        super().__init__(parent)

        # 定义高亮规则
        self.highlightingRules = []

        # 标签格式 <tag>
        tagFormat = QTextCharFormat()
        tagFormat.setForeground(QColor(163, 21, 21))  # 深红色
        tagFormat.setFontWeight(QFont.Bold)
        self.highlightingRules.append((QRegExp("<[^>]*>"), tagFormat))

        # 属性名格式 name=
        attributeFormat = QTextCharFormat()
        attributeFormat.setForeground(QColor(0, 0, 255))  # 蓝色
        self.highlightingRules.append((QRegExp("\\b[A-Za-z0-9_]+(?=\\=)"), attributeFormat))

        # 属性值格式 ="value"
        valueFormat = QTextCharFormat()
        valueFormat.setForeground(QColor(0, 128, 0))  # 绿色
        self.highlightingRules.append((QRegExp("\"[^\"]*\""), valueFormat))

        # 注释格式 <!-- -->
        commentFormat = QTextCharFormat()
        commentFormat.setForeground(QColor(128, 128, 128))  # 灰色
        self.highlightingRules.append((QRegExp("<!--[^>]*-->"), commentFormat))

    def highlightBlock(self, text):
        for pattern, format in self.highlightingRules:
            expression = QRegExp(pattern)
            index = expression.indexIn(text)
            while index >= 0:
                length = expression.matchedLength()
                self.setFormat(index, length, format)
                index = expression.indexIn(text, index + length)


def format_xml_content(xml_str):
    """优化XML格式（压缩多余空行）"""
    lines = []
    prev_empty = False
    for line in xml_str.splitlines():
        stripped = line.strip()
        if not stripped:
            if not prev_empty:
                lines.append("")  # 保留一个空行
                prev_empty = True
        else:
            lines.append(line)
            prev_empty = False
    return "\n".join(lines)
