from PyQt5 import QtWidgets, QtGui, QtCore
import re

# SQL关键字
keywords = [
    'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CODE',
    'JOIN', 'INNER', 'OUTER', 'LEFT', 'RIGHT', 'ON', 'AS',
    'AND', 'OR', 'NOT', 'NULL', 'ORDER', 'BY', 'GROUP',
    'HAVING', 'LIMIT', 'OFFSET', 'DISTINCT', 'UNION',
    'CREATE', 'ALTER', 'DROP', 'TABLE', 'INDEX', 'VIEW',
    'DATABASE', 'TRUNCATE', 'EXPLAIN', 'DESCRIBE', 'SHOW',
    'USE', 'SET', 'VALUES', 'INTO', 'DEFAULT', 'PRIMARY',
    'KEY', 'FOREIGN', 'REFERENCES', 'UNIQUE', 'CHECK',
    'CASCADE', 'COMMIT', 'ROLLBACK', 'BEGIN', 'TRANSACTION',
    'ADD', 'COLUMN', 'CHANGE', 'CALL', 'SQL', 'SECURITY', 'INVOKER',
    'PROCEDURE', 'IF', 'EXISTS', 'DEFINER', 'WHILE', 'END', 'BIGINT', 'INT', 'VARCHAR',
    'CHAR', 'TEXT', 'BLOB', 'FLOAT', 'DOUBLE', 'DECIMAL', 'BOOLEAN',
    'TINYINT', 'SMALLINT', 'MEDIUMINT', 'ENUM', 'SET', 'YEAR', 'TIME', 'TIMESTAMP',
    'AUTO_INCREMENT', 'NOT NULL', 'DEFAULT', 'AFTER', 'BEFORE', 'CURRENT_TIMESTAMP',
    'CONSTRAINT', 'FULLTEXT', 'ON DELETE', 'ON UPDATE',
    'ASC', 'DESC', 'WITH', 'REPLACE', 'RENAME', 'PARTITION', 'TEMPORARY', 'SAVEPOINT', 'ROLLBACK TO',
    'LOCK', 'UNLOCK', 'WAIT', 'NOWAIT', 'START', 'END', 'DELIMITER', 'WITH', 'RECURSIVE',
    'DECLARATIVE', 'PERSIST', 'ISNULL', 'IS NOT NULL', 'INTERSECT', 'EXCEPT',
    'LEAVE', 'ITERATE', 'CONTINUE', 'CONVERT', 'CAST', 'SUBSTRING', 'EXTRACT',
    'POSITION', 'COALESCE', 'IFNULL', 'NULLIF', 'DATE_ADD', 'DATE_SUB', 'DATE_FORMAT',
    'GROUP_CONCAT', 'UUID', 'INET_ATON', 'INET_NTOA', 'AES_ENCRYPT', 'AES_DECRYPT', 'UUID_SHORT',
    'SUBDATE', 'DATEDIFF', 'TIMESTAMPDIFF', 'DAYOFWEEK', 'DAYOFYEAR', 'WEEKDAY', 'LAST_INSERT_ID',
    'FETCH', 'ROW_COUNT', 'FOUND_ROWS', 'SIGN', 'ROUND', 'CEIL', 'FLOOR', 'PI', 'RAND',
    'GREATEST', 'LEAST', 'CONCAT', 'CONCAT_WS', 'TRIM', 'LENGTH', 'CHAR_LENGTH', 'REPLACE',
    'INSTR', 'REGEXP', 'REGEXP_REPLACE', 'RLIKE', 'MATCH', 'AGAINST', 'STRCMP', 'SOUNDEX',
    'CRC32', 'SHA1', 'SHA2', 'MD5', 'PASSWORD', 'ENCODE', 'DECODE', 'ELT', 'FIELD', 'ASCII', 'CHAR',
    'ORD', 'CHARACTER_LENGTH', 'DATE', 'TIME', 'NOW', 'CURDATE', 'CURTIME', 'UTC_DATE', 'UTC_TIME',
    'UTC_TIMESTAMP', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'MICROSECOND',
    'TIME_TO_SEC', 'SEC_TO_TIME', 'TIME_FORMAT', 'UNIX_TIMESTAMP',
    'FROM_UNIXTIME', 'MONTHNAME', 'DAYNAME', 'DAYOFWEEK', 'DAYOFYEAR',
    'WEEK', 'WEEKOFYEAR', 'QUARTER', 'LAST_DAY', 'EXTRACT', 'DATEDIFF', 'STR_TO_DATE',
    'DATETIME',
    ]


class SQLHighlighter(QtGui.QSyntaxHighlighter):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.highlight_rules = []

        # 关键字格式
        keyword_format = QtGui.QTextCharFormat()
        keyword_format.setForeground(QtGui.QColor('#0A84FF'))  # 蓝色
        keyword_format.setFontWeight(QtGui.QFont.Bold)

        for word in keywords:
            pattern = r'\b' + word + r'\b'
            self.highlight_rules.append((QtCore.QRegularExpression(pattern), keyword_format))

        # 字符串格式
        string_format = QtGui.QTextCharFormat()
        string_format.setForeground(QtGui.QColor('#FF3B30'))  # 红色
        self.highlight_rules.append((QtCore.QRegularExpression(r"'.*?'"), string_format))
        self.highlight_rules.append((QtCore.QRegularExpression(r'".*?"'), string_format))

        # 数字格式
        number_format = QtGui.QTextCharFormat()
        number_format.setForeground(QtGui.QColor('#FF9F0A'))  # 橙色
        self.highlight_rules.append((QtCore.QRegularExpression(r'\b\d+\b'), number_format))

        # 注释格式
        self.comment_format = QtGui.QTextCharFormat()
        self.comment_format.setForeground(QtGui.QColor('#30D158'))  # 绿色
        self.highlight_rules.append((QtCore.QRegularExpression(r'--[^\n]*'), self.comment_format))
        self.highlight_rules.append((QtCore.QRegularExpression(r'#[^\n]*'), self.comment_format))

        # 多行注释正则表达式
        self.comment_start = QtCore.QRegularExpression(r'/\*')
        self.comment_end = QtCore.QRegularExpression(r'\*/')

    def highlightBlock(self, text):
        # 处理单行规则
        for pattern, format in self.highlight_rules:
            match_iterator = pattern.globalMatch(text)
            while match_iterator.hasNext():
                match = match_iterator.next()
                self.setFormat(match.capturedStart(), match.capturedLength(), format)

        # 处理多行注释
        self.setCurrentBlockState(0)  # 默认状态
        start_index = 0
        if self.previousBlockState() != 1:
            # 如果上一行不是注释状态，查找注释开始
            start_match = self.comment_start.match(text)
            start_index = start_match.capturedStart()

        while start_index >= 0:
            # 查找注释结束
            end_match = self.comment_end.match(text, start_index)
            if not end_match.hasMatch():
                # 如果没有找到注释结束，说明注释跨越多行
                self.setCurrentBlockState(1)  # 设置为注释状态
                comment_length = len(text) - start_index
            else:
                # 找到注释结束
                comment_length = end_match.capturedEnd() - start_index

            # 应用注释格式
            self.setFormat(start_index, comment_length, self.comment_format)

            # 查找下一个注释开始
            start_match = self.comment_start.match(text, start_index + comment_length)
            start_index = start_match.capturedStart()


class SQLTextEdit(QtWidgets.QTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.highlighter = SQLHighlighter(self.document())
        self.setStyleSheet("""
            QTextEdit {
                font-family: Consolas, "Courier New", monospace;
                font-size: 14px;
                color: #ffffff;
                background-color: #1d1d1f;
                border: 1px solid #3a3a3c;
                border-radius: 6px;
                padding: 8px;
            }
        """)
        # 自动转换为大写
        self.textChanged.connect(self.auto_uppercase)

        # 初始化自动补全
        self.completer = QtWidgets.QCompleter(keywords, self)
        self.completer.setWidget(self)
        self.completer.setCaseSensitivity(QtCore.Qt.CaseInsensitive)
        self.completer.setCompletionMode(QtWidgets.QCompleter.PopupCompletion)
        self.completer.activated.connect(self.insert_completion)
        
        # 支持撤销/重做
        self.setUndoRedoEnabled(True)
        
        # 支持复制格式
        self.copy_format = True

    def auto_uppercase(self):
        cursor = self.textCursor()
        position = cursor.position()
        
        # 获取当前文本
        text = self.toPlainText()
        
        # 将SQL关键字转换为大写
        for word in self.highlighter.highlight_rules:
            if word[1].fontWeight() == QtGui.QFont.Bold:  # 只处理关键字
                pattern = word[0].pattern()
                regex = QtCore.QRegularExpression(pattern, QtCore.QRegularExpression.CaseInsensitiveOption)
                match_iterator = regex.globalMatch(text)
                
                while match_iterator.hasNext():
                    match = match_iterator.next()
                    text = text[:match.capturedStart()] + match.captured().upper() + text[match.capturedEnd():]
        
        # 更新文本
        self.blockSignals(True)
        self.setPlainText(text)
        self.blockSignals(False)
        
        # 恢复光标位置
        cursor.setPosition(position)
        self.setTextCursor(cursor)

    def insert_completion(self, completion):
        """插入补全内容"""
        tc = self.textCursor()
        extra = len(completion) - len(self.completer.completionPrefix())
        tc.movePosition(QtGui.QTextCursor.Left)
        tc.movePosition(QtGui.QTextCursor.EndOfWord)
        tc.insertText(completion[-extra:])
        self.setTextCursor(tc)

    def textUnderCursor(self):
        """获取光标下的单词"""
        tc = self.textCursor()
        tc.select(QtGui.QTextCursor.WordUnderCursor)
        return tc.selectedText()

    def keyPressEvent(self, event):
        """处理键盘事件"""
        # Tab键补全
        if event.key() == QtCore.Qt.Key_Tab and self.completer.popup().isVisible():
            completion = self.completer.currentCompletion()
            if completion:
                self.insert_completion(completion)
            return
        
        # 处理其他按键
        super().keyPressEvent(event)
        
        # 显示补全提示
        if event.text() and len(event.text()) > 0:
            prefix = self.textUnderCursor()
            if len(prefix) >= 2:
                self.completer.setCompletionPrefix(prefix)
                if self.completer.completionCount() > 0:
                    cr = self.cursorRect()
                    cr.setWidth(self.completer.popup().sizeHintForColumn(0)
                                + self.completer.popup().verticalScrollBar().sizeHint().width())
                    self.completer.complete(cr)
            else:
                self.completer.popup().hide()

    def insertFromMimeData(self, source):
        """处理粘贴操作，立即触发高亮和大写"""
        if self.copy_format:
            # 获取粘贴的纯文本
            text = source.text()
            
            # 禁用信号防止重复处理
            self.blockSignals(True)
            
            # 插入文本
            cursor = self.textCursor()
            cursor.insertText(text)
            
            # 立即处理高亮和大写
            self.process_text_changes()
            
            # 恢复信号
            self.blockSignals(False)
        else:
            super().insertFromMimeData(source)

    def process_text_changes(self):
        """统一处理文本变化"""
        # 获取当前文本
        text = self.toPlainText()
        
        # 处理自动大写
        uppercase_text = self.apply_auto_uppercase(text)
        
        # 更新文本
        if uppercase_text != text:
            cursor = self.textCursor()
            position = cursor.position()
            self.setPlainText(uppercase_text)
            cursor.setPosition(position)
            self.setTextCursor(cursor)
        
        # 触发高亮
        self.highlighter.rehighlight()

    def apply_auto_uppercase(self, text):
        """应用自动大写"""
        for word in keywords:
            # 使用正则表达式替换关键字
            pattern = re.compile(r'\b' + re.escape(word) + r'\b', re.IGNORECASE)
            text = pattern.sub(word, text)
        return text

    def canInsertFromMimeData(self, source):
        """允许粘贴操作"""
        return True