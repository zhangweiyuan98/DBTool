import datetime
import re

import pandas as pd
import pymysql
import sqlparse


from utils.DBconnectServer import popup_manager
from utils.logger import logger

global Process_df
Process_df = pd.DataFrame(columns=['Process_id', 'Status', '服务器组'])

def split_statements(sql_text):
    # 正则匹配所有CREATE语句
    create_pattern = r"(CREATE\s+(PROCEDURE|FUNCTION|TRIGGER|EVENT|VIEW)\s+`[a-zA-Z0-9_]+`)"
    matches = list(re.finditer(create_pattern, sql_text))

    if len(matches) == 1:
        return [sql_text]

    statements = []
    start = 0

    for match in matches:
        end = match.start()
        if start != end:
            statements.append(sql_text[start:end].strip())
        start = match.start()

    # 最后一段
    statements.append(sql_text[start:].strip())

    return statements


def clean_sql(sql):
    """规则"""
    print(f"规则：{sql}")
    use_pattern = r"USE\s+`?(\w+)`?\$"
    use_match = re.search(use_pattern, sql, re.IGNORECASE)
    if use_match:
        database_name = use_match.group(1)
        database_name = f"{database_name}."
    else:
        database_name = ""

    sql = re.sub(r'/\*!\d+\s*', "", sql)
    sql = re.sub(r'\*/\s*\$\$', ";", sql)
    sql = re.sub(r'\$\$', ";", sql)
    sql = re.sub(r'\*/\s*;', ";", sql)
    sql = re.sub(r"END\$\$", "END;", sql)
    sql = re.sub(r"END\s\$\$", "END;", sql)
    sql = re.sub(r"DELIMITER\s*\$\$", "", sql)
    sql = re.sub(r"DELIMITER\s*;", "", sql)
    sql = re.sub(r"DELIMITER\s*;", "", sql)
    sql = re.sub(r"USE\s+`[a-zA-Z0-9_]+`;", "", sql)
    sql = re.sub(r"DROP PROCEDURE IF EXISTS `([a-zA-Z0-9_]+)`;", "", sql)
    return sql, database_name

def create_procedure(connection, section, sql, sql_type, database_name, _name, result_df):
    """存储过程建立"""
    create_index = sql.find("CREATE")

    sql = re.sub(rf"CREATE\s+PROCEDURE\s+`{_name}`",
                 f"CREATE PROCEDURE {database_name}`{_name}`", sql, flags=re.IGNORECASE)
    sql = re.sub(rf"CREATE\s+DEFINER=`[^`]+`@`[^`]+`\s+PROCEDURE\s+`{_name}`",
                 f"CREATE PROCEDURE {database_name}`{_name}`", sql, flags=re.IGNORECASE)

    String_sql = ''
    check_proc_exist_sql = ''
    if create_index != -1:
        String_sql = sql[create_index:]
    try:
        cursor = connection.cursor()
        if sql_type == 'PROCEDURE':
            check_proc_exist_sql = f"SHOW CREATE PROCEDURE {database_name}{_name} ;"
        elif sql_type == 'FUNCTION':
            check_proc_exist_sql = f"SHOW CREATE FUNCTION {database_name}{_name} ;"
        elif sql_type == 'TRIGGER':
            check_proc_exist_sql = f"SHOW CREATE TRIGGER {database_name}{_name} ;"
        else:
            pass
        print(String_sql)
        try:
            cursor.execute(check_proc_exist_sql)
            result = cursor.fetchone()
        except pymysql.err.OperationalError as e:
            error_code, error_message = e.args

            if error_code == 1305:  # 不存在
                result = None
            else:
                print("存在的")
                raise e
        if result is None:
            # 不存在，直接创建
            print(f"直接创建：{database_name}{String_sql}")
            cursor.execute(String_sql)
            print("创建完成")
            # connection.commit()
            temp_df = pd.DataFrame(
                {'db_name': [database_name.replace(".", "")], sql_type: [_name], '执行结果': ['成功'],
                 '服务器组': [section]})
            result_df = pd.concat([result_df, temp_df], ignore_index=True)
            print(f"{section}：{database_name}.{_name}：创建成功")
            logger.info(f"{section}：{database_name}.{_name}：创建成功")
        else:
            print(f"{database_name}{_name}已经存在")
            if sql_type == 'PROCEDURE':
                cursor.execute(f"DROP PROCEDURE IF EXISTS {database_name}{_name} ;")
            elif sql_type == 'FUNCTION':
                cursor.execute(f"DROP FUNCTION IF EXISTS {database_name}{_name} ;")
            elif sql_type == 'TRIGGER':
                cursor.execute(f"DROP TRIGGER IF EXISTS {database_name}{_name} ;")
            print(f"{database_name}{_name}已经删除")
            print(f"执行{String_sql}")
            cursor.execute(String_sql)
            connection.commit()
            temp_df = pd.DataFrame(
                {'db_name': [database_name.replace(".", "")], sql_type: [_name], '执行结果': ['成功'],
                 '服务器组': [section]})
            result_df = pd.concat([result_df, temp_df], ignore_index=True)
            print(f"{database_name}{_name}已经更新")
            logger.info(f"{section}：{database_name}{_name}：更新成功")

        cursor.close()
    except pymysql.err.InternalError as e:
        error_code, error_message = e.args
        logger.error(f"{section}：{database_name}{_name}：操作失败: {error_message}")
        popup_manager.message_signal.emit(
            f"{section}：{database_name}{_name}：操作失败: {error_message}")

        temp_df = pd.DataFrame({'执行结果': [f'失败:{str(error_message)}'], '服务器组': [section]})
        result_df = pd.concat([result_df, temp_df], ignore_index=True)


    return result_df


def execute_sql(connection, sql, section, type, db_name, _name, result_df):
    """执行sql语句"""
    try:
        global Process_df
        cursor = connection.cursor()
        Process_id = connection.thread_id()
        row = {'Process_id': Process_id, 'Status': 'Running', '服务器组': section}
        Process_df = Process_df.append(row, ignore_index=True)
        logger.info(f"当前执行进程->{section}:{Process_id}")
        print(f"{datetime.datetime.now()}当前执行进程->{section}:{Process_id}")

        if (sql.strip().upper().startswith('SELECT') or sql.strip().upper().startswith('SHOW')
                or sql.strip().upper().startswith('WITH') or sql.strip().upper().startswith(
                    'DESC') or sql.strip().upper().startswith('EXPLAIN')):
            sql_statements = sqlparse.split(sql)
            for sql_statement in sql_statements:
                logger.info(f"执行SQL语句：{sql_statement}")
                cursor.execute(sql_statement)
            results = cursor.fetchall()
            temp_df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
            temp_df['服务器组'] = section
            result_df = pd.concat([result_df, temp_df], ignore_index=True)
            logger.info(f"{section}：执行成功!!")
        elif sql.upper().startswith('CALL'):
            cursor.execute(sql)
            if cursor.rowcount == -1:
                # connection.commit()
                temp_df = pd.DataFrame({'执行结果': ['成功'], '服务器组': [section]})
                result_df = pd.concat([result_df, temp_df], ignore_index=True)

            else:
                # connection.commit()
                results = cursor.fetchall()

                if cursor.rowcount == 0 and cursor.description:
                    temp_df = pd.DataFrame(columns=[desc[0] for desc in cursor.description])
                    temp_df['服务器组'] = section
                    result_df = pd.concat([result_df, temp_df], ignore_index=True)
                elif cursor.rowcount == 0 and not cursor.description:
                    temp_df = pd.DataFrame({'执行结果': ['成功'], '服务器组': [section]})
                    result_df = pd.concat([result_df, temp_df], ignore_index=True)
                else:
                    temp_df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
                    temp_df['服务器组'] = section
                    result_df = pd.concat([result_df, temp_df], ignore_index=True)
        elif re.search(r"CREATE\s+(?:FUNCTION|PROCEDURE|VIEW|EVENT|TRIGGER)", sql):
            print("创建存储过程/函数/事件/视图/触发器")
            try:
                result_df = create_procedure(connection, section, sql, type, db_name, _name, None)
            except Exception as e:
                logger.error(f"{section}:{e} ")
        else:
            sql_statements = sqlparse.split(sql)
            print(f"执行中：{sql_statements}")
            success_count = 0
            failure_count = 0
            try:
                for sql_statement in sql_statements:
                    logger.info(f"执行SQL语句：{sql_statement}")
                    cursor.execute(sql_statement)
                    # connection.commit()
                    count = cursor.rowcount
                    if count > 0:
                        success_count += count
                    elif count == 0:
                        success_count += 1
                    else:
                        failure_count += 1
                temp_df = pd.DataFrame(
                    {'成功数': [success_count], '失败数': [failure_count], '服务器组': [section]})
                result_df = pd.concat([result_df, temp_df], ignore_index=True)
                logger.info(f"SQL statements executed successfully!")
            except Exception as e:
                popup_manager.message_signal.emit(f"{section}错误内容: {str(e)}")

    except pymysql.err.OperationalError as e:
        if e.args[0] == 2013:
            logger.error(f"{section} 执行失败: 连接到 MySQL 服务器时连接丢失")

            temp_df = pd.DataFrame({'执行结果': ['执行失败: 连接到 MySQL 服务器时连接丢失'], '服务器组': [section]})
            result_df = pd.concat([result_df, temp_df], ignore_index=True)

        else:
            popup_manager.message_signal.emit(f"{section} 执行失败: {str(e)}")
            logger.error(f"{section} 执行失败: {str(e)}")

            temp_df = pd.DataFrame({'执行结果': [f'失败:{str(e)}'], '服务器组': [section]})
            result_df = pd.concat([result_df, temp_df], ignore_index=True)

    except pymysql.err.ProgrammingError as e:

        popup_manager.message_signal.emit(f"错误内容: {section}:{str(e)}")
        logger.error(f"{section} 执行失败: {str(e)}")

        temp_df = pd.DataFrame({'执行结果': [f'执行失败: {str(e)}'], '服务器组': [section]})
        result_df = pd.concat([result_df, temp_df], ignore_index=True)

    return result_df

def kill_sql(connection, section, result_df):
    """杀进程"""
    matching_processes = Process_df[Process_df['服务器组'] == section]
    if not matching_processes.empty:
        for index, process_to_kill in matching_processes.iterrows():
            process_id_to_kill = process_to_kill['Process_id']
            server_group = process_to_kill['服务器组']
            try:
                with connection.cursor() as kill_cursor:
                    kill_cursor.execute(f"KILL {int(process_id_to_kill)}")
                    logger.info(f"KILL进程ID-> {server_group}:{process_id_to_kill}")

                    temp_df = pd.DataFrame({
                        'Process_id': [process_id_to_kill],
                        'Status': ["stopped"],
                        '服务器组': [server_group]
                    })

                    result_df = pd.concat([result_df, temp_df], ignore_index=True)
            except Exception as e:
                logger.error(f"执行 KILL 命令时发生错误: {str(e)} 进程ID: {process_id_to_kill}")

                temp_df = pd.DataFrame({
                    'Process_id': [process_id_to_kill],
                    'Status': ["failed"],
                    '服务器组': [server_group]
                })
                result_df = pd.concat([result_df, temp_df], ignore_index=True)

        return result_df
    else:
        logger.info(f"没有找到属于 {section} 服务器组的进程ID")
        return result_df