import pymysql
import concurrent.futures
import queue
import threading
import signal
import sys

# 数据库连接配置
db_configs = [
    {'name': '都城1', 'host': '10.242.118.115', 'user': 'root', 'password': 'xq1CTaUKmBIVsGkyNX#2',
     'database': 'portal_pms'},
    {'name': '都城2', 'host': '10.242.118.95', 'user': 'root', 'password': '7%7zLZr9fLE#Y8iyMlSR',
     'database': 'portal_pms'},
]

# 用来存储数据库连接的进程ID
active_processes = []

# 捕获退出信号
def handle_exit_signal(signal, frame):
    print("\n程序被终止，正在关闭所有数据库连接...")

    # 杀掉所有数据库进程
    for process_id in active_processes:
        try:
            print(f"正在终止数据库进程 {process_id}...")
            for db_config in db_configs:
                conn = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'],
                                       database=db_config['database'], autocommit=True)
                cursor = conn.cursor()
                cursor.execute(f"KILL {process_id}")  # 使用 KILL 命令杀死进程
                cursor.close()
                conn.close()
            print(f"成功终止进程 {process_id}")
        except Exception as e:
            print(f"终止进程 {process_id} 失败: {e}")
    sys.exit(0)

# 处理每个线程的数据
def process_data_in_db(db_config, task_queue, thread_id):
    conn = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'],
                           database=db_config['database'], autocommit=True)
    cursor = conn.cursor()
    process_id = conn.thread_id()
    try:
        if process_id:
            active_processes.append(process_id)  # 保存当前进程ID
            print(f"线程 {thread_id} 在数据库 {db_config['name']} 上执行，数据库进程ID: {process_id}")

        while not task_queue.empty():  # 从队列中获取任务
            hotel_id = task_queue.get()  # 获取一个酒店ID
            try:
                print(f"线程 {thread_id} 在数据库 {db_config['name']} 正在执行酒店 ID {hotel_id}")
                # 调用存储过程：
                cursor.callproc('up_ihotel_call_hotelcall', (2, hotel_id))
                print(f"线程 {thread_id} 在数据库 {db_config['name']} 执行酒店 ID {hotel_id} 已完成")
            except Exception as e:
                print(f"Error processing hotel_id {hotel_id} in thread {thread_id} of DB {db_config['name']}: {e}")
            finally:
                task_queue.task_done()  # 标记任务已完成
    except Exception as e:
        print(f"Error in thread {thread_id} of DB {db_config['name']}: {e}")
    finally:
        cursor.close()
        conn.close()

# 启动多线程，在多个数据库上执行
def run_threads_on_multiple_databases(db_configs):
    # 启动线程池来处理多个数据库
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        # 遍历所有数据库配置
        for db_config in db_configs:
            # 为每个数据库启动一个线程池来处理该数据库的任务
            futures.append(executor.submit(run_db_tasks, db_config))
        # 等待所有数据库处理完毕
        for future in futures:
            future.result()

# 为每个数据库配置启动任务
def run_db_tasks(db_config):
    conn = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'],
                           database=db_config['database'])
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM hotel WHERE sta = 'I' ORDER BY id")
        hotel_ids = [row[0] for row in cursor.fetchall()]  # 获取所有ID
        print(f"数据库 {db_config['name']} 共执行酒店 {len(hotel_ids)} 条任务")
        # 创建任务队列并将ID加入队列
        task_queue = queue.Queue()
        for hotel_id in hotel_ids:
            task_queue.put(hotel_id)
        # 启动线程池来处理任务
        num_threads = 8  # 每个数据库上启动的线程数
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for i in range(num_threads):
                futures.append(executor.submit(process_data_in_db, db_config, task_queue, i + 1))
            # 等待所有线程完成
            for future in futures:
                future.result()
    except Exception as e:
        print(f"Error fetching data from DB {db_config['name']}: {e}")
    finally:
        cursor.close()
        conn.close()

# 设置退出信号处理
signal.signal(signal.SIGINT, handle_exit_signal)  # 捕获Ctrl+C

# 运行
if __name__ == "__main__":
    run_threads_on_multiple_databases(db_configs)
