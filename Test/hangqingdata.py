import mysql.connector
import akshare as ak
import time
from datetime import datetime

# 初始化数据库连接和建表
def init_db():
    try:
        # 连接到 MySQL 数据库
        conn = mysql.connector.connect(
            host="localhost",
            user="howei",
            password="howei",
            database="PYTHON"
        )
        cursor = conn.cursor()

        # 创建股票数据表
        create_data_table_query = """
        CREATE TABLE IF NOT EXISTS stock_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts_code VARCHAR(20),
            trade_date VARCHAR(20),
            open_price DECIMAL(10, 2),
            high_price DECIMAL(10, 2),
            low_price DECIMAL(10, 2),
            close_price DECIMAL(10, 2),
            volume BIGINT,
            amount DECIMAL(15, 2),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_stock (ts_code, trade_date)
        )
        """
        cursor.execute(create_data_table_query)

        # 创建股票代码表（用于手动维护股票列表）
        create_list_table_query = """
        CREATE TABLE IF NOT EXISTS stock_list (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(20) NOT NULL UNIQUE,
            name VARCHAR(100),
            create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_list_table_query)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("数据库表初始化完成")
    except mysql.connector.Error as err:
        print(f"数据库初始化失败: {err}")

# 从数据库获取股票列表
def get_stock_list():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="howei",
            password="howei",
            database="PYTHON"
        )
        cursor = conn.cursor()
        
        # 查询有效的股票代码
        cursor.execute("SELECT code FROM stock_list")
        stock_list = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        print(f"从数据库获取股票列表成功，共 {len(stock_list)} 只股票")
        return stock_list
    except Exception as e:
        print(f"获取股票列表失败: {str(e)}")
        return []

# 获取股票行情数据（保持原有逻辑）
def fetch_stock_data(symbol):
    try:
        # 注意：日期参数需要调整为有效日期，这里仅为示例格式
        df = ak.stock_zh_a_minute(
            symbol=symbol,
            period="5",
            start_date="20250325",  # 调整为实际需要的日期
            end_date="20250325"       # 调整为实际需要的日期
        )
        if not df.empty:
            latest_data = df.iloc[-1]
            data_tuple = (
                symbol,
                latest_data['日期'].strftime('%Y-%m-%d %H:%M:%S'),  # 格式化时间戳
                float(latest_data['开盘']),
                float(latest_data['最高']),
                float(latest_data['最低']),
                float(latest_data['收盘']),
                int(latest_data['成交量']),
                float(latest_data['成交额'])
            )
            return data_tuple
        else:
            print(f"股票 {symbol} 无数据")
            return None
    except Exception as e:
        print(f"获取股票 {symbol} 数据失败: {str(e)}")
        return None

# 存储数据到数据库（保持原有逻辑）
def save_to_db(data_list):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="howei",
            password="howei",
            database="PYTHON"
        )
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO stock_data (ts_code, trade_date, open_price, high_price,
                               low_price, close_price, volume, amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open_price = VALUES(open_price),
            high_price = VALUES(high_price),
            low_price = VALUES(low_price),
            close_price = VALUES(close_price),
            volume = VALUES(volume),
            amount = VALUES(amount)
        """

        batch_size = 500
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            conn.commit()
            print(f"成功存储 {len(batch)} 条数据")

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"数据存储失败: {err}")

# 定时任务逻辑保持不变
def run_scheduler():
    while True:
        try:
            print(f"\n开始获取数据 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            stock_list = get_stock_list()
            if not stock_list:
                print("未获取到股票列表，请检查 stock_list 表是否有数据")
                time.sleep(300)
                continue

            data_list = []
            for symbol in stock_list:  # 遍历所有从数据库获取的股票
                print(f"正在获取股票 {symbol} 的数据...")
                data = fetch_stock_data(symbol)
                if data:
                    data_list.append(data)

            if data_list:
                save_to_db(data_list)

            time.sleep(300)

        except KeyboardInterrupt:
            print("\n程序已停止")
            break
        except Exception as e:
            print(f"发生错误: {str(e)}")
            time.sleep(300)

if __name__ == "__main__":
    init_db()
    run_scheduler()
