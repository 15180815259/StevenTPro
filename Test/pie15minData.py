import akshare as ak
import psycopg2
import psycopg2.extras
from psycopg2 import pool, sql
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm
import threading
import sys
import io
import logging

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_ingest.log'),
        logging.StreamHandler()
    ]
)

# 数据库配置（请根据实际修改）
PG_CONFIG = {
    'host': 'localhost',
    'user': 'postgres',
    'password': 'difyai123456',
    'database': 'python',
    'port': 5432,
    'sslmode': 'prefer',
    'application_name': 'stock_ingest',
    'client_encoding': 'UTF8'
}

# 连接池配置
CONNECTION_POOL = pool.ThreadedConnectionPool(
    minconn=8,
    maxconn=32,
    **PG_CONFIG
)

# 运行参数
NUM_THREADS = 32                 # 根据CPU核心数调整
BATCH_SIZE = 2000                # 每批次插入记录数
MAX_RETRIES = 3                  # 单个股票最大重试次数
REQUEST_INTERVAL = 0.05          # 请求间隔（秒）
PERIOD = '15'                     # 数据周期（5分钟线）
TODAY = datetime.now().strftime('%Y%m%d')

# 全局统计
success_counter = 0
counter_lock = threading.Lock()

def init_database():
    """初始化数据库表结构（语法验证版）"""
    ddl_script = """
    CREATE TABLE IF NOT EXISTS stock_codes (
        id SERIAL PRIMARY KEY,
        code VARCHAR(10) UNIQUE NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS stock_15minute_data (
        id SERIAL PRIMARY KEY,
        code VARCHAR(10) NOT NULL,
        trade_time TIMESTAMP NOT NULL,
        open NUMERIC(10,2),
        high NUMERIC(10,2),
        low NUMERIC(10,2),
        close NUMERIC(10,2),
        volume BIGINT,
        amount NUMERIC(15,2),
        minute_time VARCHAR(20),
        period VARCHAR(5) DEFAULT '5',
        CONSTRAINT uniq_code_time15 UNIQUE (code, trade_time)
    );
    
    CREATE INDEX IF NOT EXISTS idx_trade_time15 ON stock_15minute_data USING BRIN(trade_time);
    """
    
    init_data_sql = """
    INSERT INTO stock_codes (code) 
    VALUES ('000001')
    ON CONFLICT (code) DO NOTHING;
    """

    conn = None
    try:
        conn = CONNECTION_POOL.getconn()
        conn.autocommit = False
        
        with conn.cursor() as cur:
            # 执行DDL
            cur.execute(ddl_script)
            # 初始化数据
           # cur.execute(init_data_sql)
            conn.commit()
            logging.info("数据库初始化成功")
    except Exception as e:
        logging.error(f"数据库初始化失败: {str(e)}")
        sys.exit(1)
    finally:
        if conn:
            CONNECTION_POOL.putconn(conn)

def get_stock_codes():
    """获取股票代码列表（稳定版）"""
    conn = None
    try:
        conn = CONNECTION_POOL.getconn()
        with conn.cursor(name='server_side_cursor',
                       cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT code FROM stock_codes")
            return [row['code'] for row in cur]
    except Exception as e:
        logging.error(f"获取股票代码失败: {str(e)}")
        return []
    finally:
        if conn:
            CONNECTION_POOL.putconn(conn)

def fetch_stock_data(code):
    """获取股票数据（加强校验版）"""
    for attempt in range(MAX_RETRIES + 1):
        try:
            time.sleep(REQUEST_INTERVAL * (attempt + 0.5))
            df = ak.stock_zh_a_hist_min_em(
                symbol=code,
                period=PERIOD,
                start_date=TODAY,
                end_date=TODAY
            )
            
            if df.empty:
                logging.debug(f"股票 {code} 无数据")
                return None
                
            # 数据标准化处理
            df['code'] = code.strip().zfill(6)  # 规范代码格式
            df['trade_time'] = pd.to_datetime(df['时间'])
            return df[['code', 'trade_time', '开盘', '收盘', '最高', 
                     '最低', '成交量', '成交额', '时间']].rename(columns={
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '时间': 'minute_time'
            })
        except Exception as e:
            if attempt == MAX_RETRIES:
                logging.warning(f"股票 {code} 获取失败: {str(e)[:100]}")
                return None
            time.sleep(2 ** attempt)

def bulk_insert(data_batch):
    """批量插入数据（语法修正终版）"""
    if not data_batch:
        return True

    conn = None
    try:
        conn = CONNECTION_POOL.getconn()
        conn.autocommit = False
        
        with conn.cursor() as cur:
            # 关键修正：正确的临时表语法
            cur.execute("""
                CREATE TEMP TABLE tmp_staging (
                    code        VARCHAR(10),
                    trade_time  TIMESTAMP,
                    open        NUMERIC(10,2),
                    high        NUMERIC(10,2),
                    low         NUMERIC(10,2),
                    close       NUMERIC(10,2),
                    volume      BIGINT,
                    amount      NUMERIC(15,2),
                    minute_time VARCHAR(20)
                ) ON COMMIT DROP;
            """)
            
            # 生成严格校验的CSV数据
            csv_buffer = io.StringIO()
            valid_count = 0
            for item in data_batch:
                try:
                    # 类型强制转换和格式化
                    csv_line = (
                        f"{str(item['code']).strip()},"
                        f"{item['trade_time'].isoformat()},"
                        f"{float(item['open']):.2f},"
                        f"{float(item['high']):.2f},"
                        f"{float(item['low']):.2f},"
                        f"{float(item['close']):.2f},"
                        f"{int(item['volume'])},"
                        f"{float(item['amount']):.2f},"
                        f"{str(item['minute_time']).strip()}\n"
                    )
                    csv_buffer.write(csv_line)
                    valid_count += 1
                except Exception as e:
                    logging.error(f"数据校验失败: {e} | 数据: {item}")
                    continue
            
            if valid_count == 0:
                logging.warning("无有效数据可插入")
                return False
            
            # 执行COPY
            csv_buffer.seek(0)
            cur.copy_expert(
                "COPY tmp_staging FROM STDIN WITH (FORMAT CSV)",
                csv_buffer
            )
            
            # 合并数据到主表
            cur.execute("""
                INSERT INTO stock_15minute_data (
                    code, trade_time, open, high, low, 
                    close, volume, amount, minute_time
                )
                SELECT 
                    code, trade_time, open, high, low, 
                    close, volume, amount, minute_time
                FROM tmp_staging
                ON CONFLICT (code, trade_time) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    amount = EXCLUDED.amount
            """)
            
            conn.commit()
            
            global success_counter
            with counter_lock:
                success_counter += cur.rowcount
            
            logging.info(f"成功插入 {cur.rowcount} 条记录")
            return True
            
    except Exception as e:
        logging.error(f"批量插入失败: {str(e)[:200]}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            CONNECTION_POOL.putconn(conn)

def process_batch(codes):
    """处理批次数据（稳定版）"""
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = {executor.submit(fetch_stock_data, code): code for code in codes}
        
        results = []
        for future in as_completed(futures):
            data = future.result()
            if data is not None and not data.empty:
                results.extend(data.to_dict('records'))
    
    if results:
        return bulk_insert(results)
    return 0

def main():
    """主程序（终版）"""
    init_database()
    
    stock_codes = get_stock_codes()
    if not stock_codes:
        logging.error("未找到股票代码")
        return
    
    logging.info(f"开始处理 {len(stock_codes)} 只股票数据，日期：{TODAY}")
    
    total_start = time.time()
    batch_size = 200  # 每批处理股票数量
    
    with tqdm(total=len(stock_codes), desc="处理进度", unit="stock") as progress:
        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i:i + batch_size]
            process_batch(batch)
            progress.update(len(batch))
            
            elapsed = time.time() - total_start
            progress.set_postfix({
                'speed': f"{success_counter / max(elapsed, 1):.1f} rec/s",
                'inserted': success_counter
            })
    
    total_time = time.time() - total_start
    logging.info(f"处理完成 | 总耗时: {total_time:.1f}s")
    logging.info(f"成功插入记录: {success_counter} | 平均速度: {success_counter / max(total_time, 1):.1f} rec/s")

if __name__ == "__main__":
    main()