#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import json
import datetime
import sys
import os
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text

# 日誌設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - BOM_ETL - %(levelname)s - %(message)s')
logger = logging.getLogger("BOM_ETL")


# 載入 DB 配置
def load_db_config(path="db.json"):
    """載入資料庫配置檔案"""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"載入 {path} 失敗: {e}")
        sys.exit(1)


# 建立 pyodbc 連線
def build_pyodbc_conn(cfg):
    """建立 pyodbc 資料庫連線"""
    drv = "{ODBC Driver 17 for SQL Server}"
    srv = cfg['server']
    port = cfg.get('port', 1433)
    db = cfg['database']
    uid = cfg['username']
    pwd = cfg['password']
    opts = cfg.get('options', {})
    enc = 'yes' if opts.get('encrypt') else 'no'
    trust = 'yes' if opts.get('trustServerCertificate') else 'no'
    conn_str = (
        f"DRIVER={drv};"
        f"SERVER={srv},{port};DATABASE={db};"
        f"UID={uid};PWD={pwd};Encrypt={enc};TrustServerCertificate={trust};")
    return pyodbc.connect(conn_str)


# 建立 SQLAlchemy 引擎
def build_sqlalchemy_engine(cfg):
    """建立 SQLAlchemy 資料庫引擎"""
    drv = 'ODBC Driver 17 for SQL Server'.replace(' ', '+')
    srv = cfg['server']
    port = cfg.get('port', 1433)
    db = cfg['database']
    uid = cfg['username']
    pwd = cfg['password']
    opts = cfg.get('options', {})
    enc = 'yes' if opts.get('encrypt') else 'no'
    trust = 'yes' if opts.get('trustServerCertificate') else 'no'
    uri = (f"mssql+pyodbc://{uid}:{pwd}@{srv},{port}/{db}?driver={drv}"
           f"&Encrypt={enc}&TrustServerCertificate={trust}")
    return create_engine(uri, fast_executemany=True)


# 讀取SQL文件
def load_sql_file(sql_file):
    """載入SQL查詢檔案"""
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_path = os.path.join(base_dir, sql_file)

        if not os.path.exists(sql_path):
            # 嘗試在 bom 子目錄中查找
            sql_path_in_subdir = os.path.join(base_dir, 'bom', sql_file)
            if os.path.exists(sql_path_in_subdir):
                sql_path = sql_path_in_subdir
            else:
                raise FileNotFoundError(f"找不到SQL檔案: {sql_file}")

        with open(sql_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"讀取SQL文件失敗: {sql_file}, {e}")
        sys.exit(1)


# 確保 BOM_Expanded 表存在
def ensure_bom_expanded_table(engine):
    """確保 BOM_Expanded 表存在且結構正確"""
    sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'BOM_Expanded')
    BEGIN
        CREATE TABLE BOM_Expanded (
            [成品料號] NVARCHAR(50),
            [成品名稱] NVARCHAR(255),
            [大分類] NVARCHAR(100),
            [中分類] NVARCHAR(100),
            [小分類] NVARCHAR(100),
            [上層料號] NVARCHAR(50),
            [上層名稱] NVARCHAR(255),
            [投入料號] NVARCHAR(50),
            [投入名稱] NVARCHAR(255),
            [投入單位] NVARCHAR(20),
            [項目群組] NVARCHAR(50),
            [物料表類型] NVARCHAR(20),
            [成品數量] DECIMAL(19,6),
            [更新日期] DATETIME,
            [階段] INT,
            [階段名稱] NVARCHAR(100),
            [順序] INT,
            [投入類型] NVARCHAR(20),
            [Level] INT,
            [LevelName] NVARCHAR(50),
            [Path] NVARCHAR(MAX),
            [單層用量] DECIMAL(19,6),
            [每單位用量展開] DECIMAL(19,6),
            [單價] DECIMAL(19,4),
            [金額] DECIMAL(19,4),
            [是否為最底層投入] BIT,
            [RunTime] DATETIME DEFAULT GETDATE()
        )
        
        -- 建立索引以提升查詢效能
        CREATE INDEX IX_BOM_Expanded_成品料號 ON BOM_Expanded([成品料號])
        CREATE INDEX IX_BOM_Expanded_RunTime ON BOM_Expanded([RunTime])
        CREATE INDEX IX_BOM_Expanded_成品料號_RunTime ON BOM_Expanded([成品料號], [RunTime])
    END
    ELSE
    BEGIN
        -- 確保 RunTime 欄位存在
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                       WHERE TABLE_NAME = 'BOM_Expanded' AND COLUMN_NAME = 'RunTime')
        BEGIN
            ALTER TABLE BOM_Expanded ADD [RunTime] DATETIME DEFAULT GETDATE()
            CREATE INDEX IX_BOM_Expanded_RunTime ON BOM_Expanded([RunTime])
            CREATE INDEX IX_BOM_Expanded_成品料號_RunTime ON BOM_Expanded([成品料號], [RunTime])
        END
    END
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        logger.info("已確保 BOM_Expanded 表存在且結構正確")
    except Exception as e:
        logger.error(f"檢查或創建 BOM_Expanded 表失敗: {e}")
        raise


# 執行 BOM ETL 處理
def run_bom_etl(src_conn, tgt_engine, sql_query, etl_type):
    """執行 BOM ETL 處理"""
    logger.info(f"開始執行 {etl_type} BOM ETL 處理...")

    try:
        # 記錄開始時間
        start_time = datetime.datetime.now()

        # 執行查詢
        logger.info("執行 BOM 查詢...")
        df = pd.read_sql(sql_query, src_conn)

        if df.empty:
            logger.warning(f"{etl_type} BOM 查詢未返回任何資料")
            return 0

        # 添加 RunTime 欄位
        df['RunTime'] = start_time

        logger.info(f"查詢完成，共取得 {len(df)} 筆資料")

        # 處理 NULL 值
        numeric_columns = df.select_dtypes(include=['int', 'float']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)

        string_columns = df.select_dtypes(include=['object']).columns
        df[string_columns] = df[string_columns].fillna('')

        # 分批寫入資料（使用 Append 模式）
        batch_size = 100
        total_rows = len(df)
        processed = 0

        logger.info("開始寫入資料至 BOM_Expanded 表...")

        for i in range(0, total_rows, batch_size):
            chunk = df.iloc[i:min(i + batch_size, total_rows)]
            chunk.to_sql('BOM_Expanded',
                         tgt_engine,
                         if_exists='append',
                         index=False,
                         method=None)
            processed += len(chunk)

            if processed % 500 == 0 or processed == total_rows:
                logger.info(
                    f"寫入進度: {processed}/{total_rows} 筆 ({int(processed/total_rows*100)}%)"
                )

        # 記錄執行結果到 ETL_SUMMARY
        try:
            with tgt_engine.begin() as conn:
                stmt = text(
                    "INSERT INTO ETL_SUMMARY ([TIMESTAMP], [SOURCE_TYPE], [QUERY_NAME], [TARGET_TABLE], [ROW_COUNT], [ETL_DATE], [SUMMARY_TYPE])"
                    " VALUES (GETDATE(), 'SAP', :query_name, 'BOM_Expanded', :row_count, GETDATE(), 'BOM_ETL')"
                )
                params = {
                    'query_name': f'BOM_{etl_type}',
                    'row_count': total_rows
                }
                conn.execute(stmt, params)
        except Exception as e:
            logger.warning(f"記錄 ETL 執行結果失敗: {e}")

        logger.info(f"{etl_type} BOM ETL 處理完成，共處理 {total_rows} 筆資料")
        return total_rows

    except Exception as e:
        logger.error(f"{etl_type} BOM ETL 處理失敗: {e}")
        raise


# 清理舊資料
def cleanup_old_data(engine, days_to_keep=90):
    """清理超過指定天數的舊資料"""
    try:
        cleanup_sql = f"""
        DELETE FROM BOM_Expanded 
        WHERE RunTime < DATEADD(DAY, -{days_to_keep}, GETDATE())
        """

        with engine.begin() as conn:
            result = conn.execute(text(cleanup_sql))
            deleted_rows = result.rowcount

        if deleted_rows > 0:
            logger.info(f"已清理 {deleted_rows} 筆超過 {days_to_keep} 天的舊資料")
        else:
            logger.info("無需清理舊資料")

    except Exception as e:
        logger.warning(f"清理舊資料失敗: {e}")


def main():
    parser = argparse.ArgumentParser(description='BOM ETL 專用處理程式')
    parser.add_argument('--daily',
                        action='store_true',
                        help='執行每日增量 BOM ETL（前一天異動）')
    parser.add_argument('--monthly',
                        action='store_true',
                        help='執行每月完整 BOM ETL（全部資料）')
    parser.add_argument('--cleanup',
                        type=int,
                        default=90,
                        help='清理超過指定天數的舊資料（預設90天）')
    parser.add_argument('--debug', action='store_true', help='啟用詳細的除錯訊息')

    args = parser.parse_args()

    # 設定日誌等級
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("已啟用詳細除錯模式")

    # 檢查參數
    if not args.daily and not args.monthly:
        logger.error("請指定 --daily 或 --monthly 參數")
        sys.exit(1)

    if args.daily and args.monthly:
        logger.error("不能同時指定 --daily 和 --monthly 參數")
        sys.exit(1)

    logger.info('=' * 60)
    logger.info(f"BOM ETL 程序啟動 - {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")

    # 載入資料庫配置
    cfg = load_db_config('db.json')

    # 建立資料庫連線
    try:
        src_sap = build_pyodbc_conn(cfg['sap_db'])
        logger.info("成功連接到 SAP 資料庫")
    except Exception as e:
        logger.error(f"連接 SAP 資料庫失敗: {e}")
        sys.exit(1)

    try:
        tgt_engine = build_sqlalchemy_engine(cfg['tableau_db'])
        logger.info("成功連接到 Tableau 資料庫")
    except Exception as e:
        logger.error(f"連接 Tableau 資料庫失敗: {e}")
        sys.exit(1)

    # 確保目標表存在
    ensure_bom_expanded_table(tgt_engine)

    try:
        if args.daily:
            # 每日增量處理
            sql_query = load_sql_file('BOM階層與用量(前一天異動) v1.sql')
            processed_rows = run_bom_etl(src_sap, tgt_engine, sql_query,
                                         'DAILY')

        elif args.monthly:
            # 每月完整處理
            sql_query = load_sql_file('BOM階層與用量 v1.sql')
            processed_rows = run_bom_etl(src_sap, tgt_engine, sql_query,
                                         'MONTHLY')

        # 清理舊資料
        if args.cleanup > 0:
            cleanup_old_data(tgt_engine, args.cleanup)

        logger.info('=' * 60)
        logger.info(f"BOM ETL 執行完成 - 處理 {processed_rows} 筆資料")

    except Exception as e:
        logger.error(f"BOM ETL 執行失敗: {e}")
        sys.exit(1)

    finally:
        # 關閉資料庫連線
        if src_sap:
            src_sap.close()

    sys.exit(0)


if __name__ == '__main__':
    main()
