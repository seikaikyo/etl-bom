#!/bin/bash

# BOM 資料清理腳本
# 用途: 清理超過指定天數的 BOM 資料

LOG_DIR="/home/ETL/logs/bom"
LOG_FILE="${LOG_DIR}/cleanup_$(date +%Y%m%d).log"

# 確保日誌目錄存在
mkdir -p $LOG_DIR

echo "===== BOM 資料清理開始 - $(date '+%Y-%m-%d %H:%M:%S') =====" | tee -a $LOG_FILE

# 切換到 ETL 專案目錄
cd /home/ETL/etl

# 啟動虛擬環境
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "已啟動虛擬環境" | tee -a $LOG_FILE
else
    echo "警告: 找不到虛擬環境" | tee -a $LOG_FILE
fi

# 執行清理（保留 90 天資料）
echo "開始清理超過 90 天的 BOM 資料..." | tee -a $LOG_FILE
python -c "
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bom_etl import load_db_config, build_sqlalchemy_engine, cleanup_old_data
import logging

# 設定日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

try:
    # 載入配置並連接資料庫
    cfg = load_db_config('db.json')
    engine = build_sqlalchemy_engine(cfg['tableau_db'])
    
    # 執行清理
    cleanup_old_data(engine, 90)
    print('清理作業完成')
    
except Exception as e:
    logger.error(f'清理作業失敗: {e}')
    sys.exit(1)
" >> $LOG_FILE 2>&1

CLEANUP_STATUS=$?

if [ $CLEANUP_STATUS -eq 0 ]; then
    echo "BOM 資料清理成功完成！" | tee -a $LOG_FILE
else
    echo "BOM 資料清理失敗，錯誤碼: $CLEANUP_STATUS" | tee -a $LOG_FILE
fi

# 清理舊的日誌檔案 (保留60天)
if [ -d "$LOG_DIR" ]; then
    find $LOG_DIR -name "cleanup_*.log" -mtime +60 -delete
    find $LOG_DIR -name "cron_*.log" -mtime +30 -delete
fi

echo "===== BOM 資料清理結束 - $(date '+%Y-%m-%d %H:%M:%S') =====" | tee -a $LOG_FILE

# 退出虛擬環境
if [ -d "venv" ]; then
    deactivate
fi

exit $CLEANUP_STATUS