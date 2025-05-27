#!/bin/bash

# BOM ETL 排程執行腳本
# 用途: 執行 BOM 相關的 ETL 處理並產生執行報告

# 設定日誌和報告目錄
LOG_DIR="/home/ETL/logs/bom"
REPORT_DIR="/home/ETL/reports/bom"
LOG_FILE="${LOG_DIR}/bom_etl_$(date +%Y%m%d).log"
REPORT_FILE="${REPORT_DIR}/bom_etl_report_$(date +%Y%m%d%H%M).txt"
LATEST_REPORT="${REPORT_DIR}/bom_latest_report.txt"
HTML_REPORT="${REPORT_DIR}/bom_etl_report_$(date +%Y%m%d%H%M).html"
LATEST_HTML="${REPORT_DIR}/bom_latest_report.html"

# 確保目錄存在
mkdir -p $LOG_DIR
mkdir -p $REPORT_DIR

# 檢查目錄是否創建成功
if [ ! -d "$LOG_DIR" ]; then
    echo "錯誤: 無法創建日誌目錄 $LOG_DIR"
    exit 1
fi

if [ ! -d "$REPORT_DIR" ]; then
    echo "錯誤: 無法創建報告目錄 $REPORT_DIR"
    exit 1
fi

# 函數：顯示使用說明
show_usage() {
    echo "使用方法: $0 [daily|monthly]"
    echo "  daily   - 執行每日 BOM ETL (前一天異動)"
    echo "  monthly - 執行每月 BOM ETL (完整資料)"
    exit 1
}

# 檢查參數
if [ $# -ne 1 ]; then
    show_usage
fi

ETL_TYPE=$1
case $ETL_TYPE in
    daily|monthly)
        ;;
    *)
        echo "錯誤: 無效的參數 '$ETL_TYPE'"
        show_usage
        ;;
esac

# 記錄開始時間
echo "===== BOM ETL 排程啟動 ($ETL_TYPE) - $(date '+%Y-%m-%d %H:%M:%S') =====" | tee -a $LOG_FILE

# 切換到 ETL 專案目錄
cd /home/ETL/etl

# 確保虛擬環境存在
if [ ! -d "venv" ]; then
    echo "建立虛擬環境..." | tee -a $LOG_FILE
    python3 -m venv venv
fi

# 啟動虛擬環境
source venv/bin/activate

# 確保安裝所需套件
pip install -r requirements.txt >> $LOG_FILE 2>&1

# 執行 BOM ETL 處理
echo "開始執行 BOM ETL 處理 ($ETL_TYPE)..." | tee -a $LOG_FILE

if [ "$ETL_TYPE" = "daily" ]; then
    python bom_etl.py --daily >> $LOG_FILE 2>&1
    ETL_STATUS=$?
elif [ "$ETL_TYPE" = "monthly" ]; then
    python bom_etl.py --monthly >> $LOG_FILE 2>&1
    ETL_STATUS=$?
fi

# 檢查執行結果
if [ $ETL_STATUS -eq 0 ]; then
    echo "BOM ETL 處理成功完成！" | tee -a $LOG_FILE
    ETL_RESULT="成功"
    STATUS_CLASS="success"
else
    echo "BOM ETL 處理失敗，錯誤碼: $ETL_STATUS" | tee -a $LOG_FILE
    ETL_RESULT="失敗"
    STATUS_CLASS="error"
fi

# 產生執行報告
echo "產生 BOM ETL 執行報告..." | tee -a $LOG_FILE

# 取得資料統計資訊
CURRENT_TIME=$(date '+%Y-%m-%d %H:%M:%S')
HOST_NAME=$(hostname)
USER_NAME=$(whoami)

# 從日誌中提取關鍵資訊
PROCESSED_ROWS=$(grep -o "共處理 [0-9]* 筆資料" $LOG_FILE | tail -1 | grep -o "[0-9]*" || echo "0")
START_TIME=$(grep "BOM ETL 程序啟動" $LOG_FILE | tail -1 | cut -d'-' -f2- | tr -d ' ')
END_TIME=$(grep "BOM ETL 執行完成" $LOG_FILE | tail -1 | cut -d'-' -f2- | tr -d ' ')

# 創建文字報告
cat > $REPORT_FILE << EOF
===========================================
BOM ETL 執行報告 ($ETL_TYPE)
===========================================

執行資訊:
- 執行時間: $CURRENT_TIME
- 執行類型: $ETL_TYPE
- 執行狀態: $ETL_RESULT
- 處理資料筆數: $PROCESSED_ROWS
- 主機名稱: $HOST_NAME
- 執行用戶: $USER_NAME

目標資料表: BOM_Expanded

執行詳細記錄:
-------------------------------------------
$(tail -50 $LOG_FILE)
-------------------------------------------

報告產生時間: $CURRENT_TIME
===========================================
EOF

# 創建 HTML 報告
cat > $HTML_REPORT << EOF
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>BOM ETL 執行報告 ($ETL_TYPE) - $CURRENT_TIME</title>
    <style>
        body { 
            font-family: Arial, sans-serif; 
            margin: 0; 
            padding: 20px; 
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1000px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        .header h1 {
            margin: 0;
            font-size: 24px;
        }
        .summary {
            background-color: #f9f9f9;
            padding: 15px;
            border-left: 5px solid #4CAF50;
            margin-bottom: 20px;
        }
        .summary.error {
            border-left-color: #f44336;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        .stat-card {
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            text-align: center;
            border: 1px solid #e9ecef;
        }
        .stat-card h3 {
            margin: 0 0 10px 0;
            color: #495057;
            font-size: 14px;
        }
        .stat-card .value {
            font-size: 24px;
            font-weight: bold;
            color: #007bff;
        }
        pre {
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 5px;
            overflow: auto;
            font-family: Consolas, monospace;
            font-size: 12px;
            max-height: 400px;
        }
        .status-badge {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: bold;
            color: white;
            font-size: 14px;
        }
        .status-success {
            background-color: #28a745;
        }
        .status-error {
            background-color: #dc3545;
        }
        .footer {
            margin-top: 30px;
            text-align: center;
            font-size: 12px;
            color: #6c757d;
            border-top: 1px solid #dee2e6;
            padding-top: 15px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>BOM ETL 執行報告 ($ETL_TYPE)</h1>
            <p>執行時間: $CURRENT_TIME</p>
        </div>
        
        <div class="summary ${STATUS_CLASS}">
            <h2>執行摘要</h2>
            <p><strong>執行狀態:</strong> 
                <span class="status-badge status-${STATUS_CLASS}">$ETL_RESULT</span>
            </p>
            <p><strong>ETL 類型:</strong> $ETL_TYPE</p>
            <p><strong>目標資料表:</strong> BOM_Expanded</p>
        </div>
        
        <div class="stats-grid">
            <div class="stat-card">
                <h3>處理資料筆數</h3>
                <div class="value">$PROCESSED_ROWS</div>
            </div>
            <div class="stat-card">
                <h3>執行主機</h3>
                <div class="value" style="font-size: 16px;">$HOST_NAME</div>
            </div>
            <div class="stat-card">
                <h3>執行用戶</h3>
                <div class="value" style="font-size: 16px;">$USER_NAME</div>
            </div>
            <div class="stat-card">
                <h3>執行狀態</h3>
                <div class="value" style="font-size: 16px;">$ETL_RESULT</div>
            </div>
        </div>
        
        <h2>執行記錄詳細資訊</h2>
        <pre>$(tail -100 $LOG_FILE)</pre>
        
        <div class="footer">
            <p>BOM ETL 系統 - 自動生成報告</p>
            <p>如需協助，請聯絡 IT 部門</p>
        </div>
    </div>
</body>
</html>
EOF

# 更新最新報告連結
echo "更新最新報告連結..." | tee -a $LOG_FILE
cp $REPORT_FILE $LATEST_REPORT || echo "無法複製到 $LATEST_REPORT" | tee -a $LOG_FILE
cp $HTML_REPORT $LATEST_HTML || echo "無法複製到 $LATEST_HTML" | tee -a $LOG_FILE

# 顯示報告位置
echo "BOM ETL 執行報告已生成:" | tee -a $LOG_FILE
echo "- 文字報告: $REPORT_FILE" | tee -a $LOG_FILE
echo "- HTML報告: $HTML_REPORT" | tee -a $LOG_FILE
echo "- 最新報告連結: $LATEST_REPORT" | tee -a $LOG_FILE
echo "- 最新HTML連結: $LATEST_HTML" | tee -a $LOG_FILE

# 清理舊日誌檔案 (保留30天)
if [ -d "$LOG_DIR" ]; then
    find $LOG_DIR -name "bom_etl_*.log" -mtime +30 -delete
fi
if [ -d "$REPORT_DIR" ]; then
    find $REPORT_DIR -name "bom_etl_report_*.txt" -mtime +30 -delete
    find $REPORT_DIR -name "bom_etl_report_*.html" -mtime +30 -delete
fi

# 記錄結束時間
echo "===== BOM ETL 排程結束 ($ETL_TYPE) - $(date '+%Y-%m-%d %H:%M:%S') =====" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

# 退出虛擬環境
deactivate

# 根據執行結果退出
if [ $ETL_STATUS -eq 0 ]; then
    exit 0
else
    exit 1
fi
