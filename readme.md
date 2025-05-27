# BOM ETL SQL 檔案設定指引

## 檔案結構

請在 ETL 目錄下建立以下 SQL 檔案：

```
/home/ETL/etl/
├── bom_etl.py
├── bom_scheduler.sh
├── bom_cleanup.sh
├── BOM階層與用量(前一天異動) v1.sql    # 每日增量查詢
└── BOM階層與用量 v1.sql                # 每月完整查詢
```

## SQL 檔案內容

### 1. BOM階層與用量(前一天異動) v1.sql
將您提供的 `BOM階層與用量(前一天異動) v1.txt` 內容複製到此檔案中

### 2. BOM階層與用量 v1.sql  
將您提供的 `BOM階層與用量 v1.txt` 內容複製到此檔案中

## 建立檔案的指令

```bash
# 進入 ETL 目錄
cd /home/ETL/etl

# 建立每日增量 SQL 檔案
cat > "BOM階層與用量(前一天異動) v1.sql" << 'EOF'
-- 將 BOM階層與用量(前一天異動) v1.txt 的內容貼上到這裡
EOF

# 建立每月完整 SQL 檔案  
cat > "BOM階層與用量 v1.sql" << 'EOF'
-- 將 BOM階層與用量 v1.txt 的內容貼上到這裡
EOF

# 設定執行權限
chmod +x bom_scheduler.sh
chmod +x bom_cleanup.sh
```

## 測試執行

### 測試每日增量 ETL
```bash
cd /home/ETL/etl
source venv/bin/activate
python bom_etl.py --daily --debug
```

### 測試每月完整 ETL
```bash
cd /home/ETL/etl  
source venv/bin/activate
python bom_etl.py --monthly --debug
```

### 測試排程腳本
```bash
# 測試每日腳本
./bom_scheduler.sh daily

# 測試每月腳本
./bom_scheduler.sh monthly
```

## 設定排程

```bash
# 編輯 crontab
crontab -e

# 加入以下內容：
0 1 * * * /home/ETL/etl/bom_scheduler.sh daily >> /home/ETL/logs/bom/cron_daily.log 2>&1
0 2 1 * * /home/ETL/etl/bom_scheduler.sh monthly >> /home/ETL/logs/bom/cron_monthly.log 2>&1
0 3 * * 0 /home/ETL/etl/bom_cleanup.sh >> /home/ETL/logs/bom/cron_cleanup.log 2>&1
```

## 監控和維護

### 檢查執行狀態
```bash
# 檢查最新的執行記錄
tail -f /home/ETL/logs/bom/bom_etl_$(date +%Y%m%d).log

# 檢查 cron 執行記錄
tail -f /home/ETL/logs/bom/cron_daily.log
tail -f /home/ETL/logs/bom/cron_monthly.log
```

### 查看報告
```bash
# 檢視最新的文字報告
cat /home/ETL/reports/bom/bom_latest_report.txt

# 檢視最新的 HTML 報告
firefox /home/ETL/reports/bom/bom_latest_report.html
```

## 資料庫結構

此 ETL 會自動建立 `BOM_Expanded` 表，包含以下欄位：
- 所有原始 BOM 查詢的欄位
- `RunTime` - 執行時間戳記 (自動添加)

## 注意事項

1. **資料寫入模式**: 使用 `APPEND` 模式，不會清除舊資料
2. **資料清理**: 建議定期清理超過 90 天的舊資料
3. **監控**: 定期檢查執行日誌和報告
4. **備份**: 重要資料建議額外備份
