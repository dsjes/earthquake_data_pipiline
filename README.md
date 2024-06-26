# earthquake_data_pipiline


### 此專案欲搜集地震、災害資料，進行資料處理，最後以視覺化報表呈現

### Data Pipeline 架構
![截圖 2024-04-13 下午1 15 17](https://github.com/dsjes/earthquake_data_pipiline/assets/106303589/613ddc57-b574-43f4-bbff-963ba7e00c72)

### 特色
1. 使用 docker 虛擬環境開發
2. 以 AWS S3 當作 Data Lake，存放 Source Data
3. 以 Airflow 為排程工具，Airlfow 相關設定存放於 PostgresSQL 中
4. 預計以 PostgresSQL 當作 Data Warehouse，存放整理過後的資料
5. 預計以 Tableau 呈現視覺化報表

### 資料來源
1. 中央氣象署開放資料平臺
2. 第二階段預計加入 https://alerts.ncdr.nat.gov.tw/indexHome.aspx 資料

### 說明
此專案仍在開發中，若開發完成會開放自行下載
