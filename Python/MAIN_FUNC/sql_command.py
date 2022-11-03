# For full loading
insert_historydata = 'insert into "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.HistoryData"' \
                 '("Date","StockName", "Open","High", "Low", "Close", "Volume") ' \
                 'VALUES (%s)'
insert_actiondata = 'insert into "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.Actions"' \
                 '("Date", "StockName", "Dividends","Stock Splits") ' \
                 'VALUES (%s)'
insert_earningdata = 'insert into "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.EarningHistory" ' \
                    '("EarningDate", "StockName", "EpsEstimate", "EpsReported", "Surprise")  ' \
                     'values(%s)'
insert_recomenddata = 'insert into "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.Recommendation" ' \
                    '("Date", "StockName", "Firm", "ToGrand", "FromGrand", "Action")' \
                     'VALUES (%s)'

insert_newsdata = 'insert into "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.News" ' \
                  '("UID", "Title", "Publisher", "LINK", "PublishTime", "Type", "RelatedTickets", "Content")' \
                  'VALUES (%s)'
# Update for delta!
update_historydata = 'UPSERT  "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.HistoryData"' \
                 '("Date","StockName", "Open","High", "Low", "Close", "Volume") ' \
                 'VALUES (%s) WITH PRIMARY KEY'

update_actiondata = 'UPSERT  "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.Actions"' \
                 '("Date", "StockName", "Dividends","Stock Splits") ' \
                 'VALUES (%s) WITH PRIMARY KEY'

update_earndata = 'UPSERT  "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.EarningHistory"' \
                 '("EarningDate", "StockName", "EpsEstimate", "EpsReported", "Surprise") ' \
                 'VALUES (%s) WITH PRIMARY KEY'

update_recommenddata = 'UPSERT  "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.Recommendation"' \
                 '("Date", "StockName", "Firm", "ToGrand", "FromGrand", "Action") ' \
                 'VALUES (%s) WITH PRIMARY KEY'

update_nasdaqdata = 'UPSERT  "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Nasdaq.BasicDataStock"' \
                 '("StockName", "FullName", "Country", "Sector", "Industry", "UrlNasdaq") ' \
                 'VALUES (%s) WITH PRIMARY KEY'
update_stockinfo = 'UPSERT  "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.StockInfo"' \
                 '("StockName", "Info") ' \
                 'VALUES (%s) WITH PRIMARY KEY'

update_newsdata = 'UPSERT "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.News" ' \
                  '("UID", "Title", "Publisher", "LINK", "PublishTime", "Type", "RelatedTickets", "Content")' \
                  'VALUES (%s) WITH PRIMARY KEY'
#
delete_historydata   = ' DELETE FROM "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.HistoryData"'
delete_actiondata    = ' DELETE FROM "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.Actions"'
delete_earningdata   = ' DELETE FROM "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.EarningHistory"'
delete_recommenddata = 'DELETE FROM "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.Recommendation" '
delete_newsdata      = 'DELETE FROM "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.News" '



insert_log = 'insert into "STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Tech.Logs"'\
                 '("User", "Action","Errors", "Timestamp") VALUES (%s)'

setKeys_command = 'select max("Date"), "StockName" FROM ' \
                  '"STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.HistoryData"' \
                  'GROUP BY "StockName";'

checkStocksInfo = 'SELECT DISTINCT "StockName" from ' \
                  '"STOCKS_INTEGRATION_STOCKS_INTEGRATION_HDI_CONTAINER_1"."Yahoo.StockInfo"'