import pandas as pd

from Python.MAIN_FUNC.lib import *
from Python.MAIN_FUNC.sql_command import *
from Python.HANA import connect_hana
from Python.MAIN_FUNC.param import *
from Python.MAIN_FUNC.NasdaqParam import headers, params

class MAIN_CLASS:

    def __init__(self):
        self.NasdaqHeader = headers
        self.NasdaqParam = params
        self.NasdaqStocks: pd.Series
        self.SqlCommandStrockCheker = checkStocksInfo
        self.connection = connect_hana.get_connection()

    def SqlExecution (self, command: str):
        cursor = self.connection.cursor()
        cursor.execute(command)
        rows, column_headers = cursor.fetchall(), [i[0] for i in cursor.description]
        self.connection.commit()
        return [row[0] for row in rows] , column_headers

    def get_nasdaq(self, batch_get: int = 1000):

        header_stocks = ['symbol', 'name','country','sector','industry', 'url']
        requests_stocks = requests.get('https://api.nasdaq.com/api/screener/stocks',
                                       headers=self.NasdaqHeader, params=self.NasdaqParam)
        NasdaqJson = requests_stocks.json()['data']
        NasdaqData = pd.DataFrame(NasdaqJson['rows'], columns=NasdaqJson['headers'])[header_stocks].rename(
            columns={"symbol":"StockName","name": "FullName",
                     "country":"Country","sector": "Sector",
                     "industry":"Industry", "url": "UrlNasdaq"})
        self.NasdaqStocks = NasdaqData["StockName"]
        # Get batch
        batch_size = 0
        iter_batch_hist = math.ceil(len(NasdaqData) / batch_get)
        for i in range(iter_batch_hist):
            dataBatchNasdaq = NasdaqData.values[batch_size:batch_size + batch_get]
            yield dataBatchNasdaq
            batch_size += batch_get

    def getInfo(self, checkList = True):

        def split(a, n):
            k, m = divmod(len(a), n)
            return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))
        if checkList == True:
            rowsDB, _ = self.SqlExecution(command= self.SqlCommandStrockCheker)
            NasdaqStocksChecker = [x for x in self.NasdaqStocks if x not in rowsDB]
        else:
            NasdaqStocksChecker = self.NasdaqStocks

        for SeriasStock in split(NasdaqStocksChecker, 3000):
            InfoList = list()
            for Stock in SeriasStock:
                try:
                    InfoList.append([Stock, yf.Ticker(Stock).info['longBusinessSummary']])
                except:
                    InfoList.append([Stock, 'No data'])
            yield


    def get_data(
        self,
        ticket_string,
        start_date,
        end_date,
        ) -> pd.DataFrame:

        ##### Earning data expansion
        try:
            EarningData = pd.DataFrame(yf.Ticker(ticket_string).earnings_history).reset_index()
            EarningData = EarningData.rename(
                columns={"Symbol":"StockName","Earnings Date": "EarningDate",
                         "EPS Estimate":"EpsEstimate","Reported EPS": "EpsReported",
                         "Surprise(%)":"Surprise"}
                                            ).drop(columns= "Company")[["EarningDate", "StockName",
                                                                         "EpsEstimate", "EpsReported", "Surprise"]
                                                                      ].fillna(0)
        except:
            EarningData = pd.DataFrame()

        ##### Earning data expansion
        try:
            RecommendData = pd.DataFrame(yf.Ticker(ticket_string).get_recommendations()).reset_index()
            RecommendData['StockName'] = ticket_string
            RecommendData = RecommendData.rename(
                columns={"To Grade": "ToGrand", "From Grade": "FromGrand"})[
                                    ["Date", "StockName", "Firm", "ToGrand", "FromGrand", "Action"]].fillna(0)
        except:
            RecommendData = pd.DataFrame()

        # News
        try:
            NewsData = pd.DataFrame(yf.Ticker(ticket_string).news)[
             ['uuid', 'title', 'publisher', 'link', 'providerPublishTime', 'type', 'relatedTickers']].fillna('')
            NewsData['relatedTickers'] = NewsData['relatedTickers'].apply(lambda x: ' '.join(x))
            try:
                NewsData['Content'] = NewsData['link'].apply(lambda x: requests.get(x).content)
            except:
                NewsData['Content'] = NewsData['link'].apply(lambda x: 'Can not get access to html url')
        except:
            NewsData = pd.DataFrame()

        ##### Main data for history and actions tables
        data = pd.DataFrame(yf.Ticker(ticket_string).history(period='max'
                                                            )).reset_index()
        # Check for delta loading
        if start_date != 'max':
            data = data[(data['Date'] >= start_date) & (data['Date'] <= end_date)]

        data['Date'] = data['Date'].apply(lambda x: str(x.date()).replace('-', ''))
        data['StockName'] = ticket_string
        HistoryData = data[['Date', 'StockName', 'Open', 'High', 'Low', 'Close', 'Volume']]

        ActionsData = data[['Date', 'StockName', 'Dividends',
                           'Stock Splits']].loc[((data['Dividends']
                != 0) | (data['Stock Splits'] != 0))]

        return HistoryData, ActionsData, EarningData, RecommendData, NewsData

    def check_data(
        self,
        ticket_string,
        start_date,
        end_date,
        ) -> pd.DataFrame:
        #### expansion new tables here!
        hist_data = pd.DataFrame()
        action_data = pd.DataFrame()
        earning_data = pd.DataFrame()
        recommend_data = pd.DataFrame()
        news_data = pd.DataFrame()
        ################################

        check_len = len(ticket_string.split(' '))
        if check_len == 1:
            # Add new table here from function
            hist_data, action_data, earning_data, recommend_data, news_data = self.get_data(ticket_string,
                    start_date, end_date)
        elif check_len > 1:
            for stock in ticket_string.split(' '):
                ltHist, ltAct, ltEarn, ltRecomm, ltNews = self.get_data(stock, start_date,
                        end_date)
                hist_data = pd.concat([hist_data, ltHist])
                action_data = pd.concat([action_data, ltAct])
                earning_data = pd.concat([earning_data, ltEarn])
                recommend_data = pd.concat([recommend_data, ltRecomm])
                news_data = pd.concat([news_data, ltNews])
        return hist_data, action_data, earning_data, recommend_data, news_data

    def add_timestamp(self, main_array):
        tmp_stamp = \
            np.array([datetime.datetime.now().strftime('%Y%m%d%H%M%S')])
        return np.hstack((main_array, np.atleast_2d([tmp_stamp]
                         * len(main_array))))

    def generator_data(
        self,
        batch_get: int = 100,
        list_tiket='MSFT',
        start_date ='max',
        end_date ='',
        ):
        hist_data, action_data, earn_data, recomm_data, news_data = \
            self.check_data(ticket_string=list_tiket,
                            start_date = start_date, end_date=end_date)
        batch_size = 0
        maxLen = np.max([len(hist_data),len(action_data),
                        len(earn_data),len(recomm_data)])
        iter_batch_hist = math.ceil(maxLen / batch_get)
        for i in range(iter_batch_hist):

            # Change 21092022 reasone: change structure table
            # batch_data = self.add_timestamp(hist.values[batch_size: batch_size + batch_get])

            dataBatchHist = hist_data.values[batch_size:batch_size
                + batch_get]
            dataBatchAct = action_data.values[batch_size:batch_size
                + batch_get]
            dataBatchEarn = earn_data.values[batch_size:batch_size
                + batch_get]
            dataBatchRecommend= recomm_data.values[batch_size:batch_size
                + batch_get]
            dataBatchNews= news_data.values[batch_size:batch_size
                + batch_get]
            yield dataBatchHist, dataBatchAct, dataBatchEarn, dataBatchRecommend, dataBatchNews
            batch_size += batch_get


class ACTIONS():

    def __init__(self, set_connection = connect_hana.get_connection(),
                       input_class = MAIN_CLASS()):
        self.connection = set_connection
        self.main_class = input_class
        self.errors_container = ''

    # Decorator time
    def decorator(func):
        #@functools.wraps(func)
        def _wrapper(self, *args, **kwargs):
            start = time.perf_counter()
            result = func(self, *args, **kwargs)
            self.decorator_logo(name_func=func, func_arg=kwargs)
            runtime = time.perf_counter() - start
            print(f"{func.__name__} took {runtime:.4f} secs")
        return _wrapper


    def decorator_logo(self, name_func, func_arg, insert_log=insert_log):
        param_list = ((
            str(os.environ['HANA_USER']), str(name_func.__name__) + "/Params:" + str(func_arg),
            str(self.errors_container), datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        ),('Parametrs for logs'))
        parms = self.get_param_Forsql(param_list)
        try:
            self.connection.cursor().execute(insert_log % parms + ';', param_list[0])
            self.errors_container = ''
        except Exception as e:
            print(e)
        try:
            self.connection.commit()
        except:
            print("Error commit!")


    def get_param_Forsql(self, param_tuple: tuple ):
        try:
            parms = ("?," * len(param_tuple[0]))[:-1]
        except Exception as e:
            parms = tuple()
            #print('Params error:' + str(e))
        return parms


    @decorator
    def execute(self,
                    idProcessFunc: str = 'InsertData',
                    SqlCommand_historydata  = insert_historydata,
                    SqlCommand_actiondata   = insert_actiondata,
                    SqlCommand_earningdata  = insert_earningdata,
                    SqlCommand_recomenddata = insert_recomenddata,
                    SqlCommand_newsdata = insert_newsdata,
                    var_batch=1000,
                    start_date = 'max',
                    end_date = '',
                    list_tiket=''
                    ):
        for hist, act, earn, recommend, news in self.main_class.generator_data(batch_get=var_batch, list_tiket=list_tiket,
                                                        start_date = start_date, end_date = end_date):
            list_hist, list_act, list_earn, list_recommd, list_news = \
                tuple(map(tuple, hist)), tuple(map(tuple, act)), \
                tuple(map(tuple, earn)),tuple(map(tuple, recommend)),tuple(map(tuple, news))

            parms_hist, parms_act, parms_earn, parms_recommd, parms_news  = \
                self.get_param_Forsql(list_hist), self.get_param_Forsql(list_act),\
                self.get_param_Forsql(list_earn), self.get_param_Forsql(list_recommd), \
                self.get_param_Forsql(list_news)


            # Try for History data
            if parms_hist != tuple():
                try:
                    self.connection.cursor().executemany(SqlCommand_historydata % (parms_hist) + ';', list_hist)
                except Exception as e:
                    self.errors_container = e
                #print(e)
            # Try for Action data
            if parms_act != tuple():
                try:
                    self.connection.cursor().executemany(SqlCommand_actiondata % (parms_act) + ';', list_act)
                except Exception as e:
                    self.errors_container = e
                    #print(e)
            if parms_earn != tuple():
                try:
                    self.connection.cursor().executemany(SqlCommand_earningdata % (parms_earn) + ';', list_earn)
                except Exception as e:
                    self.errors_container = e
                    #print(e)

            if parms_recommd != tuple():
                try:
                    self.connection.cursor().executemany(SqlCommand_recomenddata % (parms_recommd) + ';', list_recommd)
                except Exception as e:
                    self.errors_container = e
                    #print(e)

            if parms_news != tuple():
                try:
                    self.connection.cursor().executemany(SqlCommand_newsdata % (parms_news) + ';', list_news)
                except Exception as e:
                    self.errors_container = e
                    print(e)
        try:
            self.connection.commit()
        except Exception as e:
            self.errors_container = e
            print(e)

    @decorator
    def executeNasdaq(self,idProcessFunc: str = 'InsertNasdaqAllStocks',
                           SqlCommand_Nasdaqdata  = update_nasdaqdata,
                           SqlCommandStockInfo = update_stockinfo):
        for nasdaq in self.main_class.get_nasdaq():
            list_nasdaq = tuple(map(tuple, nasdaq))
            parms_nasdaq = self.get_param_Forsql(list_nasdaq)
            try:
                self.connection.cursor().executemany(SqlCommand_Nasdaqdata % (parms_nasdaq) + ';', list_nasdaq)
            except Exception as e:
                print(e)
                self.errors_container = e

        background_thread = Thread(target=self.stock_info, args=(SqlCommandStockInfo,))
        background_thread.start()

    def stock_info(self, SqlCommandStockInfo: str):
        for stockInfo in self.main_class.getInfo():
            list_info = tuple(map(tuple, stockInfo))
            parms_info = self.get_param_Forsql(list_info)
            try:
                self.connection.cursor().executemany(SqlCommandStockInfo % (parms_info) + ';', list_info)
                self.connection.commit()
            except Exception as e:
                print(e)
                self.errors_container = e


    @decorator
    def delete_stock(self, idProcessFunc = '',
                     stock_name = '',
                     SqlCommand_delete = [
                                          delete_historydata, delete_actiondata,
                                          delete_earningdata, delete_recommenddata, delete_newsdata
                                          ],
                     all_delete =False):
        for command in SqlCommand_delete:
            if all_delete == False:
                stock_name = 'WHERE "StockName" = \'%s\'' %stock_name if stock_name == '' else stock_name
                try:
                    self.connection.cursor().executemany(command + stock_name + ';')
                except Exception as e:
                    print(e)
            else:
                try:
                    self.connection.cursor().executemany(command  + ';')
                except Exception as e:
                    print(e)
        if all_delete == True:
            print('Успешное полное удаление')
        else:
            print('Успешное удаление: ' + stock_name)

    def getDelta_param(self, etKeys_command = setKeys_command):
        try:
            local_cursor = self.connection.cursor()
            local_cursor.execute(setKeys_command)
            rows = pd.DataFrame(local_cursor.fetchall())
            local_cursor.close()

            list_stock, start_date, end_date = list(rows[1]), \
                                               rows[0].min() - datetime.timedelta(days=1),\
                                               datetime.datetime.now().date()
            list_stock = ' '.join(list_stock)
            return  list_stock, str(start_date), str(end_date)

        except Exception as e:
            self.errors_container = e
            print(e)

    def SqlExecution (self, command: str):
        cursor = self.connection.cursor()
        cursor.execute(command)
        rows, column_headers = cursor.fetchall(), [i[0] for i in cursor.description]
        self.connection.commit()
        return rows, column_headers

        #@decorator
    def delta_loading(self,
                      sqlCommand_history = update_historydata,
                      sqlCommand_Action = update_actiondata,
                      sqlCommand_Earning = update_earndata,
                      sqlCommand_Recommend = update_recommenddata,
                      sqlCommand_news = update_newsdata):
        list_tiket, start_date, end_date = self.getDelta_param()
        self.execute(    idProcessFunc = 'DeltaLoading',
                         list_tiket=list_tiket, start_date=start_date, end_date=end_date,
                         SqlCommand_historydata = sqlCommand_history,
                         SqlCommand_actiondata = sqlCommand_Action,
                         SqlCommand_earningdata= sqlCommand_Earning,
                         SqlCommand_recomenddata= sqlCommand_Recommend,
                         SqlCommand_newsdata=  sqlCommand_news)
