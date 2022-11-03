import math

from Python.MAIN_FUNC import lib


def get_data( ticket_string ) -> lib.pd.DataFrame :
    hist_data = lib.pd.DataFrame(lib.yf.Ticker(ticket_string).history(period="max")).reset_index()
    hist_data['Date'] = hist_data['Date'].apply(lambda x: str(x.date()).replace('-',''))
    hist_data['STOCK_NAME'] = ticket_string
    return hist_data

def add_timestamp(main_array):
    tmp_stamp = lib.np.array([lib.datetime.datetime.now().strftime("%Y%m%d%H%M%S")])
    return lib.np.hstack((main_array, lib.np.atleast_2d([tmp_stamp] * len (main_array))))

def generator_data( batch_get = 1000, list_tiket = 'MSFT' ):
    hist = get_data(ticket_string= list_tiket )
    batch_size = 0
    iteration_batch = math.ceil(len(hist)/batch_get)
    for i in range(iteration_batch):
        batch_data = add_timestamp(hist.values[batch_size : batch_size + batch_get ])
        yield batch_data
        batch_size += batch_get