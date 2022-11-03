# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
from Python.MAIN_FUNC import lib
from Python.MAIN_FUNC import main_actions
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit

app = lib.Flask(__name__, template_folder='web_service', static_folder='web_service')


@app.route("/", methods=['GET'])
def index():
    if lib.request.method == 'GET':
        return lib.render_template('index.html')
    elif lib.request.method == 'POST':
        if lib.request.form['submit_button'] == 'LOGIN':
            var_login = lib.request.form['login']
            var_pass = lib.request.form['password']
            if var_login == 'TOROSYANKK' and var_pass == '1':
                return lib.render_template('index.html')
            else:
                return 'Ошибка входа!'


@app.route("/", methods=['POST','GET'])
def button_run():
    print(lib.request.method)
    if lib.request.method == 'POST':
        if lib.request.form['button_run'] == 'run_full':
            var_stock = lib.request.form['Stock']
            var_batch = lib.request.form['Batch']
            main_class.execute(idProcessFunc = 'InsertData', var_batch=int(var_batch), list_tiket= str(var_stock))
            return lib.render_template('index.html')
        elif lib.request.form['button_run'] == 'run_delete':
            var_stock_del = lib.request.form['Stock_delete']
            if var_stock_del != '':
                main_class.delete_stock(idProcessFunc='DeleteData', stock_name=var_stock_del)
            elif 'Stock_delete' in lib.request.form:
                main_class.delete_stock(idProcessFunc='DeleteAllData', stock_name=var_stock_del, all_delete=True)
            return lib.redirect("/")
        elif lib.request.form['button_run'] == 'run_insertmain':
            main_class.executeNasdaq()
            return lib.redirect("/")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main_class = main_actions.ACTIONS()
    #main_class.executeNasdaq()
    scheduler = BackgroundScheduler()
    scheduler.start()
    scheduler.add_job(
        func=main_class.delta_loading,
        trigger=IntervalTrigger(hours=1),
        id='printing_time_job',
        name='Data loading',
        replace_existing=True)
    # Shut down the scheduler when exiting the app
    atexit.register(lambda: scheduler.shutdown())

    app.run(debug='True')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
