from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit


#class Schedule_work():
#    def __init__(self):
#         self.scheduler = BackgroundScheduler()
#
#     def start_job(func):
#         def _wrapper(self,*args, **kwargs):
#             func(self, *args, **kwargs)
#             self.scheduler.start()
#         return _wrapper
#
#     @start_job
#     def create_job(func):
#         def _wrapper(self, *args, **kwargs):
#             self.scheduler.add_job(func = func.__name__,
#                                    trigger=IntervalTrigger(seconds=2),
#                                    id   ='job'+ str(func.__name__),
#                                    name ='job' + str(func.__name__),
#                                    replace_existing=True)
#             print(self.scheduler.print_jobs())
#             return _wrapper
#
#     # Shut down the scheduler when exiting the app
#     def drop_all_job(self):
#         atexit.register(lambda: self.scheduler.shutdown())
