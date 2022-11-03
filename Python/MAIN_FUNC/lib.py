import yfinance as yf
from hdbcli import dbapi
import pandas as pd
import numpy as np
import datetime
from multiprocessing import Process
import functools
import time
import math
from flask import Flask, render_template, Response, request, redirect, url_for
import os
import requests
import html5lib
from threading import Thread
#import BeautifulSoup4
import requests