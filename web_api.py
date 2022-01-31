

import asyncio
from cmath import log
import json
import logging
import re
import sys
import time
from enum import IntEnum
from threading import Thread

import dateutil.parser
import finplot as fplt
import pandas as pd
import requests

from api_context import Context

LOG_FILE = r'logs/stream_log.txt'


class eBarStatus(IntEnum):
	Unknown = 0
	New = 1
	Real_Time = 2
	Historical = 4
	Standard_Close = 8
	End_Of_Session_Close = 16
	Ghost_Bar = 268435456
	End_Of_History_Stream = 536870912





def is_bar_status(bar_status, mask):
	return (bar_status & mask) > 0

# def getBarStatuses(bar_status):
# 	res = {}
# 	mask = 1
# 	for _ in range(4):
# 		if is_bar_status(bar_status, mask):
# 			res[eBarStatus(mask)] = True
# 		mask*=2
# 	return res

class Plotter():
	def __init__(self, symbol, data_host):
		self.plots = []
		self.data_host = data_host
		#fplt.create_plot(f"{symbol}", init_zoom_periods=75, maximize=True)
		fplt.create_plot(f"{symbol}",init_zoom_periods=100, maximize=True)
		self.update_plot()
		def upd():
			try:
				if self.data_host.changed:
					self.update_plot()
					self.data_host.changed = False
			except Exception as ex:
				print(ex)

		#fplt.timer_callback(lambda : self.update_plot(self), 0.5) # update in 2 Hz
		fplt.timer_callback(upd, 0.1) # update in 2 Hz
		fplt.show()

	def update_plot(self):
		df = self.data_host.df
		if df is not None:
			candlesticks = df[["DateTime", "Open", "Close", "High", "Low"]]
			if not self.plots: # 1st time
				candlestick_plot = fplt.candlestick_ochl(candlesticks)
				self.plots.append(candlestick_plot)
				# use bitmex colors
				candlestick_plot.colors.update(dict(
						bull_shadow = '#388d53',
						bull_frame  = '#205536',
						bull_body   = '#52b370',
						bear_shadow = '#d56161',
						bear_frame  = '#5c1a10',
						bear_body   = '#e8704f'))
			else: # update
				if self.data_host.changed:
					self.plots[0].update_data(candlesticks)



class Stream:

	status_bitmap = {
		0: 'NEW',
		1: 'REAL_TIME_DATA',
		2: 'HISTORICAL_DATA',
		3: 'STANDARD_CLOSE',
		4: 'END_OF_SESSION_CLOSE',
		5: 'UPDATE_CORPACTION',
		6: 'UPDATE_CORRECTION',
		7: 'ANALYSIS_BAR',
		8: 'EXTENDED_BAR',
		19: 'PREV_DAY_CORRECTION',
		23: 'AFTER_MARKET_CORRECTION',
		24: 'PHANTOM_BAR',
		25: 'EMPTY_BAR',
		26: 'BACKFILL_DATA',
		27: 'ARCHIVE_DATA',
		28: 'GHOST_BAR',
		29: 'END_OF_HISTORY_STREAM',
	}

	def __init__(self, context, client, symbol, bar_type, interval=5, days_back=5, session_template="Default", update_intrabar=True):
		logging.info("Initializing Stream")
		self.client = client
		self.context = context
		self.symbol = symbol
		self.bar_type = bar_type
		self.interval = interval
		self.days_back = days_back
		self.session_template = session_template
		self.update_intrabar = update_intrabar
		self.df = None
		self.data = []
		self._plotter = None
		#self.bar_status = None
		# self.close = None
		# self.volume = None
		# self.time = None
		# self.closing_prices = []
		# self.closing_times = []
		# self.closing_volumes = []
		self.url = self.getStreamUrl()

	# Gets streaming url based on instance attributes
	def getStreamUrl(self):

		access_token =self.context.refreshAccessToken()

		if 'tick' in self.bar_type.lower():
			# 10 is the highest number of ticks back
			url = Context.API_BASE_URL + \
				f"/stream/tickbars/{self.symbol}/{str(self.interval)}/10?access_token={access_token}&heartbeat=true"
		else:
			url = Context.API_BASE_URL + \
				f"/stream/barchart/{self.symbol}/{str(self.interval)}/{self.bar_type.capitalize()}?SessionTemplate={self.session_template}&daysBack={str(self.days_back)}&access_token={access_token}&heartbeat=true"

		return url

	def stream(self, on_data):
		self.context.refreshAccessToken()	
		r : requests.Response = requests.get(self.url, stream=True)
		if r.status_code != 200:
			raise Exception("Could not retrieve stream")

		logging.debug("stream has been received")
		try:
			for line in r.iter_lines():
				if line:
					if len(line) != 0:
						try:
							decoded_line = line.decode('utf-8')

							#print(decoded_line)

							data = json.loads(decoded_line)

							if "Status" in data:
								if data["Status"] > 13:
									st = self.getBarStatusDescription(data["Status"])
									print(st)

								if "TimeStamp" in data:
									timestamp = data['TimeStamp']
									m = re.search(r"(\d+)", timestamp)

									if m:
										data["DateTime"] = pd.to_datetime(Stream.convertEpocTime(int(m.group(0))))

										on_data(data)

						except Exception as ex:
							logging.error(ex)

					# If not hearbeat (hearbeat timestamp key has no capitalization)
										

		except Exception as e:

			logging.error(e)

			logging.warning('Restarting connection')

			self.stream(on_data)				

	# Converts an epoch number into a formatted text string
	@staticmethod
	def convertEpocTime(epoch_time):

		try:
			return time.strftime(
				"%m/%d/%Y %H:%M:%S", time.localtime(epoch_time/1000))  # Add %Z to show timezone
		except:
			return "Could not convert - {}".format(epoch_time)

	# Converts a bar status ID number into a text description
	@staticmethod
	def getBarStatusDescription(id):

		binary = "{:b}".format(id)
		description = ""

		for bit_idx, bit_val in enumerate(reversed(binary)):
			if bit_val == '1':
				description += "{} ".format(Stream.status_bitmap[bit_idx])

		return description

	# Returns true if the tick is real-time
	@staticmethod
	def isTickRealtime(id):
		binary = "{:b}".format(id)
		return binary[::-1][1] == '1'

	# Returns true if the tick is the closing tick of the bar
	@staticmethod
	def isTickClose(id):
		binary = "{:b}".format(id)
		if len(binary) >= 5:
			return binary[::-1][4] == '1' or binary[::-1][3] == '1'
		elif len(binary) == 4:
			return binary[::-1][3] == '1'
		else:
			return False

	# Returns true if the tick is the opening tick of the bar
	# Note: It is possible for this to return true on the last
	# historical bar or real-time bars
	@staticmethod
	def isTickOpen(id):
		binary = "{:b}".format(id)
		# Tick is new and not closed
		return binary[::-1][0] == '1' and not Stream.isTickClose(id)




class Client:

	def __init__(self, context, account, symbol, quantity):
		self.context = context

		self.account = account

		self.symbol = symbol.upper()

		self.quantity = int(quantity)  # integer

		self.symbol_asset_type = Client.getAssetTypeFromSymbol(self.symbol)

	def stream(self, on_data, symbol, bar_type, interval, days_back=5, session_template='Default', update_intrabar=True):

		logging.info('Calling Client.stream')

		self.context.refreshAccessToken()

		logging.info('Creating new Stream')

		stream = Stream(
			context=self.context,
			client=self,
			symbol=symbol,
			bar_type=bar_type,
			interval=interval,
			days_back=days_back,
			session_template=session_template,
			update_intrabar=update_intrabar
		)

		def start():
			stream.stream(on_data)	

		thread = Thread(target=start)
		thread.daemon = True
		thread.start()


	# Determines asset type based on symbol string
	@staticmethod
	def getAssetTypeFromSymbol(symbol):

		# If symbol has space, it's an option symbol
		if " " in symbol:
			asset_type = "OP"
		# If symbol has no space and at least one digit, it's a futures symbol
		elif any(i.isdigit() for i in symbol):
			asset_type = "FU"
		# If symbol has no spaces and no digits, it's an equity symbol
		else:
			asset_type = "EQ"

		logging.info("Client.getAssetTypeFromSymbol({}) returning asset type {}".format(
			symbol, asset_type))

		return asset_type


class Charting:
	def __init__(self, context, account, symbol):
		self.data = []
		self.df = None
		self.client = Client(context, account=account, symbol=symbol, quantity=1)		
		self.changed = False	

	def connect(self):
		def on_new_data(data):
			bar_status = data['Status'] 

			#print(data)
			changed = False

			#if history
			if self.data is not None: 
				if is_bar_status(bar_status, eBarStatus.Historical) and not is_bar_status(bar_status, eBarStatus.End_Of_History_Stream):
					if not is_bar_status(bar_status, eBarStatus.Ghost_Bar) and \
							(is_bar_status(bar_status, eBarStatus.Standard_Close) or is_bar_status(bar_status, eBarStatus.End_Of_Session_Close)) :
						self.data.append(data)
				else:
					changed = True
					sdata = self.data[-1]
					if sdata["DateTime"] != data["DateTime"] and not is_bar_status(bar_status, eBarStatus.Ghost_Bar):
						self.data.append(data)
					self.df = pd.DataFrame(self.data)
					self.df.to_csv("data/test.csv", index=False)
					self.data = None

			if self.df is not None:
				l = len(self.df)-1
				sdata = self.df.iloc[l]
				stime = sdata["DateTime"]
				sclose = sdata["Close"]
				dtime = data["DateTime"]
				close = data["Close"]
				if dtime>=stime:
					self.changed = changed
					if stime == dtime:
						self.changed = changed or sclose!=close
						colno = len(self.df.columns)
						if not is_bar_status(bar_status, eBarStatus.Ghost_Bar):
							for idx in range(colno):
								col = self.df.columns[idx]
								if col in data:
									self.df.iloc[l, idx] = data[col]
					else:
						self.changed = True	
						if not is_bar_status(bar_status, eBarStatus.Ghost_Bar):
							self.df = self.df.append(data, ignore_index = True)	

		self.client.stream(
			on_data=on_new_data,
			symbol=self.client.symbol,
			bar_type="Minute",
			interval=1,
			days_back=1,
			update_intrabar=True
		)

		self._plotter = Plotter(self.client.symbol, self) 


async def main():
	logging.basicConfig(
		filename=LOG_FILE,
		format='%(levelname)s - %(asctime)s - %(message)s',
		datefmt='%d-%b-%y %H:%M:%S',
		level=logging.DEBUG
	)

	ctx = Context()
	#ctx.initialize(application)
	await ctx.initialize()

	charting = Charting(ctx, "", "@ES")

	charting.connect()

	# client = Client(ctx, account="XXXXXX", symbol="@ES", quantity=1)

	# client.stream(
	# 	on_data=on_data,
	# 	symbol=client.symbol,
	# 	bar_type="Minute",
	# 	interval=1,
	# 	days_back=1,
	# 	update_intrabar=True
	# )



if __name__ == '__main__':
	asyncio.run(main())
	



