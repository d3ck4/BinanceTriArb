import websockets, asyncio, json, time, random
from threading import Thread
from binance.client import Client

class BnArber:
    """
    Triangular Arbitrage Bot for Binance.
    Trading Patterns:
        - USDT -> BTC -> ALTCOIN -> USDT
        - USDT -> ALTCOIN -> BTC -> USDT
    """
    def __init__(self, curs, public, secret, max_amount):
        """
        Inits an Instance of the Trader Class. Also get trading precision of each market.
        """
        self.url = "wss://stream.binance.com:9443/stream?streams=btcusdt@depth5@100ms"
        self.curs = curs
        self.data = {}
        self.timeout = False
        self.min_amount = 11
        self.max_amount = max_amount
        self.client = Client(public, secret)
        self.precision = {}
        for i in self.client.get_exchange_info()['symbols']:
            for f in i["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    if float(f["minQty"]) <= 1:
                        self.precision[i["symbol"]] = str(int(1/float(f["minQty"]))).count("0")
                    else:
                        self.precision[i["symbol"]] = -1*int(f["minQty"].count("0"))
                        
    async def run(self):
        """
        Connects to Websocket & Triggers functions.
        """
        print("Arbitrator started...")
        print("Operating Markets:", ', '.join(self.curs))
        print("Balance:", self.get_balance("USDT"), "USDT")
        for cur in self.curs:
            self.url += "/"+cur.lower()+"usdt@depth5@100ms/"+cur.lower()+"btc@depth5@100ms"
        async with websockets.connect(self.url) as websocket:
            async for message in websocket:
                self.handle_data(message)
                if not self.timeout:
                    self.timeout = True
                    Thread(target=self.get_rates).start()
                    
    def handle_data(self, message):
        """
        Takes websocket data and converts it to an internal orderbook (dictionary).
        """
        message = json.loads(message)
        market_id = message["stream"].split("@")[0]
        asks = [(float(a[0]), float(a[1])) for a in message["data"]["asks"]]
        ask = min(asks, key = lambda t: t[0])
        bids = [(float(a[0]), float(a[1])) for a in message["data"]["bids"]]
        bid = max(bids, key = lambda t: t[0])
        self.data[market_id.upper()] = {"ask":ask, "bid":bid}
                    
    def get_rates(self):
        """
        Main trading function. Calculates profit margins, trade size & executes trades for each currency.
        """
        for cur in self.curs:
            try:
                print(cur)
                euro_available = random.randint(self.min_amount, self.max_amount)
                x = self.floor(euro_available/self.get_ask(cur+"USDT")[0], self.precision[cur+"USDT"])
                y = self.floor(x*0.999, self.precision[cur+"BTC"])
                z = self.floor((y*self.get_bid(cur+"BTC")[0])*0.999, self.precision["BTCUSDT"])
                a = self.get_ask(cur+"USDT")[0]*x
                b = self.get_bid("BTCUSDT")[0]*z
                arbitrage = a/x*x/y*y/b
                profit = b-a
                if arbitrage < 0.99 and profit > 0 and euro_available > self.min_amount:
                    order_success = self.order(cur+"USDT", "BUY", x)
                    if order_success:
                       order_success = self.order(cur+"BTC", "SELL", y)
                       if order_success:
                          order_success = self.order("BTCUSDT", "SELL", z)
                          if order_success:
                             print(a, "USDT, BUY", x, cur+"USDT, SELL", y, cur+"BTC, SELL", z, "BTCUSDT, GET", b, "USDT - ARBITRAGE:", arbitrage, "PROFIT:", profit, "USDT")
                          else:
                             order_success = self.order(cur+"BTC", "BUY", y)
                             order_success = self.order(cur+"USDT", "SELL", x)
                             print(z, "BTCUSDT SELL order fail. Revert previous order(s).")
                       else:
                          order_success = self.order(cur+"USDT", "SELL", x)
                          print(y, cur+"BTC SELL order fail. Revert previous order(s).")
                    else:
                       print(x, cur+"USDT BUY order fail!")
                    print("Balance:", self.get_balance("USDT"), "USDT", self.get_balance("BTC"), "BTC")   
                
                euro_available = random.randint(self.min_amount, self.max_amount)
                x = self.floor(euro_available/self.get_ask("BTCUSDT")[0], self.precision["BTCUSDT"])
                y = self.floor((x/self.get_ask(cur+"BTC")[0])*0.999, self.precision[cur+"BTC"])
                z = self.floor(y*0.999, self.precision[cur+"USDT"])
                a = self.get_ask("BTCUSDT")[0]*x
                b = self.get_bid(cur+"USDT")[0]*z
                arbitrage = a/x*x/y*y/b
                profit = b-a
                if arbitrage < 0.99 and profit > 0 and euro_available > self.min_amount:
                    order_success = self.order("BTCUSDT", "BUY", x)
                    if order_success:
                       order_success = self.order(cur+"BTC", "BUY", y)
                       if order_success:
                          order_success = self.order(cur+"USDT", "SELL", z)
                          if order_success:
                             print(a, "USDT, BUY", x, "BTCUSDT, BUY", y, cur+"BTC, SELL", z, cur+"USDT, GET", b, "USDT - ARBITRAGE:", arbitrage, "PROFIT:", profit, "USDT")
                          else:
                             order_success = self.order(cur+"BTC", "SELL", y)
                             order_success = self.order("BTCUSDT", "SELL", x)
                             print(z, cur+"USDT SELL order fail. Revert previous order(s).")
                       else:
                          order_success = self.order("BTCUSDT", "SELL", x)
                          print(y, cur+"BTC BUY order fail. Revert Previous order(s).")
                    else:
                       print(x, "BTCUSDT BUY order fail!")
                    print("Balance:", self.get_balance("USDT"), "USDT", self.get_balance("BTC"), "BTC")
                    
            except KeyError:
                pass
        self.timeout = False

    def get_balance(self, cur):
        """
        Return available balance for given currency.
        """
        try:
            re = self.client.get_asset_balance(asset=cur)
            return re["free"] 
        except:
            return 0

    def order(self, market, side, amount):
        """
        Create an order.
        """
        try:
            if side.lower() == "buy":
                re = self.client.create_order(symbol=market, side=Client.SIDE_BUY, type=Client.ORDER_TYPE_MARKET,quantity=str(amount))
                print("BUY", amount, market)
            elif side.lower() == "sell":
                re = self.client.create_order(symbol=market, side=Client.SIDE_SELL, type=Client.ORDER_TYPE_MARKET,quantity=str(amount))
                print("SELL", amount, market)
            if re["status"] == "FILLED":
                return True
        except:
            return False
    
    def get_bid(self, market):
        """
        Get price & size of best bid.
        """
        return self.data[market]["bid"]

    def get_ask(self, market):
        """
        Get price & size of best ask.
        """
        return self.data[market]["ask"]

    def floor(self, nbr, precision):
        """
        Cut any number at 'precision' decimals.
        """
        if precision == 0:
            return int(nbr)
        else:
            return int(nbr*10**precision)/10**precision
        
        
        

with open("config.json", "r") as file:
    data = json.loads(file.read())
 
bn = BnArber(data["currencies"], data["public"], data["secret"], data["max_amount"])
asyncio.get_event_loop().run_until_complete(bn.run())
