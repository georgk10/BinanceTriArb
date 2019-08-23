import websockets, asyncio, json, datetime, requests, time, uuid
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
        self.url = "wss://stream.binance.com:9443/stream?streams=btcusdt@depth5/"
        self.curs = curs
        self.data = {}
        self.timeout = False
        self.money = 0
        self.min_amount = 5
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
        print("Arbitrator started...")
        print("Operating Markets:", ', '.join(self.curs))
        heartbeat = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        for cur in self.curs:
            self.url += cur.lower()+"usdt@depth5/"+cur.lower()+"btc@depth5/"
        async with websockets.connect(self.url) as websocket:
            async for message in websocket:
                self.handle_data(message)
                if not self.timeout:
                    self.timeout = True
                    Thread(target=self.get_rates, args=(websocket,)).start()
                if heartbeat < datetime.datetime.utcnow() - datetime.timedelta(hours=1):
                    print("Balance:", self.get_balance("USDT"), "USDT")
                    heartbeat = datetime.datetime.utcnow()
                    

    def handle_data(self, message):
        message = json.loads(message)
        market_id = message["stream"].split("@")[0]
        asks = {}
        for ask in message["data"]["asks"]:
            asks[float(ask[0])] = float(ask[1])
        bids = {}
        for bid in message["data"]["bids"]:
            bids[float(bid[0])] = float(bid[1])
        self.data[market_id.upper()] = {"ask":asks, "bid":bids}
                    
    def get_rates(self, websocket):
        fee = 1-(0.999**3)
        for cur in self.curs:
            
            try:
                r = (1/self.get_ask(cur+"USDT")[0]*self.get_bid(cur+"BTC")[0]*self.get_bid("BTCUSDT")[0])-1
                am1 = (self.get_ask(cur+"USDT")[1]*self.get_bid(cur+"USDT")[0])
                am2 = (self.get_bid(cur+"BTC")[1]*self.get_bid(cur+"BTC")[0])*self.get_bid("BTCUSDT")[0]
                am3 = (self.get_bid("BTCUSDT")[1]*self.get_bid("BTCUSDT")[0])
                am4 = (self.get_bid(cur+"BTC")[1]*self.get_bid(cur+"USDT")[0])
                euro_available = min(am1, am2, am3, am4)
                if r > fee and euro_available > self.min_amount:
                    euro_available = min([euro_available, self.max_amount])
                    trade_amount = self.floor(euro_available/self.get_ask(cur+"USDT")[0], self.precision[cur+"USDT"])
                    order_success = self.order(cur+"USDT", "BUY", trade_amount)
                    if order_success:
                        trade_amount = self.floor(trade_amount*0.999, self.precision[cur+"BTC"])
                        order_success = self.order(cur+"BTC", "SELL", trade_amount)
                        if not order_success:
                            self.sell_all()
                            time.sleep(10)
                            print("Balance:", self.get_balance("USDT"), "USDT")
                            continue
                        trade_amount = self.floor((trade_amount*0.999)*self.get_bid(cur+"BTC")[0], self.precision["BTCUSDT"])
                        order_success = self.order("BTCUSDT", "SELL", trade_amount)
                        if not order_success:
                            self.sell_all()
                            time.sleep(10)
                            print("Balance:", self.get_balance("USDT"), "USDT")
                            continue
                        print("Balance:", self.get_balance("USDT"), "USDT")
                    else:
                        pass    
                
                r = (1/self.get_ask("BTCUSDT")[0]/self.get_ask(cur+"BTC")[0]*self.get_bid(cur+"USDT")[0])-1
                am1 = (self.get_ask("BTCUSDT")[1]*self.get_bid("BTCUSDT")[0])
                am2 = (self.get_ask(cur+"BTC")[1]*self.get_bid(cur+"BTC")[0])*self.get_bid(cur+"USDT")[0]
                am3 = (self.get_bid(cur+"USDT")[1]*self.get_bid(cur+"USDT")[0])
                am4 = (self.get_bid(cur+"BTC")[1]*self.get_bid(cur+"USDT")[0])
                euro_available = min(am1, am2, am3, am4)
                if r > fee and euro_available > self.min_amount:
                    euro_available = min([euro_available, self.max_amount])
                    trade_amount = self.floor(euro_available/self.get_ask("BTCUSDT")[0], self.precision["BTCUSDT"])
                    order_success = self.order("BTCUSDT", "BUY", trade_amount)
                    if order_success:
                        trade_amount = self.floor(trade_amount*0.999, self.precision[cur+"BTC"])
                        order_success = self.order(cur+"BTC", "BUY", trade_amount)
                        if not order_success:
                            self.sell_all()
                            print("Balance:", self.get_balance("USDT"), "USDT")
                            time.sleep(10)
                            continue
                        trade_amount = self.floor(trade_amount*0.999, self.precision[cur+"USDT"])
                        order_success = self.order(cur+"USDT", "SELL", trade_amount)
                        if not order_success:
                            self.sell_all()
                            print("Balance:", self.get_balance("USDT"), "USDT")
                            time.sleep(10)
                            continue
                        print("Balance:", self.get_balance("USDT"), "USDT")
                        print(self.old_data)
                    else:
                        pass
            except KeyError:
                pass
        self.timeout = False

    def get_balance(self, cur):
        try:
            re = self.client.get_asset_balance(asset=cur)
            return re["free"] 
        except:
            return 0

    def sell_all(self):
        try:
            for cur in self.curs + ["BTC"]:
                time.sleep(5)
                amount = self.floor(self.get_balance(cur), self.precision[cur+"USDT"])
                if amount*self.get_bid(cur+"USDT")[0] > self.min_amount:
                    self.order(cur+"USDT", "SELL", amount)
        except:
            pass

    def order(self, market, side, amount):
        try:
            print(market, side, amount)
            if side.lower() == "buy":
                re = self.client.create_order(symbol=market, side=Client.SIDE_BUY, type=Client.ORDER_TYPE_MARKET,quantity=str(amount))
            elif side.lower() == "sell":
                re = self.client.create_order(symbol=market, side=Client.SIDE_SELL, type=Client.ORDER_TYPE_MARKET,quantity=str(amount))
            if re["status"] == "FILLED":
                return True
        except:
            return False
    
    def get_bid(self, market):
        price = max(self.data[market]["bid"].keys())
        size = self.data[market]["bid"][price]
        return (price, size)

    def get_ask(self, market):
        price = min(self.data[market]["ask"].keys())
        size = self.data[market]["ask"][price]
        return (price, size)

    def floor(self, nbr, precision):
        nbr = float(nbr)
        if precision == 0:
            return int(nbr)
        else:
            return int(nbr*10**precision)/10**precision
        
        
        

with open("config.json", "r") as file:
    data = json.loads(file.read())
 
bn = BnArber(data["currencies"], data["public"], data["secret"], data["max_amount"])
asyncio.get_event_loop().run_until_complete(bn.run())


