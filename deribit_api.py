import asyncio

import time

import websockets
import json
import uuid
from enum import Enum

from secrets import CLIENT_SECRET, CLIENT_ID

DERIBIT_URL = "wss://test.deribit.com/ws/api/v2"


class Constants(Enum):
    BUY = 0
    SELL = 1


class DeribitAPI:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.responses = []

    def __exit__(self, exc_type, exc_val, exc_tb):
        # todo: close all positions or inform open positions or etc
        print(self.responses)
        pass

    @staticmethod
    def get_prices_in_list(price_amount_lst):
        return [price for price, amount in price_amount_lst]

    @staticmethod
    def generate_id():
        return uuid.uuid4()

    @staticmethod
    def parse_account_balance(balance_data):
        result_data = balance_data.get("result")
        if result_data:
            return result_data.get("balance")

    def async_loop(self, message):
        return asyncio.get_event_loop().run_until_complete(self.private_api(message))

    async def public_api(self, msg):
        async with websockets.connect(DERIBIT_URL) as websocket:
            await websocket.send(msg)
            while websocket.open:
                response = self.handle_server_response(await websocket.recv())
                break
            return json.loads(response)

    async def private_api(self, msgs):
        if not isinstance(msgs, list):
            msgs = [msgs]

        async with websockets.connect(DERIBIT_URL) as websocket:
            await websocket.send(self.get_authentication_message())
            while websocket.open:
                self.handle_server_response(await websocket.recv())
                for msg in msgs:
                    await websocket.send(msg)
                    response = self.handle_server_response(await websocket.recv())
                break
            return response

    async def do_all_with_one_connection(self):
        async with websockets.connect(DERIBIT_URL) as websocket:
            await websocket.send(self.get_authentication_message())
            self.handle_server_response(await websocket.recv())
            while websocket.open:
                await websocket.send(
                    self.get_top_level_orders_data_msg("BTC-PERPETUAL")
                )
                market_data = self.handle_server_response(await websocket.recv())

                orders_msgs = self.generate_orders(market_data)
                for order in orders_msgs:
                    await websocket.send(order)
                    self.handle_server_response(await websocket.recv())
                time.sleep(10)

                await websocket.send(self.get_account_balance_msg("BTC"))
                balance_data = self.handle_server_response(await websocket.recv())
                print(
                    "Current account balance: {}".format(
                        self.parse_account_balance(balance_data)
                    )
                )

                await websocket.send(
                    self.get_cancel_all_by_instrument_msg("BTC-PERPETUAL")
                )
                self.handle_server_response(await websocket.recv())

                await websocket.send(
                    self.get_close_position_msg("BTC-PERPETUAL", "market")
                )
                self.handle_server_response(await websocket.recv())

    def handle_server_response(self, response):
        response = json.loads(response)
        if "error" in response:
            error_msg = "Errors in last response:\n[{}], \nERROR MSG: {}".format(
                response, str(response.get("error"))
            )
            print(error_msg) #or raise
        self.responses.append(response)
        return response

    def create_msg(self, id, method, params):
        msg = {
            "jsonrpc": "2.0",
            "id": str(id),
            "method": method,
        }
        if params:
            msg.update(dict(params=params))

        return json.dumps(msg)

    def get_close_position_msg(self, instrument, type):
        params = {"instrument_name": instrument, "type": type}
        return self.create_msg(self.generate_id(), "private/close_position", params)

    def get_cancel_all_by_instrument_msg(self, instrument):
        params = {"instrument_name": instrument}
        return self.create_msg(
            self.generate_id(), "private/cancel_all_by_instrument", params
        )

    def get_account_balance_msg(self, currency):
        params = {"currency": currency}
        return self.create_msg(
            self.generate_id(), "private/get_account_summary", params
        )

    def get_top_level_orders_data_msg(self, instrument, depth=10):
        params = {"instrument_name": instrument, "depth": depth}
        return self.create_msg(self.generate_id(), "public/get_order_book", params)

    def get_authentication_message(self):
        params = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        return self.create_msg(self.generate_id(), "public/auth", params)

    def generate_orders(self, market_data):
        response_result = market_data.get("result", {})
        bids_prices = self.get_prices_in_list(response_result.get("bids"))
        asks_prices = self.get_prices_in_list(response_result.get("asks"))
        orders_msgs = self.get_minimium_limit_orders_msgs(
            Constants.BUY, "BTC-PERPETUAL", bids_prices
        )
        orders_msgs += self.get_minimium_limit_orders_msgs(
            Constants.SELL, "BTC-PERPETUAL", asks_prices
        )
        return orders_msgs

    def get_top_level_orders_data(self, instrument, depth=10):
        return self.async_loop(self.get_top_level_orders_data_msg(instrument, depth))

    def get_top_level_prices(self, instrument, depth=10):
        response = self.get_top_level_orders_data(instrument, depth)
        response_result = response.get("result", {})
        return self.get_prices_in_list(
            response_result.get("bids")
        ), self.get_prices_in_list(response_result.get("asks"))

    def get_minimium_limit_orders_msgs(self, side, instrument, prices):
        params = {"instrument_name": instrument, "type": "limit", "amount": 10}
        if side == Constants.BUY:
            method = "private/buy"
        elif side == Constants.SELL:
            method = "private/sell"
        else:
            raise Exception("Please specify BUY or SELL SIDE")
        msgs = []
        for price in prices:
            params.update(dict(price=price))
            msgs.append(self.create_msg(self.generate_id(), method, params))
        return msgs

    def run(self):
        asyncio.get_event_loop().run_until_complete(self.do_all_with_one_connection())

    def runv2(self):
        while True:
            bids, asks = self.get_top_level_prices("BTC-PERPETUAL")
            bids_orders = self.get_minimium_limit_orders_msgs(
                Constants.BUY, "BTC-PERPETUAL", bids
            )
            ask_orders = self.get_minimium_limit_orders_msgs(
                Constants.SELL, "BTC-PERPETUAL", asks
            )
            self.async_loop(bids_orders + ask_orders)
            time.sleep(10)
            balance_data = self.async_loop(self.get_account_balance_msg("BTC"))
            print(self.parse_account_balance(balance_data))
            self.async_loop(self.get_cancel_all_by_instrument_msg("BTC-PERPETUAL"))
            self.async_loop(self.get_close_position_msg("BTC-PERPETUAL", "market"))


if __name__ == "__main__":
    api = DeribitAPI(CLIENT_ID, CLIENT_SECRET)
    # api.run()
    api.runv2()
