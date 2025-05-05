import json
from loguru import logger
from websocket import create_connection
from services.producer.trade import Trade

class KrakenWebSocketAPI:
    URL = "wss://ws.kraken.com/v2"

    def __init__(
            self,
            product_ids : list[str],
    ):
        self.product_ids = product_ids

        # Create a WebSocket connection
        self._connection = create_connection(self.URL)

        # send initial subscription message
        self._subscribe(product_ids)

    def get_trades(self) -> list[Trade]:
        data:str = self._connection.recv()

        if 'heartbeat' in data:
            logger.info('Heartbeat received')
            return []
        
        #transform raw string into a JSON Onject
        try:
            data = json.loads(data)
        except json.JSONDecodeError as e:
            logger.info(f'Error decoding JSON : {e}')
            return []
        
        try:
            trades_data = data['data']
        except KeyError as e:
            logger.error(f'No `data` field with trades in the message {e}')
            return []
        
        trades = [
            Trade.from_kraken_websocket_response(
                product_ids=trade['symbol'],
                price=trade['price'],
                quantity=trade['qty'],
                timestamp=trade['timestamp'],
            )
            for trade in trades_data
        ]

        return trades

    def _subscribe(self, product_ids: list[str]):
        """
        Subscribes to the websocket for the given `product_ids`
        and waits for the initial snapshot.
        """
        # send a subscribe message to the websocket
        self._connection.send(
            json.dumps(
                {
                    'method': 'subscribe',
                    'params': {
                        'channel': 'trade',
                        'symbol': product_ids,
                        'snapshot': False,
                    },
                }
            )
        )

        # discard the first 2 messages for each product_id
        # as they contain no trade data
        for _ in product_ids:
            _ = self._connection.recv()
            _ = self._connection.recv()

    def is_done(self) -> bool:
        """
        Returns True if the websocket is done, False otherwise.
        """
        return False


