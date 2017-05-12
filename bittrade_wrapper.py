#!/usr/bin/env python
# encoding: utf-8

import asyncio
import asyncio_redis

class redis_pool:
    """
        Connect to redis pub/sub protocol.
    """

    def __init__(self):
        self.host = None
        self.port = None
        self.password = None
        self.db = None
        self.auto_reconnect = None
        self.connection = None
        self.subscriber = None
        self.channels = None

    def check_self(self):
        if not self.connection:
            try:
                self.connect(self.host,self.port,self.password,self.db,self.auto_reconnect)
            except Exception as e:
                print('Connect error', repr(e))
        if not self.subscriber:
            try:
                self.subscribe(self.channels)
            except Exception as e:
                print('Subscribe error', repr(e))
        return True

    async def connect(self, host='localhost', port=6379, password=None, db=0,
                auto_reconnect=True):
        self.host, self.port, self.password, self.db, self.auto_reconnect = host, port, password, db, auto_reconnect
        self.connection = await asyncio_redis.Connection.create(host=host, port=port)
        return self.connection

    async def subscribe(self, channels):
        if self.subscriber:
            self.connection.close()
            self.connect(self.host, self.port, self.password, self.db, self.auto_reconnect)
        self.subscriber = await self.connection.start_subscribe()
        self.channels = channels
        await self.subscriber.subscribe(channels)

    async def receive(self):
        try:
            self.check_self()
            reply = await self.subscriber.next_published()
            return reply.value
        except Exception as e:
            print('Receive error', repr(e))

class publish_pool(redis_pool):
    """
        publish pool is the pool used for publish message to the redis.
    """
    def __init__(self):
        super(self).__init__()

    await def subscribe(self,channels):
        self.channels = channels

    async def publish(self, text):
        try:
            self.check_self()
            for channel in self.channels:
                await self.connection.publish(channel, text)
        except asyncio_redis.Error as e:
            print('Publish failed', repr(e))

class subscribe_pool(redis_pool):
    """
        subscribe pool is used for receive message from redis.
    """
    def __init__(self):
        super(self).__init__()

class Trade_wrapper:
    """
        Wrapper get data from market and send the data to pub/sub protocol.
        Control the connect with the market (reconnect).
        Get historial data from database.
    """
    def __init__(self, wrapper=None):
        self.publish_pool = publish_pool()
        self.subscribe_pool = subscribe_pool()
        if wrapper:
            self.wrapper = wrapper
            await self.publish_pool.connect()
            await self.subscribe_pool.connect()
            await self.publish_pool.subscribe([wrapper.__module__])
            await self.subscribe_pool.subscribe([wrapper.__module__])
        else:
            return False

    async def connect(self, url=None):
        if self.wrapper:
            self.connection = await self.wrapper.connect(url)
            return self.connection
        else:
            return False

    async def get_current_price(self):
        current_price = self.wrapper.get_current_price()
        self.publish_pool.publish(current_price)

    async def get_orders(self):
        orders = self.wrapper.get_orders()
        return orders

    async def cancle_orders(self, order_ids):
        if isinstance(order_ids,list):
            self.wrapper.cancle_orders(order_ids)
        elif isinstance(order_ids,str):
            self.wrapper.cancle_order(order_ids)
