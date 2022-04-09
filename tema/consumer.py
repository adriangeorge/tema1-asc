"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from threading import Thread
from time import sleep
from typing import List

from tema.marketplace import Marketplace


class Consumer(Thread):
    """
    Class that represents a consumer.
    """

    def __init__(self, carts: List, marketplace: Marketplace, retry_wait_time: float, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """
        Thread.__init__(self)

        # Init variables
        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time
        self.kwargs = kwargs
        self.logger = marketplace.logger

    def run(self):
        # Log starting confirmation messasge
        log_msg = "Started consumer " + str(self.kwargs['name'])
        self.marketplace.log(log_msg, str(self.kwargs['name']))

        # Main consumer loop
        for cart in self.carts:
            # Register cart
            cart_id = self.marketplace.new_cart()
            self.marketplace.assign_owner(cart_id, str(self.kwargs['name']))
            cart_iter = iter(cart)

            # Find next item in cart to request
            req_item = next((item for item in cart_iter), None)

            # Keep requesting until item is available
            while req_item is not None:

                if req_item['type'] == 'add':
                    # Log ADD request
                    # log_msg = "Requesting ADD " + str(req_item['product'])
                    # self.marketplace.log(log_msg, str(self.kwargs['name']))
                    res = self.marketplace.add_to_cart(
                        cart_id, req_item['product'])

                    if(res):
                        # Log request success
                        # log_msg = "I have RECEIVED " + str(req_item['product'])
                        # self.marketplace.log(log_msg, str(self.kwargs['name']))
                        req_item['quantity'] -= 1

                        if(req_item['quantity'] == 0):
                            req_item = next((item for item in cart_iter), None)
                    else:
                        # Log request failure
                        # log_msg = "I was DENIED " + \
                        #     str(req_item['product']) + " WAITING"
                        # self.marketplace.log(log_msg, str(self.kwargs['name']))

                        sleep(self.retry_wait_time)

                elif req_item['type'] == 'remove':
                    self.marketplace.remove_from_cart(
                        cart_id, req_item['product'])

                    req_item['quantity'] -= 1

                    if(req_item['quantity'] == 0):
                        req_item = next((item for item in cart_iter), None)

            # All items in cart were processed, place the order
            # log_msg = "Got all items! PLACING ORDER!"
            # self.marketplace.log(log_msg, str(self.kwargs['name']))
            self.marketplace.place_order(cart_id)

        self.marketplace.sign_out(str(self.kwargs['name']))
