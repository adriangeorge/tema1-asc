"""
This module represents the Producer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from itertools import product
import logging
from math import prod
from threading import Thread
from time import sleep
from typing import List

from tema.marketplace import Marketplace
from tema.product import Product


class Producer(Thread):
    """
    Class that represents a producer.
    """

    def __init__(self, products: List[Product], marketplace: Marketplace, republish_wait_time: float, **kwargs):
        """
        Constructor.

        @type products: List()
        @param products: a list of products that the producer will produce

        @type marketplace: Marketplace
        @param marketplace: a reference to the marketplace

        @type republish_wait_time: Time
        @param republish_wait_time: the number of seconds that a producer must
        wait until the marketplace becomes available

        @type kwargs:
        @param kwargs: other arguments that are passed to the Thread's __init__()
        """
        Thread.__init__(self)
        self.products = products
        self.marketplace = marketplace
        self.republish_wait_time = republish_wait_time
        self.kwargs = kwargs
        self.curr_index = 0
        self.curr_product = list(self.products[0])

    def produce(self) -> Product:
        """
        Produce an item
        """
        # Remove one unit from production line
        self.curr_product[1] -= 1

        # Simulate duration of fabrication
        sleep(float(self.curr_product[2]))

        # If this is the last item to be produced of this type
        # Go to next type of item
        if(self.curr_product[1] == 0):
            self.curr_index += 1

        return self.curr_product[0]

    def run(self):
        log_msg = "Started producer " + str(self.kwargs['name'])
        self.marketplace.log(log_msg, str(self.kwargs['name']))

        self.prodId = self.marketplace.register_producer()
        loopFlag = True
        while loopFlag:

            # Produces an item and checks what the next produced item should be
            produced_item = self.produce()
            log_msg = "Produced a " + produced_item.name
            self.marketplace.log(log_msg, str(self.kwargs['name']))
            if(len(self.products) > self.curr_index):
                if self.curr_product[1] == 0:
                    self.curr_product = list(self.products[self.curr_index])
            else:
                self.curr_product = list(self.products[0])
                self.curr_index = 0

            # Publishes the new item on the marketplace or waits until it can be
            # published
            wasPublished = False
            while not wasPublished:
                log_msg = "Sending publish " + produced_item.name
                self.marketplace.log(log_msg, str(self.kwargs['name']))
                wasPublished = self.marketplace.publish(
                    self.prodId, produced_item)
                if(not wasPublished):
                    log_msg = "Waiting"
                    self.marketplace.log(log_msg, str(self.kwargs['name']))
                    sleep(self.republish_wait_time)
