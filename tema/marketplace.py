"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from cmath import log
from math import prod
from threading import Lock, Semaphore
from typing import List
import unittest
from tema.product import Product

import logging
from logging.handlers import RotatingFileHandler


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """

    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.q_limit = queue_size_per_producer
        self.producers = []
        self.consumers = []
        self.lock = Lock()

        # File logging
        logger = logging.getLogger("log_asc")
        logger.setLevel(logging.INFO)
        rfh = RotatingFileHandler(
            'my_log.log', maxBytes=32000, backupCount=10)
        rfh.setLevel(logging.INFO)
        formatter = logging.Formatter(
            # %(asctime)s - %(name)s - %(levelname)s - 
            '%(message)s')
        rfh.setFormatter(formatter)
        logger.addHandler(rfh)
        self.logger = logger
        self.mname = "market"

    # Wrapper on logger
    def log(self, msg, src):
        self.logger.info(src + ":" + msg)

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        new_producer = {
            'id': len(self.producers),
            'queue': [],
            'empty_sem': Semaphore(value=self.q_limit),
            'full_sem': Semaphore(0)
        }
        self.producers.append(new_producer)

        log_msg = "Registered new PROD id: " + str(new_producer['id'])
        self.log(log_msg, self.mname)
        return new_producer['id']

    def publish(self, producer_id: int, product: Product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """

        prod_queue = self.producers[producer_id]['queue']
        prod_esem = self.producers[producer_id]['empty_sem']
        prod_fsem = self.producers[producer_id]['full_sem']

        log_msg = "Got PUB req from " + \
            str(producer_id) + " for " + str(product)
        self.log(log_msg, self.mname)
        acquired = prod_esem.acquire(blocking=False)
        if not acquired:
            log_msg = "REJECT PUB req from " + \
                str(producer_id) + " for " + str(product)
            self.log(log_msg, self.mname)
            return False

        prod_queue.append([product, True])
        log_msg = "ACCEPT PUB req from " + \
            str(producer_id) + " for " + \
            str(product) + " remaining slots " + \
            str(self.q_limit - len(prod_queue))
        self.log(log_msg, self.mname)
        prod_fsem.release()
        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        new_cart = {
            'id': len(self.consumers),
            'items': []
        }
        self.consumers.append(new_cart)

        # Log register success
        log_msg = "Registered new CART id: " + str(new_cart['id'])
        self.log(log_msg, self.mname)
        return new_cart['id']

    def product_search(self, name: str):
        """
        Given a product name will return a pair of the first match and the producer that
        has that product

        :type name: String
        :param name: Name of product to be searched

        :returns Pair of item and its availabilty as first element of tuple and reference to producer 
        """
        item_prod = None
        for producer in self.producers:
            req_item = None
            for p in producer['queue']:

                if p[0].name == name and p[1]:
                    item_prod = (p, producer)
                    log_msg = "Found " + \
                        str(p) + " at PROD[" + \
                        str(producer['id']) + ']'
                    self.log(log_msg, self.mname)
                    return item_prod     

    def add_to_cart(self, cart_id: int, product: Product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """

        log_msg = "Got ADD req from " + str(cart_id) + " for " + str(product)
        self.log(log_msg, self.mname)

        # Get the referenced cart
        c_iter = iter(self.consumers)
        cart = next((c for c in c_iter if c['id'] == cart_id), None)

        # Search requested product in all producer catalogues
        item_prod = self.product_search(product.name)
        # Protect from multiple accesses
        # If an item has been found and IS available log an appropiate message
        if item_prod is not None:

            req_item = item_prod[0]
            if req_item[1]:
                req_item[1] = False

                # Add item to user's cart
                cart['items'].append(item_prod)
                for e in cart['items']:
                    self.log(str(e[0]), "M")
                return True

        else:
            log_msg = "Could not find " + str(product)
            self.log(log_msg, self.mname)
            return False

    def remove_from_cart(self, cart_id: int, product: Product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """

        # Get the referenced cart
        c_iter = iter(self.consumers)
        cart = next((c for c in c_iter if c['id'] == cart_id), None)

        req_prod_name = product.name
        for p in self.consumers[cart_id]['items']:
            if p[0].name == req_prod_name:
                req_item = p

    def place_order(self, cart_id: int):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        pass


class TestMarketplace(unittest.TestCase):
    def setUp(self):
        self.marketplace = Marketplace(5)

    def test_1_register_producer(self):
        pass

    def test_2_new_cart(self):
        pass

    def test_3_publish(self):
        pass

    def test_4_add_to_cart(self):
        pass

    def test_5_remove_from_cart(self):
        pass

    def test_6_place_order(self):
        pass
