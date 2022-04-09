"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import logging
from logging.handlers import RotatingFileHandler

from threading import Lock, Semaphore
import unittest
from .product import Product


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
        self.carts = []
        self.consumers = []
        self.lock = Lock()

        # File logging
        logger = logging.getLogger("log_asc")
        logger.setLevel(logging.INFO)
        rfh = RotatingFileHandler('my_log.log', mode='w')
        rfh.setLevel(logging.INFO)
        formatter = logging.Formatter(
            # %(asctime)s - %(name)s - %(levelname)s -
            '%(message)s')
        rfh.setFormatter(formatter)
        logger.addHandler(rfh)
        self.logger = logger
        self.mname = "MK"
        self.all_completed = False

    def log(self, msg, src):
        """
            Simple wrapper for logging
        """
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

        log_msg = "REG PROD [" + str(new_producer['id']) + ']'
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

        acquired = prod_esem.acquire(blocking=False)
        if not acquired:
            log_msg = "REJ PUB REQ S:PROD[" + \
                str(producer_id) + "] " + str(product)
            self.log(log_msg, self.mname)
            return False

        prod_queue.append([product, True])
        log_msg = "ACC PUB REQ S:PROD[" + \
            str(producer_id) + "] " + \
            str(product) + " SLOTS [" + \
            str(self.q_limit - len(prod_queue)) + \
            "]"
        self.log(log_msg, self.mname)
        prod_fsem.release()
        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        self.lock.acquire()
        new_cart = {
            'id': len(self.carts),
            'items': [],
            'completed': False,
            'owner': ""
        }
        self.carts.append(new_cart)
        self.lock.release()

        # Log register success
        log_msg = "REG CART [" + str(new_cart['id']) + "]"
        self.log(log_msg, self.mname)
        return new_cart['id']

    def assign_owner(self, cart_id: int, owner: str):
        """
        Add owner to cart and to customer list if they were not already added
        """
        for cart in self.carts:
            if cart['id'] == cart_id:
                cart['owner'] = owner

        if owner not in self.consumers:
            self.consumers.append(owner)

    def product_search(self, name: str):
        """
        Given a product name will return a pair of the first match and the producer that
        has that product

        :type name: String
        :param name: Name of product to be searched

        :returns Pair of item and its availabilty as first
        element of tuple and reference to producer
        """
        item_prod = None
        for producer in self.producers:
            for prod in producer['queue']:

                if prod[0].name == name and prod[1]:
                    item_prod = (prod, producer)
                    # log_msg = "ITEM AVAILABLE " + \
                    #     str(prod) + " IN PROD[" + \
                    #     str(producer['id']) + ']'
                    # self.log(log_msg, self.mname)
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

        log_msg = "ADD REQ [" + self.carts[cart_id]['owner'] + \
            "][C" + str(cart_id) + "] " + str(product)

        # Get the referenced cart
        c_iter = iter(self.carts)
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
                log_msg = "ACC " + log_msg
                self.log(log_msg, self.mname)
                return True

        else:
            log_msg = "REJ " + log_msg
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

        # Search for matching item in cart and remove it
        req_prod_name = product.name
        prod_to_remove = None
        for prod in self.carts[cart_id]['items']:
            if prod[0][0].name == req_prod_name:
                prod[0][1] = True
                prod_to_remove = prod

        bf = len(self.carts[cart_id]['items'])
        self.carts[cart_id]['items'].remove(prod_to_remove)
        # Log DEL request
        af = len(self.carts[cart_id]['items'])
        log_msg = "DEL REQ " + str(prod_to_remove) + str(bf) + " " + str(af)
        self.log(log_msg, self.mname)

    def place_order(self, cart_id: int):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        # Mark cart as completed
        self.carts[cart_id]['completed'] = True

        # Remove bought items from producer storage

        log_msg = "\n"
        for item in self.carts[cart_id]['items']:
            producer = item[1]
            prod_esem = producer['empty_sem']
            prod_fsem = producer['full_sem']

            prod_fsem.acquire()
            if item[0] in producer['queue']:
                producer['queue'].remove(item[0])
            else:
                err_log = "ERR COULD NOT FIND " + str(item[0])
                self.log(err_log, self.mname)
            prod_esem.release()

        # Print output
        for item in self.carts[cart_id]['items']:
            log_msg += self.carts[cart_id]['owner'] + \
                ' bought ' + str(item[0][0]) + '\n'

        print(log_msg[1:-1])
        self.log(log_msg, self.mname)

    def sign_out(self, cons: str):
        """
        Function called by consumers when finishing all their requests
        When all consumers have signed out all producer processes will also stop
        """
        self.consumers.remove(cons)
        log_msg = "LOGOUT " + cons + " REMAINING " + str(len(self.consumers))
        self.log(log_msg, self.mname)

# class TestMarketplace(unittest.TestCase):
#     def setUp(self):
#         self.marketplace = Marketplace(5)

#     def test_1_register_producer(self):
#         pass

#     def test_2_new_cart(self):
#         pass

#     def test_3_publish(self):
#         pass

#     def test_4_add_to_cart(self):
#         pass

#     def test_5_remove_from_cart(self):
#         pass

#     def test_6_place_order(self):
#         pass
