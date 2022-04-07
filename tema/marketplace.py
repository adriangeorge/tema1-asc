"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""


from threading import Lock
from tema.product import Product


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
        self.lock = Lock()

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """

        # The producer list is a shared variable
        # Protect against concurrent access by multiple producers
        self.lock.acquire()
        new_producer = {
            'id': len(self.producers),
            'queue': []
        }
        self.producers.append(new_producer)
        self.lock.release()

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
        print("Got publish req from " + str(producer_id) + " for " + str(product))

        prod_queue = self.producers[producer_id]['queue']
        if(len(prod_queue) < self.q_limit):
            self.producers[producer_id]['queue'].append(product)
            return True
        else:
            return False

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        pass

    def add_to_cart(self, cart_id: int, product: Product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        pass

    def remove_from_cart(self, cart_id: int, product: Product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        pass

    def place_order(self, cart_id: int):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        pass
