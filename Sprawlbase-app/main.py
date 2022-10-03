"""Sprawlbase - The community driven computational and storage platform.

Author: Marcel Coetzee
"""
__author__ = "Marcel Coetzee"

import asyncio
import logging
import os
import random
import threading

import kivy
from dask.distributed import Nanny
from kivy.app import App
from kivy.clock import Clock
from kivy.event import EventDispatcher

kivy.require("2.1.0")

logger = kivy.logger.Logger.getChild("SprawlerApp")


def read_user_balance(user_id: str = None, keypass: str = None):
    """
    TODO:
    Read user's credit balance from servers
    :param user_id:
    :param keypass:
    :return: Total amount
    """
    return float(random.randint(0, 100))


def register_unit_worked(user_id: str = None, keypass: str = None) -> bool:
    """
    TODO:
    Register a unit worked to sprawlhub, i.e. earn one sprawlbuck

    :param user_id:
    :param keypass:
    :return successfully_registered:
    """
    return NotImplementedError


class EventLoopWorkerDask(EventDispatcher):
    __events__ = ("on_get_scheduler",)

    def __init__(self, ip, port):
        super().__init__()

        self.ip = ip
        self.port = port

        self._thread = threading.Thread(target=self._run_worker)
        self._thread.daemon = True
        self.nanny_ = None

    def _run_worker(self):
        self.loop = asyncio.get_event_loop_policy().new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.get_scheduler())

    async def get_scheduler(self, nthreads=8):
        """Connect to provider/dask scheduler"""
        async with Nanny(self.ip, self.port, nthreads=nthreads, death_timeout=3) as n:
            self.nanny_ = n
            await n.finished()

    def on_get_scheduler(self, *_):
        pass

    def get_conn_status(self):
        return self.nanny_

    def start(self):
        self._thread.start()


class SprawlbaseApp(App):
    # number of seconds that make up a unit of work
    # i.e. how many seconds earns one sprawlbuck
    unit_of_work = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.event_loop_worker_dask = None
        self.connected_to_scheduler = False

        self.prev_balance = read_user_balance(user_id=None, keypass=None)
        self.balance = read_user_balance(user_id=None, keypass=None)

    def change_button_color_delayed(self, *args, **kwargs):
        earn_sprawlbucks_button = self.root.ids["earn_sprawlbucks_button"]
        earn_sprawlbucks_button.text = "Tap to Earn Sprawlbucks!"
        earn_sprawlbucks_button.background_color = kwargs["color"]

    def connect_to_scheduler(self, *args, **kwargs):

        # prevents obtaining another worker if one already connected
        if self.event_loop_worker_dask and self.connected_to_scheduler:
            return

        earn_sprawlbucks_button = self.root.ids["earn_sprawlbucks_button"]
        earn_sprawlbucks_button.background_color = [1, 1, 1, 1]
        earn_sprawlbucks_button.text = "Searching for a Provider..."

        # connect to a provider
        self.event_loop_worker_dask = EventLoopWorkerDask(
            ip=os.environ["SCHEDULER_IP"], port=os.environ["SCHEDULER_PORT"]
        )
        self.event_loop_worker_dask.start()

        Clock.schedule_interval(
            lambda dt: self.check_conn_status(), SprawlbaseApp.unit_of_work
        )
        Clock.schedule_interval(
            lambda dt: self.update_credit_balance(), SprawlbaseApp.unit_of_work
        )

    def check_conn_status(self):

        earn_sprawlbucks_button = self.root.ids["earn_sprawlbucks_button"]

        # if self.event_loop_worker_dask and self.event_loop_worker_dask.nanny_:
        try:
            if not self.event_loop_worker_dask.nanny_._Server__stopped:
                earn_sprawlbucks_button.text = "Connected to a Provider!"
                earn_sprawlbucks_button.background_color = "green"
                self.connected_to_scheduler = True
            else:
                self.connected_to_scheduler = False
        except AttributeError:
            self.connected_to_scheduler = False

        # register unit of work for credits to sprawlbase server:
        if self.connected_to_scheduler:
            self.register_unit_of_work()

        if not self.connected_to_scheduler:
            earn_sprawlbucks_button.text = "Lost connection to scheduler!"
            earn_sprawlbucks_button.background_color = "red"
            logger.log(
                logging.ERROR,
                f"Lost connection dask scheduler "
                f"""on {os.environ["SCHEDULER_IP"] + ' port ' + str(os.environ["SCHEDULER_PORT"])}""",
            )

    def update_amount_bucks(self):
        bucks_amount_label = self.root.ids["amount_bucks_label"]
        bucks_amount_label.text = str(round(bucks_amount_label.amount_bucks, 2))
        if self.prev_balance < self.balance:
            bucks_amount_label.color = "green"
            bucks_amount_label.text += "  ^"
            bucks_amount_label.size = (0.45, 0.45)
        else:
            bucks_amount_label.color = "white"
            bucks_amount_label.size = (0.1, 0.1)
        logger.log(
            logging.INFO, f"Update credit balance: {bucks_amount_label.amount_bucks}"
        )

    def build(self):
        # read user's total credits from servers
        bucks_amount_label = self.root.ids["amount_bucks_label"]
        bucks_amount_label.amount_bucks += read_user_balance(None, None)

    def register_unit_of_work(self):
        pass

    def update_credit_balance(self):
        self.prev_balance = self.balance
        self.balance = read_user_balance()
        self.root.ids["amount_bucks_label"].amount_bucks = self.balance


if __name__ == "__main__":
    app = SprawlbaseApp(title="Sprawlbase")
    app.run()
