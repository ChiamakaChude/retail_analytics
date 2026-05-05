import logging
from utils.logging import log_event

logger = logging.getLogger(__name__)


def build_orders_gold(datasets):

    log_event(logger, "INFO", "transform_orders_gold_started")

    orders = datasets["orders"]
    order_products = datasets["order_products"]
    products = datasets["products"]

    df = orders.join(order_products, "order_id") \
               .join(products, "product_id")

    return df