import logging
from utils.logging import log_event

logger = logging.getLogger(__name__)


def build_orders_gold(datasets):

    log_event(logger, "INFO", "transform_orders_gold_started")

    orders = datasets["orders"]
    order_products = datasets["order_products"]
    products = datasets["products"]
    departments = datasets["departments"]
    aisles = datasets["aisles"]

    log_event(logger, "INFO", "transform_orders_gold_joining", tables=["orders", "order_products", "products"])

    fact_order_items = (
    orders
    .join(order_products, "order_id")
    .join(products.select("product_id", "aisle_id", "department_id"), "product_id")
    )
    log_event(logger, "INFO", "transform_orders_gold_joined", tables=["orders", "order_products", "products"], rows=fact_order_items.count())

    log_event(logger, "INFO", "transform_orders_gold_building_dimensions", tables=["products", "aisles", "departments"])
    dim_products = products.select(
    "product_id",
    "product_name",
    "aisle_id",
    "department_id"
    ).dropDuplicates()
    log_event(logger, "INFO", "transform_orders_gold_built_dimensions", tables=["products"], rows=dim_products.count())
    
    dim_aisles = aisles.select(
    "aisle_id",
    "aisle"
    ).dropDuplicates()
    log_event(logger, "INFO", "transform_orders_gold_built_dimensions", tables=["aisles"], rows=dim_aisles.count())

    dim_departments = departments.select(
    "department_id",
    "department_name"
    ).dropDuplicates()
    log_event(logger, "INFO", "transform_orders_gold_built_dimensions", tables=["departments"], rows=dim_departments.count())


    return {
        "orders": fact_order_items,
        "products": dim_products,
        "aisles": dim_aisles,
        "departments": dim_departments
    }