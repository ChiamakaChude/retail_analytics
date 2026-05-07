import logging
from utils.logging import log_event
from utils.config import TYPE_MAPPING


logger = logging.getLogger(__name__)


def build_orders_gold(datasets, resolved_models):
    try:

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

        log_event(logger, "INFO", "fact_order_items_schema", schema={field.name: str(field.dataType) for field in fact_order_items.schema.fields})


        log_event(logger, "INFO", "transform_orders_gold_building_dimensions", tables=["products", "aisles", "departments"])
        dim_products = (
        products.select(
            "product_id",
            "product_name",
            "aisle_id",
            "department_id"
        )
        .dropna(subset=[
            "product_id",
            "product_name",
            "aisle_id",
            "department_id"
        ])
        .dropDuplicates()
    )
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

        fact_order_items = enforce_schema(fact_order_items, resolved_models["fact_orders"]["schema"])
        dim_products = enforce_schema(dim_products, resolved_models["dim_products"]["schema"])
        dim_aisles = enforce_schema(dim_aisles, resolved_models["dim_aisles"]["schema"])
        dim_departments = enforce_schema(dim_departments, resolved_models["dim_departments"]["schema"])

        return {
            "orders": fact_order_items,
            "products": dim_products,
            "aisles": dim_aisles,
            "departments": dim_departments
        }
    except Exception as e:
        log_event(logger, "ERROR", "transform_orders_gold_failed", error=str(e))
        raise

def enforce_schema(df, spark_schema):
    for field in spark_schema.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, df[field.name].cast(field.dataType))
    return df