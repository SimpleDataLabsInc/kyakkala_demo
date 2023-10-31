from customer_order.graph.Subgraph_1.config.Config import SubgraphConfig as Subgraph_1_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, Subgraph_1: dict=None, db_name: str=None, enable_customer_order_laod: bool=None, **kwargs):
        self.spark = None
        self.update(Subgraph_1, db_name, enable_customer_order_laod)

    def update(
            self,
            Subgraph_1: dict={},
            db_name: str="prophecy_demos",
            enable_customer_order_laod: bool=True,
            **kwargs
    ):
        prophecy_spark = self.spark
        self.Subgraph_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_1, 
            Subgraph_1_Config
        )
        self.db_name = db_name
        self.enable_customer_order_laod = self.get_bool_value(enable_customer_order_laod)
        pass
