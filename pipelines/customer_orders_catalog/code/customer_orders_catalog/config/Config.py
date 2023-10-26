from customer_orders_catalog.graph.Subgraph_1.config.Config import SubgraphConfig as Subgraph_1_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, Subgraph_1: dict=None, db_name: str=None, **kwargs):
        self.spark = None
        self.update(Subgraph_1, db_name)

    def update(self, Subgraph_1: dict={}, db_name: str="prophecy_demos", **kwargs):
        prophecy_spark = self.spark
        self.Subgraph_1 = self.get_config_object(
            prophecy_spark, 
            Subgraph_1_Config(prophecy_spark = prophecy_spark), 
            Subgraph_1, 
            Subgraph_1_Config
        )
        self.db_name = db_name
        pass
