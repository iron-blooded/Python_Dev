import bson
from datetime import datetime


class AggregationTime:
    def __init__(self, location="./sampleDB/sample_collection.bson") -> None:
        with open(location, "rb") as f:
            self.dataset = bson.decode_all(f.read())

    def input(self, task: dict[str:str]) -> dict[str : (list | str)]:
        if not min([i in task for i in ["dt_from", "dt_upto", "group_type"]]):
            raise ValueError("Структура не прошла валидацию")
        for i in ["dt_from", "dt_upto"]:
            task[i] = datetime.fromisoformat(task[i])
        result = self.aggregations(task=task)
        return result

    def aggregations(
        self, task: dict[str : (str | datetime)]
    ) -> dict[str : (list | str)]: 
        ...
