import bson
class AggregationTime:
    def __init__(self, location="./sampleDB/sample_collection.bson") -> None:
        with open(location, "rb") as f:
            self.dataset = bson.decode_all(f.read())
    
    def input(self, task: dict[str:str]) -> dict[str : (list | str)]:
        ...