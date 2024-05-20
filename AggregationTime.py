import bson
from datetime import datetime, timedelta
from collections import defaultdict


class AggregationTime:
    def __init__(self, location="./sampleDB/sample_collection.bson") -> None:
        with open(location, "rb") as f:
            self.dataset = bson.decode_all(f.read())
            self.groups = {
                "hour": timedelta(hours=1),
                "day": timedelta(days=1),
                "week": timedelta(weeks=1),
                "month": timedelta(days=33),
            }

    def main(
        self, task: dict[str:str]
    ) -> dict[str : (list | str)]:  # Точка входа и выхода данных
        if not min([i in task for i in ["dt_from", "dt_upto", "group_type"]]):
            raise ValueError("Структура не прошла валидацию")
        for i in ["dt_from", "dt_upto"]:
            task[i] = datetime.fromisoformat(
                task[i]
            )  # Преобразовываем str iso даты в datetime формат
        result = self.aggregations(task=task)  # Агрегируем данные
        return result

    def aggregations(
        self, task: dict[str : (str | datetime)]
    ) -> dict[str : (list | str)]:  # Агрегация данных
        dates = self.create_dict_results(
            start_date=task["dt_from"],
            end_date=task["dt_upto"],
            step=task["group_type"],
        )  # Формируем массив дат, в который предстоит записывать данные
        for line in self.filter_by_date(
            task["dt_from"], task["dt_upto"]
        ):  # Перебираем отфильтрованные даты
            formatted_date = self.format_date(
                line["dt"], task["group_type"]
            )  # Форматируем дату в соответствии с типом группировки
            dates[formatted_date] += line[
                "value"
            ]  # Прибавляем значение к группированной дате
        result = {
            "dataset": list(dates.values()),
            "labels": list(dates.keys()),
        }  # Преобразуем массив дат в массив заданого формата
        return result

    def filter_by_date(self, min_date: datetime, max_date: datetime) -> dict[str:str]:
        filtered_data = []

        for line in self.dataset:
            entry_date = line["dt"]
            if min_date <= entry_date <= max_date:
                filtered_data.append(line)
        return filtered_data

    def format_date(self, dt: datetime, interval: str) -> str:
        if interval == "hour":
            return dt.strftime("%Y-%m-%dT%H:00:00")
        elif interval == "day":
            return dt.strftime("%Y-%m-%dT00:00:00")
        elif interval == "week":
            start_of_week = dt - datetime.timedelta(
                days=dt.weekday()
            )  # Так как начинается неделя с понедельника
            return start_of_week.strftime("%Y-%m-%dT00:00:00")
        elif interval == "month":
            return dt.strftime("%Y-%m-01T00:00:00")
        else:
            raise ValueError(f"Неподдерживаемый интервал: {interval}")

    def create_dict_results(
        self, start_date: datetime, end_date: datetime, step: str
    ) -> dict[str:0]:
        date = start_date
        result = defaultdict()
        while date <= end_date:
            if step == "hour":
                date = datetime.fromisoformat(date.strftime("%Y-%m-%dT%H:00:00"))
            elif step == "day":
                date = datetime.fromisoformat(date.strftime("%Y-%m-%dT00:00:00"))
            elif step == "week":
                date -= timedelta(days=date.weekday())
                date = datetime.fromisoformat(date.strftime("%Y-%m-%dT00:00:00"))
            elif step == "month":
                date -= timedelta(days=(date.day - 1))
                date = datetime.fromisoformat(date.strftime("%Y-%m-01T00:00:00"))
            else:
                raise ValueError("Переменная step не валидна")
            result[date.strftime("%Y-%m-%dT%H:00:00")] = 0
            date += self.groups[step]
        return result
