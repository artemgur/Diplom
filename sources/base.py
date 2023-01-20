from abc import ABC, abstractmethod

from groupby import Groupby

#sources = {}  # TODO change from dict to specialized type?

# Add Clickhouse Kafka source?
class Source(ABC):
    def __init__(self, name, **kwargs):
        self._name = name
        #sources[name] = self
        self._subscribed_materialized_views: set[Groupby] = set()


    def subscribe(self, materialized_view: Groupby):
        self._subscribed_materialized_views.add(materialized_view)

    def unsubscribe(self, materialized_view: Groupby):
        self._subscribed_materialized_views.remove(materialized_view)


    def _update_materialized_views(self, old_row: dict, new_row: dict):
        if old_row is None:  # Insert
            if new_row is None:
                raise ValueError("old_row and new_row can't both be None")

            for materialized_view in self._subscribed_materialized_views:
                materialized_view.insert(new_row)
        elif new_row is None: # Delete
            for materialized_view in self._subscribed_materialized_views:
                materialized_view.delete(old_row)
        else:
            for materialized_view in self._subscribed_materialized_views:
                materialized_view.update(old_row, new_row)

    @property
    def name(self):
        return self._name

    @property
    def views(self):
        return self._subscribed_materialized_views

    @abstractmethod
    def listen(self):
        ...