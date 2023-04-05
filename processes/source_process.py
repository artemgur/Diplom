import json
import time

from api import json_api
from constants import SLEEP_TIME_BETWEEN_QUERIES
from groupby import Groupby
from sources import Source
import rows_formatter
import utilities.list


def start_source_process(source: Source, queries_dict: dict, responses_dict: dict, view_names: dict):
    source_process = SourceProcess(source, queries_dict, responses_dict, view_names)
    source_process.run()


class SourceProcess:
    def __init__(self, source: Source, queries_dict: dict, responses_dict: dict, view_names: dict):
        self.source = source
        self.queries_dict = queries_dict
        self.responses_dict = responses_dict
        self.view_names = view_names

        self.views_to_drop = {}
        self.drop_source_request_dict = None


    def run(self):
        while True:
            self.source.listen()

            self.check_source_queries()
            self.check_view_queries()

            self.finalize_drop_view_queries()

            if self.drop_source_request_dict:
                self.finalize_drop_source(self.drop_source_request_dict)
                break

            time.sleep(SLEEP_TIME_BETWEEN_QUERIES)


    def check_source_queries(self):
        """
        Checks if there are queries to the source itself. If such queries are found, executes them.
        """
        source_name = 'source.' + self.source.name
        if source_name in self.queries_dict:
            queries = self.queries_dict[source_name]
            for value in queries:
                request_dict = json.loads(value)
                self.run_source_query(request_dict)
            self.queries_dict[source_name] = utilities.list.difference(self.queries_dict[source_name], queries)


    def check_view_queries(self):
        """
        Checks if there are queries to views of the source. If such queries are found, executes them.
        """
        for view in self.source.views:
            view_name = 'view.' + view.name
            if view_name in self.queries_dict:
                queries = self.queries_dict[view_name]
                for value in queries:
                    request_dict = json.loads(value)
                    self.run_view_query(request_dict, view)
                self.queries_dict[view_name] = utilities.list.difference(self.queries_dict[view_name], queries)


    def finalize_drop_view_queries(self):
        """
        Finalizes all DROP MATERIALIZED VIEW queries.

        Deletes views which were marked for deletion in self.drop_view(...)
        """
        for view_name in self.views_to_drop:
            request_dict, view = self.views_to_drop[view_name]
            self.drop_view_finalize(request_dict, view)
        self.views_to_drop = {}


    def run_source_query(self, request_dict: dict):
        """
        Maps source queries to relevant functions.
        """
        match json_api.query_type(request_dict):
            case 'CREATE MATERIALIZED VIEW':
                self.create_view(request_dict)
            case 'DROP SOURCE':
                self.drop_source(request_dict)


    def run_view_query(self, request_dict: dict, view: Groupby):
        """
        Maps view queries to relevant functions.
        """
        match json_api.query_type(request_dict):
            case 'SELECT' | 'SELECT EXTRAPOLATED':
                self.select(request_dict, view)
            case 'DROP MATERIALIZED VIEW':
                self.drop_view(request_dict, view)


    @json_api.error_decorator
    def create_view(self, request_dict: dict):
        """
        Executes CREATE MATERIALIZED VIEW query.
        """
        name = json_api.name(request_dict)
        if name in self.view_names:
            raise ValueError(f'View with name {name} already exists')
        groupby_columns = json_api.groupby_columns(request_dict)
        where = json_api.view_where(request_dict)
        parameters = json_api.parameters(request_dict)
        column_aliases = json_api.column_aliases(request_dict)
        aggregate_initializers = json_api.aggregate_initializers(request_dict)
        view = Groupby(name=name, groupby_columns=groupby_columns, aggregate_initializers=aggregate_initializers,
                       where=where, column_aliases=column_aliases, **parameters)
        self.view_names[name] = 1
        #source_name = json_api.view_source_name(request_dict)
        #source = sources.base.sources[source_name]
        self.source.subscribe(view)
        json_api.send_response('OK', request_dict, self.responses_dict)


    def drop_view(self, request_dict: dict, view: Groupby):
        """
        Executes DROP MATERIALIZED VIEW query.

        Actually just marks the view for deletion, actual deletion is performed in self.finalize_drop_view_queries()
        """
        self.views_to_drop[view.name] = (request_dict, view)


    def drop_source(self, request_dict: dict):
        """
        Executes DROP SOURCE query.

        Actually just marks the source for deletion, actual deletion is performed in self.finalize_drop_source()
        """
        self.drop_source_request_dict = request_dict

    @json_api.error_decorator
    def drop_view_finalize(self, request_dict: dict, view: Groupby):
        """
        Finalizes DROP MATERIALIZED VIEW query.
        """
        name = json_api.name(request_dict)
        self.source.unsubscribe(view)
        del self.view_names[name]
        json_api.send_response('OK', request_dict, self.responses_dict)


    @json_api.error_decorator
    def select(self, request_dict: dict, view: Groupby):
        """
        Executes SELECT or SELECT EXTRAPOLATED query.
        """
        columns = json_api.columns(request_dict)
        where = json_api.select_where(request_dict, view.column_names)
        orderby_list = json_api.orderby(request_dict)

        if json_api.query_type(request_dict) == 'SELECT EXTRAPOLATED':
            # SELECT EXTRAPOLATED query
            extrapolation_timestamp = json_api.extrapolation_timestamp(request_dict)
            rows = view.select_extrapolated(column_names=columns, where=where, extrapolation_timestamp=extrapolation_timestamp)
        else:
            # SELECT query
            rows = view.select(column_names=columns, where=where)

        if len(orderby_list) > 0:
            rows = view.orderby(column_names=columns, rows=rows, orderby_list=orderby_list)
        if columns is None:
            columns = view.column_names
        result = rows_formatter.format(rows, column_names=columns, format_type=json_api.format(request_dict))
        json_api.send_response(result, request_dict, self.responses_dict)


    @json_api.error_decorator
    def finalize_drop_source(self, request_dict: dict):
        """
        Finalizes the SourceProcess.
        """
        for view in self.source.views:
            del self.view_names[view.name]
        json_api.send_response('OK', request_dict, self.responses_dict)


