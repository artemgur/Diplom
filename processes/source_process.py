import json
import time

from api import json_api
from constants import SLEEP_TIME_BETWEEN_QUERIES
from materialized_view import MaterializedView
from sources import Source
import rows_formatter
import utilities.list


def start_source_process(source: Source, queries_dict: dict, responses_dict: dict, view_names: dict):
    source_process = SourceProcess(source, queries_dict, responses_dict, view_names)
    source_process.run()


class SourceProcess:
    def __init__(self, source: Source, queries_dict: dict, responses_dict: dict, view_names: dict):
        self._source = source
        self.queries_dict = queries_dict
        self.responses_dict = responses_dict
        self._view_names = view_names

        self._views_to_drop = {}
        self._drop_source_request_dict = None


    def run(self):
        while True:
            self._source.listen()

            self._check_source_queries()
            self._check_view_queries()

            self._finalize_drop_view_queries()

            if self._drop_source_request_dict:
                self._finalize_drop_source(self._drop_source_request_dict)
                break

            time.sleep(SLEEP_TIME_BETWEEN_QUERIES)


    def _check_source_queries(self):
        """
        Checks if there are queries to the source itself. If such queries are found, executes them.
        """
        source_name = 'source.' + self._source.name
        if source_name in self.queries_dict:
            queries = self.queries_dict[source_name]
            for value in queries:
                request_dict = json.loads(value)
                self._run_source_query(request_dict)
            self.queries_dict[source_name] = utilities.list.difference(self.queries_dict[source_name], queries)


    def _check_view_queries(self):
        """
        Checks if there are queries to views of the source. If such queries are found, executes them.
        """
        for view in self._source.views:
            view_name = 'view.' + view.name
            if view_name in self.queries_dict:
                queries = self.queries_dict[view_name]
                for value in queries:
                    request_dict = json.loads(value)
                    self._run_view_query(request_dict, view)
                self.queries_dict[view_name] = utilities.list.difference(self.queries_dict[view_name], queries)


    def _finalize_drop_view_queries(self):
        """
        Finalizes all DROP MATERIALIZED VIEW queries.

        Deletes views which were marked for deletion in self.drop_view(...)
        """
        for view_name in self._views_to_drop:
            request_dict, view = self._views_to_drop[view_name]
            self._drop_view_finalize(request_dict, view)
        self._views_to_drop = {}


    def _run_source_query(self, request_dict: dict):
        """
        Maps source queries to relevant functions.
        """
        match json_api.query_type(request_dict):
            case 'CREATE MATERIALIZED VIEW':
                self._create_view(request_dict)
            case 'DROP SOURCE':
                self._drop_source(request_dict)


    def _run_view_query(self, request_dict: dict, view: MaterializedView):
        """
        Maps view queries to relevant functions.
        """
        match json_api.query_type(request_dict):
            case 'SELECT' | 'SELECT FORECASTED':
                self._select(request_dict, view)
            case 'DROP MATERIALIZED VIEW':
                self._drop_view(request_dict, view)


    @json_api.error_decorator
    def _create_view(self, request_dict: dict):
        """
        Executes CREATE MATERIALIZED VIEW query.
        """
        name = json_api.name(request_dict)
        if name in self._view_names:
            raise ValueError(f'View with name {name} already exists')
        groupby_columns = json_api.groupby_columns(request_dict)
        where = json_api.view_where(request_dict)
        parameters = json_api.parameters(request_dict)
        column_aliases = json_api.column_aliases(request_dict)
        aggregate_initializers = json_api.aggregate_initializers(request_dict)
        view = MaterializedView(name=name, groupby_columns=groupby_columns, aggregate_initializers=aggregate_initializers,
                                where=where, column_aliases=column_aliases, **parameters)
        self._view_names[name] = 1
        #source_name = json_api.view_source_name(request_dict)
        #source = sources.base.sources[source_name]
        self._source.subscribe(view)
        json_api.send_response('OK', request_dict, self.responses_dict)


    def _drop_view(self, request_dict: dict, view: MaterializedView):
        """
        Executes DROP MATERIALIZED VIEW query.

        Actually just marks the view for deletion, actual deletion is performed in self.finalize_drop_view_queries()
        """
        self._views_to_drop[view.name] = (request_dict, view)


    def _drop_source(self, request_dict: dict):
        """
        Executes DROP SOURCE query.

        Actually just marks the source for deletion, actual deletion is performed in self.finalize_drop_source()
        """
        self._drop_source_request_dict = request_dict

    @json_api.error_decorator
    def _drop_view_finalize(self, request_dict: dict, view: MaterializedView):
        """
        Finalizes DROP MATERIALIZED VIEW query.
        """
        name = json_api.name(request_dict)
        self._source.unsubscribe(view)
        del self._view_names[name]
        json_api.send_response('OK', request_dict, self.responses_dict)


    @json_api.error_decorator
    def _select(self, request_dict: dict, view: MaterializedView):
        """
        Executes SELECT or SELECT FORECASTED query.
        """
        columns = json_api.columns(request_dict)
        where = json_api.select_where(request_dict, view.column_names)
        orderby_list = json_api.orderby(request_dict)

        if json_api.query_type(request_dict) == 'SELECT FORECASTED':
            # SELECT FORECASTED query
            extrapolation_timestamp = json_api.extrapolation_timestamp(request_dict)
            extrapolation_offset = json_api.extrapolation_offset(request_dict)
            rows = view.select_extrapolated(column_names=columns, where=where,
                                            extrapolation_timestamp=extrapolation_timestamp, extrapolation_offset=extrapolation_offset)
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
    def _finalize_drop_source(self, request_dict: dict):
        """
        Finalizes the SourceProcess.
        """
        for view in self._source.views:
            del self._view_names[view.name]
        json_api.send_response('OK', request_dict, self.responses_dict)


