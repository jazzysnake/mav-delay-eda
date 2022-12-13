# originally from https://github.com/NLeSC/dask-cassandra-loader
# minor mods to enable port specification
from cassandra.cluster import Cluster
from cassandra.protocol import NumpyProtocolHandler
from cassandra.auth import PlainTextAuthProvider
from dask.distributed import Client, LocalCluster
import copy
import dask
import dask.dataframe as dd
import logging
import pandas as pd
from sqlalchemy import sql
from sqlalchemy.sql import text
from threading import Event


class PagedResultHandler(object):
    """ An handler for paged loading of a Cassandra's query result. """

    def __init__(self, future):
        """
        Initialization of PagedResultHandler
        > handler = PagedResultHandler(future)
        :param future: Future from Cassandra session asynchronous execution.
        """
        self.error = None
        self.finished_event = Event()
        self.future = future
        self.future.add_callbacks(callback=self.handle_page,
                                  errback=self.handle_error)
        self.df = None

    def handle_page(self, rows):
        """
        It pages the result of a Cassandra query.
        > handle_page(rows)
        :param rows: Cassandra's query result.
        :return:
        """
        if self.df is None:
            self.df = rows
        else:
            self.df = pd.concat([self.df, rows], ignore_index=True)

        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
        else:
            self.finished_event.set()

    def handle_error(self, exc):
        """
        It handles and exception.
        > handle_error(exc)
        :param exc: It is a Python Exception.
        :return:
        """
        self.error = exc
        self.finished_event.set()


class Connector(object):
    """ It sets and manages a connection to a Cassandra Cluster. """

    def __init__(self, cassandra_clusters, cassandra_keyspace, username,
                 password, port=9042, timeout = 10):
        """
        Initialization of CassandraConnector. It connects to a Cassandra cluster defined by a list of IPs.
        If the connection is successful, it then establishes a session with a Cassandra keyspace.

        > CassandraConnector(['10.0.1.1', '10.0.1.2'], 'test')

        :param cassandra_clusters: It is a list of IPs with each IP represented as a string.
        :param cassandra_keyspace: It is a string which contains an existent Cassandra keyspace.
        :param username: It is a String.
        :param password: It is a String.
        """
        self.logger = logging.getLogger(__name__)
        self.clusters = cassandra_clusters
        self.keyspace = cassandra_keyspace
        self.auth = None
        self.port = port

        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)

        # Connect to Cassandra
        self.logger.info("connecting to:" + str(self.clusters) + ".\n")
        if username is None:
            self.cluster = Cluster(self.clusters)
        else:
            self.auth = PlainTextAuthProvider(username=username,
                                              password=password)
            self.cluster = Cluster(self.clusters,
                                   auth_provider=self.auth,
                                   port=port,
                                   connect_timeout = timeout,
                                   )
        self.session = self.cluster.connect(self.keyspace)
        self.session.default_timeout = timeout

        # Configure session to return a Pandas dataframe
        self.session.client_protocol_handler = NumpyProtocolHandler
        self.session.row_factory = pandas_factory

        # Tables
        self.tables = dict()
        return

    def shutdown(self):
        """
        Shutdowns the existing connection with a Cassandra cluster.

        > shutdown()

        """
        self.session.shutdown()
        self.cluster.shutdown()
        return


class Operators(object):
    """ Operators for a valida SQL select statement over a Cassandra Table. """

    def __init__(self):
        """
        Initialization of CassandraOperators.

         > CassandraOperators()

        """
        self.logger = logging.getLogger(__name__)
        self.operators = [
            "less_than_equal", "less_than", "greater_than_equal",
            "greater_than", "equal", "between", "like", "in_", "notin_"
        ]
        self.si_operators = [
            "less_than_equal", "less_than", "greater_than_equal",
            "greater_than", "equal", "like"
        ]
        self.bi_operators = ["between"]
        self.li_operators = ["in_", "notin_"]
        return

    @staticmethod
    def create_predicate(table, col_name, op_name, values):
        """
        It creates a single predicate over a table's column using an operator. Call CassandraOperators.print_operators()
         to print all available operators.

        > create_predicate(table, 'month', 'les_than', 1)

        :param table: Instance of CassandraTable.
        :param col_name: Table's column name as string.
        :param op_name: Operators name as string.
        :param values: List of values. The number of values depends on the operator.
        """
        if op_name == "less_than_equal":
            return table.predicate_cols[col_name] <= values[0]
        elif op_name == "less_than":
            return table.predicate_cols[col_name] < values[0]
        elif op_name == "greater_than_equal":
            return table.predicate_cols[col_name] >= values[0]
        elif op_name == "greater_than":
            return table.predicate_cols[col_name] > values[0]
        elif op_name == "equal":
            return table.predicate_cols[col_name] == values[0]
        elif op_name == "between":
            return table.predicate_cols[col_name].between(values[0], values[1])
        elif op_name == "like":
            return table.predicate_cols[col_name].like(values[0])
        elif op_name == "in_":
            return table.predicate_cols[col_name].in_(values)
        elif op_name == "notin_":
            return table.predicate_cols[col_name].notin_(values)
        else:
            raise Exception("Invalid operator!!!")
        return

    def print_operators(self):
        """
        Print all the operators that can be used in a SQL select statement over a Cassandra's table.

        > print_operators()

        """
        print("The single value operators - op(x) - are: " +
              str(self.si_operators) + ".")
        print("The binary operators - op(x,y) - are: " +
              str(self.bi_operators) + ".")
        print("The list of values operators - op([x,y,...,z]) - are: " +
              str(self.li_operators) + ".")


class LoadingQuery(object):
    """ Class to define a SQL select statement over a Cassandra table. """

    def __init__(self):
        """
        Initialization of CassandraLoadingQuery

        > CassandraLoadingQuery()

        """
        self.logger = logging.getLogger(__name__)
        self.projections = None
        self.and_predicates = None
        self.sql_query = None
        return

    def set_projections(self, table, projections):
        """
        It set the list of columns to be projected, i.e., selected.

        > set_projections(table, ['id', 'year', 'month', 'day'])

        :param table: Instance of class CassandraTable
        :param projections: A list of columns names. Each column name is a String.
        """
        if projections is None or len(projections) == 0:
            self.logger.info("All columns will be projected!!!")
            self.projections = projections
        else:
            for col in projections:
                if col not in table.cols:
                    raise Exception(
                        "set_projections failed: Invalid column, please use one of the following columns: "
                        + str(table.cols) + "!!!")
            self.projections = list(dict.fromkeys(projections))
        return

    def drop_projections(self):
        """
        It drops the list of columns to be projected, i.e., selected.

        > drop_projections()

        """
        self.projections = None
        return

    def set_and_predicates(self, table, predicates):
        """
        It sets a list of predicates with 'and' clause over the non partition columns of a Cassandra's table.

        > set_and_predicates(table, [('month', 'less_than', 1), ('day', 'in\_', [1,2,3,8,12,30])])

        :param table: Instance of class CassandraTable.
        :param predicates: List of triples. Each triple contains column name as String,
         operator name as String, and a list of values depending on the operator. CassandraOperators.print_operators()
         prints all available operators. It should only contain columns which are not partition columns.
        """
        if predicates is None or len(predicates) == 0:
            self.logger.info(
                "No predicates over the non primary key columns were defined!!!"
            )
        else:
            operators = Operators()
            for predicate in predicates:
                (col, op, values) = predicate
                if col not in table.predicate_cols:
                    raise Exception(
                        "set_and_predicates failed: Predicate " + str(predicate) +
                        " has an primary key column. Pick a non-primary key column "
                        + str(table.predicate_cols.keys() + "!!!\n"))
                else:
                    if self.and_predicates is None:
                        self.and_predicates = [
                            operators.create_predicate(table, col, op, values)
                        ]
                    else:
                        self.and_predicates.append(
                            operators.create_predicate(table, col, op, values))
        return

    def remove_and_predicates(self):
        """
        It drops the list of predicates with 'and' clause over the non partition columns of a Cassandra's table.

        > remove_and_predicates()

        """
        self.and_predicates = None
        return

    @staticmethod
    def partition_elimination(table, partitions_to_load, force):
        """
        It does partition elimination when by selecting only a range of partition key values.

        > partition_elimination( table, [(id, [1, 2, 3, 4, 5, 6]), ('year',[2019])] )

        :param table: Instance of a CassandraTable
        :param partitions_to_eliminate: List of tuples. Each tuple as a column name as String
         and a list of keys which should be selected. It should only contain columns which are partition columns.
        :param force: It is a boolean. In case all the partitions need to be loaded, which is not recommended,
         it should be set to 'True'.
        """
        part_cols_prun = dict.fromkeys(table.partition_cols)

        if partitions_to_load is None or len(partitions_to_load) == 0:
            if force is True:
                return
            else:
                raise Exception(
                    "partition_elimination failed: ATTENTION, all partitions will be loaded, query might be aborted!!!"
                    + " To proceed re-call the function with force = True.")
        else:
            for partition in partitions_to_load:
                (col, part_keys) = partition
                if col not in table.partition_cols:
                    raise Exception(
                        "partition_elimination failed: Column " + str(col) +
                        " is not a partition column. It should be one of " +
                        str(table.partition_cols) + ".")
                else:
                    try:
                        part_cols_prun[col] = list(map(float, part_keys))
                    except Exception as e:
                        raise ("partition_elimination failed: Invalid value in the partition keys list: " +
                               str(e) + " !!!")

        for col in list(part_cols_prun.keys()):
            if col in list(table.partition_cols):
                if part_cols_prun[col] is not None:
                    table.partition_keys = table.partition_keys[
                        table.partition_keys[col].isin(part_cols_prun[col])]
        return

    def build_query(self, table):
        """
        It builds and compiles the query which will be used to load data from a Cassandra table into a Dask Dataframe.

        > build_query(table)

        :param table: Instance of CassandraTable.
        """
        if self.projections is None:
            self.sql_query = sql.select([text('*')
                                         ]).select_from(text(table.name))
        else:
            self.sql_query = sql.select([text(f) for f in self.projections
                                         ]).select_from(text(table.name))

        if self.and_predicates is not None:
            self.sql_query = self.sql_query.where(
                sql.expression.and_(*self.and_predicates))
        return

    def print_query(self):
        """
        It prints the query which will be used to load data from a Cassandra table into a Dask Dataframe.

        > print_query()

        """
        if self.sql_query is None:
            raise Exception("print_query failed: The query needs first to be defined!!! ")
        else:
            self.logger.info(
                self.sql_query.compile(compile_kwargs={"literal_binds": True}))
        return


class Table():
    """It stores and manages metadata and data from a Cassandra table loaded into a Dask DataFrame."""

    def __init__(self, keyspace, name):
        """
        Initialization of a CassandraTable.

        > table = CassandraTable('test', 'tab1')

        :param keyspace: It is a string which contains an existent Cassandra keyspace.
        :param name: It is a String.
        """
        self.logger = logging.getLogger(__name__)
        self.keyspace = keyspace
        self.name = name
        self.cols = None
        self.partition_cols = None
        self.partition_keys = None
        self.predicate_cols = None

        # loading query
        self.loading_query = None
        self.data = None
        return

    def load_metadata(self, cassandra_connection):
        """
        It loads metadata from a Cassandra Table. It loads the columns names, partition columns,
        and partition columns keys.

        > load_metadata( cassandra_con)

        :param cassandra_connection: It is an instance from a CassandraConnector
        """
        keyspaces = cassandra_connection.session.cluster.metadata.keyspaces
        self.cols = list(keyspaces[self.keyspace].tables[self.name].columns.keys())
        self.partition_cols = [f.name for f in keyspaces[self.keyspace].tables[self.name].partition_key[:]]

        # load partition keys
        sql_query = sql.select([text(f) for f in self.partition_cols
                                ]).distinct().select_from(text(self.name))
        future = cassandra_connection.session.execute_async(str(sql_query))
        handler = PagedResultHandler(future)
        handler.finished_event.wait()

        if handler.error:
            raise Exception("load_metadata failed: " + str(handler.error))
        else:
            self.partition_keys = handler.df

        # Create dictionary for columns which are not partition columns.
        self.predicate_cols = dict.fromkeys(
            [f for f in self.cols if f not in list(self.partition_cols)])
        for col in self.cols:
            self.predicate_cols[col] = sql.expression.column(col)
        return

    def print_metadata(self):
        """
        It prints the metadata of a CassandraTable.

        > print_metadata()

        """
        self.logger.info("The table columns are:" + str(self.cols))
        self.logger.info("The partition columns are:" + str(self.partition_cols))
        return

    @staticmethod
    def __read_data(sql_query, clusters, keyspace, username, password, port=9042, timeout = 60):
        """
        It sets a connection with a Cassandra Cluster and loads a partition from a Cassandra table using a SQL
        statement.

        > __read_data(
            'SELECT id, year, month, day from tab1 where month<1 and day in (1,2,3,8,12,30) and id=1 and year=2019',
            ['10.0.1.1', '10.0.1.2'],
            'test' )

        :param sql_query: A SQL query as string.
        :param clusters: It is a list of IPs with each IP represented as a string.
        :param keyspace: It is a string which contains an existent Cassandra keyspace.
        :param username: It is a string.
        :param password: It is a string.
        """
        from cassandra.cluster import Cluster
        from cassandra.protocol import NumpyProtocolHandler
        from cassandra.auth import PlainTextAuthProvider

        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)

        df = None

        # Set connection to a Cassandra Cluster

        if username is None:
            cluster = Cluster(clusters),
        else:
            auth = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster(clusters,
                              auth_provider=auth,port=port)

        session = cluster.connect(keyspace)

        # Configure session to return a Pandas dataframe
        session.client_protocol_handler = NumpyProtocolHandler
        session.row_factory = pandas_factory
        # Query Cassandra
        try:
            future = session.execute_async(sql_query, timeout=timeout)
            handler = PagedResultHandler(future)
            handler.finished_event.wait()
        except Exception as e:
            raise AssertionError("__read_data failed: " + str(e))
        else:
            if handler.error:
                raise Exception("__read_data failed: " +
                                str(handler.error))
            else:
                df = handler.df

        # Shutdown session
        session.shutdown()
        return df

    def load_data(self, cassandra_connection, ca_loading_query):
        """
        It defines a set of SQL queries to load partitions of a Cassandra table in parallel into a Dask DataFrame.

        > load_data( cassandra_con, ca_loading_query)

        :param cassandra_connection: Instance of CassandraConnector.
        :param ca_loading_query: Instance of CassandraLoadingQuery.
        """
        futures = []

        if self.cols is None:
            self.load_metadata(cassandra_connection)

        # Reset the table's query
        self.loading_query = ca_loading_query

        # Schedule the reads
        partition_keys = self.partition_keys.to_numpy()
        for key_values in partition_keys:
            self.logger.info("Schedule a read.")
            sql_query = copy.deepcopy(self.loading_query.sql_query)
            sql_query.append_whereclause(
                text(' and '.join('%s=%s' % t for t in zip(self.partition_cols, key_values)) + ' ALLOW FILTERING'))
            query = str(
                sql_query.compile(compile_kwargs={"literal_binds": True}))
            future = dask.delayed(self.__read_data)(query,
                                                    cassandra_connection.session.cluster.contact_points,
                                                    self.keyspace,
                                                    cassandra_connection.auth.username,
                                                    cassandra_connection.auth.password,
                                                    cassandra_connection.port)

            futures.append(future)

        # Collect results
        if len(futures) == 0:
            self.data = None
        else:
            self.logger.info("Wait for reads.")
            df = dd.from_delayed(futures)
            self.logger.info("Start computing.")
            self.data = df
            self.logger.info("Computing ended.")
        return


class DaskCassandraLoaderException(Exception):
    """ Raise when the DaskCassandraLoader fails. """
    pass


class Loader(object):
    """  A loader to populate a Dask Dataframe with data from a Cassandra table. """

    def __init__(self):
        """
        Initialization of DaskCassandraLoader

        > DaskCassandraLoader()

        """
        self.logger = logging.getLogger(__name__)
        self.cassandra_con = None
        self.dask_client = None
        return

    def connect_to_local_dask(self):
        """
        Connects to a local Dask cluster.

        > connect_to_local_dask()

        """
        self.logger.info("Connecting to Dask")
        self.logger.info('Create and connect to a local Dask cluster.')
        dask_cluster = LocalCluster(
            scheduler_port=0,
            silence_logs=True,
            processes=True,
            asynchronous=False,
        )
        self.dask_client = Client(dask_cluster, asynchronous=False)
        self.logger.info("Connected to Dask")
        return

    def connect_to_dask(self, dask_cluster):
        """
        Connect to a Dask Cluster

        > connect_to_Dask('127.0.0.1:8786')
        or
        > connect_to_Dask(cluser)

        :param dask_cluster: String with format url:port or an instance of Cluster
        """

        self.logger.info("Connecting to Dask")
        self.logger.info('Create and connect to a local Dask cluster.')
        self.dask_client = Client(dask_cluster, asynchronous=False)
        self.logger.info("Connected to Dask")
        return

    def disconnect_from_dask(self):
        """
        Ends the established Dask connection.

        > disconnect_from_dask()

        """
        self.dask_client.close()
        return

    def connect_to_cassandra(self, cassandra_clusters, cassandra_keyspace,
                             username, password, port=9042, timeout=10):
        """
        Connects to a Cassandra cluster specified by a list of IPs.

        > connect_to_cassandra('test', ['10.0.1.1', '10.0.1.2'])

        :param cassandra_keyspace: It is a string which contains an existent Cassandra keyspace.
        :param cassandra_clusters: It is a list of IPs with each IP represented as a string.
        :param username: It is a string.
        :param password: It is a string.
        """
        if cassandra_keyspace == "":
            raise DaskCassandraLoaderException("connect_to_cassandra failed: Key space can't be an empty string!!!")
        try:
            self.cassandra_con = Connector(cassandra_clusters,
                                           cassandra_keyspace,
                                           username, password,
                                           port = port,
                                           timeout = timeout
                                          )
        except Exception as e:
            raise DaskCassandraLoaderException(
                "connect_to_cassandra failed: It was not possible to set a connection with the Cassandra cluster: "
                + str(e))
        return

    def disconnect_from_cassandra(self):
        """
        Ends the established Cassandra connection.

        > disconnect_from_cassandra()

        """
        if self.cassandra_con is not None:
            self.cassandra_con.shutdown()
        return

    def load_cassandra_table(self, table_name, projections, and_predicates,
                             partitions_to_load, force=False):
        """
        It loads a Cassandra table into a Dask dataframe.

        > load_cassandra_table('tab1',
                               ['id', 'year', 'month', 'day'],
                               [('month', 'less_than', [1]), ('day', 'in\_', [1,2,3,8,12,30])],
                               [('id', [1, 2, 3, 4, 5, 6]), ('year',[2019])])

        :param table_name: It is a String.
        :param projections: A list of columns names. Each column name is a String.
        :param and_predicates: List of triples. Each triple contains column name as String,
         operator name as String, and a list of values depending on the operator. CassandraOperators.print_operators()
         prints all available operators. It should only contain columns which are not partition columns.
        :param partitions_to_load: List of tuples. Each tuple as a column name as String.
         and a list of keys which should be selected. It should only contain columns which are partition columns.
        :param force: It is a boolean. In case all the partitions need to be loaded, which is not recommended,
         it should be set to 'True'. By Default it is set to 'False'.
        """

        table = Table(self.cassandra_con.keyspace, table_name)

        try:
            table.load_metadata(self.cassandra_con)
        except Exception as e:
            raise DaskCassandraLoaderException("load_cassandra_table failed: ") from e

        loading_query = LoadingQuery()

        try:
            loading_query.set_projections(table, projections)
        except Exception as e:
            raise DaskCassandraLoaderException("load_cassandra_table failed: ") from e

        try:
            loading_query.set_and_predicates(table, and_predicates)
        except Exception as e:
            raise DaskCassandraLoaderException("load_cassandra_table failed: ") from e

        try:
            loading_query.partition_elimination(table, partitions_to_load, force)
        except Exception as e:
            raise DaskCassandraLoaderException("load_cassandra_table failed: ") from e

        try:
            loading_query.build_query(table)
        except Exception as e:
            raise DaskCassandraLoaderException("load_cassandra_table failed: ") from e

        try:
            table.load_data(self.cassandra_con, loading_query)
        except Exception as e:
            raise DaskCassandraLoaderException("load_cassandra_table failed: ") from e

        return table