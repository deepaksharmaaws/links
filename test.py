snowflake.py

import copy
import json
import random
import time
from datetime import datetime
from typing import List, Optional

from pyspark.sql import DataFrame

from transformation.sdk.common import config
from transformation.sdk.common.chime_df import ParserAwareDataFrame
from transformation.sdk.common.config import BQL_END, BQL_LOG
from transformation.sdk.connectors.dts_utils import DTSUtils

# List of modes where schema validation is required.
# We removed 'append' from the list as it caused https://chime.slack.com/archives/C02DSUTKD54/p1742487025000359.
# To be revisited in the future.
SCHEMA_VALIDATION_MODES = []


class SchemaValidationException(Exception):
    """Exception raised when schema validation fails between source and destination tables.

    Attributes:
        message -- explanation of the schema mismatch
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class Snowflake:
    def __init__(self, glue):
        self.glue = glue
        self.log = glue.log
        self.query_tags = {
            "source": "DTS",
            "job": str(self.glue.config_file).split(".")[0] if self.glue.config_file else self.glue.job,
            "team": "",
            "service": "",
            "department": "",
            "owner": "",
            "group owner": "",
            "folder": self.glue.folder,
            "job_run_id": self.glue.parameters["JOB_RUN_ID"] if "JOB_RUN_ID" in self.glue.parameters else "",
            "ff_name": "",
        }
        for key, value in self.query_tags.items():
            self.query_tags[key] = (
                self.glue.parameters["tags"][key]
                if "tags" in self.glue.parameters and key in self.glue.parameters["tags"]
                else value
            )
        self.glue.sfOptions["query_tag"] = str(json.dumps(self.query_tags))
        self.glue.sfOptions["tracing"] = "all"
        self.start_time = datetime.now()
        self.config = config.Config()

        self.job_config = self.config.parse_configuration(self.glue.folder, self.glue.config_file)
        # Getting watermark details only when config file has snowflake source with watermarking enabled
        self.log.info(f"Job Config: {str(self.job_config)}")
        self.is_watermark_enabled = DTSUtils.is_connector_type_exist(
            self.job_config, "snowflake", "watermark"
        )
        self.log.info(f"is_watermark_enabled: {self.is_watermark_enabled}")
        self.watermarks = self.get_all_watermarks() if self.is_watermark_enabled else {}
        self._set_snowflake_query_pushdown()

    def write_df_to_snowflake(self, df: DataFrame, object_type: str, object: str, mode: str,
                              is_backward_compatible=False):
        """
        This function ensures the correct database and schema are used for loading dataframe to Snowflake in sfOptions

        Parameters:
        df - Dataframe name which needs to load in Snowflake.
        object type - Snowflake object type.
        object - Snowflake object name, usually denoted as DATABASE.SCHEMA.TABLE
        mode - Write mode, i.e., upsert, append...
        is_backward_compatible (bool) - Flag to indicate whether to use the fallback configuration.
                                           Defaults to False (primary configuration).
        """
        options = self.glue.sfOptions if not is_backward_compatible else self.glue.sfOptions_backward_compatibility
        try:
            options_strict = copy.deepcopy(options)
            if object and len(object.split(".")) == 3:
                table_ref = object.split(".")
                options_strict["sfDatabase"] = table_ref[0]
                options_strict["sfSchema"] = table_ref[1]
            df.write.format(config.SNOWFLAKE_SOURCE_NAME). \
                options(**options_strict). \
                option(object_type, object). \
                mode(mode). \
                save()
        except Exception as e:
            if self.glue.sfOptions_backward_compatibility and not is_backward_compatible:
                self.log.error(
                    f"Writing dataframe to Snowflake failed under JSSA with {e}, now trying to write "
                    f"the same dataframe with default Snowflake user and role"
                )
                self.write_df_to_snowflake(df, object_type, object, mode, is_backward_compatible=True)
            else:
                # Raise the exception if fallback is unavailable or also fails
                raise e

    def _execute_query_with_backward_compatibility(self, query, is_backward_compatible=False):
        """
        Executes a Snowflake query with backward compatibility.
        If query execution fails with the primary configuration (JSSA), it retries with a fallback
        configuration (default Snowflake user and role).
        Parameters:
            query (str): Query to be executed.
            is_backward_compatible (bool): Flag to indicate whether to use the fallback configuration.
                                           Defaults to False (primary configuration).
        Returns:
            DataFrame: The result of the query.
        Raises:
            Exception: If both primary and fallback configurations fail.
        """
        # Choose the appropriate Snowflake connection options
        options = self.glue.sfOptions if not is_backward_compatible else self.glue.sfOptions_backward_compatibility
        try:
            # Attempt to execute the query
            return (
                self.glue.spark.read.format(config.SNOWFLAKE_SOURCE_NAME)
                .options(**options)
                .option("query", query)
                .load()
            )
        except Exception as e:
            # If primary execution fails, attempt fallback if available and not already attempted
            if self.glue.sfOptions_backward_compatibility and not is_backward_compatible:
                self.log.error(
                    f"Snowflake query execution failed under JSSA with {e}, now trying to execute "
                    f"the same query with default Snowflake user and role"
                )
                return self._execute_query_with_backward_compatibility(query, is_backward_compatible=True)
            else:
                # Raise the exception if fallback is unavailable or also fails
                raise e

    def _execute_statement_with_backward_compatibility(self, query, is_backward_compatible=False):
        """
        Executes a Snowflake DML/DDL statement with backward compatibility.
        If statement execution fails with the primary configuration (JSSA), it retries with a fallback
        configuration (default Snowflake user and role).
        Parameters:
            query (str): Query to be executed.
            is_backward_compatible (bool): Flag to indicate whether to use the fallback configuration.
                                           Defaults to False (primary configuration).
        Raises:
            Exception: If both primary and fallback configurations fail.
        """
        # Choose the appropriate Snowflake connection options
        options = self.glue.sfOptions if not is_backward_compatible else self.glue.sfOptions_backward_compatibility
        try:
            # Attempt to execute the query
            self.glue.sc._gateway.jvm.net.snowflake.spark.snowflake.Utils.runQuery(options, query)
        except Exception as e:
            # If primary execution fails, attempt fallback if available and not already attempted
            if self.glue.sfOptions_backward_compatibility and not is_backward_compatible:
                self.log.error(
                    f"Snowflake statement execution failed under JSSA with {e}, now trying to execute "
                                  f"the same statement with default Snowflake user and role"
                )
                return self._execute_statement_with_backward_compatibility(query, is_backward_compatible=True)
            else:
                # Raise the exception if fallback is unavailable or also fails
                raise e

    def execute_statement(self,
                          query):
        """
        This function uses to execute DML/DDL directly in Snowflake through glue. If you would like to delete the delta data from Snowflake, we can use this function. It offers all the DDL/DML functionality like alter, drop, insert, update, merge, create etc.

        Parameters:
        query - It is a mandatory parameter. We can pass the statement.

        API Call:
        snowflake.execute_statement(session, "merge into analytics.looker.device_sessions t using operation_db.stage_glue.tmp_user_activity s " \ " on s.user_id=t.user_id when matched then delete ")
        """
        self.log.info(f"{BQL_LOG} [inside Snowflake.execute_statement] {query}")

        if self.glue.OPS == "execute":
            self._execute_statement_with_backward_compatibility(query)

    def execute_query(self,
                      query,
                      connector="snowflake",
                      register_table=None,
                      test_data_file=None,
                      file_format='csv',
                      **kwargs):
        """
        Data Preparation library helps to prepare or retrieve the data from Snowflake, S3, Local files, etc in the form DataFrame. Currently, we have set up this library to retrieve data from Snowflake and local files. We can source the data from table, view, queries as the Dataframe. We can register the Dataframe as a table and write the transformation queries against the table.

        API Call - snowflake.execute_query function will take three parameters:

        Parameters:
        query - It is a mandatory parameter. We can pass the Snowflake queries.
        register_table - It is an optional parameter. Most of us going to SQL kind of ETL pipe, to enable subsequent ETL
        transformation logic, we need to specify this parameter. If you are comfortable writing programming kind of ETL
        pipeline, then you can skip this parameter.
        test_data_file - It is an optional parameter. Name and path of test mockup data file. However, it will be mandatory
        in case of unit testing.
        folder - It is an optional parameter. We can provide the test data location by using this parameter.
        file_format - It is an optional parameter. Unit testing will support csv and json file format. Default value will be
        csv.
        **kwargs: any additional parameters in job config that can be read through it

        SQL
        df_tmp_user_activity = snowflake.execute_query(session,"select * from operation_db.stage_glue.tmp_user_activity ","tmp_user_activity")
        df_user_count = glue.execute_query(session,'select count(*) from tmp_user_activity')

        API
        df_tmp_user_activity = snowflake.execute_query(session,"select * from operation_db.stage_glue.tmp_user_activity ","tmp_user_activity")
        df_tmp_user_activity.count()
        """
        self.log.info("Creating dataframe for query ---" + str(query))
        query_after_parameter_substitution = str(query).format(**self.glue.parameters)
        self.log.info("Creating dataframe for query ---" + query_after_parameter_substitution)
        if self.glue.OPS == "test":
            df = self.glue.get_test_dataframe(test_data_file, file_format)
        else:
            df = self._execute_query_with_backward_compatibility(query_after_parameter_substitution)
        df = ParserAwareDataFrame.assimilate(df, sql_query=query_after_parameter_substitution, name=register_table)

        if register_table:
            df.createOrReplaceTempView(register_table)

        return df

    def log_count(self, table):

        query = f"select count(*) cnt from {table}"
        df = self.execute_query(query)
        cnt = df.toJSON().collect()[0].replace('{"CNT":','').replace('}','')
        self.log.info(f"count:{cnt}")
        self.log.put_metric("recordscount", cnt)

    def get_columns(self, object, delta=False):

        target_db = object.split(".")[0]
        meta_column_sql = f"select column_name " \
                          f"from {target_db}.INFORMATION_SCHEMA.COLUMNS " \
                          f"where table_catalog||'.'||table_schema||'.'||table_name=upper('{object}') "
        if delta:
            meta_column_sql += "and column_name not in ('META_FEATURE_FAMILY_NAME','META_DATA_SOURCE','META_JOB','META_DW_CREATED_AT','META_SINK_EVENT_TIMESTAMP') "

        meta_df = self.execute_query(meta_column_sql)
        dict_meta = meta_df.toJSON().collect()
        return dict_meta

    def get_columns_with_defaults(self, object):
        target_db = object.split(".")[0]
        meta_column_sql = f"""
                select distinct 'coalesce(' ||
                case when data_type in ('NUMBER','FLOAT')  then  'round(s.' || column_name  || ',5),' || \'-9999999999\'
                when data_type = 'TEXT' then  's.' || column_name  || '::string,\'\'_NULL_\'\''
                when data_type in ('TIMESTAMP_LTZ','TIME','TIMESTAMP_NTZ','TIMESTAMP_TZ') then  's.' || column_name  || ', to_timestamp(\'\'1600-01-01 00:00:00\'\')'
                when data_type = 'DATE' then  's.' || column_name  || ',to_date(\'\'1600-01-01\'\')'
                when data_type = 'BOOLEAN' then 's.' || column_name  || ',\'\'True\'\''
                when data_type = 'ARRAY' then  's.' || column_name  || ',\'\'[]\'\''
                else  's.' || column_name   ||',\'\'\'\'' end || ') = coalesce(' ||
                case when data_type in ('NUMBER','FLOAT')  then  'round(t.' || column_name  || ',5),' || \'-9999999999\'
                when data_type = 'TEXT' then  't.' || column_name  || '::string,\'\'_NULL_\'\''
                when data_type in ('TIMESTAMP_LTZ','TIME','TIMESTAMP_NTZ','TIMESTAMP_TZ') then  't.' || column_name  || ', to_timestamp(\'\'1600-01-01 00:00:00\'\')'
                when data_type = 'DATE' then  't.' || column_name  || ',to_date(\'\'1600-01-01\'\')'
                when data_type = 'BOOLEAN' then  't.' || column_name  || ',\'\'True\'\''
                when data_type = 'ARRAY' then 't.' || column_name  || ',\'\'[]\'\''
                else  't.' || column_name  ||',\'\'\'\'' end || ') ' COLUMN_NAME
                from {target_db}.INFORMATION_SCHEMA.COLUMNS
                where table_catalog||'.'||table_schema||'.'||table_name=upper('{object}')
                and column_name not in ('META_FEATURE_FAMILY_NAME','META_DATA_SOURCE','META_JOB','META_DW_CREATED_AT','META_SINK_EVENT_TIMESTAMP', 'PREDICTION_TS', 'SNAPSHOT_TIMESTAMP')
            """
        self.log.info(meta_column_sql)
        meta_df = self.execute_query(meta_column_sql)
        dict_meta = meta_df.toJSON().collect()

        return dict_meta


    def load(self,
             df,
             object,
             mode,
             object_type='dbtable',
             connector="snowflake",
             source=None,
             test_data_file=None,
             file_format='csv',
             grain_cols=None,
             additional_filter=None,
             audit_attributes={},
             delta=False,
             delta_default = False,
             exclude_delta_column_list=[],
             **kwargs):
        """
        This function uses to load the Dataframe into Snowflake through glue. We can load the Dataframe as truncate/load or append in Snowflake.

        Parameters:
        Dataframe - Dataframe name which needs to load in Snowflake.
        object type - Snowflake object type.
        target object - Snowflake object name.
        Loading strategy - We can specify append, overwrite or truncate_and_load load strategy in Snowflake.
        test_data_file - It is an optional parameter. Name and path of test mockup data file. However, it will be mandatory in case of unit testing.
        folder - It is an optional parameter. We can provide the test data location by using this parameter.
        file_format - It is an optional parameter. Unit testing will support csv and json file format. Default value will be
        csv.
        **kwargs: any additional parameters in job config that can be read through it

        API Call:
        snowflake.load(session,df_device_sessions,"dbtable", "analytics.looker.device_sessions","overwrite")
        """
        stage_glue_schema = config.get_operation_db_and_stage_glue_schema(self.glue.ENV)
        transformation_schema = config.get_operation_db_and_transformation_schema(self.glue.ENV)

        if self.glue.OPS == "test":
            self.log.info(f"test_data_file={test_data_file}")
            self.glue.load_test_dataframe(df, test_data_file, file_format)
        else:
            self.glue.job_status = 0
            if mode in ['upsert', 'delete', 'snapshot']:
                if grain_cols:
                    joins = "1=1"
                    dist_cols = ''
                    for col in grain_cols:
                        joins = "s." + col + "=" + "t." + col + " and " + joins
                        dist_cols += '{},'.format(col)
                    dist_cols += '1'

                    if additional_filter:
                        joins = joins + " and ( " + additional_filter + " ) "

                    df.registerTempTable("df")
                    df.persist()

                    # generate a source dataset to be loaded into target table
                    source_db_object = stage_glue_schema + ".tmp_" + self.glue.jobname.replace(" ", "_")
                    sql_stmt = "select * from df "
                    df_source_db_object = self.glue.execute_query(sql_stmt)
                    self.write_df_to_snowflake(df_source_db_object, object_type, source_db_object,
                                               "overwrite", False)
                    self.log.info(f"{BQL_LOG} [inside Snowflake.load]: INSERT OVERWRITE "
                                  f"INTO {source_db_object} {sql_stmt}")
                    # initialy the source dataset and grain datasets are the same
                    source_object = source_db_object
                    source_grain = source_db_object

                    if delta and mode == 'upsert':
                        dict_meta = self.get_columns(object, delta)
                        delta_condition = " 1=1 "
                        for col in dict_meta:
                            col_name = json.loads(col)
                            if col_name["COLUMN_NAME"].lower() not in exclude_delta_column_list:
                                delta_condition += " and s." + col_name["COLUMN_NAME"] + "=" + "t." + col_name["COLUMN_NAME"]

                        self.execute_statement(f"create table {object} if not exists "
                                               f"as select * from {source_object} where 1=2")

                        if delta_condition != " 1=1 ":
                            delete_statement = """ merge into """ + source_object + """ t
                            using """ + object + """ s
                            on """ +  delta_condition + """
                            when matched then delete """

                            self.execute_statement(delete_statement)
                            self.log.info(delete_statement)

                    if delta_default and mode == 'upsert':
                        dict_meta = self.get_columns_with_defaults(object)
                        delta_condition = " 1=1 "
                        for col in dict_meta:
                            col_name = json.loads(col)
                            if col_name["COLUMN_NAME"].lower() not in exclude_delta_column_list:
                                delta_condition += " and " + col_name["COLUMN_NAME"]

                        self.execute_statement(f"create table {object} if not exists "
                                               f"as select * from {source_object} where 1=2")

                        if delta_condition != " 1=1 ":
                            delete_statement = """ merge into """ + source_object + """ t
                            using """ + object + """ s
                            on """ +  delta_condition + """
                            when matched then delete """

                            self.execute_statement(delete_statement)
                            self.log.info(delete_statement)

                    # for the snapshot mode the grain dataset contains distinct values of grain columns because
                    # the source dataset can be huge and the "merge-delete" statement will likely get stuck,
                    # thus, the grain dataset will be used in "merge-delete" and the source dataset will be
                    # used in "insert" operation
                    if mode == 'snapshot':
                        source_db_grain= stage_glue_schema + ".tmp_" + self.glue.jobname.replace(" ", "_") + "_grain"
                        sql_stmt="select distinct {} from df".format(dist_cols)
                        df_source_grain = self.glue.execute_query(sql_stmt)
                        self.write_df_to_snowflake(df_source_grain, object_type, source_db_grain,
                                                   "overwrite", False)
                        self.log.info(f"{BQL_LOG} [inside Snowflake.load]: INSERT OVERWRITE "
                                      f"INTO {source_db_grain} {sql_stmt}")

                        source_grain = source_db_grain

                    delete_statement = """ merge into """ + object + """ t
                        using """ + source_grain + """ s
                        on """ + joins + """
                        when matched then delete """
                    self.dml_operation(mode, delete_statement, source_object, object)
                    self.log_count(source_grain)
                    if source_grain.startswith(stage_glue_schema + ".tmp_"):
                        self.execute_statement(f"drop table {source_grain}")
                else:
                    self.log.exception("Upsert Mode must have join condition column list.")
                    raise ValueError("Upsert Mode must have join condition column list.")
            elif mode in ['overwrite', 'append']:
                if mode in SCHEMA_VALIDATION_MODES:
                    destination_cols = self.get_table_columns(object)
                    if destination_cols is not None:
                        self.log.info(f"Validating schema compatibility between source and destination {object}")
                        self.validate_schema_compatibility(df.columns, destination_cols)
                        df = df.select_case_insensitive(*destination_cols)
                self.write_df_to_snowflake(df, object_type, object, mode, False)
                insert_type = 'OVERWRITE' if mode == 'overwrite' else ''
                try:
                    df_sql = df.sql_query or '«UNKNOWN»'
                except AttributeError as e:
                    df_sql = 'UNKNOWN'
                    print(e)
                if object != transformation_schema + ".QDC":
                    self.log_count(object)
                self.log.info(f"{BQL_LOG} [inside Snowflake.load]: INSERT {insert_type} INTO {str(object)} {df_sql}")
            elif mode == 'truncate_and_load':
                df.registerTempTable("df")
                df.persist()

                # generate a source dataset to be loaded into target table
                source_db_object = stage_glue_schema + ".tmp_" + self.glue.jobname.replace(" ", "_")
                sql_stmt = "select * from df "
                df_source_db_object = self.glue.execute_query(sql_stmt)
                self.write_df_to_snowflake(df_source_db_object, object_type, source_db_object,
                                           "overwrite", False)
                self.log.info(f"{BQL_LOG} [inside Snowflake.load]: INSERT OVERWRITE INTO {source_db_object} {sql_stmt}")

                truncate_statement = """ truncate table """ + object

                self.dml_operation(mode, truncate_statement, source_db_object, object)
                self.log_count(source_db_object)
                if source_db_object.startswith(stage_glue_schema + ".tmp_"):
                    self.execute_statement(f"drop table {source_db_object} ")
            self.glue.job.commit()

        #self.log.info("No. of records loaded in data frame is " + str(df.count()))
        self.log.info("Dataframe is loaded as " + mode + " in Snowflake " + object + ".")
        self.log.info(f"{BQL_LOG} {BQL_END}")
        self.glue.job_status = 1

    def validate_schema_compatibility(self, source_cols, destination_cols):
        """
        This function is used to validate the schema compatibility between source and destination.
        :param source_cols:
        :param destination_cols:
        :return: None
        :raises: SchemaMismatchException
        """
        # Convert to lowercase for case-insensitive comparison
        normalized_source_cols = {col.lower() for col in source_cols}
        normalized_destination_cols = {col.lower() for col in destination_cols}

        extra_source_cols = normalized_source_cols - normalized_destination_cols
        extra_destination_cols = normalized_destination_cols - normalized_source_cols
        if len(extra_source_cols) > 0:
            raise SchemaValidationException(
                f"Source and destination schemas do not match. Extra source columns: {extra_source_cols}"
            )
        if len(extra_destination_cols) > 0:
            self.log.warn(
                f"Source and destination schemas do not match, "
                f"Loading with null in following columns: {extra_destination_cols}"
            )

    def is_table_exists(self, table_name: str) -> bool:
        """
        This function is used to check if the table exists in Snowflake or not.

        Parameters:
        table - Snowflake table name.

        Returns:
        [bool] True if table exists, False otherwise.
        """
        try:
            self.execute_query(f"select 1 from {table_name} limit 1")
            return True
        except Exception as e:
            if "does not exist or not authorized" in str(e).lower():
                return False
            self.log.error(f"Unexpected error checking table {table_name}: {str(e)}")
            raise e

    def get_table_columns(self, table_name: str) -> Optional[List[str]]:
        """
        This function is used to get the destination columns from the Snowflake table.

        Parameters:
        table_name - Snowflake table name.

        Returns:
        [List[str]] Destination columns if table exists, None otherwise.
        """
        if not self.is_table_exists(table_name):
            self.log.info(f"Destination table {table_name} does not exist")
            return None
        table_cols = self.execute_query(f"SELECT * FROM {table_name} WHERE 1=0").columns
        return table_cols

    def execute_dml_with_retry(self, query, operation, max_retries=5):
        """
        This function uses to execute DML/DDL directly in Snowflake through glue.
        If you would like to delete the delta data from Snowflake, we can use this function.
        It offers all the DDL/DML functionality like alter, drop, insert, update, merge, create etc.
        :param query: Execute DML/DDL directly in Snowflake through glue
        :param operation:  operation name like DQC, PRELOAD, POSTLOAD, etc.
        :param max_retries: Number of retries to execute the query in case of failure
        :return: None
        """
        attempt = 0
        while attempt < max_retries:
            try:
                attempt += 1
                self.log.info(f"Execute {operation} Attempt {attempt}/{max_retries}")
                # Sleep if attempt until reached max_retries
                if attempt > 1:
                    time.sleep(random.randint(20, 30))
                self.execute_statement(query)
                break
            except Exception as e:
                self.log.error(f"Exception occurred while {operation} load, error: {e}")
                if max_retries == attempt:
                    raise e

    def dml_operation(self,
                      mode,
                      delete_statement,
                      source_object,
                      target_object):

        if delete_statement is None:
            self.log.exception("Delete statement is missing.")
            raise ValueError("Delete statement is missing.")

        self.execute_statement(f"create table {target_object} if not exists "
                                             f"as select * from {source_object} where 1=2")

        if mode in ['upsert', 'delete', 'snapshot', 'truncate_and_load']:
            self.execute_statement(delete_statement)

        if mode in ['upsert', 'snapshot', 'truncate_and_load']:
            target_dict_meta = self.get_columns(target_object)
            source_dict_meta = self.get_columns(source_object)
            col_list = " "
            for col in target_dict_meta:
                if col in source_dict_meta:
                    col_name = json.loads(col)
                    col_list += col_name["COLUMN_NAME"].lower() + " ,"
            col_list = col_list.strip(",")
            self.execute_statement(f"insert into {target_object}({col_list})  select {col_list} from {source_object}")

    def get_dataframe(self, df_name, df_config=None):
        """
        Function to load dataframe

        Parameters: df_name name to be loaded.  It is part of the pipeline yaml configuration.
        if a dataframe needs to load incremental values only, then watermark details need to be
        added to yaml file.

        Returns: dataframe
        """

        if self.glue.OPS == "test" or not self.is_watermark_enabled:
            watermark = None
            self.glue.parameters.update({"watermark": None})
        else:
            watermark = self.get_watermark(df_name)
        if watermark:
            previous_max = self.get_previous_max(watermark)
            if previous_max is not None:
                self.glue.parameters.update({'watermark': str(previous_max)})

        if type(df_config) != dict:
            df_config = self.config.get_configuration(self.glue.folder, self.glue.config_file, df_name)

        # Number of partitions are total workers-1 (1 worker for driver)
        num_partitions = 0
        if "repartition" in df_config:
            try:
                num_partitions = int(df_config["repartition"])
            except ValueError as ve:
                self.log.error(f"repartition value {df_config['repartition']} is not a number: {ve}")
                pass
        df = self.execute_query(**df_config)
        if num_partitions > 0:
            return df.repartition(num_partitions)
        else:
            return df


    def load_dataframe(self,
                       df_config,
                       df):
        """
        Loads dataframe to snowflake.
        If the pipeline has incremental loading enabled, then the max processed values are persisted.
        """
        if type(df_config) != dict:
            df_config = self.config.get_configuration(self.glue.folder, self.glue.config_file, df_config, "", df)
            if "confluence_url" in df_config:
                del df_config["confluence_url"]

        df_config["df"] = df
        self.load(**df_config)

        # We will call the persist_run_details() function in job.start() function too, so that watermarks can be stored
        # for all connectors.
        # We will keep below block since there are some snowflake_etl jobs that use watermarks and directly call
        # the `load_dataframe` method. But, we will take care of that in DTS-3812
        if len(self.watermarks) > 0:
            self.persist_run_details()

    def get_all_watermarks(self):
        """
        Function to get all watermarks from operation_db.transformation.dim_etl_process
        Returns dictionary.
        """

        if self.glue.OPS != "test":
            c = config.Config(use_job_arg_override=False)
            watermark_filename = config.get_watermark_filename(self.glue.ENV)
            data_preparation_parameters = c.get_configuration(config.watermark_folder,  watermark_filename,  "job_details")
            data_preparation_parameters['query'] = data_preparation_parameters['query'].format(self.glue.folder, self.glue.config_file)
            df_watermarks = self.execute_query(**data_preparation_parameters)
            watermarks = {}
            for row in df_watermarks.collect():
                watermarks[row["INPUT_DATAFRAME"]] = {}
                watermarks[row["INPUT_DATAFRAME"]]['watermark'] = row
            return watermarks
        else:
            return {}

    def get_watermark(self, df_name):
        """
        Function to get data from self.watermarks OR get from pipelines yaml file.
        """
        df_watermark = {}
        if self.glue.OPS== "test":
            return None

        elif len(self.watermarks) > 0 and self.watermarks.get(df_name) is not None:
            return self.watermarks[df_name]['watermark']
        else:

            df_watermark = self.set_watermark(self.glue.folder, self.glue.config_file, df_name)
            if df_watermark is not None:
                self.watermarks[df_name] = {}
                self.watermarks[df_name]['watermark'] =df_watermark[0]
                return df_watermark[0]
            else:
                # self.glue.log.error("watermark not defined in config file {}".format(self.glue.config_file))
                return None

    def get_previous_max(self, watermark):
        """
            Fetch the max proessed value
        """
        return watermark["LAST_CDC_ID_PROCESSED"] if watermark["COLUMN_TYPE"] == 'number' else watermark["LATEST_CDC_TIMESTAMP_PROCESSED"]

    def get_next_run_id(self):
        """
        Get next sequence value for OPERATION_DB.TRANSFORMATION.FCT_ETL_PROCESS_RUN
        """
        if self.glue.OPS != "test":

            c = config.Config(use_job_arg_override=False)
            watermark_filename = config.get_watermark_filename(self.glue.ENV)
            data_preparation_parameters = c.get_configuration(config.watermark_folder, watermark_filename, "get_run_id_input")
            df_next_run_id = self.execute_query(**data_preparation_parameters)

            return  df_next_run_id.collect()[0]["RUN_ID"]
        else:
            return None

    def persist_run_details(self):
        """
        Assumption is one table per glue pipeline.  So any incremental dataframes runs are all persisted
        upon calling load_dataframe.
        """
        transformation_schema = config.get_operation_db_and_transformation_schema(self.glue.ENV)

        if self.start_time is not None:
            for df_name, value in self.watermarks.items():
                id = self.get_next_run_id()
                watermark = value['watermark']
                previous_max = str(self.get_previous_max(watermark))
                current_max = str(self.get_snowflake_max(watermark, df_name))
                start_id = previous_max if watermark["COLUMN_TYPE"] == 'number' else 'null'
                start_cdc_timestamp = "'" +  previous_max + "'"  if watermark["COLUMN_TYPE"] != 'number' else 'null'
                end_id = current_max if watermark["COLUMN_TYPE"] == 'number' else 'null'
                end_cdc_timestamp = "'" + current_max + "'"  if watermark["COLUMN_TYPE"] != 'number' else 'null'
                end_time = str(datetime.now())
                run_log_query = """insert into """ + transformation_schema + """.fct_etl_process_run(id
                                                                                , process_id
                                                                                , status
                                                                                , process_start_time
                                                                                , process_end_time
                                                                                , start_id
                                                                                , end_id
                                                                                , start_cdc_timestamp
                                                                                , end_cdc_timestamp)
                        values(""" + str(id)  \
                        + """,""" + str(watermark["ID"]) \
                        + """,'""" + 'Success' + """'""" \
                        + """,convert_timezone( 'UTC', 'America/Los_Angeles','""" + str(self.start_time) +  """'::timestamp_ntz)""" \
                        + """,convert_timezone( 'UTC', 'America/Los_Angeles','""" + end_time +  """'::timestamp_ntz)""" \
                        + """,""" + start_id \
                        + """,""" + end_id \
                        + """,convert_timezone( 'UTC', 'America/Los_Angeles',""" + start_cdc_timestamp + """::timestamp_ntz)""" \
                        + """,convert_timezone( 'UTC', 'America/Los_Angeles',""" + end_cdc_timestamp + """::timestamp_ntz));"""

                process_log_query = """ update """ + transformation_schema + """.dim_etl_process d
                                set last_cdc_id_processed = f.end_id
                                    ,latest_cdc_timestamp_processed = f.end_cdc_timestamp """ + \
                                """, last_successful_run = '""" + end_time + """'""" + \
                                """ from """ + transformation_schema + """.fct_etl_process_run f """ + \
                                """ where f.id = """ +  str(id) + \
                                """ and f.process_id = d.id """

                self.execute_statement(run_log_query)
                self.execute_statement(process_log_query)
            self.start_time = None

    def set_watermark(self,
                      folder,
                      filename,
                      df_name):
        """
        It will set watermark details, if specified in pipeline's yaml and are not in operation_db.transformation.dim_etl_process
        """
        transformation_schema = config.get_operation_db_and_transformation_schema(self.glue.ENV)

        c = config.Config(use_job_arg_override=False)
        watermark = c.get_watermark_configuration(self.glue.folder, self.glue.config_file, df_name)

        if watermark is not None:
            last_cdc_id_processed = watermark['initial_processed_value'] if watermark['column_type'] == 'number' else '0'
            latest_cdc_timestamp_processed = str(watermark['initial_processed_value']) if watermark['column_type'] != 'number' else '1970-01-01'
            watermark_filename = config.get_watermark_filename(self.glue.ENV)

            query = """ insert into """ + transformation_schema + """.dim_etl_process(id,
                                                                                        config_file,
                                                                                        folder,
                                                                                        type,
                                                                                        input_dataframe,
                                                                                        watermark_table,
                                                                                        column_name,
                                                                                        column_type,
                                                                                        last_successful_run,
                                                                                        last_cdc_id_processed,
                                                                                        latest_cdc_timestamp_processed)
                            select """ + transformation_schema + """.seq_dim_etl_process_run__id.nextval,""" + \
                    """'""" + filename + """',""" + \
                    """'""" + folder + """',""" + \
                    """'incremental',""" + \
                    """'""" + df_name + """',""" + \
                    """'""" + watermark['watermark_table'] + """',""" + \
                    """'""" + watermark['column_name'] + """',""" + \
                    """'""" + watermark['column_type'] + """',""" + \
                    """convert_timezone( 'UTC', 'America/Los_Angeles','""" + '1979-01-01' + """'::timestamp_ntz),""" + \
                    str(last_cdc_id_processed) + """,""" + \
                    """convert_timezone( 'UTC', 'America/Los_Angeles','""" + latest_cdc_timestamp_processed + """'::timestamp_ntz)"""
            self.execute_statement(query)
            data_preparation_parameters = c.get_configuration(config.watermark_folder,  watermark_filename,  "df_details")
            data_preparation_parameters['query'] = data_preparation_parameters['query'].format(self.glue.folder, self.glue.config_file, df_name)
            df_watermark = self.execute_query(**data_preparation_parameters)
            return df_watermark.collect()

        else:
            return None

    def get_snowflake_max(self, watermark, df_name):
        query = """select max( """ + watermark['COLUMN_NAME'] + """) as max from """ + watermark['WATERMARK_TABLE']
        df = self.execute_query(query)
        max = df.collect()[0]["MAX"]
        if max is not None:
            return max
        elif watermark['COLUMN_TYPE'] == 'number':
            return 0
        else:
            return '1970-01-01'

    def _set_snowflake_query_pushdown(self) -> None:
        """
        this function is to enable or disable Snowflake query autopushdown option.
        """
        if self.glue.ENV == "CI":
            return
        if "job" in self.job_config and "snowflake" in self.job_config["job"] \
                and self.job_config["job"]["snowflake"].get("disable_query_autopushdown", False) is True:
            self.log.info("disabling the query autopushdown option.")
            self.glue.spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(
                self.glue.spark._jsparkSession)
            self.glue.sfOptions["autopushdown"] = "off"

============

tr_base.py

from transformation.sdk.batch.glue import Glue
from transformation.sdk.batch.snowflake import Snowflake
import json
from transformation.sdk.common import config as config2
from transformation.sdk.common.dqc import DQC

class Base:

    def __init__(self, folder, file, operation_type):
        self.step_aggregate = ""
        self.attribution_stage_df = ""
        self.attribution_stage_rank = ""
        self.fact_funnel_no_gap_unpivot_table = ""
        self.session_table = ""
        self.stage_ranking_table = ""
        self.table = ""
        self.fact_funnel_aggregate_table = ""
        self.fact_funnel_aggregate_strict_table = ""
        self.tmp_define_session_table = "define_session"
        self.fact_funnel_aggregate_no_gap_table = ""
        self.fact_funnel_aggregate_unpivot_table = ""
        self.step_status_str = None
        self.steps_str = None
        self.funnel_steps = None
        self.incremental_query = ""
        self.cdc_table = ""
        self.source_df_name = ""
        self.dqc_query = ""
        self.dqc_message = ""
        self.cdc_df = None
        self.funnel_name = "funnel"
        self.job_name = file.replace(".yaml", "")
        self.glue = Glue(self.job_name, folder, file)
        self.glue.spark.conf.set("spark.driver.maxResultSize", "4g")
        self.glue.spark.conf.set("spark.driver.memory", "16g")
        self.folder = folder
        self.type = operation_type
        self.snowflake = Snowflake(self.glue)
        self.dqc_obj = DQC(self.glue, self.snowflake)
        self.dts_config = self.snowflake.config
        self.job_config = self.dts_config.parse_configuration(folder, file)
        self.config = {}
        self.setup_config()
        self.funnel_schema = "edw_db.funnel" if "funnel_schema" not in self.config else self.config["funnel_schema"]
        self.attribution_schema = "edw_db.funnel" if "attribution_schema" not in self.config else self.config["attribution_schema"]
        self.bio_schema = "edw_db.bio" if "bio_schema" not in self.config else self.config["bio_schema"]
        self.funnel_setup_table = f"""{self.funnel_schema}.funnel_config"""
        self.config["source_grain"] = self.config["source_grain"] if "source_grain" in self.config else ["id"]
        self.look_back_window = 2 if "look_back_window" not in self.glue.parameters else self.glue.parameters[
            "look_back_window"]
        self.load_type = "incremental" if "load_type" not in self.glue.parameters else self.glue.parameters[
            "load_type"]
        self.start_date = "" if "start_date" not in self.glue.parameters else self.glue.parameters[
            "start_date"]
        self.end_date = "" if "end_date" not in self.glue.parameters else self.glue.parameters[
            "end_date"]
        self.set_target_table()
        self.grain = Base.get_str_from_list(self.config["grain"])
        self.fact_grain = Base.get_str_from_list(self.config["grain"],",","f")
        self.grain_is_not_null = Base.get_grain_is_not_null(self.config["grain"])
        self.ranking = Base.get_str_from_list(self.config["ranking_columns"])
        self.grain_join = Base.get_grain_join(self.config["grain"])
        self.debug_flag = self.config["debug"] if "debug" in self.config else False
        self.cdc_column = self.config["cdc_column"] if "cdc_column" in self.config else ""
        self.set_dqc()
        self.dqc_variance = self.config["dqc_variance"] if "dqc_variance" in self.config else 25
        self.glue.spark.conf.set("spark.sql.shuffle.partitions", 25000)
        self.glue.spark.conf.set("spark.driver.maxResultSize", "4g")
        self.EXCLUDE_COLUMNS = [
                   "tmp_ranking",
                   "ranking"]

        self.EXCLUDE_GRAIN_COLUMNS = ["interaction_id",
                                "event",
                                "tmp_ranking",
                                "ranking",
                                "event",
                                "timestamp",
                                "step_rank"]

    def debug_output(self, df, name=None):
        if self.debug_flag:
            print(f"\nDebug output for: {name if name else 'DataFrame'}")
            if self.config.get("truncate_results", True):
                df.show(truncate=True)
            else:
                df.show(truncate=False)

    def set_target_table(self):
        if self.type == "bio":
            self.table = f"""{self.bio_schema}.fact_{self.config["object"]}"""
            self.job_config[self.table] = self.get_snowflake_dataframe_config(self.table, "delete")

        elif self.type == "funnel":
            self.session_table = f"""{self.funnel_schema}.dim_{self.config["object"]}_interaction"""
            self.stage_ranking_table = f"""{self.funnel_schema}.stage_{self.config["object"]}_ranking"""
            self.table = f"""{self.funnel_schema}.fact_{self.config["object"]}_raw"""
            self.cdc_table = f"""{self.funnel_schema}.cdc_{self.config["object"]}"""
            self.fact_funnel_aggregate_table = f"""{self.funnel_schema}.fact_{self.config["object"]}_aggregate"""
            self.fact_funnel_aggregate_unpivot_table = f"""{self.funnel_schema}.fact_{self.config["object"]}_curated"""
            self.fact_funnel_aggregate_strict_table = f"""{self.funnel_schema}.fact_{self.config["object"]}_aggregate_strict"""
            self.job_config[self.session_table] = self.get_snowflake_dataframe_config(self.session_table, "delete")
            self.fact_funnel_aggregate_no_gap_table = f"""{self.funnel_schema}.fact_{self.config["object"]}_aggregate_no_gap"""
            self.fact_funnel_no_gap_unpivot_table = f"""{self.funnel_schema}.fact_{self.config["object"]}_no_gap"""
            self.job_config[self.stage_ranking_table] = self.get_snowflake_dataframe_config(self.stage_ranking_table,
                                                                                            "delete")
            self.job_config[self.fact_funnel_aggregate_unpivot_table] = self.get_snowflake_dataframe_config(self.fact_funnel_aggregate_unpivot_table,
                                                                                            "delete")
            self.job_config[self.cdc_table] = self.get_snowflake_dataframe_config(self.cdc_table,
                                                                                            "overwrite")
            self.job_config[self.table] = self.get_snowflake_dataframe_config(self.table,
                                                                              "delete")
            self.job_config[self.fact_funnel_aggregate_table] = self.get_snowflake_dataframe_config(
                self.fact_funnel_aggregate_table, "delete")
            self.job_config[self.fact_funnel_no_gap_unpivot_table] = self.get_snowflake_dataframe_config(
                self.fact_funnel_no_gap_unpivot_table, "delete")
            self.job_config[self.fact_funnel_aggregate_strict_table ] = self.get_snowflake_dataframe_config(
                self.fact_funnel_aggregate_strict_table , "delete")

            self.job_config[self.fact_funnel_aggregate_no_gap_table] = self.get_snowflake_dataframe_config(
                self.fact_funnel_aggregate_no_gap_table, "delete")

            if self.load_type == "full_load":
                self.tmp_define_session_table = f"""{self.funnel_schema}.tmp_define_{self.config["object"]}_session"""
                self.job_config[self.tmp_define_session_table] = self.get_snowflake_dataframe_config(
                    self.tmp_define_session_table, "overwrite")
        elif self.type == "attribution":
            self.attribution_stage_rank = f"""{self.attribution_schema}.stage_{self.config["object"]}_rank"""
            self.attribution_stage_df = f"""stage_{self.config["object"]}_rank"""
            self.cdc_table = f"""{self.attribution_schema}.cdc_{self.config["object"]}_attribution"""
            self.job_config[self.cdc_table] = self.get_snowflake_dataframe_config(self.cdc_table,
                                                                                  "overwrite")
            self.job_config[self.attribution_stage_rank] = self.get_snowflake_dataframe_config(self.attribution_stage_rank,
                                                                                  "upsert")
            self.session_rank_table = f"""{self.attribution_schema}.stage_{self.config["object"]}_session_rank"""
            self.job_config[self.session_rank_table] = self.get_snowflake_dataframe_config(self.session_rank_table, "upsert")
        elif self.type == "step_aggregate":
            self.cdc_table = f"""{self.attribution_schema}.cdc_{self.config["object"]}_step_aggregate"""
            self.job_config[self.cdc_table] = self.get_snowflake_dataframe_config(self.cdc_table,
                                                                                  "overwrite")
            self.step_aggregate = f"""{self.attribution_schema}.fact_{self.config["object"]}_step_aggregate"""
            self.job_config[self.step_aggregate] = self.get_snowflake_dataframe_config(self.step_aggregate,
                                                                                  "upsert")

    def get_snowflake_dataframe_config(self, table, mode="upsert", grain=None):

        return {
            "type": "output",
            "connector": "snowflake",
            "object_type": "dbtable",
            "mode": mode,
            "grain_cols": self.config["grain"] if not grain else grain,
            "object": table
        }

    @staticmethod
    def get_str_from_list(ls, separate=",", alais=""):
        if alais != "":
            s = f"{separate} {alais}.".join(ls)
            return f"{alais}.{s}"
        else:
            return f"{separate}".join(ls)

    @staticmethod
    def get_grain_join(ls):
        grain_str = ""
        for col in ls:
            grain_str += f""" f.{col} = s.{col}  and"""
        grain_str = grain_str.rstrip("and")
        return grain_str

    @staticmethod
    def get_grain_is_not_null(ls):
        grain_null_str = ""
        for col in ls:
            grain_null_str += f""" {col} is not null and"""
        grain_null_str = grain_null_str.rstrip("and")
        return grain_null_str

    def getTargetTableColumns(self, table):
        dict_meta = self.snowflake.get_columns(table)
        snowflake_columns = []
        for column in dict_meta:
            col_name = json.loads(column)
            snowflake_columns.append(col_name["COLUMN_NAME"].lower())
        return snowflake_columns

    def addMissingColumns(self, source, target, table):
        for col in source:
            if col not in target:
                datatype = "VARCHAR(16777216)"
                if col.split("_")[-1] in ["timestamp", "date"]:
                    datatype = "TIMESTAMP_LTZ(9)"
                elif col.split("_")[-1] in ["decimaltype(38, 0)"]:
                    datatype = "INTEGER"
                elif col.split("_")[-1] in ["doubletype", "decimaltype"]:
                    datatype = "DOUBLE"
                elif col.split("_")[-1] in ["booleantype"]:
                    datatype = "BOOLEAN"

                statement = f"""ALTER TABLE {table}
                ADD COLUMN IF NOT EXISTS {col} {datatype};"""
                self.snowflake.execute_statement(statement)

    def performDDL(self, source, table):
        target_columns = self.getTargetTableColumns(table)
        source_columns = config2.get_dataframe_schema(source)

        if len(target_columns) != 0 and len(source_columns) != len(target_columns):
            self.addMissingColumns(source_columns, target_columns, table)

    def setup_config(self):
        for df in self.job_config:
            if self.job_config[df]["type"] == self.type and self.type in ["bio", "funnel", "attribution", "step_aggregate"]:
                self.config = self.job_config[df]
                if self.type in ["funnel"]:
                    self.source_df_name = f'{self.job_config[df]["source"]}_incremental'
                break

    def get_data(self):
        for df in self.job_config:
            if self.job_config[df]["type"] == "input":
                if "connector" in self.job_config[df] and self.job_config[df]["connector"] == "snowflake":
                    print(self.job_config[df]["query"])
                    query = self.job_config[df]["query"]
                    if self.cdc_column != "" and df != self.source_df_name:
                        query = f""" SELECT * FROM (
                                    {query}
                                    )
                                    WHERE  {self.cdc_column}::date {self.get_data_selection()}
                                """
                        self.job_config[df]["query"] = query
                    print(self.job_config[df]["query"])
                    dataframe = self.snowflake.execute_query(**self.job_config[df])
                    self.debug_output(dataframe, name=df)
                    dataframe.createOrReplaceTempView(df)

        for df in self.job_config:
            if self.job_config[df]["type"] == "transformation":
                self.glue.execute_query(self.job_config[df]["query"], df)
            elif self.job_config[df]["type"] == "dqc":
                self.config["dqc"][df] = self.job_config[df]

    def cleanup(self, table):
        if len(self.snowflake.get_columns(self.job_config[table]["object"])) > 0:
            self.snowflake.load_dataframe(self.job_config[table], self.cdc_df)

    def set_dqc_property(self, dqc, table):
        if self.config["dqc"][dqc]["type"] == "check_null":
            where_condition = Base.get_str_from_list(self.config["dqc"][dqc]["columns"],
                                                     " is null or \n") + " is null \n"
            self.dqc_query = f""" select
                count(*)
                from {table}
                where {where_condition}
            """
            self.dqc_message = f"""There should not be NULL values in
                                {Base.get_str_from_list(self.config["dqc"][dqc]["columns"], " or ")} in {table} table"""
        elif self.config["dqc"][dqc]["type"] == "check_duplicate":
            group_by_condition = Base.get_str_from_list(self.config["dqc"][dqc]["columns"], ",")
            self.dqc_query = f""" select count(*) cnt
                from (select {group_by_condition},
                count(*)
                from {table}
                group by {group_by_condition}
                having count(*) > 1
                )
            """
            self.dqc_message = f"""There should not be DUPLICATE values across
                                {Base.get_str_from_list(self.config["dqc"][dqc]["columns"], " and ")} columns in {table} table"""
        elif self.config["dqc"][dqc]["type"] == "check_variance":
            timestamp = self.config["dqc"][dqc]["timestamp"] if "timestamp" in self.config["dqc"][dqc] else "timestamp"

            self.dqc_query = f""" select
                coalesce((abs(yesterday_cnt - avg_cnt)/avg_cnt)*100,0) variance
                 from (
                 select avg(enrollment_cnt)::int avg_cnt,
                     min(case when rnk=1 then enrollment_cnt end) yesterday_cnt
                     from (
                             select {timestamp}::date,
                                     count(*) enrollment_cnt ,
                                     row_number() over(order by {timestamp}::date desc) rnk
                             from {table}
                             {self.get_look_days(dqc)} - 1
                             group by {timestamp}::date
                         )
                 );"""
            self.dqc_message = f"""There is variance for today data load in {table} table."""
        elif self.config["dqc"][dqc]["type"] == "check_drop_rate":
            self.dqc_query = f""" select count(*) cnt
                                    from
                                        (
                                        select  step_rank,
                                        cnt current_cnt,
                                        lag(cnt) over(order by step_rank) previous_cnt
                                        from (
                                            select step_rank,
                                                count(*) cnt
                                            from {table}
                                            {self.get_look_days(dqc)}
                                            group by step_rank )
                                        )
                                    where previous_cnt is not null
                                    and current_cnt > previous_cnt
                    """
            self.dqc_message = f"""There is an issues in drop rate in {table} table."""

    def get_look_days(self, dqc, timestamp="timestamp"):

        look_back_days = self.config["dqc"][dqc]["look_back_days"] \
            if "look_back_days" in self.config["dqc"][dqc] \
            else 0

        where_condition = f" where {timestamp}::date between current_date - {look_back_days}  and current_date" \
            if look_back_days > 0 \
            else ""

        return where_condition

    def get_data_selection(self):

        if self.start_date != "" and self.end_date != "":
            return f"""between '{self.start_date}'  and '{self.end_date}' """
        elif self.start_date != "" and self.end_date == "":
            return f""">= '{self.start_date}'  """
        elif self.start_date == "" and self.end_date != "":
            return f"""<= '{self.end_date}'  """
        else:
            return f""">= dateadd(day,-{self.look_back_window},current_timestamp())"""


    def get_dqc_config(self, dqc):
        table = self.config["dqc"][dqc]["table"] if "table" in self.config["dqc"][dqc] else self.table
        self.set_dqc_property(dqc, table)
        expected = {"value": self.config["dqc"][dqc]["expected"] if "expected" in self.config["dqc"][dqc] else 0}
        if self.config["dqc"][dqc]["type"] == "check_variance":
            expected = {"value": self.dqc_variance}

        df = {
            "type": "dqc",
            "connector": "snowflake",
            "query": self.dqc_query,
            "expected": expected,
            "object": table
        }

        df["operator"] = self.config["dqc"][dqc]["operator"] if "operator" in self.config["dqc"][dqc] else "="
        df["is_slack"] = self.config["dqc"][dqc]["is_slack"] if "is_slack" in self.config["dqc"][dqc] else False
        df["is_pagerduty"] = self.config["dqc"][dqc]["is_pagerduty"] if "is_pagerduty" in self.config["dqc"][
            dqc] else False
        df["action"] = self.config["dqc"][dqc]["action"] if "action" in self.config["dqc"][dqc] else "info"
        df["message"] = self.dqc_message

        return df

    def post_dqc(self):
        """
        Helper function to run DQCs
        """
        if "dqc" in self.config:
            for dqc in self.config["dqc"]:
                dqc_dict = self.get_dqc_config(dqc)
                print(dqc_dict)
                self.dqc_obj.execute(self.job_name, dqc, dqc_dict)

    def set_dqc(self):
        self.config["dqc"] = {}
        self.add_common_qdc('table', self.table)

        if self.type == "funnel":
            if self.debug_flag:
                self.add_common_qdc('session', self.session_table)
                self.add_common_qdc('stage', self.stage_ranking_table)
            self.add_common_qdc('aggregate', self.fact_funnel_aggregate_table)


            # self.config["dqc"]["check_overall_drop_rate"] = {'type': 'check_drop_rate', 'table': self.table}
            # self.config["dqc"]["check_last_365_days_drop_rate"] = {'type': 'check_drop_rate', 'look_back_days': 365,
            #                                                        'table': self.table}

    def add_common_qdc(self, table_type, table):
        self.config["dqc"][f'check_null_columns_{table_type}'] = {'type': 'check_null',
                                                                  'columns': self.get_dqc_grain('null', table),
                                                                  'table': table}
        self.config["dqc"][f'check_grain_duplicate_{table_type}'] = {'type': 'check_duplicate',
                                                                     'columns': self.get_dqc_grain('grain', table),
                                                                     'table': table}
        self.config["dqc"][f'check_variance_event_{table_type}'] = {'type': 'check_variance',
                                                                    'look_back_days': 8,
                                                                    'table': table,
                                                                    'operator': '<'}

    def get_dqc_grain(self, dqc_type='grain', table=None):

        dqc_grain = []
        if table != self.stage_ranking_table:
            for col in self.config["grain"]:
                dqc_grain.append(col)
        else:
            for col in self.config["source_grain"]:
                dqc_grain.append(col)

        if dqc_type == 'null':
            for col in self.config["ranking_columns"]:
                dqc_grain.append(col)
        if self.type == "funnel":
            dqc_grain.append("interaction_id")

        if self.type == "funnel" and table != self.fact_funnel_aggregate_table and dqc_type == 'grain':
            dqc_grain.append("event")

        return dqc_grain

    def get_columns_excluding_grain(self, df_name):
        query = f"""
                select
                    *
                from  {df_name}
            """
        df = self.glue.execute_query(query)

        s = "\t\t\n"
        for col in config2.get_dataframe_schema(df):
            if col not in self.config["grain"] and col not in self.EXCLUDE_GRAIN_COLUMNS:

                s += f"\t\t\t{col},\n"
        return s.rstrip(",\n")

    def get_grain_coalesce(self):
        s = ""
        for col in self.config["grain"]:
            s += f"coalesce(f.{col},s.{col}) {col},"
        return s.rstrip(",")

    def get_transformation(self, type="transformation"):
        transformations = {}
        for dataframe in self.job_config:
            if not dataframe.startswith("test_") and dataframe != 'runtime_parameters' and dataframe != "include" and dataframe != "parameters" and dataframe != "udf" and \
                    self.job_config[dataframe]["type"] == type:
                transformations[self.job_config[dataframe]["execution_order"]] = [dataframe, self.job_config[dataframe]]
        return transformations

    def apply_transformation(self, type="transformation"):
        transformations = self.get_transformation(type)
        for key in sorted(transformations):
            query = transformations[key][1]["query"]

            df = self.glue.execute_query(query)
            self.debug_output(df, name=transformations[key][0])
            if "is_persist" in transformations[key][1] and transformations[key][1]["is_persist"]:
                df.persist()

            df.createOrReplaceTempView(transformations[key][0])

    def set_cdc_table(self):
        print("CDC Table Started")
        query = f"""select distinct {self.grain}
            from ({self.job_config[self.config["source"]]["query"]}) s
            where {self.config["cdc_column"]} {self.get_data_selection()}"""
        self.cdc_df = self.snowflake.execute_query(query, "cdc_table")
        self.debug_output(self.cdc_df, "cdc_table")
        self.cdc_df.persist()
        self.load(self.cdc_table, self.cdc_df, "overwrite" )
        print("CDC Table Ended")

    def load(self, table, df, mode="upsert"):
        print("Load Started")
        is_cdc_table = table == self.cdc_table
        is_tmp_table = table == self.tmp_define_session_table

        if self.type == "funnel" and self.cdc_column and not is_cdc_table and not is_tmp_table:
            quoted_cdc_column = f'"{self.cdc_column.upper()}"'
            cdc_filter = f"{quoted_cdc_column} {self.get_data_selection()}"
            fully_qualified_table = self.job_config[table]["object"]
            delete_stmt = f"DELETE FROM {fully_qualified_table} WHERE {cdc_filter}"
            print(f"[CDC DELETE] {delete_stmt}")
            self.snowflake.execute_statement(delete_stmt)
            self.performDDL(df, table)
            df_config = self.get_snowflake_dataframe_config(table, "append")
        else:
            self.cleanup(table)
            df_config = self.get_snowflake_dataframe_config(table, mode)
            self.performDDL(df, table)
        self.snowflake.load_dataframe(df_config, df)
        print("Load Ended")

    def get_funnel_event_list_condition(self):
        events = [event.lower() for event in list(self.config["entry"].keys()) + list(self.config["steps"].keys())]
        events_str = ", ".join([f"'{event}'" for event in events])
        condition = f""" and lower(event) in ({events_str})"""
        return condition

    def read_incremental_table(self):
        print("Read Incremental Table Started")
        condition = self.get_funnel_event_list_condition() if self.type == "funnel" else ""
        query = f"""select f.*
                from (
                select * from
                ({self.job_config[self.config["source"]]["query"]})
                where 1=1 {condition}
                and {self.grain_is_not_null}
                ) f
                inner join {self.cdc_table} s on {self.grain_join}"""

        self.incremental_query = query

        if self.load_type == "incremental":
            self.snowflake.execute_query(query, register_table=self.source_df_name)
        print("Read Incremental Table Ended")


-------------

tr_bio.py

from transformation_ext.snowflake_etl.funnel.tr_base import Base
from transformation.sdk.common import config as config2

class BIO(Base):

    def __init__(self, folder, file):
        Base.__init__(self, folder, file, "bio")
        self.ranking_order = self.config["ranking_order"] if "ranking_order" in self.config else "desc"
        self.schema = []
        self.event_schemas = {}
        self.get_data()
        self.apply_transformation()

    def dedup(self, df):
        query = f"""
            select *
            from
                (
                    select
                        row_number() over(partition by {self.grain} order by {self.ranking} {self.ranking_order}) tmp_ranking,
                        *
                    from {df}
                 )
            where tmp_ranking = 1
        """
        event_df = self.glue.execute_query(query)
        self.debug_output(event_df,f"{df}_events")
        event_df.createOrReplaceTempView(f"{df}_events")

        return config2.get_dataframe_schema(event_df)

    def get_consolidate_schema(self):
        schema = []
        for event in self.event_schemas:
            for column in self.event_schemas[event]:
                if column not in schema and column not in self.EXCLUDE_COLUMNS:
                    schema.append(column)
        self.schema = sorted(schema)

    def get_column_str(self, df):
        df_str = ""
        for column in self.schema:
            if column in self.event_schemas[df]:
                df_str += f"{column},\n"
            else:
                df_str += f"null {column},\n"
        return df_str.rstrip(f",\n")

    def load(self):
        combine_query = ""
        for df in self.config["combine"]:
            combine_query += f""" select  {self.get_column_str(df)},
                                    current_timestamp() dw_created_at,
                                    current_timestamp() dw_updated_at
                                from {df}_events
                                union all\n"""
        combine_query = combine_query.rstrip("union all\n")

        event_df = self.glue.execute_query(combine_query)
        df_config = self.get_snowflake_dataframe_config(self.table, "upsert")
        self.debug_output(event_df,f"{df_config}")
        self.snowflake.load_dataframe(df_config, event_df)

    def build_bio(self):
        self.event_schemas = {}
        for df in self.config["combine"]:
            self.event_schemas[df] = self.dedup(df)

        self.get_consolidate_schema()
        self.load()
        self.post_dqc()
=========


job:
  name: "Pre Enrollment"
  description: "Build bio for Pre Enrollment"
  alert: false
  type : "batch"
  env:
    - prod
  worker_counts: 200

first_web_page_viewed_input :
  type : input
  connector: snowflake
  query: >
    select *
    from
        (
          select distinct
              p.id,
              REGEXP_REPLACE(p.user_id, '^"|"$', '') as user_id, --sometimes null
              REGEXP_REPLACE(p.anonymous_id, '^"|"$', '') as anonymous_id,
              p.timestamp::date as date,
              p.timestamp,
              COALESCE(p.is_embedded,FALSE) as is_embedded,
              p.context_page_url,
              p.context_page_referrer,  -- sometimes null
              p.context_user_agent,
              p.ad_campaign_id, -- sometimes null
              p.device_id  ,-- null
              'First Page Viewed' as event,
              'PAGES' as source_table,
              CASE
                WHEN LOWER(CAST(PARSE_URL(p.context_page_url, 1):parameters:ad AS STRING)) = 'tp_' THEN 
                  'tp_' || COALESCE(
                    cast(PARSE_URL(p.context_page_referrer, 1):parameters:ref as string),
                    cast(PARSE_URL(p.context_page_url, 1):parameters:ref as string),
                    ''
                  )
                WHEN LOWER(CAST(PARSE_URL(p.context_page_url, 1):parameters:ad AS STRING)) = 'cj' THEN 
                  'cj_' || COALESCE(
                    cast(PARSE_URL(p.context_page_referrer, 1):parameters:pid as string),
                    cast(PARSE_URL(p.context_page_url, 1):parameters:pid as string),
                    ''
                  )
                WHEN LOWER(CAST(PARSE_URL(p.context_page_url, 1):parameters:ad AS STRING)) = 'impact' THEN 
                  COALESCE(
                    cast(PARSE_URL(p.context_page_referrer, 1):parameters:ad_id as string),
                    cast(PARSE_URL(p.context_page_url, 1):parameters:ad_id as string),
                    ''
                  )
                ELSE CAST(PARSE_URL(p.context_page_url, 1):parameters:ad AS STRING)
              END AS ad_id,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:utm_source AS STRING) AS utm_source,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:utm_medium AS STRING) AS utm_medium,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:utm_campaign AS STRING) AS utm_campaign,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:utm_id AS STRING) AS utm_id,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:utm_content AS STRING) AS utm_content,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:utm_term AS STRING) AS utm_term,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:gclid AS STRING) AS gclid,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:fbclid AS STRING) AS fbclid,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:gad_source AS STRING) AS gad_source,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:irgwc AS STRING) AS irgwc,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:cadid AS STRING) AS cadid,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:sub_publisher AS STRING) AS sub_publisher,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:value_prop AS STRING) AS value_prop,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:marketing_type AS STRING) AS marketing_type,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:creative AS STRING) AS creative,
              CAST(PARSE_URL(p.context_page_url, 1):parameters:traffic_source AS STRING) AS traffic_source,
              CASE
                  WHEN p.is_embedded = TRUE AND LOWER(p.context_user_agent) LIKE '%android%' THEN 'Android App'
                  WHEN p.is_embedded = TRUE AND (
                    LOWER(p.context_user_agent) LIKE '%iphone%' OR LOWER(p.context_user_agent) LIKE '%ipad%'
                  ) THEN 'iOS App'
                  WHEN LOWER(p.context_user_agent) LIKE '%android%' THEN 'Android Web'
                  WHEN LOWER(p.context_user_agent) LIKE '%iphone%' THEN 'iPhone Web'
                  WHEN LOWER(p.context_user_agent) LIKE '%ipad%' THEN 'iPad Web'
                  WHEN LOWER(p.context_user_agent) LIKE '%macintosh%' THEN 'Mac Web'
                  WHEN LOWER(p.context_user_agent) LIKE '%windows%' THEN 'Windows Web'
                  WHEN LOWER(p.context_user_agent) LIKE '%linux%' OR LOWER(p.context_user_agent) LIKE '%x11%' THEN 'Linux'
                  WHEN LOWER(p.context_user_agent) LIKE '%facebook%' THEN 'Facebook Bot'
                  WHEN LOWER(p.context_user_agent) LIKE '%google-speakr%' THEN 'Google Speaker'
                  WHEN LOWER(p.context_user_agent) LIKE '%bb10%' OR LOWER(p.context_user_agent) LIKE '%kbd%' THEN 'Blackberry'
                  WHEN LOWER(p.context_user_agent) LIKE '%google%' THEN 'Google Bot'
                  WHEN LOWER(p.context_user_agent) LIKE '%bot%' OR LOWER(p.context_user_agent) LIKE '%spider%' THEN 'Other Crawlers'
                  WHEN LOWER(p.context_user_agent) LIKE '%mobile%' THEN 'Other Mobile Web'
                  ELSE 'All Other Devices'
                END AS device_type,
              null as enrollment_device,
              CASE
                  WHEN LOWER(p.context_user_agent) LIKE '%android%' THEN 'Android'
                  WHEN LOWER(p.context_user_agent) LIKE '%iphone%' OR LOWER(p.context_user_agent) LIKE '%ipad%' OR LOWER(p.context_user_agent) LIKE '%ios%' THEN 'iOS'
                  WHEN LOWER(p.context_user_agent) LIKE '%windows%' THEN 'Windows'
                  WHEN LOWER(p.context_user_agent) LIKE '%macintosh%' THEN 'Mac'
                  WHEN LOWER(p.context_user_agent) LIKE '%linux%' OR LOWER(p.context_user_agent) LIKE '%x11%' THEN 'Linux'
                  ELSE 'Other OS'
                END AS operating_system,
              'na' as session_id
          from segment.chime_prod.pages as p
          left join SEGMENT.CHIME_PROD.ENROLLMENT_FLOW_ENTERED as e on p.anonymous_id = e.anonymous_id
                                                                      and p.timestamp::date = e.timestamp::date
                                                                      and e.is_embedded = TRUE
          where 1=1
              and p.name = 'First Page Viewed'
              and e.is_embedded is null -- did not enter enrollment on app
              and p.timestamp::date < date '2025-05-08'
          
          union all 

          select 
              _message_id AS id,
              REGEXP_REPLACE(CAST(_user_id AS STRING), '^"|"$', '') AS user_id,
              REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
              timestamp::DATE AS date,
              timestamp,
              CAST(browser:is_embedded AS BOOLEAN) AS is_embedded,
              CAST(page:url AS STRING) AS context_page_url,
              CAST(page:referrer_url AS STRING) AS context_page_referrer,
              CAST(browser:user_agent AS STRING) AS context_user_agent,
              'na' AS ad_campaign_id,
              _device_id AS device_id,
              'First Page Viewed' AS event,
              'WEB_ACTION_TRIGGERED' AS source_table,
              CASE
                WHEN SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2) = 'tp_' THEN 
                  'tp_' || COALESCE(
                    cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:ref as string),
                    cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:ref as string),
                    ''
                  )
                WHEN LOWER(SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)) = 'cj' THEN 
                  'cj_' || COALESCE(
                    cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:pid as string),
                    cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:pid as string),
                    ''
                  )
                WHEN LOWER(SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)) = 'impact' THEN 
                  COALESCE(
                    cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:ad_id as string),
                    cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:ad_id as string),
                    ''
                  )
                ELSE SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)
              END AS ad_id,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_source AS STRING) AS utm_source,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_medium AS STRING) AS utm_medium,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_campaign AS STRING) AS utm_campaign,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_id AS STRING) AS utm_id,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_content AS STRING) AS utm_content,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_term AS STRING) AS utm_term,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:gclid AS STRING) AS gclid,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:fbclid AS STRING) AS fbclid,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:gad_source AS STRING) AS gad_source,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:irgwc AS STRING) AS irgwc,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:cadid AS STRING) AS cadid,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:sub_publisher AS STRING) AS sub_publisher,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:value_prop AS STRING) AS value_prop,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:marketing_type AS STRING) AS marketing_type,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:creative AS STRING) AS creative,
              CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:traffic_source AS STRING) AS traffic_source,
              CASE 
                WHEN browser:is_embedded = TRUE THEN CONCAT(CAST(os:name AS STRING), ' App')
                ELSE CONCAT(COALESCE(CAST(os:name AS STRING), 'Other'), ' Web')
              END AS device_type,
              null as enrollment_device,
              CAST(os:name AS STRING) AS operating_system,
              session_id

          FROM STREAMING_PLATFORM.SEGMENT_AND_HAWKER_PRODUCTION.WEB_EVENT_ROUTER_V1_WEB_ACTION_TRIGGERED
          WHERE 1 = 1
            AND unique_id = 'first_entry_point'
            AND timestamp::DATE >= DATE '2025-05-08'
       )
    where 1=1


lp_signup_cta_tapped_input :
  type : input
  connector: snowflake
  query: >
    select  id,
            REGEXP_REPLACE(user_id, '^"|"$', '') AS user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            false as is_embedded,
            'Enter Enrollment CTA Tapped' event,
            'ENTER_ENROLLMENT_CTA_TAPPED' as source_table,
            context_page_url,
            CASE
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'tp_' THEN 
                'tp_' || COALESCE(
                  cast(PARSE_URL(context_page_referrer, 1):parameters:ref as string),
                  cast(PARSE_URL(context_page_url, 1):parameters:ref as string),
                  ''
                )
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'cj' THEN 
                'cj_' || COALESCE(
                  cast(PARSE_URL(context_page_referrer, 1):parameters:pid as string),
                  cast(PARSE_URL(context_page_url, 1):parameters:pid as string),
                  ''
                )
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'impact' THEN 
                COALESCE(
                  cast(PARSE_URL(context_page_referrer, 1):parameters:ad_id as string),
                  cast(PARSE_URL(context_page_url, 1):parameters:ad_id as string),
                  ''
                )
              ELSE CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)
            END AS ad_id,          
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_source AS STRING) AS utm_source,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_medium AS STRING) AS utm_medium,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_campaign AS STRING) AS utm_campaign,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_id AS STRING) AS utm_id,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_content AS STRING) AS utm_content,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_term AS STRING) AS utm_term,
            CAST(PARSE_URL(context_page_url, 1):parameters:gclid AS STRING) AS gclid,
            CAST(PARSE_URL(context_page_url, 1):parameters:fbclid AS STRING) AS fbclid,
            CAST(PARSE_URL(context_page_url, 1):parameters:gad_source AS STRING) AS gad_source,
            CAST(PARSE_URL(context_page_url, 1):parameters:irgwc AS STRING) AS irgwc,
            CAST(PARSE_URL(context_page_url, 1):parameters:cadid AS STRING) AS cadid,
            CAST(PARSE_URL(context_page_url, 1):parameters:sub_publisher AS STRING) AS sub_publisher,
            CAST(PARSE_URL(context_page_url, 1):parameters:value_prop AS STRING) AS value_prop,
            CAST(PARSE_URL(context_page_url, 1):parameters:marketing_type AS STRING) AS marketing_type,
            CAST(PARSE_URL(context_page_url, 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
            CAST(PARSE_URL(context_page_url, 1):parameters:creative AS STRING) AS creative,
            CAST(PARSE_URL(context_page_url, 1):parameters:traffic_source AS STRING) AS traffic_source,
            CASE
                  WHEN LOWER(context_user_agent) LIKE '%android%' THEN 'Android Web'
                  WHEN LOWER(context_user_agent) LIKE '%iphone%' THEN 'iPhone Web'
                  WHEN LOWER(context_user_agent) LIKE '%ipad%' THEN 'iPad Web'
                  WHEN LOWER(context_user_agent) LIKE '%macintosh%' THEN 'Mac Web'
                  WHEN LOWER(context_user_agent) LIKE '%windows%' THEN 'Windows Web'
                  WHEN LOWER(context_user_agent) LIKE '%linux%' OR LOWER(context_user_agent) LIKE '%x11%' THEN 'Linux'
                  WHEN LOWER(context_user_agent) LIKE '%facebook%' THEN 'Facebook Bot'
                  WHEN LOWER(context_user_agent) LIKE '%google-speakr%' THEN 'Google Speaker'
                  WHEN LOWER(context_user_agent) LIKE '%bb10%' OR LOWER(context_user_agent) LIKE '%kbd%' THEN 'Blackberry'
                  WHEN LOWER(context_user_agent) LIKE '%google%' THEN 'Google Bot'
                  WHEN LOWER(context_user_agent) LIKE '%bot%' OR LOWER(context_user_agent) LIKE '%spider%' THEN 'Other Crawlers'
                  WHEN LOWER(context_user_agent) LIKE '%mobile%' THEN 'Other Mobile Web'
                  ELSE 'All Other Devices'
                END AS device_type,
              null as enrollment_device,
            CASE
                  WHEN LOWER(context_user_agent) LIKE '%android%' THEN 'Android'
                  WHEN LOWER(context_user_agent) LIKE '%iphone%' OR LOWER(context_user_agent) LIKE '%ipad%' OR LOWER(context_user_agent) LIKE '%ios%' THEN 'iOS'
                  WHEN LOWER(context_user_agent) LIKE '%windows%' THEN 'Windows'
                  WHEN LOWER(context_user_agent) LIKE '%macintosh%' THEN 'Mac'
                  WHEN LOWER(context_user_agent) LIKE '%linux%' OR LOWER(context_user_agent) LIKE '%x11%' THEN 'Linux'
                  ELSE 'Other OS'
              END AS operating_system,
            'na' as session_id
    from SEGMENT.CHIME_PROD.ENTER_ENROLLMENT_CTA_TAPPED
    where timestamp::date < date '2025-04-30'

    union distinct

    select  id,
            REGEXP_REPLACE(user_id, '^"|"$', '') as user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            false as is_embedded,
            'Enter Enrollment CTA Tapped' event,
            'LEAD_GENERATION_INTERACTION_CAPTURED' as source_table,
            context_page_url,
            CASE
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'tp_' THEN 
                'tp_' || COALESCE(
                  cast(PARSE_URL(context_page_referrer, 1):parameters:ref as string),
                  cast(PARSE_URL(context_page_url, 1):parameters:ref as string),
                  ''
                )
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'cj' THEN 
                'cj_' || COALESCE(
                  cast(PARSE_URL(context_page_referrer, 1):parameters:pid as string),
                  cast(PARSE_URL(context_page_url, 1):parameters:pid as string),
                  ''
                )
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'impact' THEN 
                COALESCE(
                  cast(PARSE_URL(context_page_referrer, 1):parameters:ad_id as string),
                  cast(PARSE_URL(context_page_url, 1):parameters:ad_id as string),
                  ''
                )
              ELSE CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)
            END AS ad_id,          
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_source AS STRING) AS utm_source,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_medium AS STRING) AS utm_medium,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_campaign AS STRING) AS utm_campaign,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_id AS STRING) AS utm_id,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_content AS STRING) AS utm_content,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_term AS STRING) AS utm_term,
            CAST(PARSE_URL(context_page_url, 1):parameters:gclid AS STRING) AS gclid,
            CAST(PARSE_URL(context_page_url, 1):parameters:fbclid AS STRING) AS fbclid,
            CAST(PARSE_URL(context_page_url, 1):parameters:gad_source AS STRING) AS gad_source,
            CAST(PARSE_URL(context_page_url, 1):parameters:irgwc AS STRING) AS irgwc,
            CAST(PARSE_URL(context_page_url, 1):parameters:cadid AS STRING) AS cadid,
            CAST(PARSE_URL(context_page_url, 1):parameters:sub_publisher AS STRING) AS sub_publisher,
            CAST(PARSE_URL(context_page_url, 1):parameters:value_prop AS STRING) AS value_prop,
            CAST(PARSE_URL(context_page_url, 1):parameters:marketing_type AS STRING) AS marketing_type,
            CAST(PARSE_URL(context_page_url, 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
            CAST(PARSE_URL(context_page_url, 1):parameters:creative AS STRING) AS creative,
            CAST(PARSE_URL(context_page_url, 1):parameters:traffic_source AS STRING) AS traffic_source,
            CASE
                  WHEN LOWER(context_user_agent) LIKE '%android%' THEN 'Android Web'
                  WHEN LOWER(context_user_agent) LIKE '%iphone%' THEN 'iPhone Web'
                  WHEN LOWER(context_user_agent) LIKE '%ipad%' THEN 'iPad Web'
                  WHEN LOWER(context_user_agent) LIKE '%macintosh%' THEN 'Mac Web'
                  WHEN LOWER(context_user_agent) LIKE '%windows%' THEN 'Windows Web'
                  WHEN LOWER(context_user_agent) LIKE '%linux%' OR LOWER(context_user_agent) LIKE '%x11%' THEN 'Linux'
                  WHEN LOWER(context_user_agent) LIKE '%facebook%' THEN 'Facebook Bot'
                  WHEN LOWER(context_user_agent) LIKE '%google-speakr%' THEN 'Google Speaker'
                  WHEN LOWER(context_user_agent) LIKE '%bb10%' OR LOWER(context_user_agent) LIKE '%kbd%' THEN 'Blackberry'
                  WHEN LOWER(context_user_agent) LIKE '%google%' THEN 'Google Bot'
                  WHEN LOWER(context_user_agent) LIKE '%bot%' OR LOWER(context_user_agent) LIKE '%spider%' THEN 'Other Crawlers'
                  WHEN LOWER(context_user_agent) LIKE '%mobile%' THEN 'Other Mobile Web'
                  ELSE 'All Other Devices'
                END AS device_type,
              null as enrollment_device,
              CASE
                  WHEN LOWER(context_user_agent) LIKE '%android%' THEN 'Android'
                  WHEN LOWER(context_user_agent) LIKE '%iphone%' OR LOWER(context_user_agent) LIKE '%ipad%' OR LOWER(context_user_agent) LIKE '%ios%' THEN 'iOS'
                  WHEN LOWER(context_user_agent) LIKE '%windows%' THEN 'Windows'
                  WHEN LOWER(context_user_agent) LIKE '%macintosh%' THEN 'Mac'
                  WHEN LOWER(context_user_agent) LIKE '%linux%' OR LOWER(context_user_agent) LIKE '%x11%' THEN 'Linux'
                  ELSE 'Other OS'
                END AS operating_system,
              'na' as session_id
    from SEGMENT.CHIME_PROD.LEAD_GENERATION_INTERACTION_CAPTURED
    where interaction_type = 'INTERACTION_TYPE_ENTER_ENROLLMENT_CTA_TAPPED'
      and timestamp::date < date '2025-04-30'

    union all 

    select 
          _message_id as id,
          REGEXP_REPLACE(CAST(_user_id AS STRING), '^"|"$', '') AS user_id,
          REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
          timestamp::date as date,
          timestamp,
          false as is_embedded,
          'Enter Enrollment CTA Tapped' as event,
          'web_item_clicked' as source_table,
          CAST(page:url AS STRING) AS context_page_url,
          CASE
            WHEN SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2) = 'tp_' THEN 
              'tp_' || COALESCE(
                cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:ref as string),
                cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:ref as string),
                ''
              )
            WHEN LOWER(SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)) = 'cj' THEN 
              'cj_' || COALESCE(
                cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:pid as string),
                cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:pid as string),
                ''
              )
            WHEN LOWER(SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)) = 'impact' THEN 
              COALESCE(
                cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:ad_id as string),
                cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:ad_id as string),
                ''
              )
            ELSE SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)
          END AS ad_id,

          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_source AS STRING) AS utm_source,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_medium AS STRING) AS utm_medium,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_campaign AS STRING) AS utm_campaign,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_id AS STRING) AS utm_id,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_content AS STRING) AS utm_content,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_term AS STRING) AS utm_term,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:gclid AS STRING) AS gclid,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:fbclid AS STRING) AS fbclid,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:gad_source AS STRING) AS gad_source,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:irgwc AS STRING) AS irgwc,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:cadid AS STRING) AS cadid,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:sub_publisher AS STRING) AS sub_publisher,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:value_prop AS STRING) AS value_prop,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:marketing_type AS STRING) AS marketing_type,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:creative AS STRING) AS creative,
          CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:traffic_source AS STRING) AS traffic_source,
          CONCAT(COALESCE(CAST(os:name AS STRING), 'Other'), ' Web') AS device_type,  
          null AS enrollment_device,
          CAST(os:name AS STRING) AS operating_system,
          session_id
    from           
      streaming_platform.segment_and_hawker_production.web_event_router_v1_web_item_clicked 
      where unique_id = 'enrollment_cta_tapped' 
        and timestamp::date >= date '2025-04-30'



app_signup_cta_tapped_input :
  type : input
  connector: snowflake
  query: >
    select  id,
            REGEXP_REPLACE(user_id, '^"|"$', '') as user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            false as is_embedded,
            device_id,
            app_version,
            'App Signup CTA Tapped' as event,
            'SIGN_UP_BUTTON_TAPPED' as source_table,
            concat(context_device_type,' App') as device_type,
            null as enrollment_device,
            context_device_type as operating_system,
            'na' as session_id
    from SEGMENT.CHIME_PROD.SIGN_UP_BUTTON_TAPPED
    where timestamp::date < date '2025-04-30'

    union all

    select  
            _message_id as id,
            REGEXP_REPLACE(CAST(_user_id AS STRING), '^"|"$', '') AS user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            false as is_embedded,
            _device_id as device_id,
            cast(app:version as string) as app_version,
            'App Signup CTA Tapped' as event,
            'MOBILE_ITEM_TAPPED' as source_table,
            CONCAT(COALESCE(CAST(os:name AS STRING), 'Other'), ' Web') AS device_type,  
            null AS enrollment_device,
            CAST(os:name AS STRING) AS operating_system,
            session_id
    from streaming_platform.segment_and_hawker_production.mobile_event_router_v1_mobile_item_tapped
    where unique_id = 'signup_button' 
      and  timestamp::date >= date '2025-04-30' 




app_welcome_screen_input :
  type : input
  connector: snowflake
  query: >
    select
         distinct 
         id,
         REGEXP_REPLACE(user_id, '^"|"$', '') as user_id,
         REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
         date,
         timestamp,
         is_embedded,
         app_version,
         device_id,
         event,
         source_table,
         user_enrolled_flag,
         device_enrolled_flag,
         user_enrollment_time,
         device_enrollment_time
    from edw_db.bio.stage_pre_enrollment_app_welcome


enrollment_flow_entered_input :
  type : input
  connector: snowflake
  query: >
    select  id,
            REGEXP_REPLACE(user_id, '^"|"$', '') as user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            is_embedded,
            'Enrollment Flow Entered' as event,
            'ENROLLMENT_FLOW_ENTERED' as source_table,
            context_page_url,
            CASE
            WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'tp_' THEN 
                'tp_' || COALESCE(
                    CAST(PARSE_URL(context_page_referrer, 1):parameters:ref AS STRING),
                    CAST(PARSE_URL(context_page_url, 1):parameters:ref AS STRING),
                    ''
                )
            WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'cj' THEN 
                'cj_' || COALESCE(
                    CAST(PARSE_URL(context_page_referrer, 1):parameters:pid AS STRING),
                    CAST(PARSE_URL(context_page_url, 1):parameters:pid AS STRING),
                    ''
                )
            WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'impact' THEN 
                COALESCE(
                    CAST(PARSE_URL(context_page_referrer, 1):parameters:ad_id AS STRING),
                    CAST(PARSE_URL(context_page_url, 1):parameters:ad_id AS STRING),
                    ''
                )
            ELSE CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)
            END AS ad_id,

            CAST(PARSE_URL(context_page_url, 1):parameters:utm_source AS STRING) AS utm_source,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_medium AS STRING) AS utm_medium,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_campaign AS STRING) AS utm_campaign,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_id AS STRING) AS utm_id,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_content AS STRING) AS utm_content,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_term AS STRING) AS utm_term,
            CAST(PARSE_URL(context_page_url, 1):parameters:gclid AS STRING) AS gclid,
            CAST(PARSE_URL(context_page_url, 1):parameters:fbclid AS STRING) AS fbclid,
            CAST(PARSE_URL(context_page_url, 1):parameters:gad_source AS STRING) AS gad_source,
            CAST(PARSE_URL(context_page_url, 1):parameters:irgwc AS STRING) AS irgwc,
            CAST(PARSE_URL(context_page_url, 1):parameters:cadid AS STRING) AS cadid,
            CAST(PARSE_URL(context_page_url, 1):parameters:sub_publisher AS STRING) AS sub_publisher,
            CAST(PARSE_URL(context_page_url, 1):parameters:value_prop AS STRING) AS value_prop,
            CAST(PARSE_URL(context_page_url, 1):parameters:marketing_type AS STRING) AS marketing_type,
            CAST(PARSE_URL(context_page_url, 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
            CAST(PARSE_URL(context_page_url, 1):parameters:creative AS STRING) AS creative,
            CAST(PARSE_URL(context_page_url, 1):parameters:traffic_source AS STRING) AS traffic_source,

            -- Device Type
            CASE
                WHEN is_embedded = TRUE AND LOWER(context_user_agent) LIKE '%android%' THEN 'Android App'
                WHEN is_embedded = TRUE AND (
                    LOWER(context_user_agent) LIKE '%iphone%' OR LOWER(context_user_agent) LIKE '%ipad%'
                ) THEN 'iOS App'
                WHEN LOWER(context_user_agent) LIKE '%android%' THEN 'Android Web'
                WHEN LOWER(context_user_agent) LIKE '%iphone%' THEN 'iPhone Web'
                WHEN LOWER(context_user_agent) LIKE '%ipad%' THEN 'iPad Web'
                WHEN LOWER(context_user_agent) LIKE '%macintosh%' THEN 'Mac Web'
                WHEN LOWER(context_user_agent) LIKE '%windows%' THEN 'Windows Web'
                WHEN LOWER(context_user_agent) LIKE '%linux%' OR LOWER(context_user_agent) LIKE '%x11%' THEN 'Linux'
                WHEN LOWER(context_user_agent) LIKE '%facebook%' THEN 'Facebook Bot'
                WHEN LOWER(context_user_agent) LIKE '%google-speakr%' THEN 'Google Speaker'
                WHEN LOWER(context_user_agent) LIKE '%bb10%' OR LOWER(context_user_agent) LIKE '%kbd%' THEN 'Blackberry'
                WHEN LOWER(context_user_agent) LIKE '%google%' THEN 'Google Bot'
                WHEN LOWER(context_user_agent) LIKE '%bot%' OR LOWER(context_user_agent) LIKE '%spider%' THEN 'Other Crawlers'
                WHEN LOWER(context_user_agent) LIKE '%mobile%' THEN 'Other Mobile Web'
                ELSE 'All Other Devices'
            END AS device_type,

            -- Enrollment Device
            CASE
                WHEN is_embedded = TRUE THEN 'Mobile App'
                WHEN is_embedded = FALSE AND (
                    LOWER(context_user_agent) LIKE '%macintosh%' OR
                    LOWER(context_user_agent) LIKE '%windows%' OR
                    LOWER(context_user_agent) LIKE '%linux%' OR
                    LOWER(context_user_agent) LIKE '%x11%'
                ) THEN 'Desktop Web'
                ELSE 'Mobile Web'
            END AS enrollment_device,

            -- Operating System
            CASE
                WHEN LOWER(context_user_agent) LIKE '%android%' THEN 'Android'
                WHEN LOWER(context_user_agent) LIKE '%iphone%' OR LOWER(context_user_agent) LIKE '%ipad%' OR LOWER(context_user_agent) LIKE '%ios%' THEN 'iOS'
                WHEN LOWER(context_user_agent) LIKE '%windows%' THEN 'Windows'
                WHEN LOWER(context_user_agent) LIKE '%macintosh%' THEN 'Mac'
                WHEN LOWER(context_user_agent) LIKE '%linux%' OR LOWER(context_user_agent) LIKE '%x11%' THEN 'Linux'
                ELSE 'Other OS'
            END AS operating_system,
            'na' AS session_id
    from  SEGMENT.CHIME_PROD.ENROLLMENT_FLOW_ENTERED
    where timestamp::date < date '2025-04-30'

    union all 

    select  
            _message_id as id,
            REGEXP_REPLACE(CAST(_user_id AS STRING), '^"|"$', '') AS user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            cast(browser:is_embedded as boolean) as is_embedded,          
            'Enrollment Flow Entered' as event,
            'WEB_FLOW_STATUS_CHANGED' as source_table,
            CAST(page:url AS STRING) AS context_page_url,
            CASE
                WHEN SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2) = 'tp_' THEN 
                  'tp_' || COALESCE(
                    cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:ref as string),
                    cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:ref as string),
                    ''
                  )
                WHEN LOWER(SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)) = 'cj' THEN 
                  'cj_' || COALESCE(
                    cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:pid as string),
                    cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:pid as string),
                    ''
                  )
                WHEN LOWER(SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)) = 'impact' THEN 
                  COALESCE(
                    cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:ad_id as string),
                    cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:ad_id as string),
                    ''
                  )
                ELSE SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)
              END AS ad_id,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_source AS STRING) AS utm_source,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_medium AS STRING) AS utm_medium,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_campaign AS STRING) AS utm_campaign,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_id AS STRING) AS utm_id,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_content AS STRING) AS utm_content,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:utm_term AS STRING) AS utm_term,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:gclid AS STRING) AS gclid,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:fbclid AS STRING) AS fbclid,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:gad_source AS STRING) AS gad_source,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:irgwc AS STRING) AS irgwc,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:cadid AS STRING) AS cadid,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:sub_publisher AS STRING) AS sub_publisher,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:value_prop AS STRING) AS value_prop,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:marketing_type AS STRING) AS marketing_type,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:creative AS STRING) AS creative,
            CAST(PARSE_URL(CAST(page:url AS STRING), 1):parameters:traffic_source AS STRING) AS traffic_source,
            CASE 
              WHEN browser:is_embedded = TRUE THEN CONCAT(CAST(os:name AS STRING), ' App')
              ELSE CONCAT(COALESCE(CAST(os:name AS STRING), 'Other'), ' Web')
            END AS device_type,  
            CASE 
              WHEN browser:is_embedded = TRUE THEN 'Mobile App'
              WHEN browser:is_embedded = FALSE AND LOWER(CONCAT(COALESCE(CAST(os:name AS STRING), 'Other'), ' Web')) IN ('windows web', 'mac web', 'linux web') THEN 'Desktop Web'
              ELSE 'Mobile Web'
            END AS enrollment_device,
            CAST(os:name AS STRING) AS operating_syatem,
            session_id
    from   streaming_platform.segment_and_hawker_production.web_event_router_v1_web_flow_status_changed where unique_id ='enrollment_flow_entered'
    and timestamp::date >= date '2025-04-30' 


enrollment_section_completed_input :
  type : input
  connector: snowflake
  query: >
    select  id,
            REGEXP_REPLACE(user_id, '^"|"$', '') as user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            page_label,
            is_embedded,
            fields,
            case when page_label ilike '%verify phone%'
                or (page_label = 'Phone' and fields ilike '%verification%')
                then 'Verify Phone Completed'
                when page_label ilike '%activity%' then 'Activity Completed'
                when page_label = 'CreditTermsOfService' then 'Terms Of Service Completed'
                when page_label = 'AddressAutocompleteSmarty' then 'Address Completed'
            else concat(page_label, ' Completed') end event,
            'ENROLLMENT_SECTION_COMPLETED' as source_table,
            context_page_url,
            CASE
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'tp_' THEN 
                  'tp_' || COALESCE(
                      CAST(PARSE_URL(context_page_referrer, 1):parameters:ref AS STRING),
                      CAST(PARSE_URL(context_page_url, 1):parameters:ref AS STRING),
                      ''
                  )
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'cj' THEN 
                  'cj_' || COALESCE(
                      CAST(PARSE_URL(context_page_referrer, 1):parameters:pid AS STRING),
                      CAST(PARSE_URL(context_page_url, 1):parameters:pid AS STRING),
                      ''
                  )
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'impact' THEN 
                  COALESCE(
                      CAST(PARSE_URL(context_page_referrer, 1):parameters:ad_id AS STRING),
                      CAST(PARSE_URL(context_page_url, 1):parameters:ad_id AS STRING),
                      ''
                  )
              ELSE CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)
                END AS ad_id,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_source AS STRING) AS utm_source,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_medium AS STRING) AS utm_medium,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_campaign AS STRING) AS utm_campaign,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_id AS STRING) AS utm_id,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_content AS STRING) AS utm_content,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_term AS STRING) AS utm_term,
            CAST(PARSE_URL(context_page_url, 1):parameters:gclid AS STRING) AS gclid,
            CAST(PARSE_URL(context_page_url, 1):parameters:fbclid AS STRING) AS fbclid,
            CAST(PARSE_URL(context_page_url, 1):parameters:gad_source AS STRING) AS gad_source,
            CAST(PARSE_URL(context_page_url, 1):parameters:irgwc AS STRING) AS irgwc,
            CAST(PARSE_URL(context_page_url, 1):parameters:cadid AS STRING) AS cadid,
            CAST(PARSE_URL(context_page_url, 1):parameters:sub_publisher AS STRING) AS sub_publisher,
            CAST(PARSE_URL(context_page_url, 1):parameters:value_prop AS STRING) AS value_prop,
            CAST(PARSE_URL(context_page_url, 1):parameters:marketing_type AS STRING) AS marketing_type,
            CAST(PARSE_URL(context_page_url, 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
            CAST(PARSE_URL(context_page_url, 1):parameters:creative AS STRING) AS creative,
            CAST(PARSE_URL(context_page_url, 1):parameters:traffic_source AS STRING) AS traffic_source,
            -- Device Type
            CASE
                WHEN is_embedded = TRUE AND LOWER(context_user_agent) LIKE '%android%' THEN 'Android App'
                WHEN is_embedded = TRUE AND (
                    LOWER(context_user_agent) LIKE '%iphone%' OR LOWER(context_user_agent) LIKE '%ipad%'
                ) THEN 'iOS App'
                WHEN LOWER(context_user_agent) LIKE '%android%' THEN 'Android Web'
                WHEN LOWER(context_user_agent) LIKE '%iphone%' THEN 'iPhone Web'
                WHEN LOWER(context_user_agent) LIKE '%ipad%' THEN 'iPad Web'
                WHEN LOWER(context_user_agent) LIKE '%macintosh%' THEN 'Mac Web'
                WHEN LOWER(context_user_agent) LIKE '%windows%' THEN 'Windows Web'
                WHEN LOWER(context_user_agent) LIKE '%linux%' OR LOWER(context_user_agent) LIKE '%x11%' THEN 'Linux'
                WHEN LOWER(context_user_agent) LIKE '%facebook%' THEN 'Facebook Bot'
                WHEN LOWER(context_user_agent) LIKE '%google-speakr%' THEN 'Google Speaker'
                WHEN LOWER(context_user_agent) LIKE '%bb10%' OR LOWER(context_user_agent) LIKE '%kbd%' THEN 'Blackberry'
                WHEN LOWER(context_user_agent) LIKE '%google%' THEN 'Google Bot'
                WHEN LOWER(context_user_agent) LIKE '%bot%' OR LOWER(context_user_agent) LIKE '%spider%' THEN 'Other Crawlers'
                WHEN LOWER(context_user_agent) LIKE '%mobile%' THEN 'Other Mobile Web'
                ELSE 'All Other Devices'
            END AS device_type,
            -- Enrollment Device
            CASE
                WHEN is_embedded = TRUE THEN 'Mobile App'
                WHEN is_embedded = FALSE AND (
                    LOWER(context_user_agent) LIKE '%macintosh%' OR
                    LOWER(context_user_agent) LIKE '%windows%' OR
                    LOWER(context_user_agent) LIKE '%linux%' OR
                    LOWER(context_user_agent) LIKE '%x11%'
                ) THEN 'Desktop Web'
                ELSE 'Mobile Web'
            END AS enrollment_device,
            -- Operating System
            CASE
                WHEN LOWER(context_user_agent) LIKE '%android%' THEN 'Android'
                WHEN LOWER(context_user_agent) LIKE '%iphone%' OR LOWER(context_user_agent) LIKE '%ipad%' OR LOWER(context_user_agent) LIKE '%ios%' THEN 'iOS'
                WHEN LOWER(context_user_agent) LIKE '%windows%' THEN 'Windows'
                WHEN LOWER(context_user_agent) LIKE '%macintosh%' THEN 'Mac'
                WHEN LOWER(context_user_agent) LIKE '%linux%' OR LOWER(context_user_agent) LIKE '%x11%' THEN 'Linux'
                ELSE 'Other OS'
            END AS operating_system,
            'na' AS session_id
    from segment.chime_prod.enrollment_section_completed
    where user_id is not null
        and page_label in ( 'Account'
                           , 'Referral Account'
                           , 'P2P Account'
                           , 'Date of Birth'
                           , 'Phone'
                           , 'Verify Phone'
                           , 'AddressAutocompleteSmarty'
                           , 'Password'
                           , 'Activity'
                           , 'Social Security'
                           , 'CreditTermsOfService')
        and timestamp::date < date '2025-04-30'

    union all 


    select  
          _message_id as id,
          REGEXP_REPLACE(CAST(_user_id AS STRING), '^"|"$', '') AS user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
          timestamp::date as date,
          timestamp,
          step_name as page_label,   --similar to page_label
          cast(browser:is_embedded as boolean) as is_embedded,
          fields,
          case when step_name ilike '%verify phone%'
              or (step_name = 'Phone' and fields ilike '%verification%') then 'Verify Phone Completed'
              when step_name ilike '%activity%' then 'Activity Completed'
              when step_name = 'CreditTermsOfService' then 'Terms Of Service Completed'
              when step_name = 'AddressAutocompleteSmarty' then 'Address Completed'
          else concat(step_name, ' Completed') end as event,
          'web_form_submitted' as source_table,
          cast(page:url as string) as context_page_url,
          CASE
            WHEN SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2) = 'tp_' THEN 
              'tp_' || COALESCE(
                cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:ref as string),
                cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:ref as string),
                ''
              )
            WHEN LOWER(SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)) = 'cj' THEN 
              'cj_' || COALESCE(
                cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:pid as string),
                cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:pid as string),
                ''
              )
            WHEN LOWER(SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)) = 'impact' THEN 
              COALESCE(
                cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:ad_id as string),
                cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:ad_id as string),
                ''
              )
            ELSE SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)
          END AS ad_id,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_source AS STRING) AS utm_source,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_medium AS STRING) AS utm_medium,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_campaign AS STRING) AS utm_campaign,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_id AS STRING) AS utm_id,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_content AS STRING) AS utm_content,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_term AS STRING) AS utm_term,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:gclid AS STRING) AS gclid,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:fbclid AS STRING) AS fbclid,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:gad_source AS STRING) AS gad_source,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:irgwc AS STRING) AS irgwc,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:cadid AS STRING) AS cadid,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:sub_publisher AS STRING) AS sub_publisher,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:value_prop AS STRING) AS value_prop,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:marketing_type AS STRING) AS marketing_type,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:creative AS STRING) AS creative,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:traffic_source AS STRING) AS traffic_source,
          CASE 
            WHEN browser:is_embedded = TRUE THEN CONCAT(CAST(os:name AS STRING), ' App')
            ELSE CONCAT(COALESCE(CAST(os:name AS STRING), 'Other'), ' Web')
          END AS device_type,
          
          CASE 
            WHEN browser:is_embedded = TRUE THEN 'Mobile App'
            WHEN browser:is_embedded = FALSE AND LOWER(CONCAT(COALESCE(CAST(os:name AS STRING), 'Other'), ' Web')) IN ('windows web', 'mac web', 'linux web') THEN 'Desktop Web'
            ELSE 'Mobile Web'
          END AS enrollment_device,
          CAST(os:name AS STRING) AS operating_system,
          session_id
    from streaming_platform.segment_and_hawker_production.web_event_router_v1_web_form_submitted 
    where unique_id ='enrollment_section_completed' 
            and step_name in ( 'Account'
                           , 'Referral Account'
                           , 'P2P Account'
                           , 'Date of Birth'
                           , 'Phone'
                           , 'Verify Phone'
                           , 'AddressAutocompleteSmarty'
                           , 'Password'
                           , 'Activity'
                           , 'Social Security'
                           , 'CreditTermsOfService') and form_name ='enrollment' 
            and timestamp::date >= date '2025-04-30' 

enrollment_section_view_input :
  type : input
  connector: snowflake
  query: >
    select  id,
            REGEXP_REPLACE(user_id, '^"|"$', '') as user_id, --null on account page
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            is_embedded, --  null on success page
            concat(name,' Viewed') as event,
            'PAGES' as source_table,
            context_page_url,
            CASE
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'tp_' THEN 
                  'tp_' || COALESCE(
                      CAST(PARSE_URL(context_page_referrer, 1):parameters:ref AS STRING),
                      CAST(PARSE_URL(context_page_url, 1):parameters:ref AS STRING),
                      ''
                  )
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'cj' THEN 
                  'cj_' || COALESCE(
                      CAST(PARSE_URL(context_page_referrer, 1):parameters:pid AS STRING),
                      CAST(PARSE_URL(context_page_url, 1):parameters:pid AS STRING),
                      ''
                  )
              WHEN LOWER(CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)) = 'impact' THEN 
                  COALESCE(
                      CAST(PARSE_URL(context_page_referrer, 1):parameters:ad_id AS STRING),
                      CAST(PARSE_URL(context_page_url, 1):parameters:ad_id AS STRING),
                      ''
                  )
              ELSE CAST(PARSE_URL(context_page_url, 1):parameters:ad AS STRING)
            END AS ad_id,            
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_source AS STRING) AS utm_source,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_medium AS STRING) AS utm_medium,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_campaign AS STRING) AS utm_campaign,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_id AS STRING) AS utm_id,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_content AS STRING) AS utm_content,
            CAST(PARSE_URL(context_page_url, 1):parameters:utm_term AS STRING) AS utm_term,
            CAST(PARSE_URL(context_page_url, 1):parameters:gclid AS STRING) AS gclid,
            CAST(PARSE_URL(context_page_url, 1):parameters:fbclid AS STRING) AS fbclid,
            CAST(PARSE_URL(context_page_url, 1):parameters:gad_source AS STRING) AS gad_source,
            CAST(PARSE_URL(context_page_url, 1):parameters:irgwc AS STRING) AS irgwc,
            CAST(PARSE_URL(context_page_url, 1):parameters:cadid AS STRING) AS cadid,
            CAST(PARSE_URL(context_page_url, 1):parameters:sub_publisher AS STRING) AS sub_publisher,
            CAST(PARSE_URL(context_page_url, 1):parameters:value_prop AS STRING) AS value_prop,
            CAST(PARSE_URL(context_page_url, 1):parameters:marketing_type AS STRING) AS marketing_type,
            CAST(PARSE_URL(context_page_url, 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
            CAST(PARSE_URL(context_page_url, 1):parameters:creative AS STRING) AS creative,
            CAST(PARSE_URL(context_page_url, 1):parameters:traffic_source AS STRING) AS traffic_source,
            -- Device Type
            CASE
                WHEN is_embedded = TRUE AND LOWER(context_user_agent) LIKE '%android%' THEN 'Android App'
                WHEN is_embedded = TRUE AND (
                    LOWER(context_user_agent) LIKE '%iphone%' OR LOWER(context_user_agent) LIKE '%ipad%'
                ) THEN 'iOS App'
                WHEN LOWER(context_user_agent) LIKE '%android%' THEN 'Android Web'
                WHEN LOWER(context_user_agent) LIKE '%iphone%' THEN 'iPhone Web'
                WHEN LOWER(context_user_agent) LIKE '%ipad%' THEN 'iPad Web'
                WHEN LOWER(context_user_agent) LIKE '%macintosh%' THEN 'Mac Web'
                WHEN LOWER(context_user_agent) LIKE '%windows%' THEN 'Windows Web'
                WHEN LOWER(context_user_agent) LIKE '%linux%' OR LOWER(context_user_agent) LIKE '%x11%' THEN 'Linux'
                WHEN LOWER(context_user_agent) LIKE '%facebook%' THEN 'Facebook Bot'
                WHEN LOWER(context_user_agent) LIKE '%google-speakr%' THEN 'Google Speaker'
                WHEN LOWER(context_user_agent) LIKE '%bb10%' OR LOWER(context_user_agent) LIKE '%kbd%' THEN 'Blackberry'
                WHEN LOWER(context_user_agent) LIKE '%google%' THEN 'Google Bot'
                WHEN LOWER(context_user_agent) LIKE '%bot%' OR LOWER(context_user_agent) LIKE '%spider%' THEN 'Other Crawlers'
                WHEN LOWER(context_user_agent) LIKE '%mobile%' THEN 'Other Mobile Web'
                ELSE 'All Other Devices'
            END AS device_type,
            -- Enrollment Device
            CASE
                WHEN is_embedded = TRUE THEN 'Mobile App'
                WHEN is_embedded = FALSE AND (
                    LOWER(context_user_agent) LIKE '%macintosh%' OR
                    LOWER(context_user_agent) LIKE '%windows%' OR
                    LOWER(context_user_agent) LIKE '%linux%' OR
                    LOWER(context_user_agent) LIKE '%x11%'
                ) THEN 'Desktop Web'
                ELSE 'Mobile Web'
            END AS enrollment_device,
            -- Operating System
            CASE
                WHEN LOWER(context_user_agent) LIKE '%android%' THEN 'Android'
                WHEN LOWER(context_user_agent) LIKE '%iphone%' OR LOWER(context_user_agent) LIKE '%ipad%' OR LOWER(context_user_agent) LIKE '%ios%' THEN 'iOS'
                WHEN LOWER(context_user_agent) LIKE '%windows%' THEN 'Windows'
                WHEN LOWER(context_user_agent) LIKE '%macintosh%' THEN 'Mac'
                WHEN LOWER(context_user_agent) LIKE '%linux%' OR LOWER(context_user_agent) LIKE '%x11%' THEN 'Linux'
                ELSE 'Other OS'
            END AS operating_system,
            'na' AS session_id
    from SEGMENT.CHIME_PROD.PAGES
    where name in ('Account Page'
      , 'Referral Account Page'
      , 'P2P Account Page'
      , 'Date of Birth Page'
      , 'Phone Page'
      , 'Verify Phone Page'
      , 'Address Page'
      , 'Password Page'
      , 'Activity Page'
      , 'Social Security Number Page'
      , 'Credit Day Zero Terms of Service Page'
      , 'Success')
      and timestamp::date < date '2025-04-30'

      union all 

      select  
            _message_id as id,
            REGEXP_REPLACE(CAST(_user_id AS STRING), '^"|"$', '') AS user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            browser:is_embedded AS is_embedded,  
            case
                when page_id = 'p2p_account_page' then 'P2P Account Page View'
                when page_id = 'date_of_birth_page' then 'Date of Birth Page View'
                when page_id = 'credit_day_zero_terms_of_service_page' then 'Credit Day Zero Terms of Service Page View'
                else INITCAP(REPLACE(page_id, '_', ' ')) || ' View'
            end as event,
            'WEB_PAGE_VIEWED' as source_table,
            cast(page:url as string) as context_page_url,
            CASE
            WHEN SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2) = 'tp_' THEN 
              'tp_' || COALESCE(
                cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:ref as string),
                cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:ref as string),
                ''
              )
            WHEN LOWER(SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)) = 'cj' THEN 
              'cj_' || COALESCE(
                cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:pid as string),
                cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:pid as string),
                ''
              )
            WHEN LOWER(SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)) = 'impact' THEN 
              COALESCE(
                cast(PARSE_URL(CAST(page:referrer AS STRING), 1):parameters:ad_id as string),
                cast(PARSE_URL(CAST(page:url AS STRING), 1):parameters:ad_id as string),
                ''
              )
            ELSE SPLIT_PART(REGEXP_SUBSTR(PARSE_URL(CAST(page:url AS STRING), 1):query, '(^|&)ad=([^&]*)'), '=', 2)
           END AS ad_id,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_source AS STRING) AS utm_source,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_medium AS STRING) AS utm_medium,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_campaign AS STRING) AS utm_campaign,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_id AS STRING) AS utm_id,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_content AS STRING) AS utm_content,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:utm_term AS STRING) AS utm_term,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:gclid AS STRING) AS gclid,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:fbclid AS STRING) AS fbclid,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:gad_source AS STRING) AS gad_source,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:irgwc AS STRING) AS irgwc,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:cadid AS STRING) AS cadid,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:sub_publisher AS STRING) AS sub_publisher,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:value_prop AS STRING) AS value_prop,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:marketing_type AS STRING) AS marketing_type,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:publisher_short_name AS STRING) AS publisher_short_name,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:creative AS STRING) AS creative,
          CAST(PARSE_URL(cast(_context:client_context:context:page:url AS string), 1):parameters:traffic_source AS STRING) AS traffic_source,
          CASE 
            WHEN browser:is_embedded = TRUE THEN CONCAT(CAST(os:name AS STRING), ' App')
            ELSE CONCAT(COALESCE(CAST(os:name AS STRING), 'Other'), ' Web')
          END AS device_type,
          
          CASE 
            WHEN browser:is_embedded = TRUE THEN 'Mobile App'
            WHEN browser:is_embedded = FALSE AND LOWER(CONCAT(COALESCE(CAST(os:name AS STRING), 'Other'), ' Web')) IN ('windows web', 'mac web', 'linux web') THEN 'Desktop Web'
            ELSE 'Mobile Web'
          END AS enrollment_device,
          CAST(os:name AS STRING) AS operating_system,
          session_id
      from STREAMING_PLATFORM.SEGMENT_AND_HAWKER_PRODUCTION.WEB_EVENT_ROUTER_V1_WEB_PAGE_VIEWED 
      where page_id in ('account_page'
                        , 'referral_account_page'
                        , 'p2p_account_page'
                        , 'date_of_birth_page'
                        , 'phone_page'
                        , 'verify_phone_page'
                        , 'address_page'
                        , 'password_page'
                        , 'activity_page'
                        , 'social_security_number_page'
                        , 'credit_day_zero_terms_of_service_page'
                        , 'success')
      and timestamp::date >= date '2025-04-30' 



enrollment_request_input :
  type : input
  connector: snowflake
  query: >
    select
            id,
            REGEXP_REPLACE(user_id, '^"|"$', '') as user_id,
            timestamp::date as date,
            timestamp,
            'Enrollment Requested' as event,
            'ENROLLMENT_REQUEST_COMPLETED' as source_table
    from SEGMENT.CHIME_PROD.ENROLLMENT_REQUEST_COMPLETED


enrollment_request_succeed_input :
  type : input
  connector: snowflake
  query: >
    select
            id,
            REGEXP_REPLACE(user_id, '^"|"$', '') as user_id,
            timestamp::date as date,
            timestamp,
            'Enrollment Succeeded' as event,
            'ENROLLMENT_REQUEST_COMPLETED' as source_table
    from SEGMENT.CHIME_PROD.ENROLLMENT_REQUEST_COMPLETED
    where enrollment_succeeded = TRUE


download_cta_tapped_input :
  type : input
  connector: snowflake
  query: >
    select
            id,
            REGEXP_REPLACE(user_id, '^"|"$', '') as user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            false as is_embedded,
            'App Download CTA Tapped' as event,
            'DOWNLOAD_CTA_TAPPED' as source_table
    from SEGMENT.CHIME_PROD.DOWNLOAD_CTA_TAPPED
    where timestamp::date < date '2025-04-30'

    union all 

    select 
            _message_id as id,
            REGEXP_REPLACE(CAST(_user_id AS STRING), '^"|"$', '') AS user_id,
            REGEXP_REPLACE(anonymous_id, '^"|"$', '') as anonymous_id,
            timestamp::date as date,
            timestamp,
            false as is_embedded,
            'App Download CTA Tapped' as event,
            'WEB_ITEM_CLICKED' as source_table
      from streaming_platform.segment_and_hawker_production.web_event_router_v1_web_item_clicked
      where unique_id= 'download_cta_tapped' 
          and timestamp::date >= date '2025-04-30'

member_redirect_input :
  type : input
  connector: snowflake
  query: >
    select
            id,
            user_id::varchar as user_id,
            original_user_id::varchar as original_user_id,
            anonymous_id,
            timestamp::date as date,
            timestamp,
            'Enrollment Redirected To Login' as event,
            matched_on,
            matches,
            'ENROLLMENT_REDIRECTED_TO_LOGIN' as source_table
    from SEGMENT.CHIME_PROD.ENROLLMENT_REDIRECTED_TO_LOGIN
    union all
    select
            _message_id as id,
            enrolling_user_id::varchar as user_id,
            active_user_id::varchar as original_user_id,
            null as anonymous_id,
            _creation_timestamp::date as date,
            _creation_timestamp as timestamp,
            'Enrollment User Conflict Found' as event,
            null as matched_on,
            null as matches,
            'AUTHENTICATION_V1_ENROLLMENT_USER_CONFLICT_FOUND' as source_table
    from STREAMING_PLATFORM.SEGMENT_AND_HAWKER_PRODUCTION.AUTHENTICATION_V1_ENROLLMENT_USER_CONFLICT_FOUND

config:
  type: bio
  debug: False
  cdc_column: timestamp
  grain:
    - id
    - event
  ranking_columns:
    - timestamp
  object: pre_enrollment

  combine:
    - enrollment_section_completed_input
    - enrollment_section_view_input
    - enrollment_request_input
    - enrollment_request_succeed_input
    - first_web_page_viewed_input
    - download_cta_tapped_input
    - app_welcome_screen_input
    - lp_signup_cta_tapped_input
    - enrollment_flow_entered_input
    - app_signup_cta_tapped_input
    - member_redirect_input



----------
glue.py

import datetime
import json
import os
import shutil
import sys

from collections import defaultdict
from datetime import timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from py4j.java_gateway import java_import
from pyspark.context import SparkContext
from pyspark.sql import functions as F, SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from transformation.sdk import *
from transformation.sdk.common import config
from transformation.sdk.common.chime_df import ParserAwareDataFrame
from transformation.sdk.common.log import Log


class Glue:

    def get_GlueArguments(self,
                          default_args):
        """

        This is a wrapper for getResolvedOptions(). It accepts a dictionary containing command line arguments along with their default values.
        If the command line arg doesn't contain the given argument key then the default value is returned in the final dictionary or args.

        NOTE:
            Define and use argument keys containing `_` and not `-` since getResolvedOptions() only accepts keys with `_`.
            Reference: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-get-resolved-options.html

        Arguments:
            default_args - dict containing args with their default values

        Returns:
            dict -- dictionary containing all args along with the requested one(s) with or without their default(s).

        """
        aws_special_arguments = ['--security_configuration', '--job-bookmark-option', '--tempdir', '--job_id', '--job_run_id', '--job_name']
        matched_arg_keys = []
        if sys.argv:
            for elem in sys.argv:
                if elem.startswith(f'--') and elem.find('=') !=-1:
                    matched_arg_keys.append(elem[2:elem.find('=')])
                elif elem.startswith(f'--') and elem.lower() not in aws_special_arguments:
                    matched_arg_keys.append(elem[2:])
            retrieved_args = getResolvedOptions(sys.argv, matched_arg_keys)
            default_args.update(retrieved_args)
            return default_args

        return default_args

    def get_SparkContext(self):
        return SparkContext.getOrCreate()

    def get_GlueContext(self):

        return GlueContext(self.sc)

    def get_GlueSession(self):

        spark = self.glueContext.spark_session
        spark.conf.set("spark.sql.legacy.setCommandRejectsSparkCoreConfs", "false")  # this is introduced by spark 3.x and it's true by default
        spark.conf.set("spark.sql.adaptive.fetchShuffleBlocksInBatch", "false")  # to resolve corrupted stream issue during shuffle fetch
        spark.conf.set("spark.sql.adaptive.enabled", "false")  # Addressing Memory Leakage Issue. Note: this will be removed in Glue 4.0
        spark.conf.set("spark.sql.sources.bucketing.autoBucketedScan.enabled", "false")  # Addressing Memory Leakage Issue
        spark.conf.set("spark.sql.session.timeZone", DEFAULT_TIMEZONE)

        for key in self.jobSettings:
            spark.conf.set(key, self.jobSettings[key])
        java_import(spark._jvm, config.SNOWFLAKE_SOURCE_NAME)

        glue_version = config.Config().get_glue_version(self.folder, self.config_file)
        if glue_version == "4.0":
            self.log.info("Printing Spark configs.")
            spark_conf = spark.sparkContext.getConf().getAll()
            self.log.info(spark_conf)

        return spark

    def get_SQLContext(self):

        return SQLContext(self.spark.sparkContext,
                          self.spark)

    def execute_query(self,
                      query,
                      table=None):
        """
        Transformation library facilates to build the ETL transformation logic. We can register the Dataframe as a table and write the transformation queries against the table.

        API Call - glue.execute_query function will take three parameters:

        Parameters:
        query - It is a mandatory parameter. We can pass the transformation queries.
        table - It is an optional parameter. Most of us going to SQL kind of ETL pipe, to enable subsequent ETL transformation logic, we need to specify this parameter. If you are comfortable writing programming kind of ETL pipeline, then you can skip this parameter.

        SQL
        df_session_cnt_by_date = glue.execute_query(session,'select date,count(*) from tmp_user_activity group by date','session_cnt_by_date')

        API
        df_session_cnt_by_date = df_session_cnt_by_date.groupby('date')
        """

        df = self.sqlContext.sql(query)

        if table:
            df.createOrReplaceTempView(table)
        self.log.info("Dataframe is created for query ---" + query)
        return ParserAwareDataFrame.assimilate(df, sql_query=query, name=table)

    def __init__(self,
                 job,
                 folder,
                 config_file):
        """
        Initialization library facilitates to make a session with glue/spark, SqlContext, and Snowflake connection. To build glue ETL pipeline, this is a first and mandatory library call in ETL pipeline. Once we received the session, we will use it through the ETL pipeline to make a call to other libraries. Developers need not worry about underlined glue, spark, and snowflake setup.

        API Call - glue.init_session function will initialize Glue, Spark, SqlContext, and Snowflake connection and return the session object in form of a dictionary.  The job name is the mandatory parameter for this function. session object will be used to subsequently ETL pipeline.

        session=glue.init_session("Build Device Session")
        """

        self.folder = folder
        self.config_file = config_file
        self.job_config = config.Config().parse_configuration(self.folder, self.config_file)
        self.jobSettings = config.Config().get_job_setting(self.folder, self.config_file)
        self.ENV = config.get_environment()
        self.bucket = config.get_bucket()
        self.job_status = 0
        self.tags = {"owner": "Khandu Shinde", "group owner": "Karishma Dambe"}
        self.custom_tags = None
        self.log = Log(job, folder, self.tags, namespace=config.get_metrics_namespace_name())
        self.OPS = config.set_execution_type()
        # if JSSA is not enabled, sfOptions will hold default snowflake user/role;
        # if JSSA is enabled, sfOptions will hold JSSA user/role, defaults are in sfOptions_backward_compatibility
        # sfOptions_backward_compatibility will be removed once all jobs are onboarded to JSSA
        self.sfOptions, self.sfOptions_backward_compatibility = config.get_credentials(self.config_file, self.jobSettings)
        self.job = str(self.config_file).split(".")[0]
        self.airtable_reference_id = "DE"
        self.jobname = str(self.config_file).split(".")[0]
        self.sc = self.get_SparkContext()
        if self.jobSettings.get("spark.logLevel"):
            self.sc.setLogLevel(self.jobSettings.get("spark.logLevel"))
        self.glueContext = self.get_GlueContext()
        self.spark = self.get_GlueSession()
        self.sqlContext = self.get_SQLContext()
        self.job = Job(self.glueContext)
        self.parameters = config.Config().get_runtime_parameters(self.folder,
                                                                 self.config_file,
                                                                 self.get_GlueArguments(config.Config().get_job_parameters(self.folder, self.config_file)) if self.OPS != 'test' else config.Config().get_job_parameters(self.folder, self.config_file),
                                                                 self.spark)
        # Initialize backfill run and related attributes
        self.is_backfill_run = self.parameters.get("IS_DTS_BACKFILL_DAG", "False").lower() == "true"
        self.is_dqc_enabled = True
        if self.is_backfill_run:
            self._initialize_backfill_parameters()

        self.log.info(f"JOB CONFIG::: {self.parameters}")

        if 'tags' in self.parameters:
            self.tags = self.parameters["tags"]
            self.log.tags = self.tags
        if 'warehouse' in self.parameters and self.ENV not in config.INTERACTIVE_SUPPORTED_ENV_LIST:
            warehouse = self.parameters["warehouse"] if os.getenv('ENV') not in ["CI", "local", "docker", "de_sandbox", "nonprod"] else 'dev_wh'
            self.sfOptions['sfWarehouse'] = warehouse
            if self.sfOptions_backward_compatibility:
                self.sfOptions_backward_compatibility['sfWarehouse'] = warehouse

        if 'timezone' in self.parameters:
            self.sfOptions['sfTimezone'] = self.parameters["timezone"]

        if "role" in self.parameters and self.ENV not in config.INTERACTIVE_SUPPORTED_ENV_LIST:
            role = self.parameters["role"] if os.getenv("ENV") not in ["CI", "local", "docker", "de_sandbox"] else config.DEFAULT_SNOWFLAKE_ROLE
            if self.sfOptions_backward_compatibility:
                # jssa is enabled, so we only override backward compatible role, jssa role will remain SA__{job_name}__ROLE
                self.sfOptions_backward_compatibility["sfRole"] = role
            else:
                # jssa is not enabled, so we only override backward compatible role which is stored in sfOptions
                self.sfOptions["sfRole"] = role

        if 'logging_level' in self.parameters:
            self.log.set_logging_level(self.parameters["logging_level"])

        self.log.info("Job starting:1")
        self.log.put_metric("jobstartedstatus", 1)
        # !! DO NOT CHANGE the following log entry. It is needed in verbatim for downstream parsing. Tks
        self.log.info("Job is started executing....")
        self.log.info("Session has been initialized.")

    def _initialize_backfill_parameters(self):
        """
        Initialize backfill instance and related attributes
        :return: None
        """
        custom_tags = {
            "job_name": self.jobname,
            "job_type": "backfill",
        }
        self.log.put_metric(metric_name="glue.is_backfill_run", metric_value=1, custom_tags=custom_tags)
        self.is_dqc_enabled = self.parameters.get("IS_DQC_ALERTING_ENABLED", "True").lower() != "false"
        self.is_redirect_outputs = self.parameters.get("IS_REDIRECT_OUTPUTS", "False").lower() != "false"
        if self.is_redirect_outputs:
            self.redirection_tbl_suffix = self.parameters.get("REDIRECT_TABLE_SUFFIX", "_dts_backfill")
            self.output_redirection_map = defaultdict(str)

    def add_audit_attributes(self,
                             df,
                             attributes=None):
        col_list = []
        attr_cols = ""
        job_name = self.jobname

        if attributes:
            for k, v in attributes.items():
                col_list.append(("'" + v[0] + "'" if v[1] == 'N' else v[0]) + ' ' + k)
                if k == 'meta_feature_family_name':
                    self.custom_tags = {k: v[0]}
            attr_cols = ", ".join(col_list) + ", "

        df.registerTempTable("df")
        sql_query = f"""
            select *, {attr_cols} '{job_name}' as meta_job,
                current_timestamp() as meta_dw_created_at,
                current_timestamp() as meta_sink_event_timestamp
            from df
        """
        attributes_df = self.execute_query(sql_query, "all_user_logs")
        return attributes_df

    def add_timestamp(self, df):
        return df.withColumn('meta_dw_created_at', F.current_timestamp())

    def register_function(self, function_name, function):

        tmp_ref = udf(function, StringType())
        self.spark.udf.register(function_name, tmp_ref)

    def get_test_dataframe(self, test_data_file, file_format='csv', header=True, spark_connector_options=None):
        read_options = {} if spark_connector_options is None else spark_connector_options
        read_options.update({"header":header})
        df = self.spark.read.format(str(file_format).lower()).load(path=test_data_file, **read_options)

        return df

    def load_test_dataframe(self, df, test_data_file, file_format='csv', spark_connector_options=None):
        mode = "overwrite"
        if os.path.exists(test_data_file):
            if os.path.isfile(test_data_file):
                os.remove(test_data_file)
            else:
                shutil.rmtree(test_data_file)
        self.log.info(test_data_file)
        write_options = {} if spark_connector_options is None else spark_connector_options
        write_options.update({"header":True})
        df.write.mode(mode).save(path=test_data_file, format=str(file_format).lower(), **write_options)

    def get_dataframe_stats(self, table_name, timestamp=None, log_flag=True, max_time=None, min_time=None):

        if timestamp:
            df_stats = self.execute_query(
                """ select
                        '""" + table_name + """' dataframe,
                        count(*) count,
                        max(to_utc_timestamp(""" + timestamp + """,'UTC')) data_maxtime,
                        min(to_utc_timestamp(""" + timestamp + """,'UTC')) data_mintime,
                        current_timestamp job_runtime
                  from """ + table_name)
        else:
            df_stats = self.execute_query(
                """ select
                        '""" + table_name + """' dataframe,
                        count(*) count,
                        current_timestamp job_runtime
                  from """ + table_name)

        stats = df_stats.toJSON().collect()
        stats_dict = json.loads(stats[0])
        try:
            if max_time:
                data_maxtime = datetime.datetime.strptime(str(max_time), '%Y-%m-%dT%H:%M:%S.%f%z').timestamp()
            else:
                data_maxtime = datetime.datetime.strptime(str(stats_dict['data_maxtime']), '%Y-%m-%dT%H:%M:%S.%f%z').timestamp()
            if min_time:
                data_mintime = datetime.datetime.strptime(str(min_time), '%Y-%m-%dT%H:%M:%S.%f%z').timestamp()
            else:
                data_mintime = datetime.datetime.strptime(str(stats_dict['data_mintime']), '%Y-%m-%dT%H:%M:%S.%f%z').timestamp()
            job_runtime = datetime.datetime.strptime(str(stats_dict['job_runtime']), '%Y-%m-%dT%H:%M:%S.%f%z').timestamp()
            latency = job_runtime - data_maxtime
            if not min_time:
                max_lag = job_runtime - data_mintime
                self.log.put_metric("max_lag", max_lag)
                self.log.info(f"Max Lag:{max_lag}")
            if log_flag:
                if latency < 0:
                    self.log.info(f"Job Latency:{latency+25200}")

                    self.log.info(f"airtable_reference_id:{self.airtable_reference_id} status:SUCCESS code:1", latency+25200, self.custom_tags)
                else:
                    self.log.info(f"Job Latency:{latency}")
                    self.log.info(f"airtable_reference_id:{self.airtable_reference_id} status:SUCCESS code:1", latency, self.custom_tags)
                log_string_list = []
                for k in stats_dict:
                    log_string_list.append(f"{k}:{stats_dict[k]}")
                self.log.info(','.join(log_string_list))

        except Exception as e:
            self.log.info(f"Error parsing date: {e}")
            self.log.info(str(e))
        return stats_dict

    def get_streaming_dataframe_stats(self):
        df_stats = self.execute_query(
            """ select
                    max(to_utc_timestamp(window.end,'UTC')) data_maxtime
                from batch """)
        stats = df_stats.toJSON().collect()
        stats_dict = json.loads(stats[0])
        max_window_end = datetime.datetime.strptime(str(stats_dict['data_maxtime']), '%Y-%m-%dT%H:%M:%S.%f%z').replace(tzinfo=timezone.utc)
        return max_window_end

    def __del__(self):
        if self.job_status:
            if self.job_status == 0:
                self.log.info(f"airtable_reference_id:{self.airtable_reference_id} status:FAILED code:1")
            else:
                self.log.info(f"airtable_reference_id:{self.airtable_reference_id} status:SUCCESS code:1")


==========

import copy
import os
import json
import unicodedata
from doctest import master
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
from transformation.sdk import *
from transformation.sdk.aws_account_level_config import AWS_ACCOUNT_LEVEL_CONFIG
from transformation.sdk.common.log import Log
from transformation.sdk.common import init_job
from transformation.sdk.common.constants import DEFAULT_GLUE_VERSION, GLUE3_FOLDERS
import yaml
from pyspark.sql import DataFrame
import re

"""
These are the default parameters for AWS Glue configuration. These parameters are facilitating smooth code movement and
etl pipeline logic across environment
"""

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
BQL_LOG = "«SQL_LOG_TRACE»"
BQL_END = "«_status_mutatum_est_»"

if os.getenv("GLUE_ETL"):
    master_path = os.getenv("GLUE_ETL")
else:
    master_path = ""

etl_repo = "/transformation/"
test_case_repo = "/transformation/test/"
input_data_repo = "/transformation/test/sample_data/"
s3_pipeline_path = "etl/glue/pipelines/"
checkpoint_master_path = "/etl/glue/checkpoint/"
watermark_folder = "sdk/common"
data = "/data/"
INTERACTIVE_ROLE_ARN_SUBSTR = ":assumed-role/dts-interactive-"
INTERACTIVE_SUPPORTED_ENV_LIST = ["de_sandbox_interactive", "prod_interactive"]
INTERACTIVE_USER_SECRETS_PREFIX = "/dts/glue_interactive_dev_user/v1"
SNOWFLAKE_PROD_ENV_ALLOWED_LIST = [
    "prod",
    "prod_interactive",
]

NON_AWS_ENVS = ["CI", "local", "docker"]
DEFAULT_SNOWFLAKE_ROLE = "GLUE_ETL_ROLE"


def get_parameter(parameter: str) -> Any:
    """
    This API helps to retrieve secrets from AWS parameter store.
    :param parameter: AWS SSM parameter name
    :return: Return parameter value.
    """
    ssm = boto3.client("ssm", "us-east-1")
    parameter = ssm.get_parameter(Name=parameter, WithDecryption=True)
    return parameter["Parameter"]["Value"]


def get_secret(secret_name: str) -> str:
    """
    Fetches the secret value from the AWS Secret Manager stored at the provided secret_name key.
    :param secret_name: name of the secret to fetch from the AWS Secret Manager
    :return: the secret value associated with the provided secret_name
    """
    client = boto3.client(service_name="secretsmanager", region_name="us-east-1")
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response["SecretString"]
    return secret


def get_account() -> str:
    """
    This API helps to identify AWS environment at run time.
    :return: Return AWS account number.
    """
    return boto3.client("sts").get_caller_identity().get("Account")


def get_role_arn() -> str:
    """
    This API helps to identify the IAM role of the current AWS session.
    :return: a string type denoting the IAM Role ARN of the current session.
    """
    role_arn = boto3.client("sts").get_caller_identity().get("Arn")
    return role_arn


def is_interactive_session() -> bool:
    """
    Check whether the current env is for interactive session
    :return: return True if ENV environ is set to the value same as local var INTERACTIVE_ENV_NAME
    """
    role_arn = get_role_arn()
    if INTERACTIVE_ROLE_ARN_SUBSTR in role_arn:
        return True
    return False


def set_master_path() -> str:
    """
    This API helps to identify the master path for the given environment.
    :return: (str)
    """
    if os.getenv("GLUE_ETL"):
        master_path = os.getenv("GLUE_ETL")
    else:
        master_path = ""
    return master_path


def set_execution_type() -> str:

    return init_job.set_execution_type()


def get_value_from_env_account_level_config(
    env: str, nested_key_name: str
) -> Dict[str, Any]:
    """
    This function refers to AWS_ACCOUNT_LEVEL_CONFIG, and return the values for the nested key under
    the provided env dictionary, raises Exception otherwise
    :param env: the name of AWS env: all the supported AWS env for DTS
    :param nested_key_name: the name of the nested key as founder env in the config file
    :return: returns the string type value under account_level_config_dict[env][nested_key_name]
    """
    if (
        env not in AWS_ACCOUNT_LEVEL_CONFIG
        or nested_key_name not in AWS_ACCOUNT_LEVEL_CONFIG[env]
    ):
        raise Exception(
            f"Nested value for key: {nested_key_name} not found for env: {env}"
        )

    return AWS_ACCOUNT_LEVEL_CONFIG[env][nested_key_name]


def get_watermark_filename(env: str) -> str:
    if os.getenv("ENV") in NON_AWS_ENVS:
        return "operation_db_sql.yaml"

    watermark_filename = get_value_from_env_account_level_config(
        env=env, nested_key_name="operation_db_file_name"
    )
    return watermark_filename


def get_environment() -> str:
    if os.getenv("ENV") in NON_AWS_ENVS:
        return os.getenv("ENV")

    is_interactive = is_interactive_session()
    current_aws_account_id = get_account()
    for env, env_config in AWS_ACCOUNT_LEVEL_CONFIG.items():
        if env_config["account_id"] == current_aws_account_id:
            if not is_interactive:
                return env
            else:
                if "_interactive" in env:
                    return env

    raise Exception(
        f"Environment: env not defined in config json for account id: {current_aws_account_id}"
    )


def get_bucket(use_job_arg_override: bool = True) -> str:
    """
    Return the name of the pipeline bucket configured for the environment

    :param force_env_bucket: ignore bucket override CLI argument
    """
    if use_job_arg_override:
        override = init_job.get_pipeline_bucket_override()
        if override is not None:
            return override

    if os.getenv("ENV") in NON_AWS_ENVS:
        return "test"

    env = get_environment()
    return AWS_ACCOUNT_LEVEL_CONFIG[env]["pipeline_bucket_name"]


def get_snowflake_url(env: Optional[str] = None) -> str:
    """
    Get the FQDN url of the Chime's snowflake account
    :param env: string type defining the current environment
    :return: a string type url corresponding to the Chime's account
    """
    url_suffix = "snowflakecomputing.com"
    if env is None:
        env = get_environment()
    if env in SNOWFLAKE_PROD_ENV_ALLOWED_LIST:
        account = "chime"
    else:
        account = "chime_dev"
    return f"{account}.{url_suffix}"


def get_credentials(config_file, jobSettings):
    """
    This API returns all the secrets at run time based on operation parameter. In case unit testing, it would not return
    any results. For execution time, this API will return the secrets based on environment.
    :param session: Job session.
    :return: all the secrets at run time based on operation parameter.
    """

    timezone = get_timezone(jobSettings)
    print("===========timezone for the job runtime is==========")
    print(timezone)

    sfOptions = {}
    # stores backward compatible snowflake credentials: default ETL user, role and password
    sfOptions_backward_compatibility = {}
    env = get_environment()
    if set_execution_type() != "test":
        if env in INTERACTIVE_SUPPORTED_ENV_LIST:
            user_email = get_dts_interactive_user_email()
            sfOptions["sfURL"] = get_snowflake_url(env=env)
            sfOptions["sfUser"] = user_email
            sfOptions["sfAuthenticator"] = "oauth"
            sfOptions["sfToken"] = get_secret(
                f"{INTERACTIVE_USER_SECRETS_PREFIX}/{user_email}/token"
            )
            sfOptions["sfDatabase"] = get_secret(
                f"{INTERACTIVE_USER_SECRETS_PREFIX}/{user_email}/database"
            )
            sfOptions["sfSchema"] = get_secret(
                f"{INTERACTIVE_USER_SECRETS_PREFIX}/{user_email}/schema"
            )
            sfOptions["sfWarehouse"] = get_secret(
                f"{INTERACTIVE_USER_SECRETS_PREFIX}/{user_email}/warehouse"
            )
            sfOptions["sfRole"] = get_secret(
                f"{INTERACTIVE_USER_SECRETS_PREFIX}/{user_email}/role"
            )
            sfOptions["sfTimezone"] = timezone
        elif env not in ["local", "docker"]:
            sfOptions["sfURL"] = get_parameter("/data-transformation/glue/account")
            sfOptions["sfDatabase"] = get_parameter(
                "/data-transformation/glue/database"
            )
            sfOptions["sfSchema"] = get_parameter(
                "/data-transformation/glue/database/schema"
            )
            sfOptions["sfWarehouse"] = get_parameter(
                "/data-transformation/glue/warehouse"
            )
            sfOptions["sfTimezone"] = timezone
            try:
                # JSSA access credentials
                job_name = config_file.rpartition('.')[0]
                sfOptions["pem_private_key"] = get_secret(
                    f"/dts/glue_etl_snowflake_setup/v1/{job_name}/private_key")
                sfOptions["sfUser"] = f"{job_name.upper()}_USER"
                sfOptions["sfRole"] = f"SA__{job_name.upper()}__ROLE"
                sfOptions_backward_compatibility = copy.deepcopy(sfOptions)
                sfOptions_backward_compatibility["sfUser"] = get_parameter(
                    "/data-transformation/glue/database/user"
                )
                # backward compatibility credentials in case JSSA fails
                sfOptions_backward_compatibility["sfPassword"] = get_parameter(
                    "/data-transformation/glue/database/user/password"
                )
                sfOptions_backward_compatibility["sfRole"] = get_parameter(
                    "/data-transformation/glue/database/user/role"
                )
                sfOptions_backward_compatibility.pop("pem_private_key", None)
            except ClientError as e:
                # use default backward compatibility credentials when not onboarded to JSSA
                print(f"Retrieving JSSA credentials failed with {e}")
                sfOptions["sfUser"] = get_parameter(
                    "/data-transformation/glue/database/user"
                )
                sfOptions["sfPassword"] = get_parameter(
                    "/data-transformation/glue/database/user/password"
                )
                sfOptions["sfRole"] = get_parameter(
                    "/data-transformation/glue/database/user/role"
                )
            try:
                fse_obs_db = get_parameter(
                    "/data-transformation/glue/feature_store/obs_db"
                )
                fse_obs_db_user = get_parameter(
                    "/data-transformation/glue/feature_store/obs_db/glue_user_secret_arn"
                )
                sfOptions["sfFSEObsDB"] = fse_obs_db
                sfOptions["sfFSEObsDBUser"] = fse_obs_db_user
                if sfOptions_backward_compatibility:
                    sfOptions_backward_compatibility["sfFSEObsDB"] = fse_obs_db
                    sfOptions_backward_compatibility["sfFSEObsDBUser"] = fse_obs_db_user
            except ClientError:
                sfOptions["sfFSEObsDB"] = ""
                sfOptions["sfFSEObsDBUser"] = ""
                if sfOptions_backward_compatibility:
                    sfOptions_backward_compatibility["sfFSEObsDB"] = ""
                    sfOptions_backward_compatibility["sfFSEObsDBUser"] = ""
        else:
            sfOptions["sfURL"] = os.getenv("SNOWSQL_DT_ACCOUNT")
            sfOptions["sfUser"] = os.getenv("SNOWSQL_DT_USER")
            sfOptions["sfPassword"] = os.getenv("SNOWSQL_DT_PWD")
            sfOptions["sfDatabase"] = os.getenv("SNOWSQL_DATABASE")
            sfOptions["sfSchema"] = os.getenv("SNOWSQL_SCHEMA")
            sfOptions["sfWarehouse"] = os.getenv("SNOWSQL_DT_WAREHOUSE")
            sfOptions["sfRole"] = os.getenv("SNOWSQL_ROLE")
            sfOptions["sfTimezone"] = timezone

    return sfOptions, sfOptions_backward_compatibility


def get_timezone(jobSettings):
    if jobSettings and "spark.sql.session.timeZone" in jobSettings:
        return jobSettings["spark.sql.session.timeZone"]
    return DEFAULT_TIMEZONE


def get_amplitude_secrets() -> Dict[str, str]:
    sfOptions = {}
    if set_execution_type() != "test":
        if get_environment() not in ["local", "docker"]:
            sfOptions["amplitudeHawkerKey"] = get_parameter(
                "/data-transformation/glue/amplitude/hawker/key"
            )
        else:
            sfOptions["amplitudeHawkerKey"] = os.getenv("AMPLITUDE_HAWKER_KEY")
    return sfOptions


def get_test_file(
    df_name: str, job_config: Dict[str, Any], folder: str, file: str
) -> str:
    return get_test_file_by_key(df_name, job_config, "test_data_file", folder, file)


def get_test_file_by_key(
    df_name: str,
    job_config: Dict[str, Any],
    df_file_path_key: str,
    folder: str,
    file: str,
) -> str:

    master_path = Config().get_master_path()
    test_data_file = (
        job_config[df_name][df_file_path_key] if df_file_path_key in job_config[df_name] else None
    )
    file_format = job_config[df_name]["file_format"] if "file_format" in job_config[df_name] else "csv"
    folder_path = os.path.join(master_path, input_data_repo[1:], folder)

    # feature-store connector uses this "test_data_file" field for different tests.
    if master_path and test_data_file and df_file_path_key != 'test_data_file':
        return f"{folder_path}/{test_data_file}"

    params = {"job_config": job_config, "df_name": df_name, "folder_path": folder_path, "yaml_file": file}
    if master_path and test_data_file and df_file_path_key == 'test_data_file':
        params["test_data_file"] = test_data_file
        return get_test_data_file_path(params)

    if not test_data_file and df_name:
        params["test_data_file"] = f"{df_name}.{file_format}"
        return get_test_data_file_path(params)


def get_test_data_file_path(params):
    job_config = params["job_config"]
    df_name = params["df_name"]
    folder_path = params["folder_path"]
    yaml_file = params["yaml_file"]
    test_data_file = params["test_data_file"]
    job_name = yaml_file.split(".")[0]

    use_original_file_name = False
    if job_config[df_name].get('type') == 'input' and "sample_data" not in job_config.get(f'test_{df_name}', {}) \
            and job_config[df_name].get('test_data_file', None) is not None:
        # DTS-2789: When a sample data file was manually uploaded, use the original file name.
        use_original_file_name = True

    if job_config[df_name].get('type') == 'output' and job_config[df_name].get('test_data_file', None) is not None \
            and '/snowflake_etl/' in folder_path:
        use_original_file_name = True

    if use_original_file_name:
        return f"{folder_path}/{test_data_file}"

    return f"{folder_path}/{job_name}__{test_data_file}"


def get_value(df: DataFrame, single_value: bool = True):

    result = df.toJSON().collect()
    for record in result:
        data = json.loads(record)
        if single_value:
            for key in data:
                return data[key]
        else:
            return data


def get_dataframe_schema(dataframe: DataFrame) -> Dict[str, str]:
    schema = {}
    for i in dataframe.dtypes:
        data_type = i[1].split("(")[0]
        schema[i[0].lower()] = data_type
    return schema


def get_feature_columns(
    dataframe: DataFrame, return_type: str = "str"
) -> Union[List[str], str]:
    schema = get_dataframe_schema(dataframe)
    features = []
    for key in schema:
        if key not in [
            "meta_feature_family_name",
            "meta_dw_created_at",
            "meta_sink_event_timestamp",
            "meta_job",
            "expiration_ts",
        ]:
            features.append(key)

    if return_type != "str":
        return features

    return ",".join(features)


def get_dataframe_compare_condition(
    source_dataframe,
    target_dataframe,
    grain_cols: List[str],
    exclude_delta_column_list: List[str],
) -> str:
    source_schema = get_dataframe_schema(source_dataframe)
    target_schema = get_dataframe_schema(target_dataframe)
    target_schema.pop("expiration_ts", None)
    source_schema.pop("expiration_ts", None)
    if len(get_feature_columns(source_dataframe, "columns")) == len(
        get_feature_columns(target_dataframe, "columns")
    ):
        conditions = " 1=1 "
        for col in grain_cols:
            conditions += f"  and s.{col} == t.{col}  " ""
        conditions += " where "
        for col in target_schema:
            if (
                col in source_schema
                and col not in grain_cols
                and col
                not in [
                    "meta_feature_family_name",
                    "meta_dw_created_at",
                    "meta_sink_event_timestamp",
                    "meta_job",
                ]
                and col not in exclude_delta_column_list
            ):
                if source_schema[col] in ["string", "text"]:
                    conditions += (
                        f"""coalesce( s.{col},'') != coalesce(t.{col},'') or """
                    )
                elif source_schema[col] in [
                    "decimal",
                    "integer",
                    "float",
                    "long",
                    "bigint",
                    "double",
                ]:
                    conditions += f"""coalesce( s.{col},0) != coalesce(t.{col},0) or """
                elif source_schema[col] in ["date"] and target_schema[col] in ["date"]:
                    conditions += f"""coalesce( s.{col},current_date) != coalesce(t.{col},current_date) or """
                elif source_schema[col] in ["time", "timestamp", "datetime"]:
                    conditions += f"""coalesce( s.{col},current_timestamp) != coalesce(t.{col},current_timestamp) or """
                elif source_schema[col] in ["boolean"]:
                    conditions += (
                        f"""coalesce( s.{col},'true') != coalesce(t.{col},'true') or """
                    )
                else:
                    conditions += f"""coalesce( s.{col},0) != coalesce(t.{col},0) or """

        conditions = conditions.rstrip("or ")
        return conditions
    return ""


def get_operation_db_and_stage_glue_schema(env: str) -> str:
    """
    This function reads the operation_db_name value for the provided env and returns the appropriate stage glue
    schema for the env
    :param env: the name of AWS env: all the supported AWS env for DTS
    :return: a string type value in the format of <env_specific_operation_db_name>.stage_glue
    """
    if os.getenv("ENV") in NON_AWS_ENVS:
        operation_db_name = "operation_db"
    else:
        operation_db_name = get_value_from_env_account_level_config(
            env=env, nested_key_name="operation_db_name"
        )
    return f"{operation_db_name}.stage_glue"


def get_operation_db_and_transformation_schema(env: str) -> str:
    """
    This function reads the operation_db_name value for the provided env and returns the appropriate transformation
    schema name for the env
    :param env: the name of AWS env: all the supported AWS env for DTS
    :return: a string type value in the format of <env_specific_operation_db_name>.transformation
    """
    if os.getenv("ENV") in NON_AWS_ENVS:
        operation_db_name = "operation_db"
    else:
        operation_db_name = get_value_from_env_account_level_config(
            env=env, nested_key_name="operation_db_name"
        )
    return f"{operation_db_name}.transformation"


def debug(df, count: int = 20, truncate_flag: bool = False):
    df.show(count, truncate_flag)
    df.explain()
    df.printSchema()


def get_metrics_namespace_name() -> str:
    """
    Get the cloudwatch namespace name where the metrics should be reported
    :return: the name of the cloudwatch namespace, defaults to DTS
    """
    default_namespace_name = "DTS"
    env = get_environment()
    if env in INTERACTIVE_SUPPORTED_ENV_LIST:
        return f"{default_namespace_name}_INTERACTIVE"
    else:
        return default_namespace_name


def get_dts_interactive_user_email() -> str:
    """
    Get the cloudwatch namespace name where the metrics should be reported
    :return: the name of the cloudwatch namespace, defaults to DTS
    """
    env = get_environment()
    if env not in INTERACTIVE_SUPPORTED_ENV_LIST:
        raise Exception("Current environment is not supported for interactive session")
    role_arn = get_role_arn()
    user_email = role_arn.rpartition(INTERACTIVE_ROLE_ARN_SUBSTR)[2].rpartition("/")[0]
    return user_email


class Config:
    def __init__(self, session=None, use_job_arg_override: bool = True):
        """
        :param session: glue session unused.
        :param force_env_bucket: force using the declared bucket name for this environment
        """
        self.session = session
        self.OPS = set_execution_type()
        self.ENV = get_environment()
        self.master_path = set_master_path()
        self.bucket = get_bucket(use_job_arg_override=use_job_arg_override)
        self.log = Log(namespace=get_metrics_namespace_name())

    def get_master_path(self) -> str:
        """
        This API returns the master path for the given environment.
        :return: (str)
        """
        return self.master_path

    def get_file_location(self, folder: str, filename: str) -> str:
        """
        This API returns fully qualified the job yaml configuration or unit testing file path.
        :param folder: Folder name.
        :param filename: File name.
        :return:
        """
        if self.ENV not in NON_AWS_ENVS:
            s3_client = boto3.client("s3")
            bucket = self.bucket
            s3_prefix = s3_pipeline_path
            if is_interactive_session():
                if "operation_db_" in filename:
                    core_env_name = self.ENV.rpartition("_interactive")[0]
                    bucket = get_value_from_env_account_level_config(
                        env=core_env_name, nested_key_name="pipeline_bucket_name"
                    )
                else:
                    user_email = get_dts_interactive_user_email()
                    s3_prefix = os.path.join(user_email, s3_pipeline_path)
            self.log.info(f"s3: path={bucket}, filename={filename}, folder={folder}")
            response = s3_client.get_object(
                Bucket=bucket, Key=s3_prefix + folder + "/" + filename
            )
            return response["Body"]
        else:
            return self.master_path + etl_repo + folder + "/" + filename

    def parse_configuration(self, folder: str, filename: str) -> Dict[str, Any]:
        """
        This API parses job yaml configuration.
        :param folder: Job configuration folder name
        :param filename: Job configuration filename name
        :return:
        It will return YAML configuration dict
        """
        config = {}
        if self.ENV not in NON_AWS_ENVS:
            config = yaml.safe_load(self.get_file_location(folder, filename))
        else:
            with open(self.get_file_location(folder, filename)) as file:
                config = yaml.load(file, Loader=yaml.FullLoader)

        if config is not None and "include" in config:
            parent_config = {}
            parent_yaml_list = list(config["include"])
            for parent in parent_yaml_list:
                parent_split_list = str(parent).split(".")
                parent_key = parent_split_list[0]
                parent_config[parent_key] = self.parse_configuration(folder, parent)
            for key in config:
                if "query" in config[key] and str(config[key]["query"]).startswith("!"):
                    query_ref = str(config[key]["query"])[1:]
                    query_ref_list = query_ref.split(".")

                    # loop through the yaml to find the target query
                    # ex: query: "!merge_queries.merge_query.query"
                    resolved_query = parent_config

                    for sub_key in query_ref_list:
                        if sub_key not in resolved_query:
                            raise Exception(
                                "Failed to resolve query ref: "
                                + sub_key
                                + " when looking for !"
                                + query_ref
                            )
                        resolved_query = resolved_query[sub_key]

                    config[key]["query"] = resolved_query
        return config

    def get_configuration(
        self,
        folder: str,
        filename: str,
        df_name: str,
        query: Optional[str] = None,
        df: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        It will setup the dataframe execution setting for the given ETL pipeline.
        :param folder: Job configuration folder name
        :param filename: Job configuration filename name
        :param df_name: Dataframe name from the ETL pipeline
        :param query: Data preparation or transformation query
        :param df: default dataframe name in case of df_name is missing
        :return:
            It will return all the parameters in proper format to execute the dataframe.
        """
        parameters = {}
        config = self.parse_configuration(folder, filename)
        folder_path = os.path.join(self.master_path, input_data_repo[1:], folder)
        for key in config[df_name]:
            if key not in ("watermark", "type"):
                if (
                    config[df_name]["type"] in ["output", "input"]
                    and key == "test_data_file"
                    and self.ENV in NON_AWS_ENVS
                ):
                    params = {
                        "job_config": config,
                        "df_name": df_name,
                        "folder_path": folder_path,
                        "yaml_file": filename,
                        "test_data_file": config[df_name][key]
                    }
                    parameters[key] = get_test_data_file_path(params)
                else:
                    if key == "grain_cols":
                        grain_cols = []
                        for col in config[df_name][key]:
                            grain_cols.append(col)
                        parameters[key] = grain_cols
                    else:
                        parameters[key] = config[df_name][key]

        if (
            config[df_name]["type"] in ["output", "input"]
            and self.ENV in NON_AWS_ENVS
            and config[df_name].get("test_data_file", None) is None
        ):
            params = {
                "job_config": config,
                "df_name": df_name,
                "folder_path": folder_path,
                "yaml_file": filename,
                "test_data_file": df_name + "." + config[df_name].get("file_format", "csv")
            }
            parameters["test_data_file"] = get_test_data_file_path(params)

        if query is not None and config[df_name]["type"] == "input":
            parameters["query"] = query

        if df_name is not None and config[df_name]["type"] == "input":
            parameters["register_table"] = df_name
        else:
            parameters["df"] = df
        return parameters

    def get_test_configuration(
        self, folder: str, filename: str, df_name: str
    ) -> Dict[str, Any]:
        """
        It will setup the dataframe unit testing setting for the given ETL pipeline.
        :param folder: Job configuration folder name
        :param filename: Job configuration filename name
        :param df_name: Dataframe name from the ETL pipeline
        :return:
            It will return all the parameters in proper format to test the dataframe.
        """
        parameters = {"folder": folder}
        config = self.parse_configuration(folder, filename)

        folder_path = os.path.join(self.master_path, input_data_repo[1:], folder)
        default_name = df_name + "." + config[df_name].get("file_format", "csv")
        params = {
            "job_config": config,
            "df_name": df_name,
            "folder_path": folder_path,
            "yaml_file": filename,
            "test_data_file": config[df_name].get("test_data_file", default_name)
        }
        parameters["test_data_file"] = get_test_data_file_path(params)
        parameters["register_table"] = df_name

        if "file_format" not in config[df_name]:
            parameters["file_format"] = "csv"
        else:
            parameters["file_format"] = config[df_name]["file_format"]
        return parameters

    def get_dataframe_configuration(
        self, folder: str, filename: str, df_name: str, df: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        It will setup the dataframe unit testing setting for the given ETL pipeline.
        :param folder: Job configuration folder name
        :param filename: Job configuration filename name
        :param df_name: Dataframe name from the ETL pipeline
        :return:
            It will return all the parameters in proper format to test the dataframe.
        """
        parameters = {}
        if df:
            parameters["df"] = df
        config = self.parse_configuration(folder, filename)

        for key in config[df_name]:
            if key != "type":
                parameters[key] = config[df_name][key]
        if self.master_path:
            folder_path = os.path.join(self.master_path, input_data_repo[1:], folder)
            params = {
                "job_config": config,
                "df_name": df_name,
                "folder_path": folder_path,
                "yaml_file": filename,
                "test_data_file": config[df_name]["test_data_file"]
            }
            parameters["test_data_file"] = get_test_data_file_path(params)
        return parameters

    def get_db_configuration(
        self, folder: str, filename: str, df_name: str, df: Optional[Any] = None
    ) -> Dict[str, Any]:
        """
        It will setup the dataframe unit testing setting for the given ETL pipeline.
        :param folder: Job configuration folder name
        :param filename: Job configuration filename name
        :param df_name: Dataframe name from the ETL pipeline
        :return:
            It will return all the parameters in proper format to test the dataframe.
        """
        parameters = {}
        if df:
            parameters["df"] = df
        config = self.parse_configuration(folder, filename)

        for key in config[df_name]:
            if key != "type":
                parameters[key] = config[df_name][key]

        return parameters

    @staticmethod
    def clean_tags(tags: Dict[str, Any]) -> Dict[str, Any]:
        """
        Cleans any non ascii characters from job tags and replaces non-word characters with underscores.
        :param tags: Dictionary of job tags
        :return: Cleaned dictionary of job tags
        """
        cleaned_tags = {}
        for key, value in tags.items():
            if isinstance(value, str):
                ascii_value = unicodedata.normalize("NFKD", value) \
                    .encode("ascii", "ignore").decode("ascii")
                cleaned_value = re.sub(r'[^A-Za-z0-9_.\-/ ]', '_', ascii_value)
                cleaned_tags[key] = cleaned_value
            else:
                cleaned_tags[key] = value
        return cleaned_tags

    def get_watermark_configuration(
        self, folder: str, filename: str, df_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        It will get watermark details
        :param folder: Job configuration folder name
        :param filename: Job configuration filename name
        :param df_name: Dataframe name from the ETL pipeline
        :return:
            Will return dictionary that has watermark details
        """
        config = self.parse_configuration(folder, filename)
        watermark = (
            config[df_name]["watermark"] if config[df_name].get("watermark") else None
        )
        return watermark

    def get_job_parameters(self, folder: str, config_file: str) -> Dict[str, Any]:
        parameter_config = self.parse_configuration(folder, PARAMETER_CONFIG_FILE)
        job_config = self.parse_configuration(folder, config_file)
        default_config = (
            parameter_config[DEFAULT_SECTION]
            if parameter_config is not None and DEFAULT_SECTION in parameter_config
            else {}
        )
        job_tags = (
            job_config.get("job", {}).get("tags", {})
        )
        if job_tags:
            default_config["tags"] = {**default_config.get("tags", {}), **job_tags}
            default_config["tags"] = self.clean_tags(default_config["tags"])
        job_parameter_config = (
            job_config[PARAMETER_SECTION] if PARAMETER_SECTION in job_config else {}
        )
        final_config = {**default_config, **job_parameter_config}
        return final_config

    def get_job_setting(self, folder: str, config_file: str) -> Dict[str, Any]:
        config = self.parse_configuration(folder, config_file)
        if "job" in config and "setting" in config["job"]:
            return config["job"]["setting"]

        return {}

    def is_spark_case_sensitive(self, folder: str, config_file: str) -> bool:
        # DTS-2753: Make DTS be case-sensitive if the spark.sql.caseSensitive flag was set to true.
        job_setting = self.get_job_setting(folder, config_file)
        return job_setting.get('spark.sql.caseSensitive', False)

    def _execute_query_with_backward_compatibility(self, spark, query, sfOptions, sfOptions_backward_compatibility, is_backward_compatible=False):
        options = sfOptions if not is_backward_compatible else sfOptions_backward_compatibility
        try:
            # Attempt to execute the query
            return (
                spark.read.format("net.snowflake.spark.snowflake")
                .options(**options)
                .option("query", query)
                .load()
            )
        except Exception as e:
            # If primary execution fails, attempt fallback if available and not already attempted
            if sfOptions_backward_compatibility and not is_backward_compatible:
                self.log.error(
                    f"Snowflake query execution failed under JSSA with {e}, now trying to execute "
                    f"the same query with default Snowflake user and role"
                )
                return self._execute_query_with_backward_compatibility(spark, query,
                                                                       sfOptions, sfOptions_backward_compatibility,
                                                                       is_backward_compatible=True)
            else:
                # Raise the exception if fallback is unavailable or also fails
                raise e

    def get_runtime_parameters(
        self,
        folder: str,
        config_file: str,
        static_parameters: Dict[str, Any],
        spark: Optional[SparkSession] = None,
    ) -> Dict[str, Any]:
        job_config = self.parse_configuration(folder, config_file)
        runtime_parameters = {}
        runtime_parameter_list = (
            job_config["runtime_parameters"]
            if "runtime_parameters" in job_config
            else []
        )
        job_settings = (
            job_config["job"]["setting"]
            if "job" in job_config and "setting" in job_config["job"]
            else {}
        )
        for runtime_parameter in runtime_parameter_list:
            query = str(runtime_parameter["query"])
            final_query = query.format(**static_parameters)
            if self.OPS == "test":
                print("GETTING DEFAULT VALUES FOR TESTING")
                default_map = runtime_parameter["default"]
                print(default_map)
                runtime_parameters.update(default_map)
            else:
                sfOptions, sfOptions_backward_compatibility = get_credentials(config_file, job_settings)
                if sfOptions_backward_compatibility:
                    if (
                        "warehouse" in static_parameters and self.ENV not in INTERACTIVE_SUPPORTED_ENV_LIST
                        and os.getenv('ENV') not in ["CI", "local", "docker", "de_sandbox", "nonprod"]
                    ):
                        sfOptions["sfWarehouse"] = static_parameters["warehouse"]
                        print(f"warehouse is overridden by parameters to {static_parameters['warehouse']} for JSSA")
                    if (
                        "snowflake" in job_config["job"] and "warehouse" in job_config["job"]["snowflake"]
                        and os.getenv('ENV') not in ["CI", "local", "docker", "de_sandbox", 'nonprod']
                    ):
                        sfOptions["sfWarehouse"] = job_config["job"]["snowflake"]["warehouse"]
                        print(f"warehouse is overridden by job config to {static_parameters['warehouse']} for JSSA")
                df = self._execute_query_with_backward_compatibility(spark, final_query, sfOptions,
                                                                     sfOptions_backward_compatibility)
                for key, value in (
                    df.rdd.map(lambda row: row.asDict()).collect()[0].items()
                ):
                    runtime_parameters.update({str(key): str(value)})
        return {**static_parameters, **runtime_parameters}

    def get_glue_version(self, folder: str, filename: str) -> str:
        """
        This method is to get the Glue version defined in its yaml file. Glue version was not specified, it will return
        the default Glue version, which is 3.0 currently.

        Parameters:
        folder: Job configuration folder name
        filename: Job configuration filename name

        Returns:
        A Glue version as string
        """
        glue_version = "4.0" if folder not in GLUE3_FOLDERS else DEFAULT_GLUE_VERSION
        config = self.parse_configuration(folder, filename)
        if "job" in config and "glue_version" in config["job"]:
            return str(config["job"]["glue_version"])
        return glue_version
