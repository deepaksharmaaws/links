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



