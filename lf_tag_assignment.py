import json
import boto3
import logging
from typing import Dict, List, Optional
from typing import Dict, Set
import urllib.parse
import yaml
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""
AWS Lambda Function: LF Tag Assignment

Purpose:
    Manages Lake Formation tags for databases, tables, and columns based on YAML configuration files in S3.

Functionality:
    1. Configuration Processing:
        - Triggered by S3 events when YAML files are uploaded
        - Loads and validates YAML configuration for database, table, and column tags
        - Supports hierarchical tag inheritance (database -> table -> column)
        - Supports optional tables and columns configuration

    2. Tag Management:
        - Retrieves existing Lake Formation tags and their allowed values
        - Validates tag keys and values against allowed Lake Formation tags
        - Validate Database , Table and Column exist
        - Performs four types of tag operations:
            * Addition of new tags
            * Updates to existing tag values
            * Removal of tags
            * Tracks unchanged tags

    3. Change Tracking:
        - Maintains detailed record of all tag changes
        - Provides statistics on modifications

Input YAML Examples:
    1. Database tags only:
    database:
      name: growthdb
      tags:
        lf_layer: silver

    2. Database and table tags (no columns):
    database:
      name: growthdb
      tags:
        lf_layer: silver
      tables:
        - name: customer
          tags:
            lf_domain: risk

    3. Complete structure:
    database:
      name: growthdb
      tags:
        lf_layer: silver
      tables:
        - name: customer
          tags:
            lf_domain: risk
          columns:
            - name: customer_id
              tags:
                lf_data_sensitivity: public
"""

class LFTagConfigValidator:
    @staticmethod
    def validate_tags(tags: Optional[Dict[str, str]]) -> None:
        """Validate tag structure"""
        # If tags is None or empty, treat it as an empty dict
        if tags is None:
            return

        if tags is not None:
            if not isinstance(tags, dict):
                raise ValueError("Tags must be a dictionary")
            for key, value in tags.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    raise ValueError("Tag keys and values must be strings")

    @staticmethod
    def validate_column(column: Dict) -> None:
        """Validate column configuration"""
        if not isinstance(column, dict):
            raise ValueError("Column must be a dictionary")
        if 'name' not in column or not isinstance(column['name'], str):
            raise ValueError("Column must have a 'name' field of type string")
        LFTagConfigValidator.validate_tags(column.get('tags'))

    @staticmethod
    def validate_config(config: Dict) -> None:
        """Validate entire configuration structure"""
        if not isinstance(config, dict):
            raise ValueError("Config must be a dictionary")

        if 'database' not in config:
            raise ValueError("Database configuration must be specified")

        database = config['database']
        if not isinstance(database, dict):
            raise ValueError("Database must be a dictionary")
        if 'name' not in database or not isinstance(database['name'], str):
            raise ValueError("Database must have a 'name' field of type string")
        LFTagConfigValidator.validate_tags(database.get('tags'))

        # Make tables optional
        if 'tables' in database:
            if not isinstance(database['tables'], list):
                raise ValueError("If present, 'tables' must be a list")

            for table in database['tables']:
                if not isinstance(table, dict):
                    raise ValueError("Each table must be a dictionary")
                if 'name' not in table or not isinstance(table['name'], str):
                    raise ValueError("Each table must have a 'name' field of type string")
                LFTagConfigValidator.validate_tags(table.get('tags'))

                # Make columns optional
                if 'columns' in table:
                    if not isinstance(table['columns'], list):
                        raise ValueError("If present, 'columns' must be a list")
                    column_names = set()
                    for column in table['columns']:
                        LFTagConfigValidator.validate_column(column)
                        if column['name'] in column_names:
                            raise ValueError(f"Duplicate column name: {column['name']}")
                        column_names.add(column['name'])


class TagChangeTracker:
    def __init__(self):
        self.changes = {
            'database_changes': {},
            'table_changes': {},
            'column_changes': {}
        }
        self.has_changes = False

    def track_changes(self, resource_type: str, resource_name: str, tag_changes: Dict):
        """Track all types of changes for a resource"""
        if tag_changes['add'] or tag_changes['update'] or tag_changes['remove']:
            self.has_changes = True

            if resource_name not in self.changes[f'{resource_type}_changes']:
                self.changes[f'{resource_type}_changes'][resource_name] = {
                    'added': {},
                    'updated': {},
                    'removed': {},
                    'unchanged': {}
                }

            resource_changes = self.changes[f'{resource_type}_changes'][resource_name]
            resource_changes['added'].update(tag_changes['add'])
            resource_changes['updated'].update(tag_changes['update'])
            resource_changes['removed'].update(tag_changes['remove'])
            resource_changes['unchanged'].update(tag_changes['unchanged'])

    def get_summary(self) -> Dict:
        """Return a formatted summary of all changes"""
        if not self.has_changes:
            return {
                'message': 'No tag changes were needed.'
            }

        summary = {
            'message': 'Successfully applied the following tag changes:',
            'changes': {}
        }

        # Only include resource types that have changes
        for resource_type, resources in self.changes.items():
            if resources:
                summary['changes'][resource_type] = resources

        # Add statistics
        stats = self._calculate_statistics()
        summary['statistics'] = stats

        return summary

    def _calculate_statistics(self) -> Dict:
        """Calculate statistics for the changes made"""
        stats = {
            'total_resources_modified': 0,
            'total_tags_added': 0,
            'total_tags_updated': 0,
            'total_tags_removed': 0,
            'total_tags_unchanged': 0
        }

        for resource_type in self.changes.values():
            for resource_changes in resource_type.values():
                if resource_changes['added'] or resource_changes['updated'] or resource_changes['removed']:
                    stats['total_resources_modified'] += 1
                    stats['total_tags_added'] += len(resource_changes['added'])
                    stats['total_tags_updated'] += len(resource_changes['updated'])
                    stats['total_tags_removed'] += len(resource_changes['removed'])
                    stats['total_tags_unchanged'] += len(resource_changes['unchanged'])

        return stats


class LFTagManager:
    def __init__(self, admin_role_arn: str):
        try:
            # Assume the Lake Formation admin role
            sts_client = boto3.client('sts')
            assumed_role = sts_client.assume_role(
                RoleArn=admin_role_arn,
                RoleSessionName='LFTagManagerSession'
            )

            # Create clients with assumed role credentials
            credentials = assumed_role['Credentials']
            self.lakeformation = boto3.client(
                'lakeformation',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )

            self.glue = boto3.client(
                'glue',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )

            self.s3 = boto3.client('s3')

            logger.info(f"Successfully assumed role: {admin_role_arn}")
            self.existing_lf_tags = self._get_existing_lf_tags()
            self.change_tracker = TagChangeTracker()
        except Exception as e:
            logger.error(f"Failed to initialize LFTagManager: {str(e)}")
            raise

    def _get_existing_lf_tags(self) -> Dict[str, Set[str]]:
        """Get all existing LF tags and their allowed values"""
        existing_tags = {}
        paginator = self.lakeformation.get_paginator('list_lf_tags')

        try:
            for page in paginator.paginate():
                for tag in page.get('LFTags', []):
                    existing_tags[tag['TagKey']] = set(tag['TagValues'])
            logger.info(f"Retrieved existing LF tags: {existing_tags}")
            return existing_tags
        except Exception as e:
            logger.error(f"Error retrieving existing LF tags: {str(e)}")
            raise

    def validate_tags(self, tags: Dict[str, str], resource_type: str, resource_name: str) -> bool:
        """Validate if tag keys and values exist"""
        validation_errors = []
        for key, value in tags.items():
            if key not in self.existing_lf_tags:
                validation_errors.append(f"Tag key '{key}' does not exist for {resource_type} '{resource_name}'")
            elif value not in self.existing_lf_tags[key]:
                validation_errors.append(
                    f"Tag value '{value}' is not allowed for key '{key}' on {resource_type} '{resource_name}'. "
                    f"Allowed values are: {sorted(list(self.existing_lf_tags[key]))}"
                )

        if validation_errors:
            for error in validation_errors:
                logger.error(error)
            return False
        return True

    def validate_all_tags(self, config: Dict) -> bool:
        """Validate all tags in the configuration"""
        validation_summary = []
        is_valid = True

        database = config['database']
        # Validate database tags if present
        if 'tags' in database:
            logger.info(f"Validating database tags for: {database['name']}")
            if not self.validate_tags(database.get('tags', {}), 'database', database['name']):
                is_valid = False
                validation_summary.append(f"Invalid database tags for {database['name']}")

        # Validate table tags if tables are present
        for table in database.get('tables', []):
            if 'tags' in table:
                logger.info(f"Validating table tags for: {table['name']}")
                if not self.validate_tags(table.get('tags', {}), 'table', table['name']):
                    is_valid = False
                    validation_summary.append(f"Invalid table tags for {table['name']}")

            # Validate column tags if columns are present
            for column in table.get('columns', []):
                if 'tags' in column:
                    logger.info(f"Validating column tags for: {column['name']}")
                    if not self.validate_tags(column.get('tags', {}), 'column', f"{table['name']}.{column['name']}"):
                        is_valid = False
                        validation_summary.append(f"Invalid column tags for {table['name']}.{column['name']}")

        if not is_valid:
            logger.error("Validation Summary:\n" + "\n".join(validation_summary))
        else:
            logger.info("All tags validated successfully!")

        return is_valid

    def check_database_exists(self, database_name: str) -> bool:
        """Check if database exists in Glue catalog"""
        try:
            self.glue.get_database(Name=database_name)
            logger.info(f"Database exists: {database_name}")
            return True
        except self.glue.exceptions.EntityNotFoundException:
            logger.warning(f"Database not found in Glue catalog: {database_name}")
            return False
        except Exception as e:
            logger.error(f"Error checking database {database_name}: {str(e)}")
            return False

    def check_table_exists(self, database_name: str, table_name: str) -> bool:
        """Check if table exists in Glue catalog"""
        try:
            self.glue.get_table(DatabaseName=database_name, Name=table_name)
            logger.info(f"Table exists: {database_name}.{table_name}")
            return True
        except self.glue.exceptions.EntityNotFoundException:
            logger.warning(f"Table not found in Glue catalog: {database_name}.{table_name}")
            return False
        except Exception as e:
            logger.error(f"Error checking table {database_name}.{table_name}: {str(e)}")
            return False

    def check_columns_exist(self, database_name: str, table_name: str, column_names: List[str]) -> Dict[str, bool]:
        """Check which columns exist in Glue table"""
        try:
            table = self.glue.get_table(DatabaseName=database_name, Name=table_name)
            existing_columns = {col['Name'] for col in table['Table']['StorageDescriptor']['Columns']}

            # Check each column and return dictionary of results
            column_status = {}
            for column in column_names:
                exists = column in existing_columns
                column_status[column] = exists
                log_level = logging.INFO if exists else logging.WARNING
                logger.log(log_level, f"Column {column} {'exists' if exists else 'does not exist'} "
                                      f"in {database_name}.{table_name}")

            return column_status
        except Exception as e:
            logger.error(f"Error checking columns for {database_name}.{table_name}: {str(e)}")
            return {column: False for column in column_names}

    def validate_resources_exist(self, config: Dict) -> bool:
        """Validate if all resources (database, tables, columns) in the config exist"""
        validation_summary = []
        is_valid = True

        database = config['database']
        if not self.check_database_exists(database['name']):
            is_valid = False
            validation_summary.append(f"Database '{database['name']}' does not exist")

        for table in database.get('tables', []):
            if not self.check_table_exists(database['name'], table['name']):
                is_valid = False
                validation_summary.append(f"Table '{database['name']}.{table['name']}' does not exist")
            else:
                column_names = [col['name'] for col in table.get('columns', [])]
                if column_names:
                    column_status = self.check_columns_exist(database['name'], table['name'], column_names)
                    for column, exists in column_status.items():
                        if not exists:
                            is_valid = False
                            validation_summary.append(
                                f"Column '{database['name']}.{table['name']}.{column}' does not exist")

        if not is_valid:
            logger.error("Resource Validation Summary:\n" + "\n".join(validation_summary))
        else:
            logger.info("All resources (database, tables, columns) exist in the Glue catalog")

        return is_valid

    def get_current_resource_tags(self, resource: Dict) -> Dict[str, str]:
        """Get current LF tags assigned to a resource"""
        try:
            response = self.lakeformation.get_resource_lf_tags(Resource=resource)
            current_tags = {}

            # Determine which response field to use based on resource type
            if 'Database' in resource:
                tags = response.get('LFTagOnDatabase', [])
            elif 'Table' in resource:
                tags = response.get('LFTagsOnTable', [])
            elif 'TableWithColumns' in resource:
                # Handle column tags differently as they come in a different format
                column_tags = response.get('LFTagsOnColumns', [])
                if column_tags and 'LFTags' in column_tags[0]:
                    tags = column_tags[0]['LFTags']
                else:
                    tags = []

            # Process tags consistently regardless of resource type
            for tag in tags:
                if 'TagKey' in tag and 'TagValues' in tag and tag['TagValues']:
                    current_tags[tag['TagKey']] = tag['TagValues'][0]

            logger.info(f"Current tags for resource: {current_tags}")
            return current_tags
        except Exception as e:
            logger.error(f"Error getting current tags for resource: {str(e)}")
            return {}

    def get_tag_changes(self, current_tags: Dict[str, str], new_tags: Dict[str, str]) -> Dict:
        """
        Identify changes between current and new tags
        Returns a dict with 'add', 'update', 'remove', and 'unchanged' tags
        """
        changes = {
            'add': {},  # New tags to be added
            'update': {},  # Existing tags to be updated with new values
            'remove': {},  # Tags to be removed
            'unchanged': {}  # Tags that remain the same
        }

        # Find tags to add or update
        for key, new_value in new_tags.items():
            if key not in current_tags:
                # New tag not in current tags
                changes['add'][key] = new_value
            elif current_tags[key] != new_value:
                # Tag exists but value is different
                changes['update'][key] = {
                    'old_value': current_tags[key],
                    'new_value': new_value
                }
            else:
                # Tag exists with same value
                changes['unchanged'][key] = new_value

        # Find tags to remove (tags in current but not in new)
        for key, value in current_tags.items():
            if key not in new_tags:
                changes['remove'][key] = value

        # Log changes
        if changes['add']:
            logger.info(f"Tags to add: {changes['add']}")
        if changes['update']:
            logger.info(f"Tags to update: {changes['update']}")
        if changes['remove']:
            logger.info(f"Tags to remove: {changes['remove']}")
        if changes['unchanged']:
            logger.info(f"Tags unchanged: {changes['unchanged']}")

        return changes

    def apply_tag_changes(self, resource: Dict, tag_changes: Dict):
        """Apply tag changes (additions and removals) to a resource"""
        if not (tag_changes['add'] or tag_changes['update'] or tag_changes['remove']):
            logger.info("No tag changes to apply")
            return

        try:
            # Add or update tags
            if tag_changes['add']:
                self.lakeformation.add_lf_tags_to_resource(
                    Resource=resource,
                    LFTags=[
                        {'TagKey': key, 'TagValues': [value]}
                        for key, value in tag_changes['add'].items()
                    ]
                )
                logger.info(f"Successfully added new tags: {tag_changes['add']}")

            # Update existing tags
            if tag_changes['update']:
                update_tags = {
                    key: change['new_value']
                    for key, change in tag_changes['update'].items()
                }
                self.lakeformation.add_lf_tags_to_resource(
                    Resource=resource,
                    LFTags=[
                        {'TagKey': key, 'TagValues': [value]}
                        for key, value in update_tags.items()
                    ]
                )
                logger.info(f"Successfully updated tags: {tag_changes['update']}")

            # Remove tags
            if tag_changes['remove']:
                self.lakeformation.remove_lf_tags_from_resource(
                    Resource=resource,
                    LFTags=[
                        {'TagKey': key, 'TagValues': [value]}
                        for key, value in tag_changes['remove'].items()
                    ]
                )
                logger.info(f"Successfully removed tags: {tag_changes['remove']}")

        except Exception as e:
            logger.error(f"Error applying tag changes: {str(e)}")
            raise

    def process_database_tags(self, config: Dict):
            """Process and apply database tags"""
            try:
                database = config['database']
                resource = {'Database': {'Name': database['name']}}
                current_tags = self.get_current_resource_tags(resource)
                new_tags = database.get('tags', {}) or {}

                tag_changes = self.get_tag_changes(current_tags, new_tags)

                if tag_changes['add'] or tag_changes['update'] or tag_changes['remove']:
                    logger.info(f"Applying changes to database {database['name']}")
                    self.apply_tag_changes(resource, tag_changes)
                    self.change_tracker.track_changes('database', database['name'], tag_changes)
                else:
                    logger.info(f"No changes needed for database {database['name']}")
            except Exception as e:
                logger.error(f"Error processing database tags: {str(e)}")
                raise

    def process_table_tags(self, db_name: str, table: Dict, database_tags: Dict = None):
        """Process and apply table tags"""
        try:
            resource = {
                'Table': {
                    'DatabaseName': db_name,
                    'Name': table['name']
                }
            }

            current_tags = self.get_current_resource_tags(resource)
            logger.info(f"Current table tags: {current_tags}")

            # Convert None to empty dict for both database and table tags
            db_tags = database_tags or {}
            table_tags = table.get('tags', {}) or {}

            # Merge database tags with table tags (table tags take precedence)
            new_tags = {**db_tags, **table_tags}
            logger.info(f"New table tags (including inherited from database): {new_tags}")

            tag_changes = self.get_tag_changes(current_tags, new_tags)
            if tag_changes['add'] or tag_changes['update'] or tag_changes['remove']:
                logger.info(f"Applying changes to table {table['name']}")
                self.apply_tag_changes(resource, tag_changes)
                self.change_tracker.track_changes('table', table['name'], tag_changes)
            else:
                logger.info(f"No changes needed for table {table['name']}")
        except Exception as e:
            logger.error(f"Error processing table tags: {str(e)}")
            raise

    def process_column_tags(self, db_name: str, table: Dict, database_tags: Dict = None):
        """Process and apply column tags with inheritance from database and table"""
        try:
            # Convert None to empty dict for database and table tags
            db_tags = database_tags or {}
            table_tags = table.get('tags', {}) or {}

            # Base tags that columns will inherit (database tags overridden by table tags)
            inherited_tags = {**db_tags, **table_tags}

            for column in table.get('columns', []):
                resource = {
                    'TableWithColumns': {
                        'DatabaseName': db_name,
                        'Name': table['name'],
                        'ColumnNames': [column['name']]
                    }
                }

                current_tags = self.get_current_resource_tags(resource)
                logger.info(f"Current column tags for {column['name']}: {current_tags}")

                # Convert None to empty dict for column tags
                column_tags = column.get('tags', {}) or {}

                # Merge all tags with proper precedence:
                # column tags override table tags, which override database tags
                new_tags = {**inherited_tags, **column_tags}

                logger.info(f"New tags for column {column['name']} "
                            f"(including inherited from database and table): {new_tags}")

                tag_changes = self.get_tag_changes(current_tags, new_tags)

                if tag_changes['add'] or tag_changes['update'] or tag_changes['remove']:
                    logger.info(f"Applying changes to column {column['name']}")
                    self.apply_tag_changes(resource, tag_changes)
                    self.change_tracker.track_changes('column',
                                                      f"{table['name']}.{column['name']}",
                                                      tag_changes)
                else:
                    logger.info(f"No changes needed for column {column['name']}")

        except Exception as e:
            logger.error(f"Error processing column tags: {str(e)}")
            raise

    def load_config_from_s3(self, bucket: str, key: str) -> Dict:
            """Load configuration from S3"""
            try:
                response = self.s3.get_object(Bucket=bucket, Key=key)
                file_content = response['Body'].read().decode('utf-8')
                # Parse YAML content
                yaml_content = yaml.safe_load(file_content)
                # Log the loaded configuration
                logger.info(f"Loaded configuration from S3: {yaml_content}")
                # Validate configuration
                LFTagConfigValidator.validate_config(yaml_content)
                logger.info("Configuration validation successful")
                return yaml_content

            except yaml.YAMLError as e:
                logger.error(f"Invalid YAML format in config file: {str(e)}")
                raise ValueError(f"Invalid YAML format in config file: {str(e)}")

            except Exception as e:
                logger.error(f"Error loading config from S3: {str(e)}")
                raise

    def process_file(self, bucket: str, key: str):
        try:
            """Process a single YAML file"""
            logger.info(f"Processing file: s3://{bucket}/{key}")
            config = self.load_config_from_s3(bucket, key)
            logger.info(f"Config file: {config}")

            # Validate all tags first
            if not self.validate_all_tags(config):
                raise ValueError("Tag validation failed. Check logs for details.")

            # Validate if all resources exist
            if not self.validate_resources_exist(config):
                raise ValueError("Resource validation failed. Check logs for details.")

            # If validation passes, proceed with processing
            database = config['database']
            database_tags = database.get('tags', {}) or {}

            # Process database tags if present
            if database_tags:
                self.process_database_tags(config)

            # Process table and column tags if tables are present
            if 'tables' in database:
                for table in database['tables']:
                    # Process table tags with database inheritance
                    if 'tags' in table or database_tags:
                        self.process_table_tags(database['name'], table, database_tags)

                    # Process column tags with both database and table inheritance
                    if 'columns' in table:
                        self.process_column_tags(database['name'], table, database_tags)

            # Return the summary of changes
            return self.change_tracker.get_summary()

        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            return {
                'statusCode': 400,
                'body': json.dumps(f'Configuration validation failed: {str(e)}')
            }
        except Exception as e:
            logger.error(f"Error in lambda execution: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error processing configuration: {str(e)}')
            }


def lambda_handler(event, context):
    """
    Lambda handler function for processing Lake Formation permissions

    Args:
        event: Lambda event with S3 trigger information
        context: Lambda context

    Returns:
        dict: Response with detailed processing summary
    """
    try:
        # Validate event structure
        if 'Records' not in event:
            raise ValueError("Invalid event: Not an S3 event trigger")

        record = event['Records'][0]
        if 's3' not in record:
            raise ValueError("Invalid event: Missing S3 information")

        # Get admin role ARN from environment variables
        admin_role_arn = os.environ.get('LF_ADMIN_ROLE_ARN')
        if not admin_role_arn:
            raise ValueError("Missing required environment variable: LF_ADMIN_ROLE_ARN")

        logger.info(f"Using Lake Formation Admin Role: {admin_role_arn}")

        # Extract S3 information
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        logger.info(f"Processing file: s3://{bucket}/{key}")

        lf_manager = LFTagManager(admin_role_arn)

        # Process the file and get changes summary
        result = lf_manager.process_file(bucket, key)

        # If result contains error status, return it directly
        # If result already contains statusCode (error case), return it directly
        if isinstance(result, dict) and 'statusCode' in result:
            return result

        return {
                'statusCode': 200,
                'body': json.dumps(result)
            }


    except KeyError as e:
        logger.error(f"Invalid event structure: {str(e)}")
        return {
                'statusCode': 400,
                'body': json.dumps({
                'error': 'Invalid Event',
                'message': f'Invalid event structure: {str(e)}'
                })
            }

    except Exception as e:
        logger.error(f"Error in lambda execution: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing file: {str(e)}')
        }
