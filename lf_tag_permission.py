import boto3
import yaml
import json
import urllib.parse
import datetime
import os
import logging
from typing import List, Dict, Set, Optional
from enum import Enum
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


"""
Lake Formation Permission Manager

This module manages Lake Formation permissions through tag-based access control.
It processes YAML configurations from S3 and applies permission changes using Lake Formation batch operations.

Key Features:
-------------
1. Tag-Based Access Control:
   - Manages LF-TAG based permissions for databases and tables
   - Validates tag keys and values against existing Lake Formation tags
   - Supports multiple tag expressions per resource

2. Permission Management:
   - Handles DATABASE and TABLE level permissions
   - Supports batch grant and revoke operations
   - Maintains existing permissions that don't need changes

3. Configuration:
   - Reads YAML configuration from S3
   - Example YAML structure:
     permissions:
       - principal:
           arn: "arn:aws:iam::account:role/example"
           DATABASE:
             - tags:
                 lf_domain: ["domain1"]
                 lf_sensitivity: ["public"]
               permissions: ["DESCRIBE", "CREATE_TABLE"]
           TABLE:
             - tags:
                 lf_domain: ["domain1"]
                 lf_sensitivity: ["public"]
               permissions: ["SELECT", "DESCRIBE"]
               
    # Revoke all permissions
    permissions:
        - principal:
        arn: "arn:aws:iam::263040894588:role/data-analyst-growth"
        DATABASE: []  # Revoke all DATABASE permissions
        TABLE: []     # Revoke all TABLE permissions


4. Validation:
   - Validates YAML structure and content
   - Verifies permission types for databases and tables
   - Ensures LF tags exist in Lake Formation
   - Validates tag values against allowed values
   - No duplicate principal ARNs allowed


"""


class DatabasePermissionTypes(str, Enum):
    CREATE_TABLE = "CREATE_TABLE"
    ALTER = "ALTER"
    DROP = "DROP"
    DESCRIBE = "DESCRIBE"

class TablePermissionTypes(str, Enum):
    SELECT = "SELECT"
    INSERT = "INSERT"
    DELETE = "DELETE"
    DESCRIBE = "DESCRIBE"
    ALTER = "ALTER"
    DROP = "DROP"

class LakeFormationError(Exception):
    """Base exception class for Lake Formation operations"""
    pass

class ValidationError(LakeFormationError):
    """Exception raised for validation errors"""
    pass

class PermissionError(LakeFormationError):
    """Exception raised for permission-related errors"""
    pass


class ConfigValidator:
    @staticmethod
    def validate_tags_structure(tags: Dict) -> None:
        """Validate the structure of tags"""
        try:
            if not isinstance(tags, dict):
                raise ValidationError("Tags must be a dictionary")

            for key, value in tags.items():
                if not isinstance(key, str):
                    raise ValidationError(f"Tag key must be a string: {key}")
                if not isinstance(value, list):
                    raise ValidationError(f"Tag value must be a list for key: {key}")
                if not all(isinstance(v, str) for v in value):
                    raise ValidationError(f"All tag values must be strings for key: {key}")
        except Exception as e:
            logger.error(f"Error validating tags structure: {str(e)}")
            raise

    @staticmethod
    def validate_resource_structure(resource: Dict, resource_type: str) -> None:
        """
        Validate the structure of a resource.
        Empty resource means revocation of all permissions.

        Args:
            resource (Dict): Resource configuration
            resource_type (str): Type of resource (DATABASE or TABLE)
        """
        try:
            # If resource is empty dictionary, it indicates complete permission revocation
            if resource == {}:
                logger.info(f"Empty {resource_type} configuration - will revoke all permissions")
                return

            if not isinstance(resource, dict):
                raise ValidationError(f"{resource_type} must be a dictionary")

            required_fields = {'tags', 'permissions'}
            missing_fields = required_fields - set(resource.keys())
            if missing_fields:
                raise ValidationError(f"Missing required fields in {resource_type}: {missing_fields}")

            ConfigValidator.validate_tags_structure(resource['tags'])
            if not isinstance(resource['permissions'], list):
                raise ValidationError(f"{resource_type} permissions must be a list")

        except Exception as e:
            logger.error(f"Error validating resource structure: {str(e)}")
            raise

    @staticmethod
    def validate_principal_structure(principal: Dict) -> None:
        """
        Validate the structure of a principal.
        DATABASE and TABLE can be empty lists for permission revocation.
        """
        try:
            if not isinstance(principal, dict):
                raise ValidationError("Principal must be a dictionary")

            required_fields = {'arn', 'DATABASE', 'TABLE'}
            missing_fields = required_fields - set(principal.keys())
            if missing_fields:
                raise ValidationError(f"Missing required fields in principal: {missing_fields}")

            if not isinstance(principal['arn'], str):
                raise ValidationError("ARN must be a string")
            if not principal['arn'].startswith('arn:aws:iam::'):
                raise ValidationError("Invalid ARN format")

            # Validate DATABASE configuration
            if not isinstance(principal['DATABASE'], list):
                raise ValidationError("DATABASE must be a list")
            for db in principal['DATABASE']:
                if db:  # Only validate if not empty
                    ConfigValidator.validate_resource_structure(db, 'DATABASE')

            # Validate TABLE configuration
            if not isinstance(principal['TABLE'], list):
                raise ValidationError("TABLE must be a list")
            for table in principal['TABLE']:
                if table:  # Only validate if not empty
                    ConfigValidator.validate_resource_structure(table, 'TABLE')

        except Exception as e:
            logger.error(f"Error validating principal structure: {str(e)}")
            raise



    @staticmethod
    def check_duplicate_principals(config: Dict) -> None:
        """
        Check for duplicate principal ARNs in the configuration

        Args:
            config (Dict): Configuration to validate

        Raises:
            ValidationError: If duplicate principal ARNs are found
        """
        try:
            principal_arns = []
            duplicate_arns = set()

            for permission in config['permissions']:
                arn = permission['principal']['arn']
                if arn in principal_arns:
                    duplicate_arns.add(arn)
                principal_arns.append(arn)

            if duplicate_arns:
                error_msg = (
                    f"Duplicate principal ARNs found in configuration. "
                    f"Each principal ARN must be unique. Duplicates: {list(duplicate_arns)}"
                )
                logger.error(error_msg)
                raise ValidationError(error_msg)

            logger.info(f"Validated {len(principal_arns)} unique principal ARNs")

        except ValidationError:
            raise
        except Exception as e:
            error_msg = f"Error checking for duplicate principals: {str(e)}"
            logger.error(error_msg)
            raise ValidationError(error_msg)

    @staticmethod
    def validate_config(config: Dict) -> None:
        """
        Validate the overall configuration including checking for duplicate principals

        Args:
            config (Dict): Configuration to validate

        Raises:
            ValidationError: If configuration is invalid or contains duplicate principals
        """
        try:
            logger.info("Starting configuration validation")

            if not isinstance(config, dict):
                raise ValidationError("Config must be a dictionary")

            if 'permissions' not in config:
                raise ValidationError("Permissions configuration must be specified")

            if not isinstance(config['permissions'], list):
                raise ValidationError("Permissions must be a list")

            # Check for duplicate principals before validating structure
            ConfigValidator.check_duplicate_principals(config)

            # Validate each permission entry
            for permission in config['permissions']:
                if 'principal' not in permission:
                    raise ValidationError("Each permission must have a principal")
                ConfigValidator.validate_principal_structure(permission['principal'])

            logger.info("Successfully validated configuration")

        except Exception as e:
            logger.error(f"Error validating configuration: {str(e)}")
            raise


class PermissionsValidator:
    @staticmethod
    def validate_database_permissions(permissions: List[str]) -> None:
        """
        Validate database permissions

        Args:
            permissions (List[str]): Permissions to validate

        Raises:
            ValidationError: If permissions are invalid
        """
        try:
            valid_permissions = {p.value for p in DatabasePermissionTypes}
            invalid_permissions = [p for p in permissions if p not in valid_permissions]
            if invalid_permissions:
                raise ValidationError(
                    f"Invalid database permissions: {invalid_permissions}. "
                    f"Valid permissions are: {[p.value for p in DatabasePermissionTypes]}"
                )
        except Exception as e:
            logger.error(f"Error validating database permissions: {str(e)}")
            raise

    @staticmethod
    def validate_table_permissions(permissions: List[str]) -> None:
        """
        Validate table permissions

        Args:
            permissions (List[str]): Permissions to validate

        Raises:
            ValidationError: If permissions are invalid
        """
        try:
            valid_permissions = {p.value for p in TablePermissionTypes}
            invalid_permissions = [p for p in permissions if p not in valid_permissions]
            if invalid_permissions:
                raise ValidationError(
                    f"Invalid table permissions: {invalid_permissions}. "
                    f"Valid permissions are: {[p.value for p in TablePermissionTypes]}"
                )
        except Exception as e:
            logger.error(f"Error validating table permissions: {str(e)}")
            raise

    @staticmethod
    def validate_config_permissions(config: Dict) -> None:
        """
        Validate permissions in the configuration

        Args:
            config (Dict): Configuration to validate

        Raises:
            ValidationError: If permissions are invalid
        """
        try:
            for permission in config['permissions']:
                principal = permission['principal']
                for db in principal['DATABASE']:
                    PermissionsValidator.validate_database_permissions(db['permissions'])
                for table in principal['TABLE']:
                    PermissionsValidator.validate_table_permissions(table['permissions'])
        except Exception as e:
            logger.error(f"Error validating config permissions: {str(e)}")
            raise


class LakeFormationPermissions:
    def __init__(self, lakeformation_client, catalog_id: str):
        """
        Initialize LakeFormationPermissions

        Args:
            lakeformation_client: Boto3 Lake Formation client
            catalog_id (str): AWS account ID
        """
        self.lakeformation = lakeformation_client
        self.catalog_id = catalog_id
        self.permissions = {}

    def add_permission(self, principal: str, resource_type: str,
                       tag_expressions: List[Dict], permissions: List[str]) -> None:
        """
        Add a permission to the internal permissions store

        Args:
            principal (str): Principal ARN
            resource_type (str): Resource type (DATABASE or TABLE)
            tag_expressions (List[Dict]): List of tag expressions
            permissions (List[str]): List of permissions
        """
        try:
            if principal not in self.permissions:
                self.permissions[principal] = {}

            if resource_type not in self.permissions[principal]:
                self.permissions[principal][resource_type] = {
                    'tag_expressions': [],
                    'permissions': []
                }

            self.permissions[principal][resource_type]['tag_expressions'] = [
                {
                    'TagKey': expr['TagKey'],
                    'TagValues': expr['TagValues']
                }
                for expr in tag_expressions
            ]
            self.permissions[principal][resource_type]['permissions'] = permissions

        except Exception as e:
            logger.error(f"Error adding permission: {str(e)}")
            raise PermissionError(f"Failed to add permission: {str(e)}")

    def compare_permissions(self, formatted_config: List[Dict], current_permissions: Dict) -> Dict:
        """
        Compare formatted configuration with current permissions and identify changes

        Args:
            formatted_config (List[Dict]): List of proposed permissions
            current_permissions (Dict): Current permissions from Lake Formation

        Returns:
            Dict: Changes required (grant_permissions, revoke_permissions, no_changes)
        """
        try:
            grant_permissions = []
            revoke_permissions = []
            no_change_permissions = []

            # Process only the principals in the proposed configuration
            for proposed in formatted_config:
                principal_arn = proposed['principal_arn']
                current = current_permissions.get(principal_arn, {})
                logger.info(f"Comparing permissions for principal: {principal_arn}")

                if not current:
                    logger.info(f"No current permissions found for {principal_arn}, adding to grants")
                    self._add_to_grant_list(grant_permissions, proposed)
                    continue

                # Compare permissions for each resource type
                for resource_type in ['DATABASE', 'TABLE']:
                    proposed_resource = proposed.get(resource_type, {})
                    current_resource = current.get(resource_type, {})

                    if not proposed_resource:
                        logger.debug(f"No {resource_type} permissions proposed for {principal_arn}")
                        continue

                    # Compare permissions and tags
                    proposed_perms = set(proposed_resource.get('permissions', []))
                    current_perms = set(current_resource.get('permissions', []))

                    proposed_tags = self._normalize_tag_expressions(
                        proposed_resource.get('tag_expressions', [])
                    )
                    current_tags = self._normalize_tag_expressions(
                        current_resource.get('tag_expressions', [])
                    )

                    # Compare permissions and tags
                    permissions_match = proposed_perms == current_perms
                    tags_match = proposed_tags == current_tags

                    if not (permissions_match and tags_match):
                        logger.info(
                            f"Changes detected for {principal_arn} {resource_type}:\n"
                            f"Current permissions: {current_perms}\n"
                            f"Proposed permissions: {proposed_perms}\n"
                            f"Current tags: {current_tags}\n"
                            f"Proposed tags: {proposed_tags}"
                        )

                        if current_resource:
                            self._add_specific_revoke(
                                revoke_permissions,
                                principal_arn,
                                resource_type,
                                current_resource
                            )
                        self._add_specific_grant(
                            grant_permissions,
                            principal_arn,
                            resource_type,
                            proposed_resource
                        )
                    else:
                        logger.info(f"No changes needed for {principal_arn} {resource_type}")
                        self._add_to_no_change_list(
                            no_change_permissions,
                            principal_arn,
                            resource_type,
                            current_resource
                        )

            return {
                'grant_permissions': grant_permissions,
                'revoke_permissions': revoke_permissions,
                'no_change_permissions': no_change_permissions
            }

        except Exception as e:
            logger.error(f"Error comparing permissions: {str(e)}")
            raise PermissionError(f"Failed to compare permissions: {str(e)}")

    def apply_permissions(self, changes: Dict) -> Dict:
        """
        Apply the permission changes to Lake Formation

        Args:
            changes (Dict): Permission changes to apply

        Returns:
            Dict: Results of applying changes
        """
        # Check if there are any changes to apply
        if not changes.get('revoke_permissions') and not changes.get('grant_permissions'):
            logger.info("No changes to Lake Formation permissions required")
            return {
                'message': 'No changes to Lake Formation permissions required',
                'grants': [],
                'revokes': [],
                'errors': []
            }

        results = {
            'grants': [],
            'revokes': [],
            'errors': []
        }

        # Process revokes first
        if changes.get('revoke_permissions'):
            try:
                logger.info("Processing revoke permissions")
                revoke_entries = [
                    {
                        'Id': str(idx),
                        'Principal': revoke['Principal'],
                        'Resource': revoke['Resource'],
                        'Permissions': revoke['Permissions']
                    }
                    for idx, revoke in enumerate(changes['revoke_permissions'])
                ]

                if revoke_entries:
                    revoke_response = self.lakeformation.batch_revoke_permissions(
                        CatalogId=self.catalog_id,
                        Entries=revoke_entries
                    )

                    if revoke_response.get('Failures'):
                        failure_details = [
                            {
                                'id': failure['RequestEntry']['Id'],
                                'principal': failure['RequestEntry']['Principal']['DataLakePrincipalIdentifier'],
                                'error': failure.get('Error', {}).get('ErrorMessage', 'Unknown error')
                            }
                            for failure in revoke_response.get('Failures', [])
                        ]

                        error_msg = (
                            f"Failed to revoke permissions. Stopping process. "
                            f"Failures: {json.dumps(failure_details, indent=2)}"
                        )
                        logger.error(error_msg)
                        raise PermissionError(error_msg)

                    results['revokes'] = changes['revoke_permissions']
                    logger.info(f"Successfully revoked {len(results['revokes'])} permissions")

            except Exception as e:
                error_msg = f"Error during revoke operations: {str(e)}"
                logger.error(error_msg)
                raise PermissionError(error_msg)

        # Process grants
        if changes.get('grant_permissions'):
            try:
                logger.info("Processing grant permissions")
                grant_entries = [
                    {
                        'Id': str(idx),
                        'Principal': grant['Principal'],
                        'Resource': grant['Resource'],
                        'Permissions': grant['Permissions']
                    }
                    for idx, grant in enumerate(changes['grant_permissions'])
                ]

                if grant_entries:
                    grant_response = self.lakeformation.batch_grant_permissions(
                        CatalogId=self.catalog_id,
                        Entries=grant_entries
                    )

                    if grant_response.get('Failures'):
                        failure_details = [
                            {
                                'id': failure['RequestEntry']['Id'],
                                'principal': failure['RequestEntry']['Principal']['DataLakePrincipalIdentifier'],
                                'error': failure.get('Error', {}).get('ErrorMessage', 'Unknown error')
                            }
                            for failure in grant_response.get('Failures', [])
                        ]

                        error_msg = (
                            f"Failed to grant permissions. "
                            f"Failures: {json.dumps(failure_details, indent=2)}"
                        )
                        logger.error(error_msg)
                        raise PermissionError(error_msg)

                    results['grants'] = changes['grant_permissions']
                    logger.info(f"Successfully granted {len(results['grants'])} permissions")

            except Exception as e:
                error_msg = f"Error during grant operations: {str(e)}"
                logger.error(error_msg)
                raise PermissionError(error_msg)

        # Add success message if operations were successful
        if results['grants'] or results['revokes']:
            results['message'] = 'Successfully applied Lake Formation permission changes'

        return results

    def _normalize_tag_expressions(self, expressions: List[Dict]) -> Set[tuple]:
        """
        Normalize tag expressions for comparison

        Args:
            expressions (List[Dict]): List of tag expressions

        Returns:
            Set[tuple]: Normalized tag expressions
        """
        try:
            normalized = set()
            for expr in expressions:
                if isinstance(expr, dict):
                    if 'TagKey' in expr:
                        normalized.add((expr['TagKey'], tuple(sorted(expr['TagValues']))))
                    else:
                        for key, values in expr.items():
                            normalized.add((key, tuple(sorted(values))))
            return normalized

        except Exception as e:
            logger.error(f"Error normalizing tag expressions: {str(e)}")
            raise PermissionError(f"Failed to normalize tag expressions: {str(e)}")


    def _add_specific_grant(self, grant_list: List[Dict], principal_arn: str,
                            resource_type: str, resource_config: Dict) -> None:
        """
        Add specific resource type permissions to grant list

        Args:
            grant_list (List[Dict]): List to add grants to
            principal_arn (str): Principal ARN
            resource_type (str): Resource type
            resource_config (Dict): Resource configuration
        """
        try:
            if resource_config.get('tag_expressions') and resource_config.get('permissions'):
                grant_list.append({
                    'Principal': {
                        'DataLakePrincipalIdentifier': principal_arn
                    },
                    'Resource': {
                        'LFTagPolicy': {
                            'CatalogId': self.catalog_id,
                            'ResourceType': resource_type,
                            'Expression': resource_config['tag_expressions']
                        }
                    },
                    'Permissions': resource_config['permissions']
                })
        except Exception as e:
            logger.error(f"Error adding grant permission: {str(e)}")
            raise PermissionError(f"Failed to add grant permission: {str(e)}")

    def _add_specific_revoke(self, revoke_list: List[Dict], principal_arn: str,
                             resource_type: str, resource_config: Dict) -> None:
        """
        Add specific resource type permissions to revoke list

        Args:
            revoke_list (List[Dict]): List to add revokes to
            principal_arn (str): Principal ARN
            resource_type (str): Resource type
            resource_config (Dict): Resource configuration
        """
        try:
            if resource_config.get('tag_expressions'):
                revoke_list.append({
                    'Principal': {
                        'DataLakePrincipalIdentifier': principal_arn
                    },
                    'Resource': {
                        'LFTagPolicy': {
                            'CatalogId': self.catalog_id,
                            'ResourceType': resource_type,
                            'Expression': resource_config['tag_expressions']
                        }
                    },
                    'Permissions': resource_config['permissions']
                })
        except Exception as e:
            logger.error(f"Error adding revoke permission: {str(e)}")
            raise PermissionError(f"Failed to add revoke permission: {str(e)}")

    def _add_to_no_change_list(self, no_change_list: List[Dict], principal_arn: str,
                               resource_type: str, resource_config: Dict) -> None:
        """
        Add permissions that don't need changes to no_change list

        Args:
            no_change_list (List[Dict]): List to add unchanged permissions to
            principal_arn (str): Principal ARN
            resource_type (str): Resource type
            resource_config (Dict): Resource configuration
        """
        try:
            if resource_config:
                no_change_list.append({
                    'Principal': {
                        'DataLakePrincipalIdentifier': principal_arn
                    },
                    'Resource': {
                        'LFTagPolicy': {
                            'CatalogId': self.catalog_id,
                            'ResourceType': resource_type,
                            'Expression': resource_config.get('tag_expressions', [])
                        }
                    },
                    'Permissions': resource_config.get('permissions', [])
                })
        except Exception as e:
            logger.error(f"Error adding no-change permission: {str(e)}")
            raise PermissionError(f"Failed to add no-change permission: {str(e)}")

    def load_current_permissions(self) -> None:
        """Load current Lake Formation permissions"""
        try:
            permissions = self._process_lf_permissions()
            for perm in permissions:
                principal = perm['Principal']['DataLakePrincipalIdentifier']
                resource_type = perm['Resource']['LFTagPolicy']['ResourceType']
                expression = perm['Resource']['LFTagPolicy']['Expression']

                self.add_permission(
                    principal=principal,
                    resource_type=resource_type,
                    tag_expressions=expression,
                    permissions=perm['Permissions']
                )
        except Exception as e:
            logger.error(f"Failed to load current permissions: {str(e)}")
            raise PermissionError(f"Failed to load current permissions: {str(e)}")

    def _process_lf_permissions(self) -> List:
        """
        Get all Lake Formation permissions

        Returns:
            List: List of current permissions
        """
        try:
            permissions = []
            next_token = None

            while True:
                try:
                    kwargs = {
                        'CatalogId': self.catalog_id,
                        'ResourceType': 'LF_TAG_POLICY'
                    }
                    if next_token:
                        kwargs['NextToken'] = next_token

                    response = self.lakeformation.list_permissions(**kwargs)
                    permissions.extend(response.get('PrincipalResourcePermissions', []))
                    next_token = response.get('NextToken')

                    if not next_token:
                        break

                except ClientError as e:
                    logger.error(f"AWS API error listing permissions: {str(e)}")
                    raise

            return permissions

        except Exception as e:
            logger.error(f"Error processing permissions: {str(e)}")
            raise PermissionError(f"Failed to process permissions: {str(e)}")

    def to_dict(self) -> Dict:
        """
        Convert permissions to dictionary

        Returns:
            Dict: Permissions dictionary
        """
        return self.permissions


class LFTagValidator:
    """Validates LakeFormation tags"""

    def __init__(self, lakeformation_client):
        """
        Initialize LFTagValidator

        Args:
            lakeformation_client: Boto3 Lake Formation client
        """
        self.lakeformation = lakeformation_client
        self.tag_cache = {}

    def get_lf_tags(self) -> Dict[str, Set[str]]:
        """
        Get all LakeFormation tags and their values

        Returns:
            Dict[str, Set[str]]: Dictionary of tag keys and their possible values

        Raises:
            PermissionError: If failed to get LakeFormation tags
        """
        try:
            # Return cached tags if available
            if self.tag_cache:
                return self.tag_cache

            tags = {}
            paginator = self.lakeformation.get_paginator('list_lf_tags')

            for page in paginator.paginate():
                for tag in page.get('LFTags', []):
                    tag_key = tag['TagKey']
                    tag_values = set(tag['TagValues'])
                    tags[tag_key] = tag_values

            self.tag_cache = tags
            logger.info(f"Retrieved {len(tags)} LF tags")
            return tags

        except ClientError as e:
            error_msg = f"Failed to get LakeFormation tags: {str(e)}"
            logger.error(error_msg)
            raise PermissionError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error getting LakeFormation tags: {str(e)}"
            logger.error(error_msg)
            raise PermissionError(error_msg)

    def validate_resource_tags(self, tags: Dict[str, List[str]]) -> None:
        """
        Validate tags against existing LakeFormation tags

        Args:
            tags (Dict[str, List[str]]): Tags to validate

        Raises:
            ValidationError: If tags are invalid
        """
        try:
            lf_tags = self.get_lf_tags()

            # Validate tag keys
            invalid_keys = set(tags.keys()) - set(lf_tags.keys())
            if invalid_keys:
                error_msg = (
                    f"Invalid LakeFormation tag keys: {invalid_keys}. "
                    f"Valid keys are: {list(lf_tags.keys())}"
                )
                logger.error(error_msg)
                raise ValidationError(error_msg)

            # Validate tag values
            for key, values in tags.items():
                invalid_values = set(values) - lf_tags[key]
                if invalid_values:
                    error_msg = (
                        f"Invalid values for tag '{key}': {invalid_values}. "
                        f"Valid values are: {list(lf_tags[key])}"
                    )
                    logger.error(error_msg)
                    raise ValidationError(error_msg)

            logger.debug(f"Successfully validated tags: {tags}")

        except ValidationError:
            raise
        except Exception as e:
            error_msg = f"Error validating resource tags: {str(e)}"
            logger.error(error_msg)
            raise ValidationError(error_msg)

    def validate_config_tags(self, config: Dict) -> None:
        """
        Validate all tags in the configuration

        Args:
            config (Dict): Configuration to validate

        Raises:
            ValidationError: If any tags are invalid
        """
        try:
            logger.info("Starting configuration tags validation")
            for permission in config['permissions']:
                principal = permission['principal']

                # Validate DATABASE tags
                logger.debug(f"Validating DATABASE tags for principal: {principal.get('arn')}")
                for db in principal['DATABASE']:
                    self.validate_resource_tags(db['tags'])

                # Validate TABLE tags
                logger.debug(f"Validating TABLE tags for principal: {principal.get('arn')}")
                for table in principal['TABLE']:
                    self.validate_resource_tags(table['tags'])

            logger.info("Successfully validated all configuration tags")

        except ValidationError:
            raise
        except Exception as e:
            error_msg = f"Error validating configuration tags: {str(e)}"
            logger.error(error_msg)
            raise ValidationError(error_msg)

    def clear_cache(self) -> None:
        """Clear the tag cache"""
        self.tag_cache = {}
        logger.debug("Cleared LF tag cache")


class PermissionsManager:
    def __init__(self, admin_role_arn: str):
        """
        Initialize PermissionsManager

        Args:
            admin_role_arn (str): ARN of admin role to assume
        """
        try:
            self.s3_client = boto3.client('s3')
            self.lakeformation = self._get_lakeformation_client(admin_role_arn)
            self.lf_validator = LFTagValidator(self.lakeformation)
            self.catalog_id = admin_role_arn.split(':')[4]
            self.lf_permissions = LakeFormationPermissions(self.lakeformation, self.catalog_id)
        except Exception as e:
            logger.error(f"Error initializing PermissionsManager: {str(e)}")
            raise

    def _get_lakeformation_client(self, admin_role_arn: str):
        """
        Create LakeFormation client with assumed role

        Args:
            admin_role_arn (str): ARN of role to assume

        Returns:
            boto3.client: LakeFormation client
        """
        try:
            sts_client = boto3.client('sts')
            assumed_role = sts_client.assume_role(
                RoleArn=admin_role_arn,
                RoleSessionName='LFTagManagerSession'
            )

            credentials = assumed_role['Credentials']
            return boto3.client(
                'lakeformation',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
        except Exception as e:
            logger.error(f"Failed to assume role and create LakeFormation client: {str(e)}")
            raise PermissionError(f"Failed to create LakeFormation client: {str(e)}")

    def read_yaml_from_s3(self, bucket: str, key: str) -> dict:
        """
        Read YAML file from S3

        Args:
            bucket (str): S3 bucket name
            key (str): S3 object key

        Returns:
            dict: Parsed YAML content
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            yaml_content = response['Body'].read().decode('utf-8')
            return yaml.safe_load(yaml_content)
        except Exception as e:
            logger.error(f"Error reading from S3: {str(e)}")
            raise

    def format_response(self, config: Dict) -> List[Dict]:
        """
        Format the configuration into a structured list of permissions

        Args:
            config (Dict): Raw configuration from YAML

        Returns:
            List[Dict]: Formatted permissions list
        """
        try:
            formatted_permissions = []

            for permission in config['permissions']:
                principal = permission['principal']
                principal_info = {
                    'principal_arn': principal['arn'],
                    'DATABASE': {
                        'tag_expressions': [],
                        'permissions': []
                    },
                    'TABLE': {
                        'tag_expressions': [],
                        'permissions': []
                    }
                }

                # Format DATABASE permissions and tags
                for db in principal.get('DATABASE', []):
                    tag_expressions = [
                        {'TagKey': key, 'TagValues': values}
                        for key, values in db['tags'].items()
                    ]
                    principal_info['DATABASE']['tag_expressions'].extend(tag_expressions)
                    principal_info['DATABASE']['permissions'] = db['permissions']

                # Format TABLE permissions and tags
                for table in principal.get('TABLE', []):
                    tag_expressions = [
                        {'TagKey': key, 'TagValues': values}
                        for key, values in table['tags'].items()
                    ]
                    principal_info['TABLE']['tag_expressions'].extend(tag_expressions)
                    principal_info['TABLE']['permissions'] = table['permissions']

                formatted_permissions.append(principal_info)

            return formatted_permissions

        except Exception as e:
            logger.error(f"Error formatting response: {str(e)}")
            raise ValueError(f"Failed to format response: {str(e)}")

    def process_permissions(self, bucket: str, key: str) -> Dict:
        """
        Process permissions from S3 YAML file

        Args:
            bucket (str): S3 bucket name
            key (str): S3 object key

        Returns:
            Dict: Results of processing permissions
        """
        try:
            logger.info(f"Processing permissions from s3://{bucket}/{key}")

            # Read and validate configuration
            config = self.read_yaml_from_s3(bucket, key)
            logger.info("Successfully read YAML configuration from S3")

            # Validate configuration
            ConfigValidator.validate_config(config)
            PermissionsValidator.validate_config_permissions(config)
            self.lf_validator.validate_config_tags(config)
            logger.info("Successfully validated configuration")

            # Format and compare permissions
            formatted_config = self.format_response(config)
            self.lf_permissions.load_current_permissions()
            current_permissions = self.lf_permissions.to_dict()

            # Compare and get changes
            permission_changes = self.lf_permissions.compare_permissions(
                formatted_config,
                current_permissions
            )

            # Log permission changes in JSON format
            logger.info("Permission changes identified:")
            logger.info(json.dumps({
                'changes': {
                    'grants': [
                        {
                            'principal': grant['Principal']['DataLakePrincipalIdentifier'],
                            'resource_type': grant['Resource']['LFTagPolicy']['ResourceType'],
                            'permissions': grant['Permissions'],
                            'tag_expressions': grant['Resource']['LFTagPolicy']['Expression']
                        }
                        for grant in permission_changes.get('grant_permissions', [])
                    ],
                    'revokes': [
                        {
                            'principal': revoke['Principal']['DataLakePrincipalIdentifier'],
                            'resource_type': revoke['Resource']['LFTagPolicy']['ResourceType'],
                            'permissions': revoke['Permissions'],
                            'tag_expressions': revoke['Resource']['LFTagPolicy']['Expression']
                        }
                        for revoke in permission_changes.get('revoke_permissions', [])
                    ],
                    'no_changes': [
                        {
                            'principal': no_change['Principal']['DataLakePrincipalIdentifier'],
                            'resource_type': no_change['Resource']['LFTagPolicy']['ResourceType'],
                            'permissions': no_change['Permissions'],
                            'tag_expressions': no_change['Resource']['LFTagPolicy']['Expression']
                        }
                        for no_change in permission_changes.get('no_change_permissions', [])
                    ]
                }
            }, indent=2))

            # Apply changes
            apply_results = self.lf_permissions.apply_permissions(permission_changes)

            # Log final results in JSON format
            final_result = {
                'status': 'success',
                'changes': permission_changes,
                'results': apply_results
            }

            logger.info("Final execution results:")
            logger.info(json.dumps({
                'status': final_result['status'],
                'summary': {
                    'grants_count': len(final_result['changes'].get('grant_permissions', [])),
                    'revokes_count': len(final_result['changes'].get('revoke_permissions', [])),
                    'no_changes_count': len(final_result['changes'].get('no_change_permissions', [])),
                },
                'apply_results': {
                    'grants_applied': len(final_result['results'].get('grants', [])),
                    'revokes_applied': len(final_result['results'].get('revokes', [])),
                    'errors': final_result['results'].get('errors', []),
                    'message': final_result['results'].get('message', '')
                }
            }, indent=2))

            return final_result

        except Exception as e:
            error_msg = f"Error processing permissions: {str(e)}"
            logger.error(error_msg)
            logger.error(json.dumps({
                'error': {
                    'type': type(e).__name__,
                    'message': str(e),
                    'source': {
                        'bucket': bucket,
                        'key': key
                    }
                }
            }, indent=2))
            raise


def lambda_handler(event, context):
    """
    Lambda handler function for processing Lake Formation permissions

    Args:
        event: Lambda event with S3 trigger information
        context: Lambda context

    Returns:
        dict: Simplified response with summary
    """
    try:
        if 'Records' not in event:
            raise ValueError("Not an S3 event trigger")

        record = event['Records'][0]
        if 's3' not in record:
            raise ValueError("Not an S3 event")

        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])

        admin_role_arn = os.environ.get('ADMIN_ROLE_ARN')
        if not admin_role_arn:
            raise ValueError("ADMIN_ROLE_ARN environment variable not set")

        permissions_manager = PermissionsManager(admin_role_arn)
        result = permissions_manager.process_permissions(bucket=bucket, key=key)

        # Create summary response
        summary = {
            'grants_count': len(result['changes'].get('grant_permissions', [])),
            'revokes_count': len(result['changes'].get('revoke_permissions', [])),
            'no_changes_count': len(result['changes'].get('no_change_permissions', []))
        }

        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'message': (
                    f"Successfully processed permissions. "
                    f"Applied {summary['grants_count']} grants, "
                    f"{summary['revokes_count']} revokes, and "
                    f"{summary['no_changes_count']} unchanged."
                ),
                'source': {
                    'bucket': bucket,
                    'key': key
                }
            })
        }

    except ValueError as ve:
        logger.error(f"Validation error: {str(ve)}")
        return {
            'statusCode': 400,
            'body': json.dumps({
                'status': 'error',
                'message': f"Validation error: {str(ve)}"
            })
        }
    except Exception as e:
        logger.error(f"Internal error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'message': f"Failed to process permissions: {str(e)}"
            })
        }


if __name__ == "__main__":
    # Test event for local testing
    test_event = {
        "Records": [{
            "s3": {
                "bucket": {"name": "your-bucket-name"},
                "object": {"key": "path/to/permissions.yaml"}
            }
        }]
    }
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
