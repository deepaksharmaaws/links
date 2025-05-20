# tests/test_lf_tag_permission.py

import os
import sys
import pytest
import json
import yaml
import boto3
import logging
from moto import mock_aws
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from lf_tag_permission import PermissionsManager
from lf_tag_permission import LFTagValidator, ValidationError
from lf_tag_permission import ConfigValidator, ValidationError
from lf_tag_permission import PermissionsValidator, ValidationError, DatabasePermissionTypes, TablePermissionTypes

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test Constants
TEST_ADMIN_ROLE_ARN = 'arn:aws:iam::123456789012:role/admin'
TEST_BUCKET = 'test-bucket'
TEST_KEY = 'test.yaml'

# Test Configurations
BASIC_CONFIG = """
permissions:
  - principal:
      arn: "arn:aws:iam::123456789012:role/analyst"
      TABLE: []
"""


@pytest.fixture(scope="class")
def aws_credentials():
    """Mocked AWS Credentials for the entire test class"""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    os.environ['ADMIN_ROLE_ARN'] = TEST_ADMIN_ROLE_ARN
    yield
    # Clean up environment variables after tests
    for key in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', 'AWS_DEFAULT_REGION',
                'ADMIN_ROLE_ARN']:
        os.environ.pop(key, None)


@pytest.fixture(scope="function")
def mock_aws_clients():
    """Mock AWS clients with basic successful responses"""
    with patch('boto3.client') as mock_boto:
        # Mock S3
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            'Body': MagicMock(read=lambda: BASIC_CONFIG.encode('utf-8'))
        }

        # Mock STS
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            'Credentials': {
                'AccessKeyId': 'test',
                'SecretAccessKey': 'test',
                'SessionToken': 'test'
            }
        }

        # Mock Lake Formation
        mock_lf = MagicMock()
        mock_lf.list_permissions.return_value = {
            'PrincipalResourcePermissions': []
        }
        mock_lf.batch_grant_permissions.return_value = {
            'Failures': []  # Empty list means success
        }
        mock_lf.batch_revoke_permissions.return_value = {
            'Failures': []  # Empty list means success
        }
        mock_lf.get_paginator.return_value.paginate.return_value = [{
            'LFTags': [
                {
                    'TagKey': 'lf_domain',
                    'TagValues': ['analytics']
                },
                {
                    'TagKey': 'lf_sensitivity',
                    'TagValues': ['public']
                }
            ]
        }]

        def get_client(service_name, **kwargs):
            if service_name == 's3':
                return mock_s3
            elif service_name == 'sts':
                return mock_sts
            elif service_name == 'lakeformation':
                return mock_lf
            return MagicMock()

        mock_boto.side_effect = get_client
        yield {
            's3': mock_s3,
            'sts': mock_sts,
            'lf': mock_lf
        }



class TestPermissionsManager:

    def test_01_initialization(self, aws_credentials, mock_aws_clients):
        """Test basic initialization"""
        manager = PermissionsManager(TEST_ADMIN_ROLE_ARN)
        assert manager is not None
        assert manager.catalog_id == '123456789012'
        logger.info("Initialization test passed")

    def test_02_read_yaml_success(self, aws_credentials, mock_aws_clients):
        """Test successful YAML reading"""
        manager = PermissionsManager(TEST_ADMIN_ROLE_ARN)
        config = manager.read_yaml_from_s3(TEST_BUCKET, TEST_KEY)
        assert 'permissions' in config
        assert len(config['permissions']) == 1
        logger.info("Read YAML success test passed")

    def test_03_read_yaml_s3_error(self, aws_credentials, mock_aws_clients):
        """Test S3 error handling"""
        mock_aws_clients['s3'].get_object.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey', 'Message': 'The specified key does not exist.'}},
            'GetObject'
        )

        manager = PermissionsManager(TEST_ADMIN_ROLE_ARN)
        with pytest.raises(Exception) as exc_info:
            manager.read_yaml_from_s3(TEST_BUCKET, TEST_KEY)

        # Check either for the exact error message or parts of it
        error_message = str(exc_info.value)
        assert any([
            'NoSuchKey' in error_message,
            'The specified key does not exist' in error_message,
            'Error reading from S3' in error_message
        ]), f"Unexpected error message: {error_message}"

        # Log the actual error message for debugging
        logger.info(f"Received error message: {error_message}")

    def test_04_process_permissions_empty_config(self, aws_credentials, mock_aws_clients):
        """Test processing permissions with empty configuration"""
        empty_config = """
        permissions: []
        """
        mock_aws_clients['s3'].get_object.return_value = {
            'Body': MagicMock(read=lambda: empty_config.encode('utf-8'))
        }

        manager = PermissionsManager(TEST_ADMIN_ROLE_ARN)
        result = manager.process_permissions(TEST_BUCKET, TEST_KEY)

        assert result is not None
        assert 'status' in result
        logger.info("Empty config processing test passed")

    def test_05_assume_role_failure(self, aws_credentials, mock_aws_clients):
        """Test handling of assume role failure"""
        mock_aws_clients['sts'].assume_role.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
            'AssumeRole'
        )

        with pytest.raises(Exception) as exc_info:
            PermissionsManager(TEST_ADMIN_ROLE_ARN)
        assert 'Failed to create LakeFormation client' in str(exc_info.value)
        logger.info("Assume role failure test passed")

    def test_06_process_permissions_with_tags(self, aws_credentials, mock_aws_clients):
        """Test processing permissions with tags"""
        config_with_tags = """
        permissions:
          - principal:
              arn: "arn:aws:iam::123456789012:role/analyst"
              DATABASE:
                - tags:
                    lf_domain: ["analytics"]
                    lf_sensitivity: ["public"]
                  permissions: ["DESCRIBE", "CREATE_TABLE"]
        """
        # Mock S3 response
        mock_aws_clients['s3'].get_object.return_value = {
            'Body': MagicMock(read=lambda: config_with_tags.encode('utf-8'))
        }

        # Mock Lake Formation batch_grant_permissions response
        mock_aws_clients['lf'].batch_grant_permissions.return_value = {
            'Failures': []  # Empty list means success
        }

        # Mock Lake Formation list_permissions response
        mock_aws_clients['lf'].list_permissions.return_value = {
            'PrincipalResourcePermissions': []
        }

        # Mock Lake Formation tags
        mock_aws_clients['lf'].get_paginator.return_value.paginate.return_value = [{
            'LFTags': [
                {
                    'TagKey': 'lf_domain',
                    'TagValues': ['analytics']
                },
                {
                    'TagKey': 'lf_sensitivity',
                    'TagValues': ['public']
                }
            ]
        }]

        manager = PermissionsManager(TEST_ADMIN_ROLE_ARN)
        result = manager.process_permissions(TEST_BUCKET, TEST_KEY)

        # Verify the result
        assert result is not None
        assert result['status'] == 'success'
        assert 'changes' in result

        # Verify that batch_grant_permissions was called
        mock_aws_clients['lf'].batch_grant_permissions.assert_called_once()

        # Verify the changes were processed
        assert len(result['changes']['grant_permissions']) > 0
        assert len(result['changes']['revoke_permissions']) == 0

        # Log successful test completion
        logger.info("Process permissions with tags test passed")

    def test_07_process_permissions_grant_failure(self, aws_credentials, mock_aws_clients):
        """Test handling of grant permission failures"""
        config_with_tags = """
        permissions:
          - principal:
              arn: "arn:aws:iam::123456789012:role/analyst"
              DATABASE:
                - tags:
                    lf_domain: ["analytics"]
                    lf_sensitivity: ["public"]
                  permissions: ["DESCRIBE", "CREATE_TABLE"]
        """
        # Mock S3 response
        mock_aws_clients['s3'].get_object.return_value = {
            'Body': MagicMock(read=lambda: config_with_tags.encode('utf-8'))
        }

        # Mock Lake Formation batch_grant_permissions failure
        mock_aws_clients['lf'].batch_grant_permissions.return_value = {
            'Failures': [{
                'RequestEntry': {
                    'Id': '0',
                    'Principal': {
                        'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789012:role/analyst'
                    }
                },
                'Error': {
                    'ErrorCode': 'AccessDenied',
                    'ErrorMessage': 'User is not authorized to perform operation'
                }
            }]
        }

        manager = PermissionsManager(TEST_ADMIN_ROLE_ARN)
        with pytest.raises(Exception) as exc_info:
            manager.process_permissions(TEST_BUCKET, TEST_KEY)

        error_message = str(exc_info.value)

        # Check for the expected error message components
        assert 'Error during grant operations' in error_message
        assert 'Failed to grant permissions' in error_message
        assert 'User is not authorized to perform operation' in error_message
        assert 'arn:aws:iam::123456789012:role/analyst' in error_message

        # Verify the error contains the failure details
        failure_indicators = [
            '"id": "0"',
            '"principal":',
            '"error":'
        ]
        for indicator in failure_indicators:
            assert indicator in error_message

        logger.info("Grant permission failure test passed")

        # Optional: Verify the log messages
        mock_aws_clients['lf'].batch_grant_permissions.assert_called_once()

    def test_08_process_permissions_grant_failure_structure(self, aws_credentials, mock_aws_clients):
        """Test the complete structure of grant permission failures"""
        config_with_tags = """
        permissions:
          - principal:
              arn: "arn:aws:iam::123456789012:role/analyst"
              DATABASE:
                - tags:
                    lf_domain: ["analytics"]
                    lf_sensitivity: ["public"]
                  permissions: ["DESCRIBE", "CREATE_TABLE"]
        """
        mock_aws_clients['s3'].get_object.return_value = {
            'Body': MagicMock(read=lambda: config_with_tags.encode('utf-8'))
        }

        mock_aws_clients['lf'].batch_grant_permissions.return_value = {
            'Failures': [{
                'RequestEntry': {
                    'Id': '0',
                    'Principal': {
                        'DataLakePrincipalIdentifier': 'arn:aws:iam::123456789012:role/analyst'
                    }
                },
                'Error': {
                    'ErrorCode': 'AccessDenied',
                    'ErrorMessage': 'User is not authorized to perform operation'
                }
            }]
        }

        manager = PermissionsManager(TEST_ADMIN_ROLE_ARN)

        try:
            manager.process_permissions(TEST_BUCKET, TEST_KEY)
            pytest.fail("Expected PermissionError was not raised")
        except Exception as e:
            error_dict = json.loads(str(e).split("Failures: ")[1])
            assert isinstance(error_dict, list)
            assert len(error_dict) == 1
            assert error_dict[0]['id'] == '0'
            assert error_dict[0]['principal'] == 'arn:aws:iam::123456789012:role/analyst'
            assert error_dict[0]['error'] == 'User is not authorized to perform operation'

        logger.info("Grant permission failure structure test passed")


class TestLFTagValidator:

    @pytest.fixture
    def mock_lf_validator(self, mock_aws_clients):
        """Create LFTagValidator instance with mocked Lake Formation client"""
        return LFTagValidator(mock_aws_clients['lf'])

    def test_01_get_lf_tags_success(self, mock_lf_validator, mock_aws_clients):
        """Test successful retrieval of LF tags"""
        # Setup mock response
        mock_aws_clients['lf'].get_paginator.return_value.paginate.return_value = [{
            'LFTags': [
                {'TagKey': 'lf_domain', 'TagValues': ['analytics', 'marketing']},
                {'TagKey': 'lf_sensitivity', 'TagValues': ['public', 'confidential']}
            ]
        }]

        tags = mock_lf_validator.get_lf_tags()

        assert isinstance(tags, dict)
        assert 'lf_domain' in tags
        assert 'lf_sensitivity' in tags
        assert tags['lf_domain'] == {'analytics', 'marketing'}
        assert tags['lf_sensitivity'] == {'public', 'confidential'}
        logger.info("Get LF tags success test passed")

    def test_02_get_lf_tags_error(self, mock_lf_validator, mock_aws_clients):
        """Test error handling in get_lf_tags"""
        mock_aws_clients['lf'].get_paginator.return_value.paginate.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
            'ListLFTags'
        )

        with pytest.raises(Exception) as exc_info:
            mock_lf_validator.get_lf_tags()
        assert 'Failed to get LakeFormation tags' in str(exc_info.value)
        logger.info("Get LF tags error test passed")

    def test_03_validate_resource_tags_success(self, mock_lf_validator):
        """Test successful validation of resource tags"""
        # Set up mock tag cache
        mock_lf_validator.tag_cache = {
            'lf_domain': {'analytics', 'marketing'},
            'lf_sensitivity': {'public', 'confidential'}
        }

        valid_tags = {
            'lf_domain': ['analytics'],
            'lf_sensitivity': ['public']
        }

        # Should not raise an exception
        mock_lf_validator.validate_resource_tags(valid_tags)
        logger.info("Validate resource tags success test passed")

    def test_04_validate_resource_tags_invalid_key(self, mock_lf_validator):
        """Test validation with invalid tag key"""
        mock_lf_validator.tag_cache = {
            'lf_domain': {'analytics', 'marketing'},
            'lf_sensitivity': {'public', 'confidential'}
        }

        invalid_tags = {
            'invalid_key': ['value'],
            'lf_sensitivity': ['public']
        }

        with pytest.raises(ValidationError) as exc_info:
            mock_lf_validator.validate_resource_tags(invalid_tags)
        assert 'Invalid LakeFormation tag keys' in str(exc_info.value)
        logger.info("Validate resource tags invalid key test passed")

    def test_05_validate_resource_tags_invalid_value(self, mock_lf_validator):
        """Test validation with invalid tag value"""
        mock_lf_validator.tag_cache = {
            'lf_domain': {'analytics', 'marketing'},
            'lf_sensitivity': {'public', 'confidential'}
        }

        invalid_tags = {
            'lf_domain': ['invalid_value'],
            'lf_sensitivity': ['public']
        }

        with pytest.raises(ValidationError) as exc_info:
            mock_lf_validator.validate_resource_tags(invalid_tags)
        assert 'Invalid values for tag' in str(exc_info.value)
        logger.info("Validate resource tags invalid value test passed")

    def test_06_validate_config_tags_success(self, mock_lf_validator):
        """Test successful validation of configuration tags"""
        mock_lf_validator.tag_cache = {
            'lf_domain': {'analytics', 'marketing'},
            'lf_sensitivity': {'public', 'confidential'}
        }

        valid_config = {
            'permissions': [{
                'principal': {
                    'arn': 'arn:aws:iam::123456789012:role/analyst',
                    'DATABASE': [{
                        'tags': {
                            'lf_domain': ['analytics'],
                            'lf_sensitivity': ['public']
                        },
                        'permissions': ['DESCRIBE', 'CREATE_TABLE']
                    }]
                }
            }]
        }

        mock_lf_validator.validate_config_tags(valid_config)
        logger.info("Validate config tags success test passed")

    def test_07_validate_config_tags_invalid(self, mock_lf_validator):
        """Test validation of configuration with invalid tags"""
        mock_lf_validator.tag_cache = {
            'lf_domain': {'analytics', 'marketing'},
            'lf_sensitivity': {'public', 'confidential'}
        }

        invalid_config = {
            'permissions': [{
                'principal': {
                    'arn': 'arn:aws:iam::123456789012:role/analyst',
                    'DATABASE': [{
                        'tags': {
                            'invalid_key': ['value'],
                            'lf_sensitivity': ['invalid_value']
                        },
                        'permissions': ['DESCRIBE', 'CREATE_TABLE']
                    }]
                }
            }]
        }

        with pytest.raises(ValidationError) as exc_info:
            mock_lf_validator.validate_config_tags(invalid_config)
        assert 'Invalid LakeFormation tag keys' in str(exc_info.value)
        logger.info("Validate config tags invalid test passed")

    def test_08_validate_empty_config(self, mock_lf_validator):
        """Test validation of empty configuration"""
        empty_config = {
            'permissions': []
        }

        # Should not raise an exception
        mock_lf_validator.validate_config_tags(empty_config)
        logger.info("Validate empty config test passed")

    def test_09_tag_cache_behavior(self, mock_lf_validator, mock_aws_clients):
        """Test tag cache behavior"""
        # First call should query Lake Formation
        tags1 = mock_lf_validator.get_lf_tags()

        # Second call should use cache
        tags2 = mock_lf_validator.get_lf_tags()

        # Both should return same data
        assert tags1 == tags2

        # Clear cache
        mock_lf_validator.clear_cache()
        assert mock_lf_validator.tag_cache == {}

        logger.info("Tag cache behavior test passed")

    def test_10_invalid_tag_structure(self, mock_lf_validator):
        """Test validation with invalid tag structure"""
        mock_lf_validator.tag_cache = {
            'lf_domain': {'analytics', 'marketing'}
        }

        invalid_structures = [
            {'lf_domain': 'not_a_list'},  # Value should be a list
            {'lf_domain': [1, 2, 3]},  # Values should be strings
            {123: ['analytics']},  # Key should be string
            None,  # Tags should be dict
            []  # Tags should be dict
        ]

        for invalid_tags in invalid_structures:
            with pytest.raises(ValidationError):
                mock_lf_validator.validate_resource_tags(invalid_tags)

        logger.info("Invalid tag structure test passed")

class TestConfigValidator:

    @pytest.fixture
    def valid_config(self):
        return {
            "permissions": [
                {
                    "principal": {
                        "arn": "arn:aws:iam::123456789012:role/analyst",
                        "DATABASE": [
                            {
                                "tags": {
                                    "lf_domain": ["analytics"],
                                    "lf_sensitivity": ["public"]
                                },
                                "permissions": ["DESCRIBE", "CREATE_TABLE"]
                            }
                        ],
                        "TABLE": [
                            {
                                "tags": {
                                    "lf_domain": ["analytics"],
                                    "lf_sensitivity": ["confidential"]
                                },
                                "permissions": ["SELECT", "DESCRIBE"]
                            }
                        ]
                    }
                }
            ]
        }

    def test_01_validate_tags_structure_success(self):
        """Test successful validation of tags structure"""
        valid_tags = {
            "lf_domain": ["analytics"],
            "lf_sensitivity": ["public"]
        }
        # Should not raise an exception
        ConfigValidator.validate_tags_structure(valid_tags)
        logger.info("Validate tags structure success test passed")

    def test_02_validate_tags_structure_invalid(self):
        """Test validation of invalid tags structure"""
        invalid_tags = [
            {"lf_domain": "not_a_list"},
            {"lf_domain": [1, 2, 3]},
            {123: ["analytics"]},
            "not_a_dict"
        ]
        for tags in invalid_tags:
            with pytest.raises(ValidationError):
                ConfigValidator.validate_tags_structure(tags)
        logger.info("Validate tags structure invalid test passed")

    def test_03_validate_resource_structure_success(self):
        """Test successful validation of resource structure"""
        valid_resource = {
            "tags": {
                "lf_domain": ["analytics"],
                "lf_sensitivity": ["public"]
            },
            "permissions": ["DESCRIBE", "CREATE_TABLE"]
        }
        # Should not raise an exception
        ConfigValidator.validate_resource_structure(valid_resource, "DATABASE")
        logger.info("Validate resource structure success test passed")

    def test_04_validate_resource_structure_invalid(self):
        """Test validation of invalid resource structure"""
        invalid_resources = [
            {},  # Empty dictionary is valid (for revoking all permissions)
            {"tags": {}},  # Missing permissions
            {"permissions": []},  # Missing tags
            {"tags": {}, "permissions": "not_a_list"},
            "not_a_dict"
        ]
        for resource in invalid_resources:
            if resource != {}:  # Empty dict is valid
                with pytest.raises(ValidationError):
                    ConfigValidator.validate_resource_structure(resource, "DATABASE")
        logger.info("Validate resource structure invalid test passed")

    def test_05_validate_principal_structure_success(self, valid_config):
        """Test successful validation of principal structure"""
        valid_principal = valid_config["permissions"][0]["principal"]
        # Should not raise an exception
        ConfigValidator.validate_principal_structure(valid_principal)
        logger.info("Validate principal structure success test passed")

    def test_06_validate_principal_structure_invalid(self):
        """Test validation of invalid principal structure"""
        invalid_principals = [
            {},  # Missing arn
            {"arn": "invalid_arn"},  # Invalid ARN format
            {"arn": "arn:aws:iam::123456789012:role/analyst", "INVALID": []},  # Invalid resource type
            {"arn": "arn:aws:iam::123456789012:role/analyst", "DATABASE": "not_a_list"},
            {"arn": "arn:aws:iam::123456789012:role/analyst", "TABLE": "not_a_list"},
            "not_a_dict"
        ]
        for principal in invalid_principals:
            with pytest.raises(ValidationError):
                ConfigValidator.validate_principal_structure(principal)
        logger.info("Validate principal structure invalid test passed")

    def test_07_check_duplicate_principals_success(self, valid_config):
        """Test successful check for duplicate principals"""
        # Should not raise an exception
        ConfigValidator.check_duplicate_principals(valid_config)
        logger.info("Check duplicate principals success test passed")

    def test_08_check_duplicate_principals_invalid(self):
        """Test check for duplicate principals with invalid config"""
        invalid_config = {
            "permissions": [
                {"principal": {"arn": "arn:aws:iam::123456789012:role/analyst"}},
                {"principal": {"arn": "arn:aws:iam::123456789012:role/analyst"}}  # Duplicate
            ]
        }
        with pytest.raises(ValidationError) as exc_info:
            ConfigValidator.check_duplicate_principals(invalid_config)
        assert "Duplicate principal ARNs found" in str(exc_info.value)
        logger.info("Check duplicate principals invalid test passed")

    def test_09_validate_config_success(self, valid_config):
        """Test successful validation of entire configuration"""
        # Should not raise an exception
        ConfigValidator.validate_config(valid_config)
        logger.info("Validate config success test passed")

    def test_10_validate_config_invalid(self):
        """Test validation of invalid configurations"""
        invalid_configs = [
            {},  # Empty config
            {"permissions": "not_a_list"},
            {"permissions": [{}]},  # Missing principal
            {"permissions": [{"principal": {}}]},  # Missing ARN in principal
            "not_a_dict"
        ]
        for config in invalid_configs:
            with pytest.raises(ValidationError):
                ConfigValidator.validate_config(config)
        logger.info("Validate config invalid test passed")

    def test_11_validate_empty_permissions(self):
        """Test validation of configuration with empty permissions"""
        empty_permissions_config = {
            "permissions": []
        }
        # Should not raise an exception
        ConfigValidator.validate_config(empty_permissions_config)
        logger.info("Validate empty permissions test passed")

    def test_12_validate_config_with_revoke_all(self):
        """Test validation of configuration with revoke all permissions"""
        revoke_all_config = {
            "permissions": [
                {
                    "principal": {
                        "arn": "arn:aws:iam::123456789012:role/analyst",
                        "DATABASE": [],
                        "TABLE": []
                    }
                }
            ]
        }
        # Should not raise an exception
        ConfigValidator.validate_config(revoke_all_config)
        logger.info("Validate config with revoke all permissions test passed")


class TestPermissionsValidator:

    @pytest.fixture
    def valid_config(self):
        """Fixture providing a valid configuration"""
        return {
            "permissions": [
                {
                    "principal": {
                        "arn": "arn:aws:iam::123456789012:role/analyst",
                        "DATABASE": [
                            {
                                "tags": {
                                    "lf_domain": ["analytics"],
                                    "lf_sensitivity": ["public"]
                                },
                                "permissions": ["DESCRIBE", "CREATE_TABLE"]
                            }
                        ],
                        "TABLE": [
                            {
                                "tags": {
                                    "lf_domain": ["analytics"],
                                    "lf_sensitivity": ["public"]
                                },
                                "permissions": ["SELECT", "DESCRIBE"]
                            }
                        ]
                    }
                }
            ]
        }

    def test_01_validate_database_permissions_success(self):
        """Test successful validation of database permissions"""
        valid_permissions = [perm.value for perm in DatabasePermissionTypes]
        # Should not raise an exception
        PermissionsValidator.validate_database_permissions(valid_permissions)
        logger.info("Validate database permissions success test passed")

    def test_02_validate_database_permissions_invalid(self):
        """Test validation of invalid database permissions"""
        invalid_permissions = [
            ["INVALID_PERMISSION"],
            ["SELECT"],  # Valid for table but not database
            ["CREATE_TABLE", "INVALID"],
            ["DROP", "DELETE"]  # DELETE is for table only
        ]

        for permissions in invalid_permissions:
            with pytest.raises(ValidationError) as exc_info:
                PermissionsValidator.validate_database_permissions(permissions)
            assert "Invalid database permissions" in str(exc_info.value)

        logger.info("Validate database permissions invalid test passed")

    def test_03_validate_table_permissions_success(self):
        """Test successful validation of table permissions"""
        valid_permissions = [perm.value for perm in TablePermissionTypes]
        # Should not raise an exception
        PermissionsValidator.validate_table_permissions(valid_permissions)
        logger.info("Validate table permissions success test passed")

    def test_04_validate_table_permissions_invalid(self):
        """Test validation of invalid table permissions"""
        invalid_permissions = [
            ["INVALID_PERMISSION"],
            ["CREATE_TABLE"],  # Valid for database but not table
            ["SELECT", "INVALID"],
            ["INSERT", "EXECUTE"]  # EXECUTE is not a valid permission
        ]

        for permissions in invalid_permissions:
            with pytest.raises(ValidationError) as exc_info:
                PermissionsValidator.validate_table_permissions(permissions)
            assert "Invalid table permissions" in str(exc_info.value)

        logger.info("Validate table permissions invalid test passed")

    def test_05_validate_config_permissions_success(self, valid_config):
        """Test successful validation of configuration permissions"""
        # Should not raise an exception
        PermissionsValidator.validate_config_permissions(valid_config)
        logger.info("Validate config permissions success test passed")

    def test_06_validate_config_permissions_invalid_database(self):
        """Test validation of configuration with invalid database permissions"""
        invalid_config = {
            "permissions": [
                {
                    "principal": {
                        "arn": "arn:aws:iam::123456789012:role/analyst",
                        "DATABASE": [
                            {
                                "tags": {
                                    "lf_domain": ["analytics"]
                                },
                                "permissions": ["INVALID_PERMISSION", "CREATE_TABLE"]
                            }
                        ]
                    }
                }
            ]
        }

        with pytest.raises(ValidationError) as exc_info:
            PermissionsValidator.validate_config_permissions(invalid_config)
        assert "Invalid database permissions" in str(exc_info.value)
        logger.info("Validate config permissions invalid database test passed")

    def test_07_validate_config_permissions_invalid_table(self):
        """Test validation of configuration with invalid table permissions"""
        invalid_config = {
            "permissions": [
                {
                    "principal": {
                        "arn": "arn:aws:iam::123456789012:role/analyst",
                        "TABLE": [
                            {
                                "tags": {
                                    "lf_domain": ["analytics"]
                                },
                                "permissions": ["INVALID_PERMISSION", "SELECT"]
                            }
                        ]
                    }
                }
            ]
        }

        with pytest.raises(ValidationError) as exc_info:
            PermissionsValidator.validate_config_permissions(invalid_config)
        assert "Invalid table permissions" in str(exc_info.value)
        logger.info("Validate config permissions invalid table test passed")

    def test_08_validate_empty_permissions(self):
        """Test validation of empty permissions lists"""
        empty_config = {
            "permissions": [
                {
                    "principal": {
                        "arn": "arn:aws:iam::123456789012:role/analyst",
                        "DATABASE": [],
                        "TABLE": []
                    }
                }
            ]
        }
        # Should not raise an exception (empty lists are valid for revoking all permissions)
        PermissionsValidator.validate_config_permissions(empty_config)
        logger.info("Validate empty permissions test passed")

    def test_09_validate_mixed_permissions(self):
        """Test validation of configuration with both valid and invalid permissions"""
        mixed_config = {
            "permissions": [
                {
                    "principal": {
                        "arn": "arn:aws:iam::123456789012:role/analyst",
                        "DATABASE": [
                            {
                                "tags": {"lf_domain": ["analytics"]},
                                "permissions": ["CREATE_TABLE"]  # Valid
                            },
                            {
                                "tags": {"lf_domain": ["marketing"]},
                                "permissions": ["INVALID"]  # Invalid
                            }
                        ]
                    }
                }
            ]
        }

        with pytest.raises(ValidationError):
            PermissionsValidator.validate_config_permissions(mixed_config)
        logger.info("Validate mixed permissions test passed")

    def test_10_validate_all_permission_types(self):
        """Test validation of all possible permission types"""
        all_permissions_config = {
            "permissions": [
                {
                    "principal": {
                        "arn": "arn:aws:iam::123456789012:role/analyst",
                        "DATABASE": [
                            {
                                "tags": {"lf_domain": ["analytics"]},
                                "permissions": [perm.value for perm in DatabasePermissionTypes]
                            }
                        ],
                        "TABLE": [
                            {
                                "tags": {"lf_domain": ["analytics"]},
                                "permissions": [perm.value for perm in TablePermissionTypes]
                            }
                        ]
                    }
                }
            ]
        }
        # Should not raise an exception
        PermissionsValidator.validate_config_permissions(all_permissions_config)
        logger.info("Validate all permission types test passed")

    def test_11_validate_permissions_case_sensitivity(self):
        """Test validation of permissions with different cases"""
        case_sensitive_config = {
            "permissions": [
                {
                    "principal": {
                        "arn": "arn:aws:iam::123456789012:role/analyst",
                        "DATABASE": [
                            {
                                "tags": {"lf_domain": ["analytics"]},
                                "permissions": ["create_table", "DESCRIBE", "Create_Table"]
                            }
                        ]
                    }
                }
            ]
        }

        with pytest.raises(ValidationError):
            PermissionsValidator.validate_config_permissions(case_sensitive_config)
        logger.info("Validate permissions case sensitivity test passed")

    def test_12_validate_duplicate_permissions(self):
        """Test validation of duplicate permissions"""
        duplicate_permissions_config = {
            "permissions": [
                {
                    "principal": {
                        "arn": "arn:aws:iam::123456789012:role/analyst",
                        "TABLE": [
                            {
                                "tags": {"lf_domain": ["analytics"]},
                                "permissions": ["SELECT", "DESCRIBE", "SELECT"]
                            }
                        ]
                    }
                }
            ]
        }
        # Should not raise an exception (duplicates are allowed but will be handled during processing)
        PermissionsValidator.validate_config_permissions(duplicate_permissions_config)
        logger.info("Validate duplicate permissions test passed")



if __name__ == '__main__':
    pytest.main(['-v', '--log-cli-level=INFO', '-s', 'tests/test_lf_tag_permission.py::TestPermissionsManager'])
