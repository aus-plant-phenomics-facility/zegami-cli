# Copyright 2018 Zegami Ltd

"""Config tests."""

import unittest
from unittest.mock import patch, mock_open, MagicMock, ANY

from jsonschema import exceptions as jx

from .. import (
    config,
    log,
)

logger = log.Logger(False)
logger.error = MagicMock(name='error')


class TestValidateConfig(unittest.TestCase):

    def _get_configuration(self, data):
        logger.error.reset_mock()
        with patch('builtins.open', mock_open(read_data=data)):
            return config.load_config('foo')

    def test_file_upload_path(self):
        config_data = """
            dataset_type: file
            file_config:
                path: test.csv
        """

        configuration = self._get_configuration(config_data)

        try:
            config.validate_config(configuration, logger)
        except jx.ValidationError:
            self.fail('Failed validation')

    def test_file_upload_directory(self):
        config_data = """
            dataset_type: file
            file_config:
                directory: test
        """

        configuration = self._get_configuration(config_data)

        try:
            config.validate_config(configuration, logger)
        except jx.ValidationError:
            self.fail('Failed validation')

    def test_unknown_configuration(self):
        config_data = """
            foo: bar
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'file_config' is a required property",
        )

    def test_unknown_dataset_type(self):
        config_data = """
            dataset_type: foo
            file_config:
                path: test
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'foo' is not one of ['file']",
        )

    def test_file_dataset_multiple_valid(self):
        config_data = """
            dataset_type: foo
            file_config:
                path: test
                directory: test
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err=(
                "{'path': 'test', 'directory': 'test'} is valid under each"
                " of {'required': ['directory']}, {'required': ['path']}"
            ),
        )

    def test_file_dataset_multiple_invalid(self):
        config_data = """
            dataset_type: foo
            file_config:
                path: test
                foo: test
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'foo' is not one of ['file']",
        )

    def test_file_unknown_config(self):
        config_data = """
            dataset_type: file
            foo_config:
                path: test
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'file' is not one of ['sql']",
        )

    def test_file_config_missing(self):
        config_data = """
            dataset_type: file
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'file' is not one of ['sql']",
        )

    def test_file_config_invalid_filetype(self):
        config_data = """
            dataset_type: file
            file_config:
                path: mock/path.unsupported.exe
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err=(
                "'mock/path.unsupported.exe' does not match "
                "'\\\\.(tab|tsv|csv|xlsx)$'"
            ),
        )

    def test_file_config_missing_value(self):
        config_data = """
            dataset_type: file
            file_config:
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err=(
                "None is valid under each of {'required': ['directory']},"
                " {'required': ['path']}"
            ),
        )

    def test_file_config_unknown(self):
        config_data = """
            dataset_type: file
            file_config:
                foo: test
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'path' is a required property",
        )

    def test_sql_config(self):
        config_data = """
            dataset_type: sql
            sql_config:
                connection: test
                query: test
        """

        configuration = self._get_configuration(config_data)

        try:
            config.validate_config(configuration, logger)
        except jx.ValidationError:
            self.fail('Failed validation')

    def test_sql_missing_config(self):
        config_data = """
            dataset_type: sql
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'sql' is not one of ['file']",
        )

    def test_sql_unknown_config(self):
        config_data = """
            dataset_type: sql
            foo_config:
                connection: test
                query: test
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'sql' is not one of ['file']",
        )

    def test_sql_config_missing_connection(self):
        config_data = """
            dataset_type: sql
            sql_config:
                query: test
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'connection' is a required property",
        )

    def test_sql_config_missing_query(self):
        config_data = """
            dataset_type: sql
            sql_config:
                connection: test
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'query' is a required property",
        )

    def test_image_config(self):
        config_data = """
            imageset_type: file
            file_config:
                paths:
                    - image.jpg
                    - a/directory/path
            collection_id: 5ad3a99b75f3b30001732f36
            dataset_id: 5ad3a99b75f3b30001732f36
            dataset_column: foo
        """

        configuration = self._get_configuration(config_data)

        try:
            config.validate_config(configuration, logger)
        except jx.ValidationError:
            self.fail('Failed validation')

    def test_image_config_file_config_missing(self):
        config_data = """
            imageset_type: file
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'file_config' is a required property",
        )

    def test_image_config_invalid_type(self):
        config_data = """
            imageset_type: foo
            file_config:
                paths:
                    - image.jpg
            collection_id: 5ad3a99b75f3b30001732f36
            dataset_id: 5ad3a99b75f3b30001732f36
            dataset_column: foo
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'path' is a required property",
        )

    def test_image_config_invalid_paths(self):
        config_data = """
            imageset_type: file
            file_config:
                paths:
                    - 123
            collection_id: 5ad3a99b75f3b30001732f36
            dataset_id: 5ad3a99b75f3b30001732f36
            dataset_column: foo
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="123 is not of type 'string'",
        )

    def test_image_config_missing_config(self):
        config_data = """
            imageset_type: file
            collection_id: 5ad3a99b75f3b30001732f36
            dataset_id: 5ad3a99b75f3b30001732f36
            dataset_column: foo
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'file_config' is a required property",
        )

    def test_image_config_missing_collection(self):
        config_data = """
            imageset_type: file
            file_config:
                paths:
                    - image.jpg
            dataset_id: 5ad3a99b75f3b30001732f36
            dataset_column: foo
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'path' is a required property",
        )

    def test_image_config_missing_dataset(self):
        config_data = """
            imageset_type: file
            file_config:
                paths:
                    - image.jpg
                    - a/directory/path
            collection_id: 5ad3a99b75f3b30001732f36
            dataset_column: foo
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'path' is a required property",
        )

    def test_image_config_missing_dataset_column(self):
        config_data = """
            imageset_type: file
            file_config:
                paths:
                    - image.jpg
                    - a/directory/path
            collection_id: 5ad3a99b75f3b30001732f36
            dataset_id: 5ad3a99b75f3b30001732f36
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'path' is a required property",
        )

    def test_collection_publish(self):
        config_data = """
            update_type: publish
            publish_config:
                publish: true
                destination_project: foo
        """

        configuration = self._get_configuration(config_data)

        try:
            config.validate_config(configuration, logger)
        except jx.ValidationError:
            self.fail('Failed validation')

    def test_collection_update_unknown(self):
        config_data = """
            update_type: foo
            publish_config:
                publish: true
                destination_project: foo
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'file_config' is a required property",
        )

    def test_collection_publish_config_missing(self):
        config_data = """
            update_type: publish
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'file_config' is a required property",
        )

    def test_collection_publish_publish_missing(self):
        config_data = """
            update_type: publish
            publish_config:
                destination_project: foo
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'publish' is a required property",
        )

    def test_collection_publish_destination_missing(self):
        config_data = """
            update_type: publish
            publish_config:
                publish: true
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'destination_project' is a required property",
        )

    def test_collection_publish_incorrect_publish_type(self):
        config_data = """
            update_type: publish
            publish_config:
                publish: foo
                destination_project: foo
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="'foo' is not of type 'boolean'",
        )

    def test_collection_publish_incorrect_destination_type(self):
        config_data = """
            update_type: publish
            publish_config:
                publish: true
                destination_project: 123
        """

        configuration = self._get_configuration(config_data)

        with self.assertRaises(jx.ValidationError):
            config.validate_config(configuration, logger)

        logger.error.assert_called_with(
            ANY,
            err="123 is not of type 'string'",
        )
