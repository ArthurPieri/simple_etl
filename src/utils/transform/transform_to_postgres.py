# pylint: disable=import-error, no-name-in-module, too-few-public-methods, relative-beyond-top-level
from ..interfaces.transform_interface import TransformInterface


class TransformPostgres(TransformInterface):
    """
    Makes all the treatment to load data into postgres
    """

    def transform(  # pylint: disable=dangerous-default-value
        self,
        data: list[dict],
        columns_to_drop: list = [],
        columns_to_rename: dict = {},
        **kwargs  # pylint: disable=unused-argument
    ) -> list[dict]:
        """
        Transform data to be sent to postgres
        """
        if columns_to_drop:
            data = self._drop_columns(data, columns_to_drop)

        if columns_to_rename:
            data = self._rename_columns(data, columns_to_rename)

        columns, data = self._treat_column_names(data)

        return columns, data
