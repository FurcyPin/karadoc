from typing import List, Optional, Tuple, Union


class HasKeys:
    def __init__(self) -> None:
        # Attributes that the user may change
        self.__primary_key: Optional[Union[Tuple[str, ...], str]] = None
        """Primary_key is a column or group of columns that must be unique and non-null.
        It is automatically used by the nonreg to perform joins."""
        self.__standardized_primary_key: Optional[Union[Tuple[str, ...]]] = None

        self.__secondary_keys: List[Union[Tuple[str, ...], str]] = []
        """Secondary_keys is a list of additional columns or group of columns that must be unique, non-null"""
        self.__standardized_secondary_keys: List[Tuple[str, ...]] = []

    @staticmethod
    def __calculate_standardized_key(new_key: Union[Tuple[str, ...], str]) -> Tuple[str, ...]:
        if isinstance(new_key, str):
            return (new_key,)
        elif isinstance(new_key, tuple):
            return new_key
        else:
            raise TypeError("Key type must be string or tuple[str,...]")

    @property
    def primary_key(self) -> Optional[Union[Tuple[str, ...], str]]:
        """Primary key for the job: must be non-null and unique.

        It is recommended to use 'job.standardized_primary_key' rather than this getter, unless if it is to get
        the raw primary_key definition set by the user.

        For the sake of keeping the code simpler, standardized_primary_key is defined automatically from primary_key.
        It is a transformed representation of primary_key used as parameters for some functions.
        """
        return self.__primary_key

    @primary_key.setter
    def primary_key(self, new_primary_key: Union[Tuple[str, ...], str]) -> None:
        """Use this to declare the job's primary_key.

        Expected format is a string or a tuple of strings for composite keys
        Examples:
            - job.primary_key = "column_name_1"
            - job.primary_key = ("column_name_1", "column_name_2")

        For the sake of keeping the code simpler, standardized_primary_key is defined
        automatically from primary_key. It is a transformed representation of primary_key used as parameters
        for some functions.

        """
        self.__primary_key = new_primary_key
        self.__standardized_primary_key = self.__calculate_standardized_key(new_primary_key)

    @property
    def standardized_primary_key(self) -> Optional[Tuple[str, ...]]:
        """Primary key for the job in a standardized format.

        Examples:
        >>> from karadoc.common import Job
        >>> job = Job()
        >>> job.primary_key = 'column_name_1'
        >>> job.standardized_primary_key
        ('column_name_1',)

        >>> job.primary_key = ('column_name_1', 'column_name_2')
        >>> job.standardized_primary_key
        ('column_name_1', 'column_name_2')
        """
        return self.__standardized_primary_key

    @property
    def secondary_keys(self) -> List[Union[Tuple[str, ...], str]]:
        """Secondary keys for the job: must be non-null and unique.

        It is recommended to use 'job.standardized_secondary_keys' rather than this getter, unless if it is to get
        the raw secondary_keys definition set by the user.

        For the sake of keeping the code simpler, standardized_secondary_keys is defined automatically
        from secondary_keys. It is a transformed representation of secondary_keys used as parameters for some functions.
        """
        return self.__secondary_keys

    @secondary_keys.setter
    def secondary_keys(self, new_secondary_keys: List[Union[Tuple[str, ...], str]]) -> None:
        """Use this to declare the job's secondary_keys.
        Expected format is List(Union[Tuple[str, ...], str])

        Examples:
            - secondary_keys = ["column_name_1", "column_name_2" , "column_name_3"]
            - secondary_keys = [("column_name_1", "column_name_2") , "column_name_3" ]
            - secondary_keys = [("column_name_1", "column_name_2") , ("column_name_3", "column_name_4")]

        For the sake of keeping the code simpler, standardized_secondary_keys are defined
        automatically from secondary_keys. They are a transformed representation of secondary_keys used as parameters
        for functions.

        Examples:
            if secondary_keys = [("column_name_1", "column_name_2") , "column_name_2" ]
            then standardized_secondary_keys = [("column_name_1", "column_name_2") , ("column_name_2",)]

        """
        self.__secondary_keys = new_secondary_keys
        self.__standardized_secondary_keys = []
        for key in new_secondary_keys:
            self.__standardized_secondary_keys.append(self.__calculate_standardized_key(key))

    @property
    def standardized_secondary_keys(self) -> List[Tuple[str, ...]]:
        return self.__standardized_secondary_keys
