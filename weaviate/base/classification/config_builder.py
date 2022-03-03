"""
BaseConfigBuilder class definition.
"""
from abc import abstractmethod, ABC
from typing import Dict, Any
from weaviate.util import capitalize_first_letter


class BaseConfigBuilder(ABC):
    """
    BaseConfigBuild abstract class that is used to configure a classification process.
    Sync/Async Client should implement each its own ConfigBuilder.
    """

    def __init__(self):
        """
        Initialize a ConfigBuilder class instance.
        """

        self._config: Dict[str, Any] = {}
        self._wait_for_completion = False

    def with_type(self, type: str) -> 'BaseConfigBuilder':
        """
        Set classification type.

        Parameters
        ----------
        type : str
            Type of the desired classification.

        Returns
        -------
        BaseConfigBuilder
            Updated BaseConfigBuilder.
        """

        self._config['type'] = type
        return self

    def with_k(self, k: int) -> 'BaseConfigBuilder':
        """
        Set k number for the kNN.

        Parameters
        ----------
        k : int
            Number of objects to use to make a classification guess.
            (For kNN)

        Returns
        -------
        BaseConfigBuilder
            Updated BaseConfigBuilder.
        """

        if 'settings' not in self._config:
            self._config['settings'] = {'k': k}
        else:
            self._config['settings']['k'] = k
        return self

    def with_class_name(self, class_name: str) -> 'BaseConfigBuilder':
        """
        What Object type to classify.

        Parameters
        ----------
        class_name : str
            Name of the class to be classified.

        Returns
        -------
        BaseConfigBuilder
            Updated BaseConfigBuilder.
        """

        self._config['class'] = capitalize_first_letter(class_name)
        return self

    def with_classify_properties(self, classify_properties: list) -> 'BaseConfigBuilder':
        """
        Set the classify properties.

        Parameters
        ----------
        classify_properties: list
            A list of properties to classify.

        Returns
        -------
        BaseConfigBuilder
            Updated BaseConfigBuilder.
        """

        self._config['classifyProperties'] = classify_properties
        return self

    def with_based_on_properties(self, based_on_properties: list) -> 'BaseConfigBuilder':
        """
        Set properties to build the classification on.

        Parameters
        ----------
        based_on_properties: list
            A list of properties to classify on.

        Returns
        -------
        BaseConfigBuilder
            Updated BaseConfigBuilder.
        """

        self._config['basedOnProperties'] = based_on_properties
        return self

    def with_source_where_filter(self, filter: dict) -> 'BaseConfigBuilder':
        """
        Set Source 'where' Filter.

        Parameters
        ----------
        filter : dict
            Filter to use, as a dict.

        Returns
        -------
        BaseConfigBuilder
            Updated BaseConfigBuilder.
        """

        if 'filters' not in self._config:
            self._config['filters'] = {}
        self._config['filters']['sourceWhere'] = filter
        return self

    def with_training_set_where_filter(self, filter: dict) -> 'BaseConfigBuilder':
        """
        Set Training set 'where' Filter.

        Parameters
        ----------
        filter : dict
            Filter to use, as a dict.

        Returns
        -------
        BaseConfigBuilder
            Updated BaseConfigBuilder.
        """

        if 'filters' not in self._config:
            self._config['filters'] = {}
        self._config['filters']['trainingSetWhere'] = filter
        return self

    def with_target_where_filter(self, filter: dict) -> 'BaseConfigBuilder':
        """
        Set Target 'where' Filter.

        Parameters
        ----------
        filter : dict
            Filter to use, as a dict.

        Returns
        -------
        BaseConfigBuilder
            Updated BaseConfigBuilder.
        """

        if 'filters' not in self._config:
            self._config['filters'] = {}
        self._config['filters']['targetWhere'] = filter
        return self

    def with_wait_for_completion(self) -> 'BaseConfigBuilder':
        """
        Wait for completion.

        Returns
        -------
        BaseConfigBuilder
            Updated BaseConfigBuilder.
        """

        self._wait_for_completion = True
        return self

    def with_settings(self, settings: dict) -> 'BaseConfigBuilder':
        """
        Set settings for the classification. NOTE if you are using 'kNN' the value 'k' can be set
        by this method or by 'with_k(...)'. This method keeps previously set 'settings'.

        Parameters
        ----------
        settings: dict
            Additional settings to be set/overwritten.

        Returns
        -------
        BaseConfigBuilder
            Updated BaseConfigBuilder.

        Raises
        ------
        TypeError
            If 'settings' is not of type 'dict'.
        """

        if not isinstance(settings, dict):
            raise TypeError(
                f"'settings' must be of type 'dict'. Given type: {type(settings)}."
            )

        if 'settings' not in self._config:
            self._config['settings'] = settings
        else:
            for key in settings:
                self._config['settings'][key] = settings[key]
        return self

    def _validate_config(self) -> None:
        """
        Validate the current classification configuration.

        Raises
        ------
        ValueError
            If a mandatory field is not set.
        """

        required_fields = ['type', 'class', 'basedOnProperties', 'classifyProperties']
        for field in required_fields:
            if field not in self._config:
                raise ValueError(
                    f"'{field}' is not set for this classification."
                )

        if self._config['type'] == 'knn':
            if 'k' not in self._config.get('settings', []):
                raise ValueError(
                    "'k' is not set for this classification."
                )

    @abstractmethod
    def do(self):
        """
        Start the classification. Sync/Async child should implement each its own 'do' method.
        """
        ...
