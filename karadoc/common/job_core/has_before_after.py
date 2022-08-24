class HasBeforeAfter:
    def __init__(self) -> None:
        # Attributes that the user is not supposed to change
        self.__before_each = None
        self.__after_each = None
        self.__before_all = None
        self.__after_all = None

    def before_each(self) -> None:
        """This method will be called before executing each action method defined in the job's definition file.

        This is similar to unittest's `setup()` method.
        """
        if self.__before_each is not None:
            return self.__before_each()

    def after_each(self) -> None:
        """This method will be called after executing each action method defined in the job's definition file.

        This is similar to unittest's `teardown()` method.
        """
        if self.__after_each is not None:
            return self.__after_each()

    def before_all(self) -> None:
        """This method will be called once before executing all the action methods defined in the job's definition file.

        This is similar to unittest's `setupClass()` method.
        """
        if self.__before_all is not None:
            return self.__before_all()

    def after_all(self) -> None:
        """This method will be called once after executing all the action methods defined in the job's definition file.

        This is similar to unittest's `teardownClass()` method.
        """
        if self.__after_all is not None:
            return self.__after_all()
