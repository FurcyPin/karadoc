from karadoc.common.job_core.package import OptionalMethod


class HasBeforeAfter:
    def __init__(self):
        def default() -> None:
            pass

        self.before_each = OptionalMethod(default, "before_each")
        self.after_each = OptionalMethod(default, "after_each")
        self.before_all = OptionalMethod(default, "before_all")
        self.after_all = OptionalMethod(default, "after_all")

    def before_each(self) -> None:
        """This method will be called before executing each action method defined in the job's definition file.

        This is similar to unittest's `setup()` method.
        """
        pass

    def after_each(self) -> None:
        """This method will be called after executing each action method defined in the job's definition file.

        This is similar to unittest's `teardown()` method.
        """
        pass

    def before_all(self) -> None:
        """This method will be called once before executing all the action methods defined in the job's definition file.

        This is similar to unittest's `setupClass()` method.
        """
        pass

    def after_all(self) -> None:
        """This method will be called once after executing all the action methods defined in the job's definition file.

        This is similar to unittest's `teardownClass()` method.
        """
        pass
