from argcomplete import CompletionFinder


class CustomCompletionFinder(CompletionFinder):
    def filter_completions(self, completions):
        """This custom filter prevent displaying options unless all propositions are options"""
        completions = super(CustomCompletionFinder, self).filter_completions(completions)
        if not all([c.startswith("-") for c in completions]):
            completions = [c for c in completions if not c.startswith("-")]
        return sorted(completions)
