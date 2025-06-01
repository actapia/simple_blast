from collections import defaultdict
from typing import Any, Optional, Iterable, Iterator

class Command:
    """A mutable command to be executed with subprocess.

    In this model of a command, there are "positional" arguments, which are
    single entries in the argv, and there are "named" arguments, which are also
    simply called "arguments." A named argument consists of one or more
    consecutive entries in the argv, the first of which is referred to as the
    name of the argument, and the remaining entries are the value associated
    with the name.

    Unlike in a dictionary, a named argument may appear more than once in the
    command with different values for each instance.
    """
    def __init__(self):
        self._dict = {}
        self._mult = defaultdict(int)
        self._cnt = 0

    def add_positional_argument(self, arg: Any):
        """Append the given argument to the command."""
        self._dict[self._cnt] = (arg,)
        self._cnt += 1

    def add_argument(self, name: str, *values):
        """Append the named argument to the command."""
        self._dict[(name, self._mult[name])] = values
        self._mult[name] += 1

    def __getitem__(self, name: str) -> list[tuple]:
        """Get all values associated with the given name."""
        return [self._dict[(name, c)] for c in range(self._mult[name])]

    def get(self, name: str, cnt: Optional[int] = None) -> tuple:
        """Get the values associated with one instance of the name.

        If the second parameter, cnt, is not specified, and there is only one
        instance of the named argument, then this function returns the value
        associated with the first and only instance.

        Parameters:
            name (str): The name of the argument for which to get the values.
            cnt (int):  A number identifying the instance of the argument.
        """
        if cnt is None:
            if self._mult[name] == 1:
                cnt = 0
            else:
                raise KeyError(
                    ("More than one argument named {}, "
                     "must specify which to get.").format(name)
                )
        if cnt >= self._mult[name]:
            raise KeyError(f"Value {cnt} for {name} does not exist.")
        return self._dict[(name, cnt)]

    def set(self, name: str, *values, cnt: Optional[int] = None):
        """Set the values associated with one instance of the name.

        If the third parameter, cnt, is not specified, and there is only one
        instance of the named argument, then this function set the value
        associated with the first and only instance.

        Parameters:
            name (str): The name of the argument for which to set the value.
            cnt (int):  A number identifying the instance of the argument.
        """
        if cnt is None:
            if self._mult[name] == 1:
                cnt = 0
            elif self._mult[name] > 1:
                raise KeyError(
                    ("More than one argument named {}, "
                     "must specify which to set.").format(name)
                )
            else:
                raise KeyError(
                    ("Argument {} not in command. "
                     "To insert a new argument, use add_argument."
                     ).format(name)
                )                    
        if cnt >= self._mult[name]:
            raise KeyError(f"Value {cnt} for {name} does not exist.")
        self._dict[(name, cnt)] = values

    def __iadd__(self, values: Iterable):
        """Add the given values to the command as positional arguments."""
        for v in values:
            self.add_positional_argument(v)
        return self

    def __ior__(self, values: Iterable):
        """Add the given values to the command as named arguments.

        If values is a dict, then the keys are treated as the argument names,
        and the values are treated as the associated values. Otherwise, the
        elements of values are treated as names with no associated values.
        """
        try:
            # noinspection PyUnresolvedReferences
            for k, v in values.items():
                self.add_argument(k, v)
        except AttributeError:
            for k in values:
                self.add_argument(k)
        return self

    def argument_iter(self) -> Iterator[str]:
        """Iterate over the elements of the argv represented by the command."""
        for k, v in self._dict.items():
            try:
                k, cnt = k
                yield str(k)
            except TypeError:
                pass
            for x in v:
                yield str(x)

    def __str__(self):
        return " ".join(self.argument_iter())
