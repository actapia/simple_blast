from collections import defaultdict

class Command:
    def __init__(self):
        self._dict = {}
        self._mult = defaultdict(int)
        self._cnt = 0

    def add_positional_argument(self, arg):
        self._dict[self._cnt] = arg
        self._cnt += 1

    def add_argument(self, name, value):
        self._dict[(name, self._mult[name])] = value
        self._mult[name] += 1

    def __getitem__(self, name):
        return [self._dict[(name, c)] for c in range(self._mult[name])]

    def get(self, name, cnt=None):
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

    def set(self, name, value, cnt=None):
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
        self._dict[(name, cnt)] = value

    def __iadd__(self, values):
        for v in values:
            self.add_positional_argument(v)
        return self

    def __ior__(self, values):
        for k, v in values.items():
            self.add_argument(k, v)
        return self

    def argument_iter(self):
        for k, v in self._dict.items():
            try:
                k, cnt = k
                yield str(k)
            except TypeError:
                pass
            yield str(v)

    def __str__(self):
        return " ".join(self.argument_iter())
        
        

    
