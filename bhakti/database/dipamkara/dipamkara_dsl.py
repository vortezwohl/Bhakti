from bhakti.const import EMPTY_SET, EMPTY_STR, EMPTY_DICT
from bhakti.database.dipamkara.exception import DipamkaraSyntaxError
from bhakti.database.dipamkara.exception.dipamkara_index_error import DipamkaraIndexError


class DipamkaraDsl:
    def __init__(
            self,
            expr: str,
            inverted_index: dict[str, dict[str, any]]
    ):
        self.__expr: str = expr
        self.__inverted_index: dict = inverted_index

    def update_expr(self, expr: str):
        self.__expr: str = expr
        return self

    def update_inverted_index(self, inverted_index: dict[str, dict[str, any]]):
        self.__inverted_index: dict[str, dict[str, any]] = inverted_index
        return self

    def tokenize(self) -> list:
        tokens = []
        i = 0
        while i < len(self.__expr):
            # Skip spaces
            if self.__expr[i].isspace():
                i += 1
                continue
            # Find the next token
            token = ''
            while i < len(self.__expr) \
                    and not self.__expr[i].isspace() \
                    and self.__expr[i] not in ('&', '|', '<', '>', '=', '!'):
                token += self.__expr[i]
                i += 1
            # check brackets
            if i < len(self.__expr) and self.__expr[i] in ('(', ')'):
                token = self.__expr[i]
            # Check for relational operators
            if i < len(self.__expr) and self.__expr[i] in ['<', '>']:
                token = self.__expr[i]
                if self.__expr[i + 1] == '=':
                    token += '='
                    i += 2
                else:
                    i += 1
            elif i < len(self.__expr) and self.__expr[i] == '!':
                if self.__expr[i + 1] == '=':
                    token = '!='
                    i += 2
                else:
                    token = '='
                    i += 1
            elif i < len(self.__expr) and self.__expr[i] == '=':
                if self.__expr[i + 1] == '=':
                    token = '=='
                    i += 2
                else:
                    token = '='
                    i += 1
            # Check for logical operators
            elif i < len(self.__expr) and self.__expr[i] in ['&', '|']:
                if self.__expr[i] == '&':
                    token = '&&'
                    i += 2
                else:
                    token = '||'
                    i += 2
            # Add the token to the list
            tokens.append(token)
        # parse the tokens into list of lists
        return self.organize(tokens)

    def organize(self, tokens: list) -> list:
        result = []
        sub_expr = []
        for token in tokens:
            if token in ['&&', '||']:
                if sub_expr:
                    result.append(sub_expr)
                    sub_expr = []
                sub_expr.append(token)
            else:
                sub_expr.append(token)
        if sub_expr:
            result.append(sub_expr)
        return result

    def equals_likely(self, challenger_str: str, object_str: str) -> bool:
        if challenger_str.startswith('%') and challenger_str.endswith('%'):
            return challenger_str[1:-1] in object_str
        elif challenger_str.startswith('%'):
            return object_str.endswith(challenger_str[1:])
        elif challenger_str.endswith('%'):
            return object_str.startswith(challenger_str[:-1])
        else:
            return challenger_str == object_str

    def process_atomic(self, tokens: list[str]) -> set:
        key = tokens[0]
        if key not in self.__inverted_index.keys():
            raise DipamkaraSyntaxError(message=f"Index \"{key}\" not exist")
        elif self.__inverted_index[key] == EMPTY_DICT():
            del self.__inverted_index[key]
            raise DipamkaraSyntaxError(message=f"Index \"{key}\" not exist")
        op = tokens[1]  # > < >= <= != ==
        if self.is_number(tokens[2]):
            value = float(tokens[2])
        else:
            if tokens[2].startswith('"') and tokens[2].endswith('"'):
                value = tokens[2].replace('"', '', 2)
            else:
                raise DipamkaraSyntaxError(message='String value should be surrounded by " "')
        mapped_vectors: dict = self.__inverted_index[key]
        result_set: set = EMPTY_SET()
        for _key, _value in mapped_vectors.items():
            if isinstance(_value, str) and not isinstance(value, str):
                value = str(value)
            if isinstance(value, str) and not isinstance(_value, str):
                _value = str(_value)
            if op == '>':
                if isinstance(_value, float | int):
                    if _value > float(value):
                        result_set.add(_key)
                else:
                    if _value > value:
                        result_set.add(_key)
            if op == '>=':
                if isinstance(_value, float | int):
                    if _value >= float(value):
                        result_set.add(_key)
                else:
                    if _value >= value:
                        result_set.add(_key)
            if op == '<':
                if isinstance(_value, float | int):
                    if _value < float(value):
                        result_set.add(_key)
                else:
                    if _value < value:
                        result_set.add(_key)
            if op == '<=':
                if isinstance(_value, float | int):
                    if _value <= float(value):
                        result_set.add(_key)
                else:
                    if _value <= value:
                        result_set.add(_key)
            if op == '==':
                if isinstance(_value, float | int):
                    if _value == float(value):
                        result_set.add(_key)
                else:
                    if self.equals_likely(challenger_str=value, object_str=_value):
                        result_set.add(_key)
            if op == '!=':
                if isinstance(_value, float | int):
                    if _value != float(value):
                        result_set.add(_key)
                else:
                    if not self.equals_likely(challenger_str=value, object_str=_value):
                        result_set.add(_key)
        return result_set

    def process_serialized(self) -> set:
        tokenized_expr = self.tokenize()
        result_set: set = EMPTY_SET()
        for tokens in tokenized_expr:
            if len(tokens) == 3:
                result_set |= self.process_atomic(tokens)
            else:
                if tokens[0] == '&&':
                    result_set &= self.process_atomic(tokens[1:])
                if tokens[0] == '||':
                    result_set |= self.process_atomic(tokens[1:])
        return result_set

    def is_number(self, s: str) -> bool:
        if s.count('.') > 1:
            return False
        if s.replace('.', '', 1).isdigit():
            return True
        return s.isdigit()

    def is_string(self, s: str) -> bool:
        return '"' in s


DIPAMKARA_DSL = DipamkaraDsl(expr=EMPTY_STR(), inverted_index=EMPTY_DICT())
DIPAMKARA_DSL_KEYWORDS = tuple(('>', '<', '>=', '<=', '==', '!=', '&&', '||'))


def find_keywords_of_dipamkara_dsl(text: str) -> bool:
    for _kw in DIPAMKARA_DSL_KEYWORDS:
        if _kw in text:
            raise DipamkaraIndexError(f'"{text}" contains keyword "{_kw}" of {DIPAMKARA_DSL_KEYWORDS}')
    return False
