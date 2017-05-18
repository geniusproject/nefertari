import re
from functools import reduce
from abc import abstractmethod, abstractclassmethod


class OperationStack(list):
    es_keywords = {'AND': 'must', 'OR': 'should', 'AND NOT': 'must_not'}

    def pop(self, index=None):
        return self.es_keywords[super(OperationStack, self).pop()]


class Node:
    def __init__(self, prev=None, next_=None):
        self.prev = prev
        self.next = next_
        self.values = []

    def parse(self):
        nodes = []
        for value in self.values:
            if isinstance(value, Node):
                nodes.append(value.parse())
            else:
                nodes.append(value)
        return nodes

    @staticmethod
    def build_tree(tokens):
        parentheses_counter = 0
        head = Node()

        for token in tokens:
            if token == '(':
                head.next = Node(head)
                head.values.append(head.next)
                head = head.next
                parentheses_counter += 1
                continue
            if token == ')':
                head = head.prev
                parentheses_counter -= 1
                continue

            head.values.append(token)

        if parentheses_counter:
            raise ValueError('Wrong numbers of parentheses. Query string could not be parsed')

        return head.parse()


class Tokenizer:
    query_keywords = {'AND', 'OR', 'NOT'}


    @classmethod
    def tokenize(cls, query_string):
        """
        split query string to tokens "(", ")", "field:value", "AND", "AND NOT", "OR"
        :param values: string
        :return: array of tokens
        """

        tokens = []
        brackets = {'(', ')'}
        buffer = ''
        cursor = 0
        keywords = re.compile('(\s+OR\s+|\s+AND NOT\s+|\s+AND\s+)')

        while cursor < len(query_string):

            if query_string[cursor] in brackets:
                tokens.append(query_string[cursor])
                cursor += 1
                continue

            while cursor < len(query_string) and query_string[cursor] not in brackets:
                buffer += query_string[cursor]
                cursor += 1
            tokens.extend(list(filter(lambda s: s, map(lambda s: s.strip(), keywords.split(buffer)))))
            buffer = ''

        Tokenizer._remove_needless_parentheses(tokens)
        return tokens

    @classmethod
    def _remove_needless_parentheses(cls, tokens):
        """
        remove top level needless parentheses
        :param tokens: list of tokens  - "(", ")", terms and keywords
        :return: list of tokens  -  "(", ")", terms and keywords
        """

        if '(' not in tokens and ')' not in tokens:
            return False
        counter = 0
        last_bracket_index = False

        for index, token in enumerate(tokens):

            if token == '(':
                counter += 1
                continue

            if token == ')':
                counter -= 1
                last_bracket_index = index
                continue

            if counter == 0:
                if token in cls.query_keywords:
                    last_bracket_index = False
                    break

        if last_bracket_index:
            for needless_bracket in [last_bracket_index, 0]:
                removed_token = tokens[needless_bracket]
                tokens.remove(removed_token)
            cls._remove_needless_parentheses(tokens)


class BoostParams:

    def __init__(self, boost_params=None):
        self.params = reduce(self._aggregate_dict,
                             map(lambda x: self._tuple_to_dict(smart_split(x)), boost_params),
                             dict())

    @staticmethod
    def _tuple_to_dict(items):
        key, value = items
        return {key: value}

    @staticmethod
    def _aggregate_dict(a, b):
        a.update(b)
        return a

    def __contains__(self, item):
        return item in self.params

    def __iter__(self):
        for item in self.params:
            yield item

    def __getitem__(self, key):
        return self.params[key]

    def __bool__(self):
        return bool(self.params)


class BaseProcessor:
    name = 'base_processor'

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    @abstractmethod
    def apply(self, term):
        pass

    @abstractmethod
    def rule(self, term):
        pass

    @abstractclassmethod
    def next(self):
        pass


class BoostProcessor(BaseProcessor):
    name = 'boost_processor'

    def __init__(self, boost_params):
        self.boost_params = boost_params

    def rule(self, term):
        return term.field in self.boost_params or (term.field == '_all' and self.boost_params)

    @classmethod
    def next(cls):
        return FinalProcessor

    def apply(self, term):
        if term.field == '_all':
            value = [{term.type: {term.field: term.value}}]
            value.extend(
                [{term.type: {field: {term.query_param: term.value, 'boost': self.boost_params[field]}}}
                 for field in self.boost_params])
            term.field = 'should'
            term.type = 'bool'
            term.value = value
        else:
            if term.query_param:
                term.value = {term.query_param: term.value,
                              'boost': int(self.boost_params[term.field])}


class WildCardProcessor(BaseProcessor):
    name = 'wildcard_processor'

    @classmethod
    def rule(cls, term):
        return '*' in term.value

    @classmethod
    def next(cls):
        return TypeProcessor

    def apply(self, term):
        term.type = 'wildcard'


class RangeProcessor(BaseProcessor):
    name = 'range_processor'

    @classmethod
    def rule(cls, term):
        return term.value.startswith('[') and term.value.endswith(']') and 'TO' in term.value

    @classmethod
    def next(cls):
        return FinalProcessor

    def apply(self, term):
        """
        convert date range to ES range query.
        https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-dsl-range-query.html
        parse date range, example [2016-07-10T00:00:00 TO 2016-08-10T01:00:00] to
        {'range': {field_name: {'gte': 2016-07-10T00:00:00, 'lte': 2016-08-10T01:00:00}}
        """

        term.type = 'range'
        value = term.value[1:len(term.value) - 1]
        from_, to = list(map(lambda string: string.strip(), value.split('TO')))
        value = {}

        if from_ != '_missing_':
            value.update({'gte': from_})
        if to != '_missing_':
            value.update({'lte': to})
        term.value = value


class OrProcessor(BaseProcessor):
    name = 'or_processor'

    @classmethod
    def rule(cls, term):
        return '|' in term.value

    @classmethod
    def next(cls):
        return TypeProcessor

    def apply(self, term):
        term.value = [{'term': {term.field: value}} for value in term.value.split('|')]
        term.field = 'should'
        term.type = 'bool'


class MissingProcessor(BaseProcessor):
    name = 'missing_processor'

    @classmethod
    def rule(cls, term):
        return term.value == '_missing_'

    @classmethod
    def next(cls):
        return FinalProcessor

    def apply(self, term):
        term.type = 'missing'
        term.value = term.field
        term.field = 'field'


class MatchProcessor(BaseProcessor):
    name = 'match_processor'

    @classmethod
    def rule(cls, term):
        return (' ' in term.value and not RangeProcessor.rule(term)) or '_all' in term.field

    @classmethod
    def next(cls):
        return WildCardProcessor

    def apply(self, term):
        term.type = 'match'


class TypeProcessor(BaseProcessor):
    query_params = {'match': 'query', 'term': 'value', 'wildcard': 'value'}
    name = 'type_processor'

    @classmethod
    def rule(cls, term):
        return True

    @classmethod
    def apply(cls, term):
        if term.type is None:
            term.type = 'term'

        term.query_param = TypeProcessor.query_params.get(term.type)

    def next(self):
        return BoostProcessor


class FinalProcessor(BaseProcessor):
    name = 'final_processor'

    @classmethod
    def rule(cls, term):
        return True

    def apply(self, term):
        pass

    @classmethod
    def next(cls):
        return None


class Term:

    def __init__(self, field, value):
        self.field = field
        self.value = value
        self.type = None
        self.processors = []

    def build(self):
        if self.field == 'should':
            return {self.type: {self.field: self.value, 'minimum_should_match': 1}}
        return {self.type: {self.field: self.value}}

    def apply_processors(self):
        processors_order = list(self.build_chain())

        for processor_name in processors_order:
            processor_name.apply(self)

    def build_chain(self):
        graph = dict()
        for processor in self.processors:
            next_processor = self.find_next_processor(processor)

            if next_processor:
                graph[processor] = next_processor

        return reversed([item for item in self.find_next_vertex(graph)])

    def find_next_processor(self, processor):
        next_processor = processor.next()

        if not next_processor:
            return None

        if next_processor in self.processors:
            return next_processor

        return self.find_next_processor(next_processor)

    @staticmethod
    def find_next_vertex(graph, next_=FinalProcessor):

        keys = list(graph.keys())

        def find_conflicts(value):
            counter = 0
            for item in graph.values():
                if item == value:
                    counter += 1

                if counter > 1:
                    return True
            return False

        for _ in range(len(keys) ** 2):

            for key in keys:
                if key in graph and graph[key] == next_:
                    if not find_conflicts(next_):
                        next_ = key
                    del graph[key]
                    yield key

            if not graph.keys():
                break

        return False


class TermBuilder:

    def __init__(self, params=()):
        self.params = params

    def __call__(self, item):
        field, value = smart_split(item)
        term = Term(field, value)
        processors = [
            TypeProcessor(),
            OrProcessor(),
            MissingProcessor(),
            RangeProcessor(),
            WildCardProcessor(),
            MatchProcessor(),
            FinalProcessor(),
            BoostProcessor(BoostParams(self.params)),
        ]

        for processor in processors:
            if processor.rule(term):
                term.processors.append(processor)
        term.apply_processors()

        return term.build()


def apply_analyzer(params, doc_type, get_document_cls):
    documents = doc_type.split(',')
    properties = {}

    for document_name in documents:
        document_cls = get_document_cls(document_name)
        mapping, _ = document_cls.get_es_mapping()
        properties.update(mapping[document_name]['properties'])

    apply_to = []
    for property_name in properties.keys():
        if 'analyzer' in properties[property_name] and property_name in params:
            apply_to.append({property_name: params[property_name]})
            del params[property_name]
    if apply_to:
        return {'bool': {'must': [{'term': term} for term in apply_to]}}
    return False


def compile_es_query(params):
    query_string = params.pop('es_q')
    boosted_params = params.pop('_boost', ())

    if boosted_params:
        boosted_params = list(map(_parse_nested_items, boosted_params.split(',')))
    term_builder = TermBuilder(boosted_params)

    # compile params as "AND conditions" on the top level of query_string
    for key, value in params.items():
        if key.startswith('_'):
            continue
        else:
            query_string += ' AND '
            # parse statement
            if '(' in value and ')' in value:
                values = Tokenizer.tokenize(re.sub('[()]', '', value))

                def attach_key(item):
                    if item != 'OR':
                        return ':'.join([key, item])
                    return item

                values = ' '.join(map(attach_key,  values))
                query_string += '({items})'.format(items=values)
            else:
                query_string += ':'.join([key, value])
    query_string = _parse_nested_items(query_string)
    query_tokens = Tokenizer.tokenize(query_string)

    if len(query_tokens) > 1:
        tree = Node.build_tree(query_tokens)
        return {'bool': {'must': [{'bool': _build_es_query(tree, term_builder)}]}}

    if _is_nested(query_string):
        aggregation = {'bool': {'must': []}}
        _attach_nested(query_tokens.pop(), aggregation['bool'], 'must', term_builder)
        return aggregation

    return {'bool': {'must': [term_builder(query_tokens.pop())]}}


def _build_es_query(values, term_builder):
    aggregation = {}
    operations_stack = OperationStack()
    values_stack = []
    keywords = {'AND', 'AND NOT', 'OR', 'OR NOT'}

    for value in values:
        if isinstance(value, str) and value in keywords:
            operations_stack.append(value)
        else:
            values_stack.append(value)

        if len(operations_stack) == 1 and len(values_stack) == 2:
            value2 = _extract_value(values_stack.pop(), term_builder)
            value1 = _extract_value(values_stack.pop(), term_builder)

            operation = operations_stack.pop()
            keyword_exists = aggregation.get(operation, False)

            if keyword_exists:
                _attach_item(value2, aggregation, operation, term_builder)
            else:
                if operation == 'must_not':
                    _attach_item(value1, aggregation, 'must', term_builder)
                    _attach_item(value2, aggregation, operation, term_builder)
                else:
                    for item in [value1, value2]:
                        _attach_item(item, aggregation, operation, term_builder)

            values_stack.append(None)

    return aggregation


def _extract_value(value, term_builder):
    is_list = isinstance(value, list)
    if is_list and len(value) > 1:
        return {'bool': _build_es_query(value, term_builder)}
    elif is_list and len(value) == 1:
        return _extract_value(value.pop(), term_builder)
    return value


def _attach_item(item, aggregation, operation, term_builder):
    """
    attach item to already existed operation in aggregation or to new operation in aggregation
    :param item: string
    :param aggregation: dict which contains aggregated terms
    :param operation: ES operation keywords {must, must_not, should, should_not}
    :return:
    """

    if item is None:
        return

    # init value or get existed
    aggregation[operation] = aggregation.get(operation, [])

    if 'should' == operation and not aggregation.get('minimum_should_match', False):
        aggregation['minimum_should_match'] = 1

    if _is_nested(item):
        _attach_nested(item, aggregation, operation, term_builder)
    elif isinstance(item, dict):
        aggregation[operation].append(item)
    else:
        aggregation[operation].append(term_builder(item))


# attach _nested to nested_document
def _parse_nested_items(query_string):
    """
    attach _nested to nested_document
    :param query_string: string
    :return: string with updated name for nested document, like assignments_nested for assignments
    """
    parsed_query_string = ''
    in_quotes = False
    for index, key in enumerate(query_string):

        if key == '"' and not in_quotes:
            in_quotes = True
            key = ''
        elif key == '"' and in_quotes:
            in_quotes = False
            key = ''

        if key == '.' and not in_quotes:
            key = '_nested.'

        parsed_query_string = parsed_query_string + key
    return parsed_query_string


def _is_nested(item):
    if isinstance(item, str):
        field, _ = smart_split(item)
        return '_nested' in field
    return False


def smart_split(item, split_key=':'):
    """
    split string in first matching with key
    :param item: string which contain field_name:value or field_name:[00:00:00 TO 01:00:00]
    :param split_key: key, which we use to split string
    :return:
    """
    split_index = item.find(split_key)
    return [item[0:split_index], item[split_index + 1:]]


def _attach_nested(value, aggregation, operation, term_builder):
    """
    apply rules related to nested queries
    https://www.elastic.co/guide/en/elasticsearch/guide/current/nested-query.html
    :param value: string
    :param aggregation: dict which contains aggregated terms
    :param operation: ES operation keywords {must, must_not, should, should_not}
    :return: None
    """
    field, _ = smart_split(value)
    path = field.split('.')[0]
    existed_items = aggregation[operation]
    invert_operation = {'must': 'must', 'must_not': 'must', 'should': 'should'}

    for item in existed_items:
        if 'nested' in item:
            item_path = item['nested'].get('path', False)
            if item_path == path:
                item['nested']['query']['bool'][invert_operation[operation]]\
                    .append(term_builder(value))

                if operation == 'should':
                    item['nested']['query']['bool']['minimum_should_match'] = 1

                break
    else:
        existed_items.append({'nested': {
            'path': path, 'query': {'bool': {invert_operation[operation]:
                                                 [term_builder(value)]}}}})
