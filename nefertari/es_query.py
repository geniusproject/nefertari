

class OperationStack(list):
    es_keywords = {'AND': 'must', 'OR': 'should', 'AND NOT': 'must_not'}

    def pop(self, index=None):
        return self.es_keywords[super(OperationStack, self).pop()]


def apply_analyzer(params, doc_type, get_document_cls):
    documents = doc_type.split(',')
    document_classes = [(get_document_cls(document), document) for document in documents]
    properties = {}

    for document_cls, document_name in document_classes:
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
    # compile params as "AND conditions" on the top level of query_string
    for key, value in params.items():
        if key.startswith('_'):
            continue
        else:
            query_string += ' AND '
            query_string += ':'.join([key, value])
    query_string = _parse_nested_items(query_string)
    query_tokens = _get_tokens(query_string)

    if len(query_tokens) > 1:
        tree = _build_tree(query_tokens)
        return {'bool': {'must': [{'bool': _build_es_query(tree)}]}}

    if _is_nested(query_string):
        aggregation = {'bool': {'must': []}}
        _attach_nested(query_tokens.pop(), aggregation['bool'], 'must')
        return aggregation

    return {'bool': {'must': [_parse_term(query_tokens.pop())]}}


def _get_tokens(values):
    """
    split query string to tokens "(", ")", "field:value", "AND", "AND NOT", "OR", "OR NOT"
    :param values: string
    :return: array of tokens
    """
    tokens = []
    brackets = {'(', ')'}
    buffer = ''
    keywords = {'AND', 'OR'}
    in_term = False

    for item in values:

        if item == '[':
            in_term = True

        if item == ']':
            in_term = False

        if item == ' ' and buffer and not in_term:
            if buffer == 'NOT':
                tmp = tokens.pop()
                # check for avoid issue with "field_name:NOT blabla"
                if tmp in keywords:
                    tokens.append(' '.join([tmp, buffer.strip()]))
            else:
                tokens.append(buffer.strip())
            buffer = ''
            continue

        if item in brackets:
            if buffer:
                tokens.append(buffer.strip())
            tokens.append(item)
            buffer = ''
            continue

        buffer += item

    if buffer:
        tokens.append(buffer)

    while True:
        tokens, removed = _remove_needless_parentheses(tokens)
        if not removed:
            break

    return tokens


def _build_tree(tokens):

    class Node:
        def __init__(self, prev=None, next_=None):
            self.prev = prev
            self.next = next_
            self.values = []

        def parse(self):
            strings = []
            for value in self.values:
                if isinstance(value, Node):
                    strings.append(value.parse())
                else:
                    strings.append(value)
            return strings

    head = Node()

    for token in tokens:
        if token == '(':
            head.next = Node(head)
            head.values.append(head.next)
            head = head.next
            continue
        if token == ')':
            head = head.prev
            continue

        head.values.append(token)
    return head.parse()


def _remove_needless_parentheses(tokens):
    """
    remove top level needless parentheses
    :param tokens: list of tokens  - "(", ")", terms and keywords
    :return: list of tokens  -  "(", ")", terms and keywords
    """

    if '(' not in tokens and ')' not in tokens:
        return tokens, False
    keywords = {'AND', 'OR', 'OR NOT', 'AND NOT'}
    brackets_count = 0
    last_bracket_index = False

    for index, token in enumerate(tokens):

        if token == '(':
            brackets_count += 1
            continue

        if token == ')':
            brackets_count -= 1
            last_bracket_index = index
            continue

        if brackets_count == 0:
            if token in keywords:
                last_bracket_index = False
                break

    if last_bracket_index:
        for needless_bracket in [last_bracket_index, 0]:
            removed_token = tokens[needless_bracket]
            tokens.remove(removed_token)
        return tokens, True
    return tokens, False


def _build_es_query(values):
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
            value2 = _extract_value(values_stack.pop())
            value1 = _extract_value(values_stack.pop())

            operation = operations_stack.pop()
            keyword_exists = aggregation.get(operation, False)

            if keyword_exists:
                _attach_item(value2, aggregation, operation)
            else:
                if operation == 'must_not':
                    _attach_item(value1, aggregation, 'must')
                    _attach_item(value2, aggregation, operation)
                else:
                    for item in [value1, value2]:
                        _attach_item(item, aggregation, operation)

            values_stack.append(None)

    return aggregation


def _extract_value(value):
    is_list = isinstance(value, list)
    if is_list and len(value) > 1:
        return {'bool': _build_es_query(value)}
    elif is_list and len(value) == 1:
        return _extract_value(value.pop())
    return value


def _attach_item(item, aggregation, operation):
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
        _attach_nested(item, aggregation, operation)
    elif isinstance(item, dict):
        aggregation[operation].append(item)
    else:
        aggregation[operation].append(_parse_term(item))


def _parse_term(item):
    """
    parse term, on this level can be implemented rules according to range, term, match and others
    https://www.elastic.co/guide/en/elasticsearch/reference/2.1/term-level-queries.html
    :param item: string
    :return: dict which contains {'term': {field_name: field_value}
    """

    field, value = smart_split(item)

    if '|' in value:
        values = value.split('|')
        return {'bool': {'should': [{'term': {field: value}} for value in values]}}
    if value == '_missing_':
        return {'missing': {'field': field}}
    if value.startswith('[') and value.endswith(']') and 'TO' in value:
        return _parse_range(field, value[1:len(value) - 1])
    return {'term': {field: value}}


def _parse_range(field, value):
    """
    convert date range to ES range query.
    https://www.elastic.co/guide/en/elasticsearch/reference/2.1/query-dsl-range-query.html
    :param field: string, searched field name
    :param value: string, date range, example [2016-07-10T00:00:00 TO 2016-08-10T01:00:00]
    :return: dict, {'range': {field_name: {'gte': 2016-07-10T00:00:00, 'lte': 2016-08-10T01:00:00}}
    """

    from_, to = list(map(lambda string: string.strip(), value.split('TO')))
    range_ = {'range': {field: {}}}

    if from_ != '_missing_':
        range_['range'][field].update({'gte': from_})

    if to != '_missing_':
        range_['range'][field].update({'lte': to})

    return range_


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


def _attach_nested(value, aggregation, operation):
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
                    .append(_parse_term(value))

                if operation == 'should':
                    item['nested']['query']['bool']['minimum_should_match'] = 1

                break
    else:
        existed_items.append({'nested': {
            'path': path, 'query': {'bool': {invert_operation[operation]: [_parse_term(value)]}}}})