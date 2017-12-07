import logging
import time

import six
import pytest
from mock import Mock, patch, call
from elasticsearch.exceptions import TransportError
from nefertari import elasticsearch as es
from nefertari.json_httpexceptions import JHTTPServiceUnavailable, JHTTPBadRequest, JHTTPNotFound, JHTTPUnprocessableEntity, JHTTPGatewayTimeout
from nefertari.utils import dictset
from nefertari.elasticsearch import ES, ESData


class MockEngine:
    
    def __init__(self, *args, **kwargs):
        self.get_document_cls_func = kwargs.pop('get_document_cls')
        super().__init__(*args, **kwargs)

    def get_document_cls(self, *args, **kwargs):
        return self.get_document_cls_func(*args, **kwargs)


def get_document_cls_with_es_mapping(name):
    mock_document = Mock()
    mock_document.get_es_mapping = lambda: ({name: {'properties': {}}}, None)
    mock_document.__name__ = name
    return mock_document


def get_document_cls_with_sort_methods_factory(order=list(), without_sort=False):
    def sort_method(limit, offset):
        return order

    def get_document_cls_with_sort_methods(name):
        mock_document = get_document_cls_with_es_mapping(name)
        if without_sort:
            mock_document.get_sort_method = lambda *args: None
        else:
            mock_document.get_sort_method = lambda *args: sort_method
        return mock_document
    return MockEngine(get_document_cls=get_document_cls_with_sort_methods)


class TestESActionRegistry(object):

    def test_get_latest_change(self):
        now = time.time()
        later = time.time()
        first_data = ESData(action={'_op_type': 'index', '_type': 'Item','_id': 2}, creation_time=now)
        second_data = ESData(action={'_op_type': 'index', '_type': 'Item','_id': 2}, creation_time=later)
        gen = ES.registry.get_latest_change([first_data, second_data])
        result = next(gen)
        assert result == second_data

        with pytest.raises(StopIteration):
            next(gen)

    def test_filter_deleted_items_from_index(self):
        now = time.time()
        later = time.time()
        first_data = ESData(action={'_op_type': 'index', '_type': 'Item','_id': 2}, creation_time=now)
        second_data = ESData(action={'_op_type': 'delete', '_type': 'Item','_id': 2}, creation_time=later)
        result = ES.registry.prepare_for_deletion([first_data, second_data])

        assert len(result) == 1
        assert result[0] == second_data

    def test_filter_deleted_items_from_index_without_deleted(self):
        now = time.time()
        later = time.time()
        first_data = ESData(action={'_op_type': 'index', '_type': 'Item','_id': 2}, creation_time=now)
        second_data = ESData(action={'_op_type': 'index', '_type': 'Item','_id': 2}, creation_time=later)
        result = ES.registry.prepare_for_deletion([first_data, second_data])

        assert len(result) == 2
        expected = [first_data, second_data]

        for item in result:
            assert item in expected
            expected.remove(item)

    def test_filter_deleted_items_from_index_without_index(self):
        now = time.time()
        later = time.time()
        first_data = ESData(action={'_op_type': 'deleted', '_type': 'Item','_id': 1}, creation_time=now)
        second_data = ESData(action={'_op_type': 'deleted', '_type': 'Item','_id': 2}, creation_time=later)
        result = ES.registry.prepare_for_deletion([first_data, second_data])

        assert len(result) == 2
        expected = [first_data, second_data]

        for item in result:
            assert item in expected
            expected.remove(item)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_reindex_conflicts(self, mock_bulk, mock_settings, mock_api, mock_engine):
        exc = Mock()
        mock_settings.asbool.return_value = True
        exc.errors = [{'index': {'status': 409, '_type': 'Doc', '_id': 1, '_index': 'foondex'}}]
        mock_engine.reload_document = lambda *args, **kwargs: {'_pk': 1, 'name': 'foo', '_type': 'Doc'}
        ES.registry.reindex_conflicts(exc)
        ES.registry.clean()

        mock_bulk.assert_called_once_with(actions=[
            {'_op_type': 'index', '_type': 'Doc', '_index': 'foondex', '_id': 1, '_source': {'_pk': 1, 'name': 'foo'}}
        ], client=mock_api, refresh=True)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_reindex_conflicts_different_status(self, mock_bulk, mock_settings, mock_api, mock_engine):
        exc = Mock()
        exc.errors = [{'index': {'status': 408, '_type': 'Doc', '_id': 1, '_index': 'foondex'}}]
        mock_engine.reload_document = lambda *args, **kwargs: {'_pk': 1, 'name': 'foo', '_type': 'Doc'}
        ES.registry.reindex_conflicts(exc)
        ES.registry.clean()
        assert  mock_bulk.called == False

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_reindex_conflicts_update_action(self, mock_bulk, mock_settings, mock_api, mock_engine):
        exc = Mock()
        exc.errors = [{'update': {'status': 409, '_type': 'Doc', '_id': 1, '_index': 'foondex'}}]
        mock_engine.reload_document = lambda *args, **kwargs: {'_pk': 1, 'name': 'foo', '_type': 'Doc'}
        mock_settings.asbool.return_value = True
        ES.registry.reindex_conflicts(exc)
        ES.registry.clean()

        mock_bulk.assert_called_once_with(actions=[
            {'_op_type': 'index', '_type': 'Doc', '_index': 'foondex', '_id': 1, '_source': {'_pk': 1, 'name': 'foo'}}
        ], client=mock_api, refresh=True)

class TestESHttpConnection(object):
    @patch('nefertari.elasticsearch.ESHttpConnection._catch_index_error')
    @patch('nefertari.elasticsearch.log')
    def test_perform_request_debug(self, mock_log, mock_catch):
        mock_log.level = logging.DEBUG
        conn = es.ESHttpConnection()
        conn.pool = Mock()
        conn.pool.urlopen.return_value = Mock(
            data=six.b('foo'), status=200)
        conn.perform_request('POST', 'http://localhost:9200')
        mock_log.debug.assert_called_once_with(
            "('POST', 'http://localhost:9200')")
        conn.perform_request('POST', 'http://localhost:9200' * 200)
        assert mock_catch.called
        assert mock_log.debug.call_count == 2

    def test_catch_index_error_no_data(self):
        conn = es.ESHttpConnection()
        try:
            conn._catch_index_error((1, 2, None))
        except:
            raise Exception('Unexpected exeption')

    def test_catch_index_error_no_data_loaded(self):
        conn = es.ESHttpConnection()
        try:
            conn._catch_index_error((1, 2, '[]'))
        except:
            raise Exception('Unexpected exeption')

    def test_catch_index_error_no_errors(self):
        conn = es.ESHttpConnection()
        try:
            conn._catch_index_error((1, 2, '{"errors":false}'))
        except:
            raise Exception('Unexpected exeption')

    def test_catch_index_error_not_index_error(self):
        conn = es.ESHttpConnection()
        try:
            conn._catch_index_error((
                1, 2,
                '{"errors":true, "items": [{"foo": "bar"}]}'))
        except:
            raise Exception('Unexpected exeption')

    def test_catch_index_error(self):
        conn = es.ESHttpConnection()
        with pytest.raises(JHTTPServiceUnavailable):
            conn._catch_index_error((
                1, 2,
                '{"errors":true, "items": [{"index": {"error": "FOO"}}]}'))

    def test_perform_request_exception(self):
        conn = es.ESHttpConnection()
        conn.pool = Mock()
        conn.pool.urlopen.side_effect = TransportError('N/A', '')
        print('here')
        with pytest.raises(JHTTPBadRequest):
            conn.perform_request('POST', 'http://localhost:9200')

    def test_request_timeout_exception(self):
        from urllib3.exceptions import ReadTimeoutError
        conn = es.ESHttpConnection()
        conn.pool = Mock()
        conn.pool.urlopen.side_effect = ReadTimeoutError(Mock(), Mock(), Mock())
        with pytest.raises(JHTTPGatewayTimeout):
            conn.perform_request('POST', 'http://localhost:9200')

    @patch('nefertari.elasticsearch.log')
    def test_perform_request_no_index(self, mock_log):
        mock_log.level = logging.DEBUG
        mock_log.debug.side_effect = TransportError(
            404, 'IndexMissingException')
        conn = es.ESHttpConnection()
        with pytest.raises(es.IndexNotFoundException):
            conn.perform_request('POST', 'http://localhost:9200')


class TestHelperFunctions(object):
    def test_process_fields_param_no_fields(self):
        assert es.process_fields_param(None) is None

    def test_process_fields_param_string(self):
        assert es.process_fields_param('foo,bar') == {
            '_source_include': ['foo', 'bar', '_type'],
            '_source': True
        }

    def test_process_fields_param_list(self):
        assert es.process_fields_param(['foo', 'bar']) == {
            '_source_include': ['foo', 'bar', '_type'],
            '_source': True
        }

    @patch('nefertari.elasticsearch.ES')
    def test_includeme(self, mock_es):
        config = Mock()
        config.registry.settings = {'foo': 'bar'}
        es.includeme(config)
        mock_es.setup.assert_called_once_with({'foo': 'bar'})

    def test_apply_sort(self):
        assert es.apply_sort('+foo,-bar ,zoo') == 'foo:asc,bar:desc,zoo:asc'

    def test_apply_sort_empty(self):
        assert es.apply_sort('') == ''

    def test_build_terms(self):
        terms = es.build_terms('foo', [1, 2, 3])
        assert terms == 'foo:1 OR foo:2 OR foo:3'

    def test_build_terms_custom_operator(self):
        terms = es.build_terms('foo', [1, 2, 3], operator='AND')
        assert terms == 'foo:1 AND foo:2 AND foo:3'

    def test_es_docs(self):
        assert issubclass(es._ESDocs, list)
        docs = es._ESDocs()
        assert docs._total == 0
        assert docs._start == 0

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers')
    def test_bulk_body(self, mock_helpers, mock_settings):
        mock_helpers.bulk.return_value = (1, [])
        request = Mock()
        request.params.mixed.return_value = {'_refresh_index': True}
        mock_settings.asbool.return_value = True
        with ES.registry as es_registry:
            es._bulk_body([{'_op_type': 'index', '_type': 'Item','_id': 2}])
            es_registry.bulk_index()
            mock_helpers.bulk.assert_called_once_with(
                client=ES.api, refresh=True, actions=[{'_op_type': 'index', '_type': 'Item','_id':2}])

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers')
    def test_bulk_body_missed_refresh_index(self, mock_helpers, mock_settings):
        mock_helpers.bulk.return_value = (1, [])
        request = Mock()
        request.params.mixed.return_value = dict()
        mock_settings.asbool.return_value = True
        with ES.registry as es_registry:
            es_registry.bind(request)
            es._bulk_body([{'_op_type': 'index', '_type': 'Item','_id': 2}])
            es_registry.bulk_index()
            mock_helpers.bulk.assert_called_once_with(
                client=ES.api, refresh=True, actions=[{'_op_type': 'index', '_type': 'Item','_id': 2}])

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers')
    def test_bulk_body_refresh_index_false(self, mock_helpers, mock_settings):
        mock_helpers.bulk.return_value = (1, [])
        request = Mock()
        request.params.mixed.return_value = {'_refresh_index': False}
        mock_settings.asbool.return_value = True

        with ES.registry as es_registry:
            es_registry.bind(request)
            es._bulk_body([{'_op_type': 'index', '_type': 'Item','_id': 2}])
            es_registry.bulk_index()
            mock_helpers.bulk.assert_called_once_with(
                client=ES.api, refresh=False, actions=[{'_op_type': 'index', '_type': 'Item','_id': 2}])

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers')
    def test_indexation_failed(self, mock_helpers, mock_settings):
        mock_helpers.bulk.return_value = (1, [])
        request = Mock()
        mock_settings.asbool.return_value = True
        request.params.mixed.return_value = {'_refresh_index': True}
        es._bulk_body([{'_op_type': 'index', '_type': 'Item','_id': 2}])

        with pytest.raises(Exception):
            with ES.registry:
                raise Exception()
            assert len(ES.registry.registry) == 0


class TestES(object):
    @patch('nefertari.elasticsearch.ES.settings')
    def test_init(self, mock_set):
        es.ES.document_proxies = {'Foo': None, 'Foo2': None}
        obj = es.ES(source='Foo')
        assert obj.index_name == mock_set.index_name
        assert obj.doc_type == 'Foo'
        assert obj.chunk_size == mock_set.asint()
        obj = es.ES(source='Foo', index_name='a', chunk_size=2)
        assert obj.index_name == 'a'
        assert obj.doc_type == 'Foo'
        assert obj.chunk_size == 2

    def test_src2type(self):
        assert es.ES.src2type('FooO') == 'FooO'

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.elasticsearch')
    def test_setup(self, mock_es, mock_engine):
        settings = dictset({
            'elasticsearch.hosts': '127.0.0.1:8080,127.0.0.2:8090',
            'elasticsearch.sniff': 'true',
            'elasticsearch.retry_on_timeout': 'true'
        })
        es.ES.setup(settings)
        mock_es.Elasticsearch.assert_called_once_with(
            hosts=[{'host': '127.0.0.1', 'port': '8080'},
                   {'host': '127.0.0.2', 'port': '8090'}],
            serializer=mock_engine.ESJSONSerializer(),
            connection_class=es.ESHttpConnection,
            sniff_on_start=True,
            sniff_on_connection_fail=True,
            retry_on_timeout=True
        )
        assert es.ES.api == mock_es.Elasticsearch()

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.elasticsearch')
    def test_setup_no_settings(self, mock_es, mock_engine):
        settings = dictset({})
        with pytest.raises(Exception) as ex:
            es.ES.setup(settings)
        assert 'Bad or missing settings for elasticsearch' in str(ex.value)
        assert not mock_es.Elasticsearch.called

    def test_build_qs(self):
        obj = es.ES('Foo', 'foondex')
        qs = obj.build_qs(dictset({'foo': 1, 'bar': '_all', 'zoo': 2}))
        assert qs == 'foo:1 AND zoo:2'

    def test_build_list(self):
        obj = es.ES('Foo', 'foondex')
        qs = obj.build_qs(dictset({'foo': [1, 2], 'zoo': 3}))
        assert qs == 'foo:1 OR foo:2 AND zoo:3'

    def test_build_dunder_key(self):
        obj = es.ES('Foo', 'foondex')
        qs = obj.build_qs(dictset({'foo': [1, 2], '__zoo__': 3}))
        assert qs == 'foo:1 OR foo:2'

    def test_build_raw_terms(self):
        obj = es.ES('Foo', 'foondex', 100)
        qs = obj.build_qs(dictset({'foo': [1, 2]}), _raw_terms='qoo:1')
        assert qs == 'foo:1 OR foo:2 AND qoo:1'

    def test_build_operator(self):
        obj = es.ES('Foo', 'foondex')
        qs = obj.build_qs(dictset({'foo': 1, 'qoo': 2}), operator='OR')
        assert qs == 'foo:1 OR qoo:2'

    def test_process_chunks(self):
        obj = es.ES('Foo', 'foondex', chunk_size=100)
        operation = Mock()
        documents = [1, 2, 3, 4, 5]
        obj.process_chunks(documents, operation)
        operation.assert_called_once_with(documents_actions=[1, 2, 3, 4, 5])

    def test_process_chunks_multiple(self):
        obj = es.ES('Foo', 'foondex', chunk_size=3)
        operation = Mock()
        documents = [1, 2, 3, 4, 5]
        obj.process_chunks(documents, operation)
        operation.assert_has_calls([
            call(documents_actions=[1, 2, 3]),
            call(documents_actions=[4, 5]),
        ])

    def test_process_chunks_no_docs(self):
        obj = es.ES('Foo', 'foondex')
        operation = Mock()
        obj.process_chunks([], operation)
        assert not operation.called

    def test_prep_bulk_documents_not_dict(self):
        obj = es.ES('Foo', 'foondex')
        with pytest.raises(ValueError) as ex:
            obj.prep_bulk_documents('', 'q')
        assert str(ex.value) == 'Document type must be `dict` not a `str`'

    def test_prep_bulk_documents(self):
        obj = es.ES('Foo', 'foondex')
        docs = [
            {'_type': 'Story', '_pk': 'story1'},
            {'_type': 'Story', '_pk': 'story2'},
        ]
        prepared = obj.prep_bulk_documents('myaction', docs)
        assert len(prepared) == 2
        doc1 = prepared[0]
        assert sorted(doc1.keys()) == sorted([
            '_type', '_id', '_index', '_source', '_op_type'])
        assert doc1['_source'] == {'_pk': 'story1'}
        assert doc1['_op_type'] == 'myaction'
        assert doc1['_index'] == 'foondex'
        assert doc1['_type'] == 'Story'
        assert doc1['_id'] == 'story1'

    def test_prep_bulk_documents_no_type(self):
        obj = es.ES('Foo', 'foondex')
        docs = [
            {'_pk': 'story2'},
        ]
        prepared = obj.prep_bulk_documents('myaction', docs)
        assert len(prepared) == 1
        doc2 = prepared[0]
        assert sorted(doc2.keys()) == sorted([
            '_op_type', '_type', '_id', '_index', '_source'])
        assert doc2['_source'] == {'_pk': 'story2'}
        assert doc2['_op_type'] == 'myaction'
        assert doc2['_index'] == 'foondex'
        assert doc2['_type'] == 'Foo'
        assert doc2['_id'] == 'story2'

    def test_bulk_no_docs(self):
        obj = es.ES('Foo', 'foondex')
        assert obj._bulk('myaction', []) is None

    @patch('nefertari.elasticsearch.partial')
    @patch('nefertari.elasticsearch.ES.prep_bulk_documents')
    @patch('nefertari.elasticsearch.ES.process_chunks')
    def test_bulk(self, mock_proc, mock_prep, mock_part):
        obj = es.ES('Foo', 'foondex', chunk_size=1)
        docs = [{
            '_op_type': 'index', '_id': 'story1',
            '_source': {'_type': 'Story', 'id': 'story1', 'timestamp': 1}
        }, {
            '_op_type': 'index', '_id': 'story2',
            '_source': {'_type': 'Story', 'id': 'story2', 'timestamp': 2}

        }]
        mock_prep.return_value = docs
        obj._bulk('index', docs)
        mock_prep.assert_called_once_with('index', docs)
        mock_part.assert_called_once_with(
            es._bulk_body)
        mock_proc.assert_called_once_with(
            documents=[{
                '_id': 'story1', '_op_type': 'index',
                '_source': {'timestamp': 1, '_type': 'Story', 'id': 'story1'}
            }, {
                '_id': 'story2', '_op_type': 'index',
                '_source': {'timestamp': 2, '_type': 'Story', 'id': 'story2'}
            }],
            operation=mock_part(),
        )

    @patch('nefertari.elasticsearch.ES.prep_bulk_documents')
    @patch('nefertari.elasticsearch.ES.process_chunks')
    def test_bulk_no_prepared_docs(self, mock_proc, mock_prep):
        obj = es.ES('Foo', 'foondex', chunk_size=1)
        mock_prep.return_value = []
        obj._bulk('myaction', ['a'])
        mock_prep.assert_called_once_with('myaction', ['a'])
        assert not mock_proc.called

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.index_documents')
    @patch('nefertari.elasticsearch.ES.index_document')
    @patch('nefertari.elasticsearch.ES._bulk')
    def test_index(self, mock_bulk, mock_index_document, mock_index_documents, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=4)

        # Indexing a list
        obj.index(['a'])
        mock_index_documents.assert_called_once_with(['a'])

        # Indexing a set
        obj.index({'a'})
        mock_index_documents.assert_called_with(['a'])
        mock_engine.is_object_document = Mock(return_value=True)

        # Indexing a document
        fake_document = Mock()
        fake_document.to_indexable_dict = Mock(return_value={"a": "b"})
        obj.index(fake_document)
        mock_bulk.assert_called_once_with("index", {'a': 'b'})

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.helpers.bulk')
    @patch('nefertari.elasticsearch.ES.api')
    def test_refresh_parent_document(self, mock_api, mock_bulk,  mock_engine):

        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)
        mock_request = Mock()
        document = Mock()
        document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})
        parent_document = Mock()
        parent_document.to_indexable_dict = Mock(return_value={'_pk': 3, 'name': 'OldName', '_type': 'Parent'})
        updated_parent_document = Mock()
        updated_parent_document.to_indexable_dict = Mock(return_value={'_pk': 3, 'name': 'ChangedName', '_type': 'Parent'})
        document.get_parent_documents = lambda *args, **kwargs:[(updated_parent_document, None)]
        mock_request.context = document
        mock_request.params = Mock()
        mock_request.params.mixed = Mock()
        mock_request.params.mixed.return_value = {'_refresh_parent': True, '_refresh_index': True}

        documents = [
            document,
            parent_document
        ]

        obj.index(documents)

        with ES.registry as es_registry:
            es_registry.bind(mock_request)
            es_registry.bulk_index()

        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_op_type': 'index', '_source': {'_pk': 1, 'name': 'foo'}, '_id': 1, '_type': 'Doc', '_index': 'foondex'},
            {'_op_type': 'index', '_source': {'_pk': 3, 'name': 'ChangedName'}, '_id': 3, '_type': 'Parent', '_index': 'foondex'}
        ]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)


    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.helpers.bulk')
    @patch('nefertari.elasticsearch.ES.api')
    def test_refresh_parent_document_parent_not_indexed(self, mock_api, mock_bulk,  mock_engine):

        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)
        mock_request = Mock()
        document = Mock()
        document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})
        updated_parent_document = Mock()
        updated_parent_document.to_indexable_dict = Mock(return_value={'_pk': 3, 'name': 'ChangedName', '_type': 'Parent'})
        document.get_parent_documents = lambda *args, **kwargs:[(updated_parent_document, None)]
        mock_request.context = document
        mock_request.params = Mock()
        mock_request.params.mixed = Mock()
        mock_request.params.mixed.return_value = {'_refresh_parent': True, '_refresh_index': True}

        documents = [
            document
        ]

        obj.index(documents)

        with ES.registry as es_registry:
            es_registry.bind(mock_request)
            es_registry.bulk_index()

        args = mock_bulk._mock_call_args[1]

        expected_actions = [
            {'_op_type': 'index', '_source': {'_pk': 1, 'name': 'foo'}, '_id': 1, '_type': 'Doc', '_index': 'foondex'},
            {'_op_type': 'index', '_source': {'_pk': 3, 'name': 'ChangedName'}, '_id': 3, '_type': 'Parent', '_index': 'foondex'}
        ]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES._bulk')
    def test_index_document(self, mock_bulk, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=4)

        fake_document = Mock()
        fake_document.to_indexable_dict = Mock(return_value={"a": "b"})
        mock_engine.is_object_document = Mock(return_value=True)
        obj.index_document(fake_document)
        mock_bulk.assert_called_once_with("index", {"a": "b"})

        mock_engine.is_object_document = Mock(return_value=False)

        with pytest.raises(TypeError):
            obj.index_document("a")

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES._bulk')
    def test_index_documents(self, mock_bulk, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=4)

        fake_document = Mock()
        fake_document.to_indexable_dict = Mock(return_value={"a": "b"})
        mock_engine.is_object_document = Mock(return_value=True)

        obj.index_documents([fake_document, fake_document])
        mock_bulk.assert_called_once_with("index", [{"a": "b"}, {"a": "b"}])

        mock_engine.is_object_document = Mock(return_value=False)

        with pytest.raises(TypeError):
            obj.index_documents([fake_document, fake_document])

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_delete(self, mock_bulk, mock_settings):
        obj = es.ES('Foo', 'foondex', chunk_size=4)
        obj.delete(ids=[1])
        mock_settings.asbool.return_value = True

        with ES.registry as es_registry:
            es_registry.bulk_index()

        mock_bulk.assert_called_once_with(actions=[{'_op_type': 'delete', '_id': 1, '_index': 'foondex', '_type': 'Foo'}],
                                          client=ES.api, refresh=True)


    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_delete_single_obj(self, mock_bulk, mock_settings):
        obj = es.ES('Foo', 'foondex', chunk_size=4)
        obj.delete(ids=1)
        mock_settings.asbool.return_value = True

        with ES.registry as es_registry:
            es_registry.bulk_index()

        mock_bulk.assert_called_once_with(
            actions=[{'_op_type': 'delete', '_id': 1, '_index': 'foondex', '_type': 'Foo'}],
            client=ES.api, refresh=True)

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    @patch('nefertari.elasticsearch.ES.api.mget')
    @patch('nefertari.elasticsearch.ES.api')
    def test_index_missing_documents(self,mock_api, mock_mget, mock_bulk, mock_settings):
        print('here')
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_settings.asbool.return_value = True
        documents = [
            {'_pk': 1, 'name': 'foo'},
            {'_pk': 2, 'name': 'bar'}
        ]
        mock_mget.return_value = {'docs': [
            {'_id': '1', 'name': 'foo', 'found': False},
            {'_id': '2', 'name': 'bar', 'found': True}
        ]}

        obj.index_missing_documents(documents)

        with ES.registry as es_registry:
            es_registry.bulk_index()

        mock_mget.assert_called_once_with(
            index='foondex',
            doc_type='Foo',
            fields=['_id'],
            body={'ids': [1, 2]}
        )
        mock_bulk.assert_called_once_with(actions=[
            {'_id': 1, '_type': 'Foo', '_source': {'_pk': 1, 'name': 'foo'}, '_op_type': 'index', '_index': 'foondex'}
        ], client=mock_api, refresh=True)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_single_document_twice(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)
        mock_settings.asbool.return_value = True

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})
        documents = [
            first_document,
            second_document
        ]

        obj.index(documents)

        with ES.registry as es_registry:
            es_registry.bulk_index()

        mock_bulk.assert_called_once_with(actions=[
            {'_source': {'name': 'foo', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Doc', '_index': 'foondex'}
        ], client=mock_api, refresh=True)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_single_document_twice_different_type(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)
        mock_settings.asbool.return_value = True

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Item'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})
        documents = [
            first_document,
            second_document
        ]

        obj.index(documents)

        with ES.registry as es_registry:
            es_registry.bulk_index()
        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_source': {'name': 'foo', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Doc', '_index': 'foondex'},
            {'_source': {'name': 'foo', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Item', '_index': 'foondex'}
        ]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_single_document_twice_multiple_index_different_type(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)
        mock_settings.asbool.return_value = True

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Item'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})


        obj.index(first_document)
        obj.index(second_document)

        with ES.registry as es_registry:
            es_registry.bulk_index()
        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_source': {'name': 'foo', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Doc', '_index': 'foondex'},
            {'_source': {'name': 'foo', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Item', '_index': 'foondex'}
        ]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_single_document_twice_multiple_index(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)
        mock_settings.asbool.return_value = True

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})

        obj.index(first_document)
        obj.index(second_document)

        with ES.registry as es_registry:
            es_registry.bulk_index()

        mock_bulk.assert_called_once_with(actions=[
            {'_source': {'name': 'foo', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Doc', '_index': 'foondex'}
        ], client=mock_api, refresh=True)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_single_document_twice_differnt_pk(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 2, 'name': 'foo', '_type': 'Doc'})


        obj.index(first_document)
        obj.index(second_document)

        with ES.registry as es_registry:
            es_registry.bulk_index()
        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_source': {'name': 'foo', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Doc', '_index': 'foondex'},
            {'_source': {'name': 'foo', '_pk': 2}, '_op_type': 'index', '_id': 2, '_type': 'Doc', '_index': 'foondex'}
        ]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_single_document_twice_differnt_pk_different_type(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Item'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 2, 'name': 'foo', '_type': 'Doc'})


        obj.index(first_document)
        obj.index(second_document)

        with ES.registry as es_registry:
            es_registry.bulk_index()
        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_source': {'name': 'foo', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Item', '_index': 'foondex'},
            {'_source': {'name': 'foo', '_pk': 2}, '_op_type': 'index', '_id': 2, '_type': 'Doc', '_index': 'foondex'}
        ]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_single_document_twice_differnt_pk_different_type(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Item'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 2, 'name': 'foo', '_type': 'Doc'})


        obj.index(first_document)
        obj.index(second_document)

        with ES.registry as es_registry:
            es_registry.bulk_index()
        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_source': {'name': 'foo', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Item', '_index': 'foondex'},
            {'_source': {'name': 'foo', '_pk': 2}, '_op_type': 'index', '_id': 2, '_type': 'Doc', '_index': 'foondex'}
        ]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)


    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_single_document_twice_with_changes_in_second_update(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'bar', '_type': 'Doc'})

        obj.index(first_document)
        obj.index(second_document)

        with ES.registry as es_registry:
            es_registry.bulk_index()

        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_source': {'name': 'bar', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Doc', '_index': 'foondex'}]

        assert 'actions' in args
        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)


    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_single_document_three_times(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'bar', '_type': 'Doc'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})
        third_document = Mock()
        third_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'bar', '_type': 'Doc'})

        obj.index(first_document)
        obj.index(second_document)
        obj.index(third_document)

        with ES.registry as es_registry:
            es_registry.bulk_index()

        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_source': {'name': 'bar', '_pk': 1}, '_op_type': 'index', '_id': 1, '_type': 'Doc', '_index': 'foondex'}]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_single_document_twice_and_delete_it(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Doc', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'bar', '_type': 'Doc'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})

        obj.index(first_document)
        obj.index(second_document)
        obj.delete([1])

        with ES.registry as es_registry:
            es_registry.bulk_index()

        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_id': 1, '_type': 'Doc', '_index': 'foondex', '_op_type': 'delete'}
        ]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_twice_delete_document(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Doc', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)

        obj.delete([1])
        obj.delete([1])

        with ES.registry as es_registry:
            es_registry.bulk_index()

        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_id': 1, '_type': 'Doc', '_index': 'foondex', '_op_type': 'delete'}
        ]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_index_after_delete(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Doc', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'bar', '_type': 'Doc'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'foo', '_type': 'Doc'})

        obj.delete([1])

        obj.index(first_document)
        obj.index(second_document)

        with ES.registry as es_registry:
            es_registry.bulk_index()

        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_id': 1, '_type': 'Doc', '_index': 'foondex', '_op_type': 'delete'}
        ]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)

    @patch('nefertari.elasticsearch.engine')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_different_actions_in_one_request(self, mock_bulk, mock_settings, mock_api, mock_engine):
        obj = es.ES('Doc', 'foondex', chunk_size=10)
        mock_engine.is_object_document = Mock(return_value=True)

        first_document = Mock()
        first_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'bar', '_type': 'Doc'})
        second_document = Mock()
        second_document.to_indexable_dict = Mock(return_value={'_pk': 1, 'name': 'changed', '_type': 'Doc'})
        third_document = Mock()
        third_document.to_indexable_dict = Mock(return_value={'_pk': 2, 'name': 'foo', '_type': 'Doc'})
        fourth_document = Mock()
        fourth_document.to_indexable_dict = Mock(return_value={'_pk': 3, 'name': 'foo', '_type': 'Doc'})

        obj.delete([3])
        obj.index(first_document)
        obj.index(second_document)
        obj.index([third_document, fourth_document])

        with ES.registry as es_registry:
            es_registry.bulk_index()

        args = mock_bulk._mock_call_args[1]
        expected_actions = [
            {'_type': 'Doc', '_index': 'foondex', '_op_type': 'delete', '_id': 3},
            {'_type': 'Doc', '_source': {'name': 'foo', '_pk': 2}, '_index': 'foondex', '_op_type': 'index', '_id': 2},
            {'_type': 'Doc', '_source': {'name': 'changed', '_pk': 1}, '_index': 'foondex', '_op_type': 'index', '_id': 1}]

        assert 'actions' in args

        for action in args['actions']:
            assert action in expected_actions
            expected_actions.remove(action)

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    def test_exit_with_exception_from_registry_will_clean_it(self, mock_bulk, mock_settings):
        obj = es.ES('Foo', 'foondex', chunk_size=4)
        obj.delete(ids=[1])
        request = Mock()

        try:
            with ES.registry as es_registry:
                es_registry.bind(request)
                raise Exception()
        except:
            pass

        es_registry = ES.registry
        print(es_registry.registry)
        assert not mock_bulk.called
        assert len(es_registry.registry) == 0
        assert es_registry.request is None

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    @patch('nefertari.elasticsearch.ES.api.mget')
    @patch('nefertari.elasticsearch.ES.api')
    def test_index_missing_documents_document_not_exist(self,mock_api, mock_mget, mock_bulk, mock_settings):
        print('here')
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        documents = [
            {'_pk': 1, 'name': 'foo'},
            {'_pk': 2, 'name': 'bar'}
        ]
        mock_mget.return_value = {'docs': [
            {'_id': '2', 'name': 'bar', 'found': True}
        ]}
        mock_settings.asbool.return_value = True

        obj.index_missing_documents(documents)

        with ES.registry as es_registry:
            es_registry.bulk_index()

        mock_mget.assert_called_once_with(
            index='foondex',
            doc_type='Foo',
            fields=['_id'],
            body={'ids': [1, 2]}
        )
        mock_bulk.assert_called_once_with(actions=[
            {'_id': 1, '_type': 'Foo', '_source': {'_pk': 1, 'name': 'foo'}, '_op_type': 'index', '_index': 'foondex'}
        ], client=mock_api, refresh=True)



    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.helpers.bulk')
    @patch('nefertari.elasticsearch.ES.api.mget')
    @patch('nefertari.elasticsearch.ES.api')
    def test_index_missing_documents_no_index(self, mock_api, mock_mget, mock_bulk, mock_settings):
        obj = es.ES('Foo', 'foondex', chunk_size=10)
        documents = [
            {'_pk': 1, 'name': 'foo'},
        ]
        mock_mget.side_effect = es.IndexNotFoundException()
        mock_settings.asbool.return_value = True
        obj.index_missing_documents(documents)
        mock_mget.assert_called_once_with(
            index='foondex',
            doc_type='Foo',
            fields=['_id'],
            body={'ids': [1]}
        )

        with ES.registry as es_registry:
            es_registry.bulk_index()

        mock_bulk.assert_called_once_with(actions=[{'_op_type': 'index', '_type': 'Foo', '_index': 'foondex', '_source': {'name': 'foo', '_pk': 1}, '_id': 1}],
                                          client=mock_api, refresh=True)



    @patch('nefertari.elasticsearch.ES._bulk')
    @patch('nefertari.elasticsearch.ES.api.mget')
    def test_index_missing_documents_no_docs_passed(self, mock_mget, mock_bulk):
        obj = es.ES('Foo', 'foondex')
        assert obj.index_missing_documents([]) is None
        assert not mock_mget.called
        assert not mock_bulk.called

    @patch('nefertari.elasticsearch.ES._bulk')
    @patch('nefertari.elasticsearch.ES.api.mget')
    def test_index_missing_documents_all_docs_found(self, mock_mget, mock_bulk):
        obj = es.ES('Foo', 'foondex')
        documents = [
            {'_pk': 1, 'name': 'foo'},
        ]
        mock_mget.return_value = {'docs': [
            {'_id': '1', 'name': 'foo', 'found': True},
        ]}
        obj.index_missing_documents(documents)
        mock_mget.assert_called_once_with(
            index='foondex',
            doc_type='Foo',
            fields=['_id'],
            body={'ids': [1]}
        )
        assert not mock_bulk.called

    def test_get_by_ids_no_ids(self):
        obj = es.ES('Foo', 'foondex')
        docs = obj.get_by_ids([])
        assert isinstance(docs, es._ESDocs)
        assert len(docs) == 0

    @patch('nefertari.elasticsearch.ES.api.mget')
    @patch('nefertari.elasticsearch.ES.api')
    def test_get_by_ids(self, api, mock_mget):
        es.ES.document_proxy.update_document_proxies('Foo', None)
        es.ES.document_proxy.update_document_proxies('Foo2', None)
        obj = es.ES('Foo', 'foondex', chunk_size=123)
        documents = [{'_id': 1, '_type': 'Story'}]
        mock_mget.return_value = {
            'docs': [{
                '_type': 'Foo2',
                '_id': 1,
                '_source': {'_id': 1, '_type': 'Story', 'name': 'bar'},
                'fields': {'name': 'bar'}
            }]
        }
        docs = obj.get_by_ids(documents, _page=0)
        mock_mget.assert_called_once_with(
            body={'docs': [{'_index': 'foondex', '_type': 'Story', '_id': 1}]}
        )
        assert len(docs) == 1
        assert docs[0]._id == 1
        assert docs[0].name == 'bar'
        assert docs[0]._type == 'Foo2'
        assert docs._nefertari_meta['total'] == 1
        assert docs._nefertari_meta['start'] == 0
        assert docs._nefertari_meta['fields'] == []

    @patch('nefertari.elasticsearch.ES.api.mget')
    def test_get_by_ids_fields(self, mock_mget):
        es.ES.document_proxies = {'Foo': None, 'Foo2': None}
        obj = es.ES('Foo', 'foondex')
        documents = [{'_id': 1, '_type': 'Story'}]
        mock_mget.return_value = {
            'docs': [{
                '_type': 'Foo',
                '_id': 1,
                '_source': {'_id': 1, '_type': 'Story', 'name': 'bar'},
            }]
        }
        docs = obj.get_by_ids(documents, _limit=1, _fields=['name'])
        mock_mget.assert_called_once_with(
            body={'docs': [{'_index': 'foondex', '_type': 'Story', '_id': 1}]},
            _source_include=['name', '_type'], _source=True
        )
        assert len(docs) == 1
        assert hasattr(docs[0], '_id')
        assert hasattr(docs[0], '_type')
        assert docs[0].name == 'bar'
        assert docs._nefertari_meta['total'] == 1
        assert docs._nefertari_meta['start'] == 0
        assert sorted(docs._nefertari_meta['fields']) == sorted([
            'name', '_type'])

    @patch('nefertari.elasticsearch.ES.api.mget')
    def test_get_by_ids_no_index_raise(self, mock_mget):
        obj = es.ES('Foo', 'foondex')
        documents = [{'_id': 1, '_type': 'Story'}]
        mock_mget.side_effect = es.IndexNotFoundException()
        with pytest.raises(JHTTPNotFound) as ex:
            obj.get_by_ids(documents, _raise_on_empty=True)
        assert 'resource not found (Index does not exist)' in str(ex.value)

    @patch('nefertari.elasticsearch.ES.api.mget')
    def test_get_by_ids_no_index_not_raise(self, mock_mget):
        obj = es.ES('Foo', 'foondex')
        documents = [{'_id': 1, '_type': 'Story'}]
        mock_mget.side_effect = es.IndexNotFoundException()
        try:
            docs = obj.get_by_ids(documents, _raise_on_empty=False)
        except JHTTPNotFound:
            raise Exception('Unexpected error')
        assert len(docs) == 0

    @patch('nefertari.elasticsearch.ES.api.mget')
    def test_get_by_ids_not_found_raise(self, mock_mget):
        obj = es.ES('Foo', 'foondex')
        documents = [{'_id': 1, '_type': 'Story'}]
        mock_mget.return_value = {'docs': [{'_type': 'foo', '_id': 1}]}
        with pytest.raises(JHTTPNotFound):
            obj.get_by_ids(documents, _raise_on_empty=True)

    @patch('nefertari.elasticsearch.ES.api.mget')
    @patch('nefertari.elasticsearch.ES.api')
    def test_get_by_ids_not_found_not_raise(self, api, mock_mget):
        obj = es.ES('Foo', 'foondex', chunk_size=50)
        documents = [{'_id': 1, '_type': 'Story'}]
        mock_mget.return_value = {'docs': [{'_type': 'foo', '_id': 1}]}
        try:
            docs = obj.get_by_ids(documents, _raise_on_empty=False)
        except JHTTPNotFound:
            raise Exception('Unexpected error')
        assert len(docs) == 0

    def test_substitute_nested_fields(self):
        es.ES.document_proxy.update_document_proxies('Foo', Mock(substitutions=['a', 'b', 'c']))
        obj = es.ES('Foo', 'foondex', 110)
        substituted = obj.substitute_nested_fields('-a,+b,c,d,e,a', ',')

        assert substituted == '-a_nested,+b_nested,c_nested,d,e,a_nested'

    def test_add_nested_fields(self):
        pass

    @patch('nefertari.elasticsearch.engine', get_document_cls_with_sort_methods_factory(order=[5, 12, 34, 42, 11]))
    def test_build_search_params_custom_sort(self):
        
        es.ES.document_proxies = {'Foo': None}
        obj = es.ES('Foo', 'foondex', chunk_size=122)
        params = obj.build_search_params(
            {'foo': 1, 'zoo': 2, 'q': 'name:5', '_limit': 10, '_custom_sort': 'default'}
        )
        assert sorted(params.keys()) == sorted([
            'body', 'doc_type', 'from_', 'size', 'index'])
        assert params['body'] == {'query': {'function_score': {'boost_mode': 'replace',
                                                               'functions': [
                                                                   {'filter': {'term': {'id': 5}}, 'weight': 5},
                                                                   {'filter': {'term': {'id': 12}}, 'weight': 4},
                                                                   {'filter': {'term': {'id': 34}}, 'weight': 3},
                                                                   {'filter': {'term': {'id': 42}}, 'weight': 2},
                                                                   {'filter': {'term': {'id': 11}}, 'weight': 1}],
                                                               'query': {'bool': {'must': [
                                                                   {'query_string': {'query': 'foo:1 AND zoo:2 AND name:5'}},
                                                                   {'ids': {'type': 'Foo',
                                                                            'values': [5, 12, 34, 42, 11]}}]}}}}}
        assert params['index'] == 'foondex'
        assert params['doc_type'] == 'Foo'

    @patch('nefertari.elasticsearch.engine', get_document_cls_with_sort_methods_factory(order=[1, 2, 3, 5]))
    def test_build_search_params_custom_sort_with_es_q(self):

        es.ES.document_proxies = {'Foo': None}
        obj = es.ES('Foo', 'foondex', chunk_size=122)
        params = obj.build_search_params(
            { 'es_q': 'name:some AND status:active', '_limit': 10, '_custom_sort': 'default'}
        )
        print(params)
        assert sorted(params.keys()) == sorted([
            'body', 'doc_type', 'from_', 'size', 'index'])
        assert params['body'] == {'query': {'function_score': {'query': {'bool': {
            'must': [
                {'bool': {'must': [
                    {'term': {'name': 'some'}},
                    {'term': {'status': 'active'}}
                ]}},
                {'ids': {'type': 'Foo', 'values': [1, 2, 3, 5]}}
            ]}},
            'boost_mode': 'replace', 'functions': [
                {'filter': {'term': {'id': 1}}, 'weight': 4}, {'filter': {'term': {'id': 2}}, 'weight': 3},
                {'filter': {'term': {'id': 3}}, 'weight': 2}, {'filter': {'term': {'id': 5}}, 'weight': 1}
            ]}}}
        assert params['index'] == 'foondex'
        assert params['doc_type'] == 'Foo'

    @patch('nefertari.elasticsearch.engine', get_document_cls_with_sort_methods_factory())
    def test_build_search_params_custom_sort_with_sort(self):
        es.ES.document_proxies = {'Foo': None}
        obj = es.ES('Foo', 'foondex', chunk_size=122)

        with pytest.raises(JHTTPBadRequest) as exc:
            obj.build_search_params(
                {'foo': 1, 'zoo': 2, 'q': 'name:2', '_custom_sort': 'default', '_sort': 'name', '_limit': 10}
            )
        assert exc.value.message == '_custom_sort and _sort not allowed in one query'

    @patch('nefertari.elasticsearch.engine', get_document_cls_with_sort_methods_factory())
    def test_build_search_params_custom_sort_multidoc(self):
        es.ES.document_proxies = {'Foo': None}
        obj = es.ES('Foo,Bar', 'foondex', chunk_size=122)

        with pytest.raises(JHTTPBadRequest) as exc:
            obj.build_search_params(
                {'foo': 1, 'zoo': 2, 'q': '5', '_limit': 10, '_custom_sort': 'default'}
            )
        assert exc.value.message == '_custom_sort parameter does not support multidoc query'

    @patch('nefertari.elasticsearch.engine', get_document_cls_with_sort_methods_factory(without_sort=True))
    def test_build_search_params_custom_sort_not_found(self):
        es.ES.document_proxies = {'Foo': None}
        obj = es.ES('Foo', 'foondex', chunk_size=122)

        with pytest.raises(JHTTPBadRequest) as exc:
            obj.build_search_params(
                {'foo': 1, 'zoo': 2, 'q': 'name:2', '_custom_sort': 'default', '_limit': 10}
            )
        assert exc.value.message == 'sort method "default" not supported'

    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_build_search_params_no_body(self):
        es.ES.document_proxies = {'Foo': None}
        obj = es.ES('Foo', 'foondex', chunk_size=122)
        params = obj.build_search_params(
            {'foo': 1, 'zoo': 2, 'q': '5', '_limit': 10}
        )
        assert sorted(params.keys()) == sorted([
            'body', 'doc_type', 'from_', 'size', 'index'])
        assert params['body'] == {
            'query': {'bool': {'must': [{'query_string': {'query': 'foo:1 AND zoo:2 AND 5'}}]}}}
        assert params['index'] == 'foondex'
        assert params['doc_type'] == 'Foo'

    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_build_search_params_no_body_no_qs(self):
        es.ES.document_proxies = {'Foo': None}
        obj = es.ES('Foo', 'foondex', chunk_size=500)
        params = obj.build_search_params({'_limit': 10})
        assert sorted(params.keys()) == sorted([
            'body', 'doc_type', 'from_', 'size', 'index'])
        assert params['body'] == {'query': {'bool': {'must': [{'match_all': {}}]}}}
        assert params['index'] == 'foondex'
        assert params['doc_type'] == 'Foo'

    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_build_search_params_no_limit(self):
        es.ES.document_proxy.update_document_proxies('Foo', None)
        obj = es.ES('Foo', 'foondex', 123)
        obj.api = Mock()
        obj.api.count.return_value = {'count': 123}
        params = obj.build_search_params({'foo': 1})
        assert params == {'size': 123, 'doc_type': 'Foo', 'index': 'foondex',
                          'body': {
                              'query': {
                                  'bool': {
                                      'must': [
                                          {'query_string': {'query': 'foo:1'}}
                                      ]}}},
                          'from_': 0}
        obj.api.count.assert_called_once_with(index='foondex')

    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_build_search_params_sort(self):
        es.ES.document_proxy.update_document_proxies('Foo', None)
        obj = es.ES('Foo', 'foondex', 100)
        params = obj.build_search_params({
            'foo': 1, '_sort': '+a,-b,c', '_limit': 10})
        assert sorted(params.keys()) == sorted([
            'body', 'doc_type', 'index', 'sort', 'from_', 'size'])
        assert params['body'] == {
            'query': {'bool': {'must': [{'query_string': {'query': 'foo:1'}}]}}}
        assert params['index'] == 'foondex'
        assert params['doc_type'] == 'Foo'
        assert params['sort'] == 'a:asc,b:desc,c:asc'

    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_build_search_params_fields(self):
        es.ES.document_proxy.update_document_proxies('Foo', None)
        es.ES.document_proxies = {'Foo': None}
        obj = es.ES('Foo', 'foondex', chunk_size=500)
        params = obj.build_search_params({
            'foo': 1, '_fields': ['a'], '_limit': 10})
        assert sorted(params.keys()) == sorted([
            'body', 'doc_type', 'index', 'fields', 'from_', 'size'])
        print(params['body'])
        assert params['body'] == {
            'query': {'bool': {'must': [{'query_string': {'query': 'foo:1'}}]}}}
        assert params['index'] == 'foondex'
        assert params['doc_type'] == 'Foo'
        assert params['fields'] == ['a']

    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_build_search_params_search_fields(self):
        es.ES.document_proxy.update_document_proxies('Foo', None)
        obj = es.ES('Foo', 'foondex', 200)
        params = obj.build_search_params({
            'foo': 1, '_search_fields': 'a,b', '_limit': 10})
        assert sorted(params.keys()) == sorted([
            'body', 'doc_type', 'from_', 'size', 'index'])

        assert params['body'] == {
            'query': {'bool': {'must': [{'query_string': {
                'query': 'foo:1', 'fields': ['b^1', 'a^2']}}]}}}
        assert params['index'] == 'foondex'
        assert params['doc_type'] == 'Foo'

    def test_build_search_params_with_body(self):
        es.ES.document_proxy.update_document_proxies('Foo', None)
        obj = es.ES('Foo', 'foondex', 100)

        with pytest.raises(JHTTPUnprocessableEntity) as ex:

            obj.build_search_params({
                'body': {'query': {'query_string': 'foo'}},
                '_raw_terms': ' AND q:5',
                '_limit': 10,
                '_search_fields': 'a,b',
                '_fields': ['a'],
                '_sort': '+a,-b,c',
            })
            assert str(ex.value) == 'Illegal parameter "body"'

    @patch('nefertari.elasticsearch.ES.api.count')
    def test_do_count(self, mock_count):
        obj = es.ES('Foo', 'foondex')
        mock_count.return_value = {'count': 123}
        val = obj.do_count(
            {'foo': 1, 'size': 2, 'from_': 0, 'sort': 'foo:asc'})
        assert val == 123
        mock_count.assert_called_once_with(foo=1)

    @patch('nefertari.elasticsearch.ES.api.count')
    def test_do_count_no_index(self, mock_count):
        obj = es.ES('Foo', 'foondex')
        mock_count.side_effect = es.IndexNotFoundException()
        val = obj.do_count(
            {'foo': 1, 'size': 2, 'from_': 0, 'sort': 'foo:asc'})
        assert val == 0
        mock_count.assert_called_once_with(foo=1)

    def test_aggregate_no_aggregations(self):
        obj = es.ES('Foo', 'foondex')
        with pytest.raises(Exception) as ex:
            obj.aggregate(foo='bar')
        assert 'Missing _aggregations_params' in str(ex.value)

    @patch('nefertari.elasticsearch.ES.build_search_params')
    @patch('nefertari.elasticsearch.ES.api.search')
    def test_aggregation(self, mock_search, mock_build):
        mock_search.return_value = {'aggregations': {'foo': 1}}
        mock_build.return_value = {
            'size': 1, 'from_': 2, 'sort': 3,
            'body': {'query': 'query1'}
        }
        obj = es.ES('Foo', 'foondex')
        resp = obj.aggregate(_aggregations_params={'zoo': 5}, param1=6)
        assert resp == {'foo': 1}
        mock_build.assert_called_once_with({'_limit': 0, 'param1': 6})
        mock_search.assert_called_once_with(
            body={'aggregations': {'zoo': 5}, 'query': 'query1'},
        )

    @patch('nefertari.elasticsearch.ES.build_search_params')
    @patch('nefertari.elasticsearch.ES.api.search')
    def test_aggregation_nothing_returned(self, mock_search, mock_build):
        mock_search.return_value = {}
        mock_build.return_value = {
            'size': 1, 'from_': 2, 'sort': 3,
            'body': {'query': 'query1'}
        }
        obj = es.ES('Foo', 'foondex')
        with pytest.raises(JHTTPNotFound) as ex:
            obj.aggregate(_aggregations_params={'zoo': 5}, param1=6)
        assert 'No aggregations returned from ES' in str(ex.value)

    @patch('nefertari.elasticsearch.ES.build_search_params')
    @patch('nefertari.elasticsearch.ES.api.search')
    def test_aggregation_index_not_exists(self, mock_search, mock_build):
        mock_search.side_effect = es.IndexNotFoundException()
        mock_build.return_value = {
            'size': 1, 'from_': 2, 'sort': 3,
            'body': {'query': 'query1'}
        }
        obj = es.ES('Foo', 'foondex')
        with pytest.raises(JHTTPNotFound) as ex:
            obj.aggregate(_aggregations_params={'zoo': 5}, param1=6,
                          _raise_on_empty=True)
        assert 'Aggregation failed: Index does not exist' in str(ex.value)

    @patch('nefertari.elasticsearch.ES.build_search_params')
    @patch('nefertari.elasticsearch.ES.do_count')
    def test_get_collection_count_without_body(self, mock_count, mock_build):
        obj = es.ES('Foo', 'foondex')
        mock_build.return_value = {'foo': 'bar'}
        obj.get_collection(_count=True, foo=1)
        mock_count.assert_called_once_with({'foo': 'bar'})
        mock_build.assert_called_once_with({'_count': True, 'foo': 1})

    def test_get_collection_count_with_body(self):
        obj = es.ES('Foo', 'foondex', 100)
        with pytest.raises(JHTTPUnprocessableEntity) as ex:
            obj.get_collection(_count=True, foo=1, body={'foo': 'bar'})
            assert str(ex.value) == 'Illegal parameter "body"'

    @patch('nefertari.elasticsearch.ES.api.search')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_get_collection_fields(self, mock_api, mock_search):
        es.ES.document_proxy.update_document_proxies('Foo', None)
        es.ES.document_proxy.update_document_proxies('Zoo', None)
        obj = es.ES('Foo', 'foondex', chunk_size=500)
        mock_search.return_value = {
            'hits': {
                'hits': [{'_source': {'foo': 'bar', 'id': 1}, '_score': 2,
                          '_type': 'Zoo'}],
                'total': 4,
            },
            'took': 2.8,
        }
        docs = obj.get_collection(
            _fields=['foo'], foo='bar', from_=0)
        mock_search.assert_called_once_with(
            body={'query': {'bool': {'must': [{'query_string': {'query': 'foo:bar AND from_:0'}}]}}},
            doc_type='Foo', index='foondex',
            _source_include=['foo', '_type'], _source=True,
            from_=0, size=1)
        assert len(docs) == 1
        assert docs[0].id == 1
        assert docs[0]._score == 2
        assert docs[0].foo == 'bar'
        assert docs[0]._type == 'Zoo'
        assert docs._nefertari_meta['total'] == 4
        assert docs._nefertari_meta['start'] == 0
        assert sorted(docs._nefertari_meta['fields']) == sorted([
            'foo', '_type'])
        assert docs._nefertari_meta['took'] == 2.8

    @patch('nefertari.elasticsearch.ES.api.search')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_get_collection_source(self, mock_api, mock_search):
        es.ES.document_proxy.update_document_proxies('Foo', None)
        es.ES.document_proxy.update_document_proxies('Zoo', None)
        es.ES.document_proxies = {'Foo': None, 'Foo2': None, 'Zoo': None}
        obj = es.ES('Foo', 'foondex', chunk_size=500)
        mock_search.return_value = {
            'hits': {
                'hits': [{
                    '_source': {'foo': 'bar', 'id': 1}, '_score': 2,
                    '_type': 'Zoo'
                }],
                'total': 4,
            },
            'took': 2.8,
        }
        docs = obj.get_collection(foo='bar', from_=0)
        mock_search.assert_called_once_with(
            body={'query': {'bool': {'must': [{'query_string': {'query': 'foo:bar AND from_:0'}}]}}},
            doc_type='Foo', from_=0, size=1,
            index='foondex')
        assert len(docs) == 1
        assert docs[0].id == 1
        assert docs[0]._score == 2
        assert docs[0].foo == 'bar'
        assert docs[0]._type == 'Zoo'
        assert docs._nefertari_meta['total'] == 4
        assert docs._nefertari_meta['start'] == 0
        assert docs._nefertari_meta['fields'] == ''
        assert docs._nefertari_meta['took'] == 2.8

    @patch('nefertari.elasticsearch.ES.api.search')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_get_collection_no_index_raise(self, mock_api, mock_search):
        obj = es.ES('Foo', 'foondex', chunk_size=500)
        mock_search.side_effect = es.IndexNotFoundException()
        with pytest.raises(JHTTPNotFound) as ex:
            obj.get_collection(
                foo='bar', _raise_on_empty=True,
                from_=0)
        assert 'resource not found (Index does not exist)' in str(ex.value)

    @patch('nefertari.elasticsearch.ES.api.search')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_get_collection_no_index_not_raise(self, mock_api,  mock_search):
        obj = es.ES('Foo', 'foondex', chunk_size=500)
        mock_search.side_effect = es.IndexNotFoundException()
        try:
            docs = obj.get_collection(
                foo='bar', _raise_on_empty=False,
                from_=0)
        except JHTTPNotFound:
            raise Exception('Unexpected error')
        assert len(docs) == 0

    @patch('nefertari.elasticsearch.ES.api.search')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_get_collection_not_found_raise(self, mock_api, mock_search):
        obj = es.ES('Foo', 'foondex', chunk_size=500)
        mock_search.return_value = {
            'hits': {
                'hits': [],
                'total': 4,
            },
            'took': 2.8,
        }
        with pytest.raises(JHTTPNotFound):
            obj.get_collection(
                foo='bar', _raise_on_empty=True,
                from_=0)

    @patch('nefertari.elasticsearch.ES.api.search')
    @patch('nefertari.elasticsearch.ES.api')
    @patch('nefertari.elasticsearch.engine', MockEngine(get_document_cls=get_document_cls_with_es_mapping))
    def test_get_collection_not_found_not_raise(self, mock_api, mock_search):
        obj = es.ES('Foo', 'foondex')
        mock_search.return_value = {
            'hits': {
                'hits': [],
                'total': 4,
            },
            'took': 2.8,
        }
        try:
            docs = obj.get_collection(
                foo='bar', _raise_on_empty=False,
                from_=0)
        except JHTTPNotFound:
            raise Exception('Unexpected error')
        assert len(docs) == 0

    @patch('nefertari.elasticsearch.ES.api.get_source')
    def test_get_item(self, mock_get):
        obj = es.ES('Foo', 'foondex')
        mock_get.return_value = {'foo': 'bar', 'id': 4, '_type': 'Story'}
        story = obj.get_item(name='foo')
        assert story.id == 4
        assert story.foo == 'bar'
        mock_get.assert_called_once_with(
            name='foo', index='foondex', doc_type='Foo')

    @patch('nefertari.elasticsearch.ES.api.get_source')
    def test_get_item_no_index_raise(self, mock_get):
        obj = es.ES('Foo', 'foondex')
        mock_get.side_effect = es.IndexNotFoundException()
        with pytest.raises(JHTTPNotFound) as ex:
            obj.get_item(name='foo')
        assert 'resource not found (Index does not exist)' in str(ex.value)

    @patch('nefertari.elasticsearch.ES.api.get_source')
    def test_get_item_no_index_not_raise(self, mock_get):
        obj = es.ES('Foo', 'foondex')
        mock_get.side_effect = es.IndexNotFoundException()
        try:
            obj.get_item(name='foo', _raise_on_empty=False)
        except JHTTPNotFound:
            raise Exception('Unexpected error')

    @patch('nefertari.elasticsearch.ES.api.get_source')
    def test_get_item_not_found_raise(self, mock_get):
        obj = es.ES('Foo', 'foondex')
        mock_get.return_value = {}
        with pytest.raises(JHTTPNotFound):
            obj.get_item(name='foo')

    @patch('nefertari.elasticsearch.ES.api.get_source')
    def test_get_item_not_found_not_raise(self, mock_get):
        obj = es.ES('Foo', 'foondex')
        mock_get.return_value = {}
        try:
            obj.get_item(name='foo', _raise_on_empty=False)
        except JHTTPNotFound:
            raise Exception('Unexpected error')

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.ES.index_documents')
    def test_index_relations(self, mock_ind, mock_settings):
        class Foo(object):
            id = None
            _index_enabled = True

            def pk_field(self):
                return "id"

        docs = [Foo()]
        docs[0].id = 1
        db_obj = Mock()
        db_obj.get_related_documents.return_value = [(Foo, docs)]
        mock_settings.index_name = 'foo'
        es.ES.index_relations(db_obj)
        mock_ind.assert_called_once_with(docs)

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.ES.index')
    def test_index_relations_index_disabled(self, mock_ind, mock_settings):
        class Foo(object):
            _index_enabled = False

        docs = [Foo()]
        db_obj = Mock()
        db_obj.get_related_documents.return_value = [(Foo, docs)]
        mock_settings.index_name = 'foo'
        es.ES.index_relations(db_obj)
        assert not mock_ind.called

    @patch('nefertari.elasticsearch.ES.settings')
    @patch('nefertari.elasticsearch.ES.index_documents')
    def test_bulk_index_relations(self, mock_index, mock_settings):
        mock_settings.index_name = 'foo'

        class Foo(int):
            _index_enabled = True

        doc1 = Foo(1)
        doc2 = Foo(2)

        db_object1 = Mock()
        db_object1.get_related_documents.return_value = [
            (Foo, [doc1])]
        db_object2 = Mock()
        db_object2.get_related_documents.return_value = [
            (Foo, [doc2])]

        es.ES.bulk_index_relations([db_object1, db_object2])
        mock_index.assert_called_once_with({doc1, doc2})

    def test_document_proxies_inheritance(self):
        es.ES.document_proxy.update_document_proxies('Foo', 'BlaBla')

        class EsChild(es.ES):
            pass

        assert 'BlaBla' in EsChild.document_proxy.get_document_proxies_by_type('Foo')

    def test_document_proxies_unknown_type(self):
        with pytest.raises(es.UnknownDocumentProxiesTypeError) as excinfo:
            es.ES.document_proxy.get_document_proxies_by_type('Bar')
        assert 'You have no proxy for this Bar document type' in str(excinfo)


class TestESQuery:
    def test_get_query(self):
        pass

    def test_replace_query(self):
        pass