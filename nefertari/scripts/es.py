import sys
import logging
import math
import time
from argparse import ArgumentParser
from multiprocessing import Process, Manager
from datetime import datetime

from pyramid.paster import bootstrap
from pyramid.config import Configurator
from pyramid_sqlalchemy import BaseObject
from sqlalchemy import text
from six.moves import urllib

from nefertari.utils import dictset, split_strip, to_dicts, to_indexable_dicts
from nefertari.elasticsearch import ES, ESActionRegistry, create_index_with_settings
from nefertari import engine


def main(argv=sys.argv):

    parser = ArgumentParser(description=__doc__)

    parser.add_argument(
            '-c', '--config', help='config.ini (required)',
            required=True)
    parser.add_argument(
            '--params', help='Url-encoded params for each model')
    parser.add_argument('--index', help='Index name', default=None)
    parser.add_argument(
            '--chunk',
            help=('Index chunk size. If chunk size not provided '
                  '`elasticsearch.chunk_size` setting is used'),
            type=int)

    parser.add_argument('--processes',
                        required=False,
                        help='Split es action by multiple process',
                        action='store',
                        default=1,
                        type=int)

    parser.add_argument('--debug',
                        required=False,
                        help='Enable debug mode to check count for processed and existed item',
                        action='store_true',
                        default=False)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
            '--models',
            help=('Comma-separated list of model names to index'))
    group.add_argument(
            '--recreate',
            help='Recreate index and reindex all documents',
            action='store_true',
            default=False)

    options = parser.parse_args()

    if not options.config:
        return parser.print_help()

    processes = options.processes
    manager = ProcessManager(processes)
    app_registry = setup_app(options=options, put_mappings=True, lock=manager.app_initialize_lock)

    if options.recreate:
        recreate_index(app_registry)
        models = engine.get_document_classes()
        model_names = [
                name for name, model in models.items()
                if getattr(model, '_index_enabled', False)]
    else:
        model_names = split_strip(options.models)

    # Close all existed connections for avoid issue with concurrent access.
    # more here: http://docs.sqlalchemy.org/en/latest/faq/connections.html#how-do-i-use-engines-connections-sessions-with-python-multiprocessing-or-os-fork
    BaseObject.metadata.bind.dispose()

    consumers = [TaskConsumer(options=options, manager=manager) for _ in range(processes)]

    producer = TaskProducer(options=options, model_names=model_names,
                            manager=manager, consumers_count=processes)

    results = process_tasks(consumers, producer, manager)

    if options.debug:
        _check_results(results)


def _check_results(result):
    initial_dict = {}
    flat_list = []
    log = get_logger()
    log.setLevel(logging.INFO)

    for item in result:
        flat_list.extend(item)

    for item in flat_list:
        for key in item:
            if key not in initial_dict:
                initial_dict[key] = []
            initial_dict[key].extend(item[key])

    for model_name in initial_dict:
        items_count = engine.get_document_cls(model_name).get_collection().count()
        log.info('Total items count {} for model {}'.format(items_count, model_name))
        log.info('Total indexed tems count {} for model {}'.format(len(set(initial_dict[model_name])), model_name))

        assert len(initial_dict[model_name]) == len(set(initial_dict[model_name]))
        assert len(initial_dict[model_name]) == items_count


def process_tasks(consumers, producer, manager):
    producer.start()

    for c in consumers:
        c.start()

    manager.wait_for_exit()
    producer.join()

    return manager.results


def get_logger():
    log = logging.getLogger()
    log.setLevel(logging.WARNING)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)
    return log


def setup_app(options, put_mappings, lock):
    # Prevent ES.setup_mappings running on bootstrap;
    # Restore ES._mappings_setup after bootstrap is over
    log = get_logger()
    mappings_setup = getattr(ES, '_mappings_setup', False)

    with lock:
        try:
            ES._mappings_setup = True
            env = bootstrap(options.config)
        finally:
            ES._mappings_setup = mappings_setup
        registry = env['registry']
        # Include 'nefertari.engine' to setup specific engine
        config = Configurator(settings=registry.settings)
        config.include('nefertari.engine')
        ES.setup(dictset(registry.settings))
        if put_mappings:
            log.setLevel(logging.INFO)
        ES.setup_mappings()
    return registry


def recreate_index(registry):
    log = get_logger()
    settings = dictset(registry.settings)
    log.info('Deleting index')
    ES.delete_index()
    log.info('Creating index')
    create_index_with_settings(settings)
    log.info('Creating mappings')
    ES.setup_mappings()


class TaskProducer(Process):

    def __init__(self, *args, **kwargs):
        self.model_names = kwargs.pop('model_names')
        self.consumers_count = kwargs.pop('consumers_count')
        self.options = kwargs.pop('options')
        self.manager = kwargs.pop('manager')
        super().__init__(*args, **kwargs)

    def run(self):
        setup_app(self.options, put_mappings=False, lock=self.manager.app_initialize_lock)

        from sqlalchemy.orm import sessionmaker
        from pyramid_sqlalchemy import BaseObject
        from nefertari import engine
        from nefertari_sqla.documents import SessionHolder

        self.Session = sessionmaker()
        self.Session.configure(bind=BaseObject.metadata.bind)
        SessionHolder().set_session_factory(self.Session)
        session = self.Session()

        for model_name in self.model_names:
            model = engine.get_document_cls(model_name)
            limit = model.get_collection().count()
            table_name = model.__tablename__
            statement = text('SELECT {} FROM public.{}'.format(model.pk_field(), table_name))
            query = session.query(model).from_statement(statement)
            items = list(map(lambda item: getattr(item, model.pk_field()) ,sorted(query.values(model.pk_field()))))
            chunks = list(TaskProducer.split_collection(limit, self.consumers_count, items))

            for p in range(0, self.consumers_count):
                if len(chunks) > p:
                    self.manager.tasks.put((model_name, chunks[p]), block=False)

        self.manager.close_task_queue()

    @staticmethod
    def split_collection(limit, n, collection):
        list_size = (math.ceil(limit / n) - 1) or 1
        iteration = 0

        for i in range(0, limit, list_size):
            iteration += 1

            if iteration == n:
                yield collection[i:]
                break

            yield collection[i: i + list_size]


class ProcessManager:

    def __init__(self, processes_count):
        manager = Manager()
        self.consumers_count = processes_count
        self.tasks = ClosedQueueAdapter(manager.Queue())
        self.results = manager.list()
        self.app_initialize_lock = manager.Semaphore(1)
        self.consumer_lock = manager.Barrier(processes_count)
        self.barrier = manager.Barrier(processes_count + 1)

    def close_task_queue(self, *args, **kwrags):
        while not self.tasks.empty():
            time.sleep(1)

        self.tasks.close(consumers=range(0, self.consumers_count))

    def wait_for_exit(self):
        self.barrier.wait()

    def wait_for_consumers(self):
        self.consumer_lock.wait()


class TaskConsumer(Process):

    def __init__(self, *args, **kwargs):
        self.options = kwargs.pop('options')
        self.manager = kwargs.pop('manager')
        super().__init__(*args, **kwargs)

    def run(self):
        from pyramid_sqlalchemy import BaseObject
        from sqlalchemy.orm import sessionmaker
        from nefertari_sqla.documents import SessionHolder

        log = get_logger()
        results = []
        setup_app(self.options, put_mappings=False, lock=self.manager.app_initialize_lock)

        self.manager.wait_for_consumers()
        self.Session = sessionmaker()
        self.Session.configure(bind=BaseObject.metadata.bind)
        SessionHolder().set_session_factory(self.Session)
        session = self.Session()

        while True:
            try:
                data = self.manager.tasks.get()
            except QueueClosedException:
                break

            model_name, ids = data
            results.append({model_name: self.index_model(model_name, ids, session)})

            self.manager.tasks.task_done()

            if self.manager.tasks.empty():
                break

        log.info('indexing finished for process {} at {}'.format(self.pid, str(datetime.now())))
        self.manager.results.append(results)
        self.manager.wait_for_exit()

    def index_model(self, model_name, ids, session):
        from nefertari import engine
        print('working with model {}'.format(model_name))

        model = engine.get_document_cls(model_name)

        chunk_size = int(self.options.chunk or len(ids))

        es = ES(source=model_name, index_name=self.options.index, chunk_size=chunk_size)

        query_set = session.query(model).filter(getattr(model, model.pk_field()).in_(ids)).all()
        ids = [getattr(item, item.pk_field()) for item in query_set]
        documents = to_indexable_dicts(query_set)

        es.index_missing_documents(documents)
        es_actions = ESActionRegistry()

        for actions in es_actions.registry.values():
            es_actions.force_indexation(actions=actions)

        es_actions.registry.clear()
        return ids


class ClosedQueueAdapter:
    QUEUE_CLOSED_MESSAGE = 1

    def __init__(self, queue):
        self.queue = queue
        self.closed = False

    def close(self, consumers):
        for _ in consumers:
            self.queue.put(self.QUEUE_CLOSED_MESSAGE, block=False)

    def get(self, *args, **kwargs):
        if not self.closed:
            message = self.queue.get(*args, **kwargs)

            if message is self.QUEUE_CLOSED_MESSAGE:
                self.closed = True
                raise QueueClosedException()
            return message
        raise QueueClosedException()

    def put(self, *args, **kwargs):
        return self.queue.put(*args, **kwargs)

    def empty(self, *args, **kwargs):
        return self.queue.empty(*args, **kwargs)

    def task_done(self, *args, **kwargs):
        return self.queue.task_done(*args, **kwargs)


class QueueClosedException(Exception):
    pass
