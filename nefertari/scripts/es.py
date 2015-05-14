from argparse import ArgumentParser
import sys
import urlparse
import logging

from pyramid.paster import bootstrap
from pyramid.config import Configurator
from zope.dottedname.resolve import resolve

from nefertari.utils import dictset, split_strip, to_dicts
from nefertari import engine


def main(argv=sys.argv, quiet=False):
    log = logging.getLogger()
    log.setLevel(logging.WARNING)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)

    command = ESCommand(argv, log)
    return command.run()


class ESCommand(object):

    bootstrap = (bootstrap,)
    stdout = sys.stdout
    usage = '%prog config_uri <models'

    def __init__(self, argv, log):
        parser = ArgumentParser(description=__doc__)

        parser.add_argument(
            '-c', '--config', help='config.ini (required)',
            required=True)
        parser.add_argument(
            '--quiet', help='Quiet mode', action='store_true',
            default=False)
        parser.add_argument(
            '--models',
            help=('Comma-separeted list of dotted paths or names '
                  'of models to index'),
            required=True)
        parser.add_argument(
            '--params', help='Url-encoded params for each model')
        parser.add_argument('--index', help='Index name', default=None)
        parser.add_argument('--chunk', help='Index chunk size', type=int)
        parser.add_argument(
            '--force',
            help=('Force reindex of all documents. Only documents that '
                  'are missing from index are indexed by default.'),
            type=bool, default=False)

        self.options = parser.parse_args()
        if not self.options.config:
            return parser.print_help()

        env = self.bootstrap[0](self.options.config)
        registry = env['registry']

        # Include 'nefertari.engine' to setup specific engine
        config = Configurator(settings=registry.settings)
        config.include('nefertari.engine')

        self.log = log

        if not self.options.quiet:
            self.log.setLevel(logging.INFO)

        self.settings = dictset(registry.settings)

    def resolve_model(self, model_str):
        """ Given a model string, returns model class and model name.

        Params:
            :model_str: Dotted path to model class or model name in the exact
                case it was defined. E.g. if you defined a model named
                'FooBar', pass 'FooBar' value here.
        """
        if '.' in model_str:
            model = resolve(model_str)
            model_name = model_str.split('.')[-1]
        else:
            model = engine.get_document_cls(model_str)
            model_name = model_str
        return model, model_name

    def run(self, quiet=False):
        from nefertari.elasticsearch import ES
        ES.setup(self.settings)
        model_strings = split_strip(self.options.models)

        for model_str in model_strings:
            model, model_name = self.resolve_model(model_str)

            params = self.options.params or ''
            params = dict([
                [k, v[0]] for k, v in urlparse.parse_qs(params).items()
            ])
            params.setdefault('_limit', params.get('_limit', 10000))
            chunk_size = self.options.chunk or params['_limit']

            es = ES(source=model_name, index_name=self.options.index)
            query_set = model.get_collection(**params)
            documents = to_dicts(query_set)

            if self.options.force:
                es.index(documents, chunk_size=chunk_size)
            else:
                es.index_missing(documents, chunk_size=chunk_size)

        return 0
