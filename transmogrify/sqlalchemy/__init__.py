import logging
import sqlalchemy
try:
    # deprecated in sqlalchemy 0.7 and removed in 0.7.3
    from sqlalchemy.exceptions import OperationalError
except ImportError:
    from sqlalchemy.exc import OperationalError
from zope.interface import provider
from zope.interface import implementer
from zope.annotation import IAnnotations
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.interfaces import ISection

import pymysql
pymysql.install_as_MySQLdb()

SQLSOURCE_KEY = 'transmogrify.sqlalchemy.sqlsourcesection'


@provider(ISectionBlueprint)
@implementer(ISection)
class SQLSourceSection(object):

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.logger = logging.getLogger(options['blueprint'])
        
        keys = list(options.keys())
        keys.sort()
        self.queries = [options[k] for k in keys if k.startswith('query')]
        
        # Allow for connection reuse along a pipeline
        dsn = options['dsn']
        conns = IAnnotations(transmogrifier).setdefault(SQLSOURCE_KEY, {})
            
        if dsn in conns:
            self.connection = conns[dsn]
        else:
            engine = sqlalchemy.create_engine(dsn)
            conns[dsn] = self.connection = engine.connect()

    def __iter__(self):
        for item in self.previous:
            yield item
            
        trans=self.connection.begin()
        try:
            for query in self.queries:
                result=self.connection.execute(query)
                for row in result:
                    # yield dict((x[0].encode('utf-8'), x[1]) for x in row.items())
                    yield dict((x[0], x[1]) for x in row.items())
            trans.commit()
        except OperationalError as e:
            trans.rollback()
            self.logger.warn("SQL operational error: %s" % e)
        except:
            trans.rollback()
            raise

