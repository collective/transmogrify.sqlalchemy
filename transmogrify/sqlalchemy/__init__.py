import logging
import sqlalchemy
from sqlalchemy.exceptions import OperationalError
from zope.interface import classProvides, implements
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.interfaces import ISection

class SQLSourceSection(object):
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous=previous

        self.logger=logging.getLogger(name)
        self.query=options["query"]
        
        # Allow for connection reuse along a pipeline
        self.dsn = options["dsn"]
        if hasattr(transmogrifier, '__sqlsource_connections'):
            self.conns = transmogrifier.__sqlsource_connections
        else:
            transmogrifier.__sqlsource_connection = self.conns = {}
            
        if self.dsn in self.conns:
            self.connection = conns[self.dsn]
            self.close = True
        else:
            engine = sqlalchemy.create_engine(self.dsn)
            self.conns[self.dsn] = self.connection = engine.connect()
            self.close = False

    def __iter__(self):
        for item in self.previous:
            yield item
            
        trans=self.connection.begin()
        try:
            result=self.connection.execute(self.query)
            for row in result:
                yield dict(row.items())
            trans.commit()
        except OperationalError, e:
            trans.rollback()
            self.logger.warn("SQL operational error: %s" % e)
        except:
            trans.rollback()
            raise

        if self.close:
            self.connection.close()
            del self.conns[self.dsn]


