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
        self.engine=sqlalchemy.create_engine(options["dsn"])
        self.connection=self.engine.connect()


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

        self.connection.close()


