from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, TIMESTAMP, text, types
from sqlalchemy.types import VARCHAR
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.schema import FetchedValue

#engine = create_engine("mysql+pymysql://UserID:UserPWD@DB_IP:DB_Port/DB_Schema", max_overflow=5,echo=True)
Base = declarative_base()

class WSDATA(Base):
    __tablename__ = 'wsdata'
    id = Column(Integer, primary_key=True,nullable=False)
    #status = Column(String(5))
    chip_id = Column(MEDIUMTEXT)
    OP1 = Column(MEDIUMTEXT)
    OP2 = Column(MEDIUMTEXT)
    OP3 = Column(MEDIUMTEXT)
    OP4 = Column(MEDIUMTEXT)
    OP5 = Column(MEDIUMTEXT)
    OP89 = Column(MEDIUMTEXT)
    OP90 = Column(MEDIUMTEXT)
    time = Column(VARCHAR(30))
    updatetime = Column(TIMESTAMP,server_default = text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),server_onupdate = FetchedValue())


class connection_info():
    def __init__(self, username, password, host, port, database, Base):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = object()
        self.Base = Base

    def createengin(self):
        try:
            self.engine = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}', max_overflow=5,echo=False)
            return self.engine
        except Exception as e:
            return e
    
    def init_db(self):
        self.Base.metadata.create_all(self.engine)
        
    def drop_db(self):
        self.Base.metadata.drop_all(self.engine)  

if __name__ == '__main__':
    ci = connection_info('UserID','UserPWD','Database_IP','Database_Port','Database_Schema',Base)
    eg =ci.createengin()
    print(eg)
    ci.init_db()
    ci.drop_db()
    if not eg.dialect.has_table(eg, 'thdata'):
       ci.init_db()
    else:
       ci.drop_db()
    
