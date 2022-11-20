from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, DateTime, Integer, create_engine
from datetime import datetime
from os import path
from configuration.extract import get_common_value
import getpass

Base = declarative_base()
ROOT_DIR = path.dirname(path.abspath(__file__))

connection_string = "sqlite:///" + ROOT_DIR + "/table/history.db"
engine = create_engine(connection_string, echo=False)
Session = sessionmaker()


class History(Base):
    __tablename__ = 'history'
    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)
    process_id = Column(String, nullable=False)
    running_time_utc = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(engine)


def add_history():
    local_session = Session(bind=engine)
    username = getpass.getuser()
    process_id = get_common_value("ProcessId")
    history = History(username=username, process_id=process_id)
    local_session.add(history)
    local_session.commit()

