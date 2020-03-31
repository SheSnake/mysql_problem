#!/usr/bin/env python3
import traceback
import json
import time
import datetime
import MySQLdb
import logging
import logzero
import threading

from logzero import logger
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DECIMAL, and_
from sqlalchemy.sql import select, insert, update, join
from sqlalchemy.orm.session import sessionmaker


Session = sessionmaker()
class PhatomProblem(object):
    def __init__(self, config):
        self.config = config
        self.init_mysqldb()

    def init_mysqldb(self):
        self.tables = config['database']['tables']
        self.engine = create_engine(str(r'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8') % (
            self.config['database']['username'], self.config['database']['password'],
            self.config['database']['host'], self.config['database']['port'], self.config['database']['database']), pool_recycle=3600)

        self.meta = MetaData(bind=self.engine)
        self.table_a = Table(self.tables['table_a'], self.meta,
            Column('user_id', Integer),
            Column('last_withdraw_id', Integer),
            )
        self.table_b = Table(self.tables['table_b'], self.meta,
            Column('user_id', Integer),
            Column('index_id', Integer),
        )

    def range_update_tx(self):
        try:
            session = Session(bind=self.engine, autoflush=True, autocommit=False)
            logger.info('start update tx')
            stmt = self.table_a.update().values(
                    last_withdraw_id=-1,
            ).where(self.table_a.c.user_id < 5)
            session.execute(stmt)
            logger.info('update ok')
            time.sleep(5)
            logger.info('try commit update')
            session.commit()
            logger.info('update commit')
        except Exception as e:
            traceback.print_exc()
        finally:
            session.close()

    # two tx update the same row
    # tx2 update after tx1 update but before tx1 commit
    # tx2 should get the write lock, so it should wait tx1 commit
    # because in the same thread, timeout and exit
    def repeate_read_update_1(self, user_id):
        try:
            session = Session(bind=self.engine, autoflush=False, autocommit=False)
            stmt = self.table_a.insert().values(
                    user_id=user_id,
                    last_withdraw_id=-1,
            )
            session.execute(stmt)
            session.commit()
            logger.info('t0: s0 insert user_id:%d', user_id)

            session_1 = Session(bind=self.engine, autoflush=False, autocommit=False)
            session_2 = Session(bind=self.engine, autoflush=False, autocommit=False)

            stmt = select([self.table_a]).where(self.table_a.c.user_id == user_id)
            row = session_1.execute(stmt).first()
            logger.info('t1: s1 first read user_id:%d withdraw:%d', row['user_id'], row['last_withdraw_id'])
            stmt = self.table_a.update().values(
                    last_withdraw_id=10,
            ).where(self.table_a.c.user_id == user_id)
            session_1.execute(stmt)
            logger.info('t2: s1 update user_id:%d', user_id)

            stmt = select([self.table_a]).where(self.table_a.c.user_id == user_id)
            row =session_1.execute(stmt).first()
            logger.info('t3: s1 second read %d %d', row['user_id'], row['last_withdraw_id'])

            stmt = select([self.table_a]).where(self.table_a.c.user_id == user_id)
            row =session_2.execute(stmt).first()
            logger.info('t4: s2 first read %d %d', row['user_id'], row['last_withdraw_id'])

            logger.info('t4.1: s2 try update user_id:%d, it will try to get the table lock first... lock..', row['user_id'])
            stmt = self.table_a.update().values(
                    last_withdraw_id=15,
            ).where(self.table_a.c.user_id == user_id)
            session_2.execute(stmt)
            # timeout and exit
            logger.info('t5: s2 update user_id:%d', user_id)

            stmt = select([self.table_a]).where(self.table_a.c.user_id == user_id)
            row =session_2.execute(stmt).first()
            logger.info('t6: s2 second read %d %d', row['user_id'], row['last_withdraw_id'])

            session_2.commit()
            logger.info('t7: s2 commit')
            time.sleep(10)
            session_1.commit()
            logger.info('t8: s1 commit')
        except Exception as e:
            traceback.print_exc()
        finally:
            session.close()
            session_1.close()
            session_2.close()

    # write skew problem
    # two tx update the same row.
    # tx2 select the row after tx1 execute update before tx1 commit.
    # tx2 will read the origin data.
    # tx1 commit
    # tx2 will still read the origin data because snapshot read.
    # but tx2 update and commit after tx1 commit.
    # so the tx1 commit result will be replace by tx2.
    # commitupdate the row
    def repeat_read_update_2(self, user_id):
        try:
            session = Session(bind=self.engine, autoflush=False, autocommit=False)
            stmt = self.table_a.insert().values(
                    user_id=user_id,
                    last_withdraw_id=-1,
            )
            session.execute(stmt)
            stmt = self.table_a.insert().values(
                    user_id=user_id + 50,
                    last_withdraw_id=-1,
            )
            session.execute(stmt)
            session.commit()
            logger.info('t0: s0 insert user_id:%d, user_id:%d', user_id, user_id + 50)

            session_1 = Session(bind=self.engine, autoflush=False, autocommit=False)
            session_2 = Session(bind=self.engine, autoflush=False, autocommit=False)

            stmt = select([self.table_a]).where(self.table_a.c.user_id == user_id)
            row = session_1.execute(stmt).first()
            logger.info('t1: s1 first read user_id:%d withdraw:%d', row['user_id'], row['last_withdraw_id'])
            stmt = self.table_a.update().values(
                    last_withdraw_id=10,
            ).where(self.table_a.c.user_id == user_id)
            session_1.execute(stmt)
            logger.info('t2.1: s1 update user_id:%d', user_id)

            stmt = self.table_a.update().values(
                    last_withdraw_id=10,
            ).where(self.table_a.c.user_id == user_id + 50)
            session_1.execute(stmt)
            logger.info('t2.2: s1 update user_id:%d', user_id + 50)

            stmt = select([self.table_a]).where(self.table_a.c.user_id == user_id)
            row =session_1.execute(stmt).first()
            logger.info('t3: s1 second read %d %d', row['user_id'], row['last_withdraw_id'])


            stmt = select([self.table_a]).where(self.table_a.c.user_id == user_id)
            row =session_2.execute(stmt).first()
            logger.info('t4: s2 first read %d %d', row['user_id'], row['last_withdraw_id'])

            session_1.commit()
            logger.info('t5: s1 commit')

            stmt = select([self.table_a]).where(self.table_a.c.user_id == user_id)
            row =session_2.execute(stmt).first()
            logger.info('t6.1: s2 second read %d %d', row['user_id'], row['last_withdraw_id'])

            stmt = select([self.table_a]).where(self.table_a.c.user_id == user_id + 50)
            row =session_2.execute(stmt).first()
            logger.info('t6.2: s2 second read %d %d', row['user_id'], row['last_withdraw_id'])

            stmt = self.table_a.update().values(
                    last_withdraw_id=15,
            ).where(self.table_a.c.user_id == user_id)
            session_2.execute(stmt)
            # timeout and exit
            logger.info('t7: s2 update user_id:%d', user_id)

            stmt = select([self.table_a]).where(self.table_a.c.user_id == user_id)
            row =session_2.execute(stmt).first()
            logger.info('t8: s2 third read %d %d', row['user_id'], row['last_withdraw_id'])

            session_2.commit()
            logger.info('t9: s2 commit')
        except Exception as e:
            traceback.print_exc()
        finally:
            session.close()
            session_1.close()
            session_2.close()

    # snapshot read, not phantom
    def phantom_read_1(self, user_id):
        try:
            session = Session(bind=self.engine, autoflush=False, autocommit=False)
            stmt = self.table_a.insert().values(
                    user_id=user_id,
                    last_withdraw_id=-1,
            )
            session.execute(stmt)
            session.commit()
            logger.info('t0: s0 insert user_id:%d', user_id)

            session_1 = Session(bind=self.engine, autoflush=False, autocommit=False)
            session_2 = Session(bind=self.engine, autoflush=False, autocommit=False)

            stmt = select([self.table_a])
            for row in session_1.execute(stmt):
                logger.info('t1: s1 first read user_id:%d withdraw:%d', row['user_id'], row['last_withdraw_id'])
            stmt = self.table_a.insert().values(
                    user_id=user_id + 1,
                    last_withdraw_id=-1,
            )
            session_2.execute(stmt)
            logger.info('t2: s2 insert user_id:%d', user_id + 1)

            stmt = select([self.table_a])
            for row in session_1.execute(stmt):
                logger.info('t3: s1 second read user_id:%d withdraw:%d', row['user_id'], row['last_withdraw_id'])

            stmt = select([self.table_a])
            for row in session_2.execute(stmt):
                logger.info('t3.1: s2 first read user_id:%d withdraw:%d', row['user_id'], row['last_withdraw_id'])

            session_2.commit()
            logger.info('t4: s2 commit')
            time.sleep(2)

            stmt = select([self.table_a]).where(self.table_a.c.user_id > 0)
            for row in session_1.execute(stmt):
                logger.info('t5: s1 third read user_id:%d withdraw:%d', row['user_id'], row['last_withdraw_id'])
            session_1.commit()

        except Exception as e:
            traceback.print_exc()
        finally:
            session.close()
            session_1.close()
            session_2.close()

    # current read, not phantom
    def phantom_read_2(self, user_id):
        try:
            session = Session(bind=self.engine, autoflush=False, autocommit=False)
            stmt = self.table_a.insert().values(
                    user_id=user_id,
                    last_withdraw_id=-1,
            )
            session.execute(stmt)
            session.commit()
            logger.info('t0: s0 insert user_id:%d', user_id)

            session_1 = Session(bind=self.engine, autoflush=False, autocommit=False)
            session_2 = Session(bind=self.engine, autoflush=False, autocommit=False)

            stmt = select([self.table_a], for_update=True)
            for row in session_1.execute(stmt):
                logger.info('t1: s1 first read user_id:%d withdraw:%d', row['user_id'], row['last_withdraw_id'])
            stmt = self.table_a.insert().values(
                    user_id=user_id + 1,
                    last_withdraw_id=-1,
            )
            session_2.execute(stmt)
            logger.info('t2: s2 insert user_id:%d', user_id + 1)

            stmt = select([self.table_a])
            for row in session_1.execute(stmt):
                logger.info('t3: s1 second read user_id:%d withdraw:%d', row['user_id'], row['last_withdraw_id'])

            stmt = select([self.table_a])
            for row in session_2.execute(stmt):
                logger.info('t3.1: s2 first read user_id:%d withdraw:%d', row['user_id'], row['last_withdraw_id'])

            session_2.commit()
            logger.info('t4: s2 commit')
            time.sleep(2)

            stmt = select([self.table_a]).where(self.table_a.c.user_id > 0)
            for row in session_1.execute(stmt):
                logger.info('t5: s1 third read user_id:%d withdraw:%d', row['user_id'], row['last_withdraw_id'])
            session_1.commit()

        except Exception as e:
            traceback.print_exc()
        finally:
            session.close()
            session_1.close()
            session_2.close()


if __name__ == '__main__':
    config = json.load(open('./config.json'))
    problem = PhatomProblem(config)
    #problem.run()
    #problem.repeate_read_update_2(15)
    #problem.phantom_read_2(10)
    problem.repeat_read_update_2(3)
