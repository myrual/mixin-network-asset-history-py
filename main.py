import requests
import datetime
import iso8601
import sqlalchemy
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
class ScannedSnapshots(Base):
    __tablename__ = 'scannedSnapshots'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    created_at = Column(String(250))
    def __repr__(self):
        return "<ScannedSnapshots (created_at = '%s')>" % (
                                  self.created_at)


class NonInternalSnapshots(Base):
    __tablename__ = 'non_internal_transfer_snap'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    amount = Column(String(250))
    source = Column(String(100))
    at_year = Column(Integer)
    at_month = Column(Integer)
    at_day = Column(Integer)
    asset_name = Column(String(250))
    asset_key = Column(String(250))
    asset_id  = Column(String(250))
    asset_chainid  = Column(String(250))
    def __repr__(self):
        return "<NonInternalSnapshots (source='%s', asset name ='%s', asset id ='%s')>" % (
                                  self.source, self.asset_name, self.asset_id)

api_url = "https://api.mixin.one/network/snapshots"
init_time = "2006-01-02T15:04:05.999999999Z"

def find_deposit_withdraw(init_time):
    payload = {'limit':500, 'offset':init_time, 'order':"ASC"}

    result_ob = requests.get(api_url, params = payload).json()
    if "data" in result_ob:
        snapshots = result_ob["data"]
        lastsnap = snapshots[-1]
        found_result = []
        for eachSnap in snapshots:
            amount = eachSnap["amount"]
            created_at = iso8601.parse_date(eachSnap["created_at"])
            in_record_created = datetime.datetime(created_at.year, created_at.month, created_at.day, tzinfo = datetime.timezone.utc)
            source = eachSnap["source"]
            if source != "WITHDRAWAL_INITIALIZED" and source != "DEPOSIT_CONFIRMED":
                break
            asset_id = eachSnap["asset"]["asset_id"]
            asset_key = eachSnap["asset"]["asset_key"]
            asset_chain_id = eachSnap["asset"]["chain_id"]
            name = eachSnap["asset"]["name"]
            obj = {"year":created_at.year, "month":created_at.month, "day":created_at.day, "amount":amount, "source":source, "asset_id": asset_id, "asset_key": asset_key, "asset_chain_id": asset_chain_id, "name": name}
            found_result.append(obj)
        print(lastsnap["created_at"])
        print(found_result)
        result = {"found_records":found_result, "lastsnap_created_at":lastsnap["created_at"]}
        return result
    return None

engine = sqlalchemy.create_engine('sqlite:///mixin_asset.db')
# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
Base.metadata.create_all(engine)
Base.metadata.bind = engine

DBSession = sqlalchemy.orm.sessionmaker(bind=engine)
session = DBSession()

def loadSnap():
    while True:
        last_record_in_database = session.query(ScannedSnapshots).order_by(ScannedSnapshots.id.desc()).first()
        if last_record_in_database != None:
            program_start = last_record_in_database.created_at
        else:
            program_start = init_time
        print(program_start)
        find_result = find_deposit_withdraw(program_start)
        if find_result != None:
            if len(find_result["found_records"]) != 0:
                for eachResult in find_result["found_records"]:
                    thisRecord = NonInternalSnapshots()
                    thisRecord.at_year = eachResult["year"]
                    thisRecord.at_month = eachResult["month"]
                    thisRecord.at_day= eachResult["day"]
                    thisRecord.amount = eachResult["amount"]
                    thisRecord.source = eachResult["source"]
                    thisRecord.asset_name = eachResult["name"]
                    thisRecord.asset_key = eachResult["asset_key"]
                    thisRecord.asset_id = eachResult["asset_id"]
                    thisRecord.asset_chain_id = eachResult["asset_chain_id"]
                    session.add(thisRecord)
                    print(thisRecord)

            init_time = find_result["lastsnap_created_at"]
            print(init_time)
            last_record_in_database = session.query(ScannedSnapshots).order_by(ScannedSnapshots.id.desc()).first()
            if last_record_in_database != None:
                last_record_in_database.created_at = init_time
                print("update current last to %s"%init_time)
            else:
                print("insert last %s"%init_time)
                the_last_record = ScannedSnapshots()
                the_last_record.created_at = init_time
                session.add(the_last_record)
            print("session commit")
            session.commit()
        else:
            break
loadSnap()
