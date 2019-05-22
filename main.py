import threading
import requests
import datetime
import iso8601
import sqlalchemy
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

BTC_ASSET_ID    = "c6d0c728-2624-429b-8e0d-d9d19b6592fa";
EOS_ASSET_ID    = "6cfe566e-4aad-470b-8c9a-2fd35b49c68d";
USDT_ASSET_ID   = "815b0b1a-2764-3736-8faa-42d694fa620a"
ETC_ASSET_ID    = "2204c1ee-0ea2-4add-bb9a-b3719cfff93a";
XRP_ASSET_ID    = "23dfb5a5-5d7b-48b6-905f-3970e3176e27";
XEM_ASSET_ID    = "27921032-f73e-434e-955f-43d55672ee31"
ETH_ASSET_ID    = "43d61dcd-e413-450d-80b8-101d5e903357";
DASH_ASSET_ID   = "6472e7e3-75fd-48b6-b1dc-28d294ee1476";
DOGE_ASSET_ID   = "6770a1e5-6086-44d5-b60f-545f9d9e8ffd"
LTC_ASSET_ID    = "76c802a2-7c88-447f-a93e-c29c9e5dd9c8";
SIA_ASSET_ID    = "990c4c29-57e9-48f6-9819-7d986ea44985";
ZEN_ASSET_ID    = "a2c5d22b-62a2-4c13-b3f0-013290dbac60"
ZEC_ASSET_ID    = "c996abc9-d94e-4494-b1cf-2a3fd3ac5714"
BCH_ASSET_ID    = "fd11b6e3-0b87-41f1-a41f-f0e9b49e5bf0"
XIN_ASSET_ID    = "c94ac88f-4671-3976-b60a-09064f1811e8"



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
    amount = Column(Float(precision = 8))
    source = Column(String(100))
    created_at = Column(DateTime)
    asset_name = Column(String(250))
    asset_key = Column(String(250))
    asset_id  = Column(String(250))
    asset_chainid  = Column(String(250))
    def __repr__(self):
        return "<NonInternalSnapshots (source='%s', asset name ='%s', asset id ='%s', created at ='%s', amount ='%f')>" % (
                                  self.source, self.asset_name, self.asset_id, str(self.created_at), self.amount)

api_url = "https://api.mixin.one/network/snapshots"
mixin_init_time = "2006-01-02T15:04:05.999999999Z"

def find_deposit_withdraw(init_time):
    payload = {'limit':500, 'offset':init_time, 'order':"ASC"}

    result_ob = requests.get(api_url, params = payload).json()
    if "data" in result_ob:
        snapshots = result_ob["data"]
        lastsnap = snapshots[-1]
        found_result = []
        for eachSnap in snapshots:
            amount = float(eachSnap["amount"])
            created_at = iso8601.parse_date(eachSnap["created_at"])
            in_record_created = datetime.datetime(created_at.year, created_at.month, created_at.day, tzinfo = datetime.timezone.utc)
            source = eachSnap["source"]
            if source != "WITHDRAWAL_INITIALIZED" and source != "DEPOSIT_CONFIRMED":
                break
            asset_id = eachSnap["asset"]["asset_id"]
            asset_key = eachSnap["asset"]["asset_key"]
            asset_chain_id = eachSnap["asset"]["chain_id"]
            name = eachSnap["asset"]["name"]
            obj = {"created_at":created_at, "amount":amount, "source":source, "asset_id": asset_id, "asset_key": asset_key, "asset_chain_id": asset_chain_id, "name": name}
            found_result.append(obj)
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
            program_start = mixin_init_time
        find_result = find_deposit_withdraw(program_start)
        if find_result != None:
            if len(find_result["found_records"]) != 0:
                for eachResult in find_result["found_records"]:
                    thisRecord = NonInternalSnapshots()
                    thisRecord.created_at = eachResult["created_at"]
                    thisRecord.amount = eachResult["amount"]
                    thisRecord.source = eachResult["source"]
                    thisRecord.asset_name = eachResult["name"]
                    thisRecord.asset_key = eachResult["asset_key"]
                    thisRecord.asset_id = eachResult["asset_id"]
                    thisRecord.asset_chain_id = eachResult["asset_chain_id"]
                    session.add(thisRecord)

            init_time = find_result["lastsnap_created_at"]
            last_record_in_database = session.query(ScannedSnapshots).order_by(ScannedSnapshots.id.desc()).first()
            print(init_time)
            if last_record_in_database != None:
                last_record_in_database.created_at = init_time
            else:
                the_last_record = ScannedSnapshots()
                the_last_record.created_at = init_time
                session.add(the_last_record)
            session.commit()
        else:
            break

while True:
    print("load snap: 1")
    print("load xin token: 2")
    print("load btc token: 3")
    print("load all token: 4")
    selection = input("your selection:")
    if(selection == "1"):
        t = threading.Thread(target = loadSnap)
        t.start()
    if(selection == "2"):
        last_record_in_database = session.query(ScannedSnapshots).order_by(ScannedSnapshots.id.desc()).first()
        print("latest scanned record is %s"%last_record_in_database.created_at)
        found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.asset_id == XIN_ASSET_ID).all()
        for each_record in found_records:
            print(each_record)

    if(selection == "3"):
        last_record_in_database = session.query(ScannedSnapshots).order_by(ScannedSnapshots.id.desc()).first()
        print("latest scanned record is %s"%last_record_in_database.created_at)
        found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.asset_id == BTC_ASSET_ID).all()
        for each_record in found_records:
            print(each_record)
    if(selection == "4"):
        last_record_in_database = session.query(ScannedSnapshots).order_by(ScannedSnapshots.id.desc()).first()
        print("latest scanned record is %s"%last_record_in_database.created_at)
        found_records = session.query(NonInternalSnapshots).all()
        total_result = {}
        for each_record in found_records:
            if each_record.asset_id in total_result:
                old = total_result[each_record.asset_id]
                old += each_record.amount
                total_result[each_record.asset_id] = old
            else:
                total_result[each_record.asset_id] = each_record.amount

            print(each_record)
        print(total_result)


