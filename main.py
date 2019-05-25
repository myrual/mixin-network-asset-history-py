import gevent
from gevent.queue import Queue
from gevent import monkey
monkey.patch_all()

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
    snapshot_id = Column(String(64), unique=True)
    def __repr__(self):
        return "<NonInternalSnapshots (snapshot id = '%s' source='%s', asset name ='%s', asset id ='%s', created at ='%s', amount ='%f')>" % (
                                  self.snapshot_id, self.source, self.asset_name, self.asset_id, str(self.created_at), self.amount)

api_url = "https://api.mixin.one/network/snapshots"
mixin_init_time = "2006-01-02T15:04:05.999999999Z"

def find_deposit_withdraw(init_time):
    payload = {'limit':500, 'offset':init_time, 'order':"ASC"}

    while True:
        try:
            result_ob = requests.get(api_url, params = payload).json()
            if "data" in result_ob:
                snapshots = result_ob["data"]
                lastsnap = snapshots[-1]
                found_result = []
                for eachSnap in snapshots:
                    amount = float(eachSnap["amount"])
                    created_at = iso8601.parse_date(eachSnap["created_at"])
                    source = eachSnap["source"]
                    if source != "WITHDRAWAL_INITIALIZED" and source != "DEPOSIT_CONFIRMED":
                        continue
                    snapshot_id = eachSnap["snapshot_id"]
                    asset_id = eachSnap["asset"]["asset_id"]
                    asset_key = eachSnap["asset"]["asset_key"]
                    asset_chain_id = eachSnap["asset"]["chain_id"]
                    name = eachSnap["asset"]["name"]
                    obj = {"snapshot_id":snapshot_id, "created_at":created_at, "amount":amount, "source":source, "asset_id": asset_id, "asset_key": asset_key, "asset_chain_id": asset_chain_id, "name": name}
                    found_result.append(obj)
                result = {"found_records":found_result, "lastsnap_created_at":lastsnap["created_at"]}
                return result
            return None
        except:
            print("except:" + init_time)
            gevent.sleep(1)
            continue

engine = sqlalchemy.create_engine('sqlite:///mixin_asset.db')
# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
Base.metadata.create_all(engine)
Base.metadata.bind = engine

DBSession = sqlalchemy.orm.sessionmaker(bind=engine)
session = DBSession()

tasks = Queue()
def loadSnapOnDateTime(start_time, end_time):

    total_result = []
    thisDate         = start_time.isoformat()
    last_snap_string = start_time.isoformat()
    while True: 
        find_result = find_deposit_withdraw(thisDate)
        if find_result != None:
            for eachResult in find_result["found_records"]:
                total_result.append(eachResult)
            last_snap_string = find_result["lastsnap_created_at"]
            theLastDate = iso8601.parse_date(last_snap_string)
            if theLastDate < end_time:
                thisDate = last_snap_string
                continue
            else:
                print("exit because %s >= %s"%(last_snap_string, str(end_time)))
                tasks.put((total_result, last_snap_string, (start_time, end_time)))
                return
        else:
            tasks.put((total_result, last_snap_string, (start_time, end_time)))
            return
    tasks.put((total_result, last_snap_string, (start_time, end_time)))
    return

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
                    thisRecord.snapshot_id = eachResult["snapshot_id"]
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

def output_asset_with_amount(each_id, amount_string):
    each_asset_info = requests.get("https://api.mixin.one/network/assets/"+each_id).json()
    if "data" in each_asset_info:
        asset_value = each_asset_info["data"]
        asset_chain_name = ""
        if asset_value["chain_id"] == BTC_ASSET_ID:
            asset_chain_name += "Bitcoin"
        elif asset_value["chain_id"] == ETH_ASSET_ID:
            asset_chain_name += "Ethereum"
        elif asset_value["chain_id"] == EOS_ASSET_ID:
            asset_chain_name += "EOS"
        print(amount_string + "%s on chain %s id: %s"%(asset_value["name"].ljust(15) ,asset_chain_name.ljust(15), asset_value["asset_id"]))

while True:
    print("load snap: 1")
    print("load xin token: 2")
    print("load btc token: 3")
    print("load all token: 4")
    selection = input("your selection:")
    if(selection == "1"):
        year = input("year:")
        month = input("month:")
        day = input("day:")
        offset_days = int(input("offset days"))

        allspawn = []
        start = datetime.datetime(int(year), int(month),int(day), 0, 0, tzinfo=datetime.timezone.utc)
        end = ""
        for i in range(offset_days):
            minutes_interval = 20
            times = 24 * 60/minutes_interval
            this_start = start + datetime.timedelta(days = i)
            end = this_start + datetime.timedelta(minutes = minutes_interval)
            d = gevent.spawn(loadSnapOnDateTime, this_start, end)
            print(this_start, end)
            allspawn.append(d)

            #replicate the operation 
            for i in range(int(times) - 1):
                this_start = end
                end = this_start + datetime.timedelta(minutes = minutes_interval)
                print(this_start, end)

                d = gevent.spawn(loadSnapOnDateTime, this_start, end)
                allspawn.append(d)
            print(end)


        gevent.joinall(allspawn)

        for i in range(len(allspawn)):
            result = tasks.get()
            print(result)
            found_records = result[0]
            for eachRecord  in found_records:
                if session.query(NonInternalSnapshots).filter(NonInternalSnapshots.snapshot_id == eachRecord["snapshot_id"]).first() == None:
                    this                = NonInternalSnapshots()
                    this.snapshot_id    = eachRecord["snapshot_id"]
                    this.amount         = eachRecord["amount"]
                    this.created_at     = eachRecord["created_at"]
                    this.source         = eachRecord["source"]
                    this.asset_id       = eachRecord["asset_id"]
                    this.asset_key      = eachRecord["asset_key"]
                    this.asset_chain_id = eachRecord["asset_chain_id"]
                    this.asset_name     = eachRecord["name"]
                    session.add(this)
            session.commit()
            last_record = result[1]
            print(last_record)
            print(result[2])
        print(end)
    if(selection == "2"):
        first_day = datetime.datetime(2017, 12, 24, 0, 0, tzinfo=datetime.timezone.utc)
        today = datetime.datetime.now(datetime.timezone.utc)
        diff = (today - first_day).days
        daily_btc_balance = []
        for i in range(diff):
            this_day = first_day + datetime.timedelta(days = i)
            found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.created_at < this_day).filter(NonInternalSnapshots.asset_id == XIN_ASSET_ID).all()
            old = 0
            for each_record in found_records:
                old += each_record.amount
            daily_btc_balance.append(old)
        print(daily_btc_balance)

    if(selection == "3"):
        first_day = datetime.datetime(2017, 12, 24, 0, 0, tzinfo=datetime.timezone.utc)
        year = int(input("year:"))
        month = int(input("month:"))
        day = int(input("day"))
        today = datetime.datetime(year, month, day, 0, 0, tzinfo = datetime.timezone.utc)
        diff = (today - first_day).days
        daily_btc_balance = []
        for i in range(diff):
            this_day = first_day + datetime.timedelta(days = i)
            found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.created_at < this_day).filter(NonInternalSnapshots.asset_id == BTC_ASSET_ID).all()
            old = 0
            for each_record in found_records:
                old += each_record.amount

            print("%s %d"%(this_day, old))
    if(selection == "5"):
        found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.asset_id == BTC_ASSET_ID).all()
        for each_record in found_records:
            print(each_record.created_at, each_record.source, each_record.amount)
    if(selection == "6"):
        found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.asset_id == XIN_ASSET_ID).all()
        for each_record in found_records:
            print(each_record.created_at, each_record.source, each_record.amount)


    if(selection == "4"):
        year = int(input("year:"))
        month = int(input("month:"))
        day = int(input("day:"))
        target = datetime.datetime(year, month, day,0,0, tzinfo=datetime.timezone.utc)
        found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.created_at < target).all()
        total_result = {}
        for each_record in found_records:
            if each_record.asset_id in total_result:
                old = total_result[each_record.asset_id]
                old += each_record.amount
                total_result[each_record.asset_id] = old
            else:
                total_result[each_record.asset_id] = each_record.amount

        all_asset_ids = total_result.keys()
        all_asset_amount_spawn = []
        for each_id in all_asset_ids:

            amount_string = str(int(total_result[each_id])).ljust(15)
            all_asset_amount_spawn.append(gevent.spawn(output_asset_with_amount, each_id, amount_string))
        gevent.joinall(all_asset_amount_spawn)
