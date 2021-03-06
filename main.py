import gevent
from gevent.queue import Queue
from gevent import monkey
from gevent import getcurrent
from gevent.pool import Group, Pool
monkey.patch_all()

import threading
import requests
import datetime
import csv
import sys
import iso8601
import sqlalchemy
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from prettytable import PrettyTable

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

Asset_group = {"BTC":BTC_ASSET_ID, "ETH":ETH_ASSET_ID, "EOS":EOS_ASSET_ID, "USDT":USDT_ASSET_ID, "XIN":XIN_ASSET_ID, "LTC":LTC_ASSET_ID, "ZEC":ZEC_ASSET_ID, "DOGE":DOGE_ASSET_ID, "XRP":XRP_ASSET_ID, "DASH":DASH_ASSET_ID, "BCH":BCH_ASSET_ID}
Important_Asset = [BTC_ASSET_ID, ETH_ASSET_ID, EOS_ASSET_ID, USDT_ASSET_ID, XIN_ASSET_ID, LTC_ASSET_ID, ZEC_ASSET_ID,DOGE_ASSET_ID, XRP_ASSET_ID, DASH_ASSET_ID, BCH_ASSET_ID]



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


class TradingSnapshots(Base):
    __tablename__ = 'trading_snapshot'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    amount = Column(Float(precision = 8))
    source = Column(String(100), index = True)
    created_at = Column(DateTime)
    asset_id  = Column(String(250), index = True)
    snapshot_id = Column(String(64), unique=True, index = True)
    def __repr__(self):
        return "<trading_snapshot (snapshot id = '%s' source='%s', asset id ='%s', created at ='%s', amount ='%f')>" % (
                                  self.snapshot_id, self.source, self.asset_id, str(self.created_at), self.amount)


class NonInternalSnapshots(Base):
    __tablename__ = 'non_internal_transfer_snap'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    amount = Column(Float(precision = 8))
    source = Column(String(100), index = True)
    created_at = Column(DateTime)
    asset_name = Column(String(250))
    asset_key = Column(String(250))
    asset_id  = Column(String(250), index = True)
    asset_chainid  = Column(String(250))
    snapshot_id = Column(String(64), unique=True, index = True)
    def __repr__(self):
        return "<NonInternalSnapshots (snapshot id = '%s' source='%s', asset name ='%s', asset id ='%s', created at ='%s', amount ='%f')>" % (
                                  self.snapshot_id, self.source, self.asset_name, self.asset_id, str(self.created_at), self.amount)

api_url = "https://api.mixin.one/network/snapshots"
mixin_init_time = "2006-01-02T15:04:05.999999999Z"

def find_deposit_withdraw(init_time, order = "ASC", input_asset_id = ""):
    if input_asset_id != "":
        payload = {'limit':500, 'offset':init_time, 'order':order, 'asset':input_asset_id}
    else:
        payload = {'limit':500, 'offset':init_time, 'order':order}

    while True:
        try:
            result_ob = requests.get(api_url, params = payload).json()
            if "data" in result_ob:
                snapshots = result_ob["data"]
                if len(snapshots) == 0:
                    return None
                lastsnap = snapshots[-1]
                found_result = []
                for eachSnap in snapshots:
                    amount = float(eachSnap["amount"])
                    created_at = iso8601.parse_date(eachSnap["created_at"])
                    source = eachSnap["source"]
                    if source == "WITHDRAWAL_INITIALIZED" or source == "DEPOSIT_CONFIRMED":
                        snapshot_id = eachSnap["snapshot_id"]
                        asset_id = eachSnap["asset"]["asset_id"]
                        asset_key = eachSnap["asset"]["asset_key"]
                        asset_chain_id = eachSnap["asset"]["chain_id"]
                        name = eachSnap["asset"]["name"]
                        obj = {"snapshot_id":snapshot_id, "created_at":created_at, "amount":amount, "source":source, "asset_id": asset_id, "asset_key": asset_key, "asset_chain_id": asset_chain_id, "name": name}
                        found_result.append(obj)
                    if source == "TRANSFER_INITIALIZED":
                        snapshot_id = eachSnap["snapshot_id"]
                        asset_id = eachSnap["asset"]["asset_id"]
                        if asset_id in Important_Asset:
                            obj = {"snapshot_id":snapshot_id, "created_at":created_at, "amount":amount, "source":source, "asset_id": asset_id}
                            found_result.append(obj)
                result = {"found_records":found_result, "lastsnap_created_at":lastsnap["created_at"]}
                return result
            return None
        except:
            print("except:" + init_time)
            print(sys.exc_info()[0])
            gevent.sleep(5)
            continue

engine = sqlalchemy.create_engine('sqlite:///mixin_asset.db')
# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
Base.metadata.create_all(engine)
Base.metadata.bind = engine

DBSession = sqlalchemy.orm.sessionmaker(bind=engine)
session = DBSession()

tasks = Queue()
def save_to_disk(found_records, dbsession):
    for eachRecord  in found_records:
        if eachRecord["source"] == "WITHDRAWAL_INITIALIZED" or eachRecord["source"] == "DEPOSIT_CONFIRMED":
            if dbsession.query(NonInternalSnapshots).filter(NonInternalSnapshots.snapshot_id == eachRecord["snapshot_id"]).first() == None:
                this                = NonInternalSnapshots()
                this.snapshot_id    = eachRecord["snapshot_id"]
                this.amount         = eachRecord["amount"]
                this.created_at     = eachRecord["created_at"]
                this.source         = eachRecord["source"]
                this.asset_id       = eachRecord["asset_id"]
                this.asset_key      = eachRecord["asset_key"]
                this.asset_chain_id = eachRecord["asset_chain_id"]
                this.asset_name     = eachRecord["name"]
                dbsession.add(this)
        if (eachRecord["source"] == "TRANSFER_INITIALIZED") and (eachRecord["asset_id"] in Important_Asset):
            if dbsession.query(TradingSnapshots).filter(TradingSnapshots.snapshot_id == eachRecord["snapshot_id"]).first() == None:
                this                = TradingSnapshots()
                this.snapshot_id    = eachRecord["snapshot_id"]
                this.amount         = eachRecord["amount"]
                this.created_at     = eachRecord["created_at"]
                this.source         = eachRecord["source"]
                this.asset_id       = eachRecord["asset_id"]
                dbsession.add(this)
    dbsession.commit()

def loadSnapOnDateTime_savedisk(start_time, end_time, dbsession, search_type  = "yesterday2today", input_asset_id = ""):
    total_result = []
    thisDate         = start_time.isoformat()
    last_snap_string = start_time.isoformat()
    print(start_time, end_time, search_type)
    if search_type == "today2yesterday":
        order = "DESC"
    else:
        order = "ASC"
    while True: 
        find_result = find_deposit_withdraw(thisDate, order, input_asset_id)
        if find_result != None:
            if len(find_result["found_records"]) < 500:
                save_to_disk(find_result["found_records"], dbsession)
                return

            last_snap_string = find_result["lastsnap_created_at"]
            theLastDate = iso8601.parse_date(last_snap_string)
            print("last " + str(last_snap_string) + search_type + str(thisDate) + str(end_time))
            if search_type == "yesterday2today":
                if theLastDate < end_time:
                    thisDate = last_snap_string
                    save_to_disk(find_result["found_records"], dbsession)
                    continue
                else:
                    save_to_disk(find_result["found_records"], dbsession)
                    return
            if search_type == "today2yesterday":
                if theLastDate > end_time:
                    thisDate = last_snap_string
                    save_to_disk(find_result["found_records"], dbsession)
                    continue
                else:
                    save_to_disk(find_result["found_records"], dbsession)
                    return
        else:
            print("exit because read error %s"%(thisDate))
            return
    return


def loadSnapOnDateTime(start_time, end_time, asset_id = ""):

    total_result = []
    thisDate         = start_time.isoformat()
    last_snap_string = start_time.isoformat()
    while True: 
        find_result = find_deposit_withdraw(thisDate, order = "ASC", asset_id = asset_id)
        if find_result != None:
            for eachResult in find_result["found_records"]:
                total_result.append(eachResult)
            last_snap_string = find_result["lastsnap_created_at"]
            theLastDate = iso8601.parse_date(last_snap_string)
            if theLastDate < end_time:
                thisDate = last_snap_string
                continue
            else:
                tasks.put((total_result, last_snap_string, (start_time, end_time)))
                return
        else:
            tasks.put((total_result, last_snap_string, (start_time, end_time)))
            return
    tasks.put((total_result, last_snap_string, (start_time, end_time)))
    return

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

def search_asset(year, month, day, asset_id):
    today = datetime.datetime(year, month, day, 0, 0, tzinfo = datetime.timezone.utc)
    diff = (today - first_day).days
    daily_btc_balance = []
    for i in range(diff):
        this_day = first_day + datetime.timedelta(days = i)
        found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.created_at < this_day).filter(NonInternalSnapshots.asset_id == asset_id).all()
        old = 0
        for each_record in found_records:
            old += each_record.amount

        print("%s %d"%(this_day, old))
def search_asset_between(year, month, day, first_day,asset_id):
    today = datetime.datetime(year, month, day, 0, 0, tzinfo = datetime.timezone.utc)
    diff = (today - first_day).days
    daily_btc_balance = []
    found_records = session.query(NonInternalSnapshots).order_by(NonInternalSnapshots.created_at).filter(NonInternalSnapshots.created_at > first_day).filter(NonInternalSnapshots.created_at < today).filter(NonInternalSnapshots.asset_id == asset_id).all()
    for each_record in found_records:
        print(each_record)


def receive_task(total_number):
    for i in range(total_number):
        result = tasks.get()
        found_records = result[0]
        for eachRecord  in found_records:
            if eachRecord["source"] == "WITHDRAWAL_INITIALIZED" or eachRecord["source"] == "DEPOSIT_CONFIRMED":
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
            if (eachRecord["source"] == "TRANSFER_INITIALIZED") and (eachRecord["asset_id"] in Important_Asset):
                if session.query(TradingSnapshots).filter(TradingSnapshots.snapshot_id == eachRecord["snapshot_id"]).first() == None:
                    this                = TradingSnapshots()
                    this.snapshot_id    = eachRecord["snapshot_id"]
                    this.amount         = eachRecord["amount"]
                    this.created_at     = eachRecord["created_at"]
                    this.source         = eachRecord["source"]
                    this.asset_id       = eachRecord["asset_id"]
                    session.add(this)
        session.commit()
        last_record = result[1]

def search_oneday_snap(start, minutes_interval):
    allspawn = []
    group = Pool(40)
    end = ""
    times = 24 * 60/minutes_interval
    this_start = start + datetime.timedelta(days = 1)
    end = this_start + datetime.timedelta(minutes = minutes_interval)
    d = gevent.spawn(loadSnapOnDateTime, this_start, end)
    allspawn.append(d)

    #replicate the operation 
    for i in range(int(times) - 1):
        this_start = end
        end = this_start + datetime.timedelta(minutes = minutes_interval)
        d = gevent.spawn(loadSnapOnDateTime, this_start, end)
        allspawn.append(d)

    receive_spawn = gevent.spawn(receive_task, len(allspawn))
    allspawn.append(receive_spawn)
    receive_spawn.start()

    print("%d greenlets: "%len(allspawn))
    for each_spawn in allspawn:
        group.start(each_spawn)

    print(end)
    gevent.joinall(allspawn)


def searchAllSnap(year, month, days, offset_days, minutes_interval, asset_id = ""):
    allspawn = []
    group = Pool(40)
    start = datetime.datetime(int(year), int(month),int(days), 0, 0, tzinfo=datetime.timezone.utc)
    end = ""
    for i in range(offset_days):
        times = 24 * 60/minutes_interval
        this_start = start + datetime.timedelta(days = i)
        end = this_start + datetime.timedelta(minutes = minutes_interval)
        d = gevent.spawn(loadSnapOnDateTime, this_start, end, asset_id)
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

    receive_spawn = gevent.spawn(receive_task, len(allspawn))
    allspawn.append(receive_spawn)
    receive_spawn.start()

    print("%d greenlets: "%len(allspawn))
    for each_spawn in allspawn:
        group.start(each_spawn)

    print(end)
    gevent.joinall(allspawn)

def interactive_():
    print("scan snap on some day: 1")
    print("show everyday asset: 2")
    print("calculate all asset trading record on some day: 3")
    print("load asset trading record between some day(ATTENTION: Very long): 4")
    print("load all asset deposit and withdraw record on some day: 5")
    print("load single asset trading on between some day: 6")
    print("load single asset deposit and withdraw on between some day: 7")




    selection = input("your selection:")
    if(selection == "11"):
        year = int(input("year:"))
        month = int(input("month:"))
        day = int(input("day:"))
        offset_days = int(input("offset days:"))
        minutes_inter = int(input("minutes interval:"))

        startday = datetime.datetime(year, month, day, 0, 0, tzinfo=datetime.timezone.utc)
        today = datetime.datetime.today()
        print(startday)
        print(datetime.datetime.today())
        while startday < datetime.datetime(today.year, today.month, today.day, 0, 0, tzinfo=datetime.timezone.utc):
            search_oneday_snap(startday, minutes_inter)
            startday += datetime.timedelta(days = 1)


    if(selection == "1"):
        year = input("year(utc):")
        month = input("month(utc):")
        day = input("day(utc):")
        minutes_inter = int(input("minutes interval:"))
        asset_keys = list(Asset_group.keys())
        k = 0
        for i in asset_keys:
            print("%d: %s"%(k, i))
            k += 1
        asset_index = int(input("your asset index:"))
        key = asset_keys[asset_index]
        asset_id = Asset_group[key]


        searchAllSnap(year, month, day, 1, minutes_inter, asset_id)
    if(selection == "2"):
        first_day = datetime.datetime(2017, 12, 24, 0, 0, tzinfo=datetime.timezone.utc)
        year = int(input("year:"))
        month = int(input("month:"))
        day = int(input("day"))
        asset_keys = list(Asset_group.keys())
        k = 0
        for i in asset_keys:
            print("%d: %s"%(k, i))
            k += 1
        asset_index = int(input("your asset index:"))
        key = asset_keys[asset_index]
        asset_id = Asset_group[key]
        today = datetime.datetime(year, month, day, 0, 0, tzinfo = datetime.timezone.utc)
        diff = (today - first_day).days
        daily_btc_balance = []
        acc = 0
        x = PrettyTable()
        x.field_names = ["date", "accumulated amount"]
        now = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        with open(key+"_accumulated_holding"+str(year) +"_"+ str(month) +"_"+ str(day)+"created_at" + now+".csv", 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.created_at < today).filter(NonInternalSnapshots.asset_id == asset_id).all()
            old = 0
            while first_day < today:
                for each_record in found_records:
                    if each_record.created_at.date() == first_day.date():
                        acc += each_record.amount
                csvwriter.writerow([first_day.date(),int(acc)])
                x.add_row([first_day.date(), int(acc)])
                first_day += datetime.timedelta(days = 1)
        print(x)
    if(selection == "5"):
        year = int(input("start year:"))
        month = int(input("start month:"))
        day = int(input("start day"))
 
        start_of_day = datetime.datetime(year, month, day, 0, 0, tzinfo = datetime.timezone.utc)
        now = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        x = PrettyTable()
        x.field_names = ["asset", "deposit", "deposit amount", "withdraw", "withdraw amount"]

        with open("daily_deposit_withdraw"+str(year) +"_"+ str(month) +"_"+ str(day) + "created_at" + now+".csv", 'a', newline='') as csvfile:

            csvwriter = csv.writer(csvfile)

            csvwriter.writerow(["asset name", "deposit transaction", "deposit value", "withdraw transaction", "withdraw value"])
            end_of_day = start_of_day + datetime.timedelta(days = 1)
            found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.created_at > start_of_day).filter(NonInternalSnapshots.created_at < end_of_day).all()
            for asset_name in Asset_group:
                this_asset_id = Asset_group[asset_name]
                print(asset_name, this_asset_id)
                deposit_total = 0
                deposit_totalAmount = 0
                withdraw_total = 0
                withdraw_totalAmount = 0
                for each_record in found_records:
                    if each_record.asset_id != this_asset_id:
                        continue
                    if each_record.source == "DEPOSIT_CONFIRMED":
                        deposit_totalAmount  += each_record.amount
                        deposit_total += 1
                    if each_record.source == "WITHDRAWAL_INITIALIZED":
                        withdraw_totalAmount += each_record.amount
                        withdraw_total += 1
                csvwriter.writerow([asset_name, deposit_total, deposit_totalAmount, withdraw_total, withdraw_totalAmount])
                x.add_row([asset_name,deposit_total, deposit_totalAmount, withdraw_total, withdraw_totalAmount])

        print(start_of_day)
        print(x)

    if(selection == "3"):
        year = int(input("year(utc):"))
        month = int(input("month(utc):"))
        day = int(input("day(utc)"))

        start_of_day = datetime.datetime(year, month, day, 0, 0, tzinfo = datetime.timezone.utc)
        end_of_day = start_of_day + datetime.timedelta(days = 1)
        now = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        x = PrettyTable()
        x.field_names = ["asset name", "transaction", "total amount"]

        with open("daily_transactions_"+str(year) +"_"+ str(month) +"_"+ str(day)+"created_at" + now+".csv", 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)

            csvwriter.writerow(["asset name","total transaction", "total value"])
            found_records = session.query(TradingSnapshots).filter(TradingSnapshots.created_at > start_of_day).filter(TradingSnapshots.created_at < end_of_day).filter(TradingSnapshots.amount > 0).all()
            for asset_name in Asset_group:
                this_asset_id = Asset_group[asset_name]
                totalAmount = 0
                total = 0
                for each_record in found_records:
                    if each_record.asset_id != this_asset_id:
                        continue
                    totalAmount += each_record.amount
                    total += 1
                csvwriter.writerow([asset_name,total, totalAmount])
                x.add_row([asset_name,total, totalAmount])
        print(start_of_day)
        print(x)
    if(selection == "4"):
        year = int(input("start year:"))
        month = int(input("start month:"))
        day = int(input("start day"))
        end_year = int(input("end year:"))
        end_month = int(input("end month:"))
        end_day = int(input("end day"))
        end_of_time = datetime.datetime(end_year, end_month, end_day, 0, 0, tzinfo=datetime.timezone.utc)

        asset_keys = list(Asset_group.keys())
        k = 0
        for i in asset_keys:
            print("%d: %s"%(k, i))
            k += 1
        asset_index = int(input("your asset index:"))
        key = asset_keys[asset_index]
        asset_id = Asset_group[key]

 
        start_of_day = datetime.datetime(year, month, day, 0, 0, tzinfo = datetime.timezone.utc)
        now = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        input_selection = input("deposit and withdraw:1\n trading : 2\nYour selection:")
        if input_selection == "2":

            found_records = session.query(TradingSnapshots).filter(TradingSnapshots.created_at > start_of_day).filter(TradingSnapshots.created_at < end_of_time).filter(TradingSnapshots.asset_id == asset_id).filter(TradingSnapshots.amount > 0).all()
            for each_record in found_records:
                print(each_record)
        if input_selection == "1":
            found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.created_at > start_of_day).filter(NonInternalSnapshots.created_at < end_of_time).filter(NonInternalSnapshots.asset_id == asset_id).filter(TradingSnapshots.amount > 0).all()
            for each_record in found_records:
                print(each_record)
    if(selection == "7"):
        year = int(input("start year:"))
        month = int(input("start month:"))
        day = int(input("start day"))
        end_year = int(input("end year:"))
        end_month = int(input("end month:"))
        end_day = int(input("end day"))
        offset_day = int(input("offset :"))
        end_of_time = datetime.datetime(end_year, end_month, end_day, 0, 0, tzinfo=datetime.timezone.utc)

        asset_keys = list(Asset_group.keys())
        k = 0
        for i in asset_keys:
            print("%d: %s"%(k, i))
            k += 1
        asset_index = int(input("your asset index:"))
        key = asset_keys[asset_index]
        asset_id = Asset_group[key]

 
        start_of_day = datetime.datetime(year, month, day, 0, 0, tzinfo = datetime.timezone.utc)
        now = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        x = PrettyTable()
        x.field_names = ["date", "deposit transactions", "deposit volume", "withdraw transactions", "withdraw volume"]

        with open(key + "daily_deposit"+str(year) +"_"+ str(month) +"_"+ str(day)+"created_at" + now+".csv", 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["date", "deposit transactions", "deposit volume", "withdraw transactions", "withdraw volume"])

            while start_of_day < end_of_time:
                deposit_trans = 0
                deposit_volume = 0
                withdraw_trans = 0
                withdraw_volume = 0
                found_records = session.query(NonInternalSnapshots).filter(NonInternalSnapshots.created_at > start_of_day).filter(NonInternalSnapshots.created_at < start_of_day + datetime.timedelta(days = offset_day)).filter(NonInternalSnapshots.asset_id == asset_id).all()
                
                for each_record in found_records:
                    if each_record.source == "DEPOSIT_CONFIRMED":
                        deposit_trans += 1
                        deposit_volume += each_record.amount
                    if each_record.source == "WITHDRAWAL_INITIALIZED":
                        withdraw_trans += 1
                        withdraw_volume += -1 * each_record.amount
                csvwriter.writerow([start_of_day.date(),deposit_trans, deposit_volume, withdraw_trans, withdraw_volume])
                x.add_row([start_of_day.date(),deposit_trans, deposit_volume, withdraw_trans, withdraw_volume])
                start_of_day += datetime.timedelta(days = offset_day)
        print(start_of_day)
        print(x)


    if(selection == "6"):
        year = int(input("start year:"))
        month = int(input("start month:"))
        day = int(input("start day"))
        end_year = int(input("end year:"))
        end_month = int(input("end month:"))
        end_day = int(input("end day"))
        offset_day = int(input("offset :"))
        end_of_time = datetime.datetime(end_year, end_month, end_day, 0, 0, tzinfo=datetime.timezone.utc)

        asset_keys = list(Asset_group.keys())
        k = 0
        for i in asset_keys:
            print("%d: %s"%(k, i))
            k += 1
        asset_index = int(input("your asset index:"))
        key = asset_keys[asset_index]
        asset_id = Asset_group[key]

 
        start_of_day = datetime.datetime(year, month, day, 0, 0, tzinfo = datetime.timezone.utc)
        now = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        x = PrettyTable()
        x.field_names = ["date", "transaction", "total amount"]

        with open(key + "daily_transactions_"+str(year) +"_"+ str(month) +"_"+ str(day)+"created_at" + now+".csv", 'a', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["date","total transaction", "total value"])

            while start_of_day < end_of_time:
                total_amount = 0

                found_records = session.query(TradingSnapshots).filter(TradingSnapshots.created_at > start_of_day).filter(TradingSnapshots.created_at < start_of_day + datetime.timedelta(days = offset_day)).filter(TradingSnapshots.asset_id == asset_id).filter(TradingSnapshots.amount > 0).all()
                for each_record in found_records:
                    total_amount += each_record.amount
                csvwriter.writerow([start_of_day.date(),len(found_records), total_amount])
                x.add_row([start_of_day.date(),len(found_records), total_amount])
                start_of_day += datetime.timedelta(days = offset_day)
        print(start_of_day)
        print(x)



if __name__ == "__main__":
    print(sys.argv)
    print("scan record from 2018 10 11 to now: python main.py 2018 10 11")
    if len(sys.argv) >= 4:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
        day   = int(sys.argv[3])
        asset_id_group = []
        if len(sys.argv) > 4:
            asset_name_group = sys.argv[4:]
            for each_asset_name in asset_name_group:
                if each_asset_name in Asset_group:
                    asset_id_group.append(Asset_group[each_asset_name])
        elif len(sys.argv) == 4:
            asset_id_group.append("")
        print(asset_id_group)

        startday = datetime.datetime(year, month, day, 0, 0, tzinfo=datetime.timezone.utc)
        today = datetime.datetime.today()
        print(startday)
        print(datetime.datetime.today())
        while startday < datetime.datetime(today.year, today.month, today.day, 0, 0, tzinfo=datetime.timezone.utc):
            middle1 = startday + datetime.timedelta(hours = 6)
            middle2 = startday + datetime.timedelta(hours = 12)
            middle3 = startday + datetime.timedelta(hours = 18)


            this_end = startday + datetime.timedelta(days = 1)
            for each_asset_id in asset_id_group:
                spawn_group = []
                d = gevent.spawn(loadSnapOnDateTime_savedisk, startday,  middle1, session, "yesterday2today", each_asset_id)
                spawn_group.append(d)
                d = gevent.spawn(loadSnapOnDateTime_savedisk, middle2,  middle1,  session, "today2yesterday", each_asset_id)
                spawn_group.append(d)
                d = gevent.spawn(loadSnapOnDateTime_savedisk, middle2,  middle3, session, "yesterday2today", each_asset_id)
                spawn_group.append(d)
                d = gevent.spawn(loadSnapOnDateTime_savedisk, this_end,  middle3,  session, "today2yesterday", each_asset_id)
                spawn_group.append(d)
                gevent.joinall(spawn_group)
                print("finish scan for %s from %s to %s"%(each_asset_id, startday, this_end))
            print("finish scan :%s for id: %s"%(startday, str(asset_id_group)))
            startday += datetime.timedelta(days = 1)
    else:
        while True:
            interactive_()
    




