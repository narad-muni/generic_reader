data = '''
MESSAGE_HEADER STRUCT 40 0
MessageType CHAR 1 40
Reserved CHAR 1 41
NoOfRecords SHORT 2 42
SPD_STATS_DATA[3] STRUCT 78 44
'''

SEC_INFO = '''
InstrumentName CHAR 6 0
Symbol CHAR 10 6
Series CHAR 2 16
ExpiryDate LONG 4 18
StrikePrice LONG 4 22
OptionType CHAR 2 26
CALevel SHORT 2 28
'''
SPD_STATS_DATA = '''
MARKETTYPE SHORT 2 0
INSTRUMENTNAME1 CHAR 6 2
SYMBOL1 CHAR 10 8
EXPIRYDATE1 LONG 4 18
STRIKEPRICE1 LONG 4 22
OPTIONTYPE1 CHAR 2 26
CALEVEL1 SHORT 2 28
INSTRUMENTNAME2 CHAR 6 30
SYMBOL2 CHAR 10 36
EXPIRYDATE2 LONG 4 46
STRIKEPRICE2 LONG 4 50
OPTIONTYPE2 CHAR 2 54
CALEVEL2 SHORT 2 56
OPENPD LONG 4 58
HIPD LONG 4 62
LOWPD LONG 4 66
LASTTRADEDPD LONG 4 70
NOOFCONTRACTSTRADED LONG 4 74
'''

MSG_HEADER = '''
TransactionCode SHORT 2 0
LogTime LONG 4 2
AlphaChar CHAR 2 6
TraderId LONG 4 8
ErrorCode SHORT 2 12
Timestamp LONG_LONG 8 14
TimeStamp1 CHAR 8 22
TimeStamp2 CHAR 8 30
MessageLength SHORT 2 38
'''

OPEN_INTEREST = '''
TokenNo LONG 4 0
CurrentOI UNSIGNED_LONG 4 4
'''

BCAST_HEADER = '''
Reserved CHAR 2 0
Reserved CHAR 2 2
LogTime LONG 4 4
AlphaChar CHAR 2 8
TransactionCode SHORT 2 10
ErrorCode SHORT 2 12
BCSeqNo LONG 4 14
Reserved CHAR 1 18
Reserved CHAR 3 19
TimeStamp2 CHAR 8 22
Filler2 CHAR 8 30
MessageLength SHORT 2 38
'''

ST_BCAST_DESTINATION = '''
TraderWorkStation BIT 1 0
ControlWorkStation BIT 1 0
Tandem BIT 1 0
JournallingRequired BIT 1 0
Reserved BIT 4 0
Reserved CHAR 1 1
'''

RecordBuffer = '''
Quantity LONG LONG 8 0
Price LONG 4 8
NumberOfOrders SHORT 2 12
BbBuySellFlag SHORT 2 14
'''

MbpIndicator = '''
LastTradeMore BIT 1 0
LastTradeLess BIT 1 0
Buy BIT 1 0
Sell BIT 1 0
Reserved BIT 4 0
Reserved CHAR 1 1
'''

struct_mapping = {
    "RecordBuffer": RecordBuffer,
    "MBP_INDICATOR": MbpIndicator,
    "OPEN_INTEREST": OPEN_INTEREST,
    "BCAST_HEADER": BCAST_HEADER,
    "ST_BCAST_DESTINATION": ST_BCAST_DESTINATION,
    "SEC_INFO": SEC_INFO,
    "MESSAGE_HEADER": MSG_HEADER,
    "SPD_STATS_DATA": SPD_STATS_DATA,
}

dtype_mapping = {
    "LONG": "i32",
    "SHORT": "short",
    "CHAR": "char",
    "DOUBLE": "f64",
    "FLOAT": "f32",
    "LONG_LONG": "i64",
    "BYTE": "byte",
    "UNSIGNED_LONG": "u32",
    "BIT": "bit",
}

def unpack(data, prefix=""):
    json = ""
    
    for line in data.split('\n'):
        if len(line.split(' ')) < 4: continue
        [name, dtype, length, _] = line.split(' ')

        length = int(length)

        if dtype == 'STRUCT':
            json += get_structs_data(name)
        else:
            json += '{"name":"'+prefix+name+'","dtype":"'+dtype_mapping[dtype]+'","length":'+str(length)+'},\n'

    return json

def get_structs_data(struct_name):
    splitted_name = struct_name.split('[')

    struct_name = ""
    arr_len = 0

    if(len(splitted_name) == 1):
        struct_name = splitted_name[0]
    else:
        struct_name, arr_len = splitted_name
        arr_len = int(arr_len.split(']')[0])

    struct_data = struct_mapping[struct_name]

    json = ""

    if arr_len > 1:
        for i in range(int(arr_len)):
            json += unpack(struct_data, struct_name+'['+str(i)+'].')
    else:
        json += unpack(struct_data, struct_name+".")

    return json

print(unpack(data))