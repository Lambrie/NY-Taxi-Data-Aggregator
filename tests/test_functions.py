import pandas as pd

from helpers.functions import *

dfTaxi = pd.DataFrame({"medallion": ['D7D598CD99978BD012A87A76A7C891B7','5455D5FF2BD94D10B304A15D4B7F2735','9A80FE5419FEA4F44DB8E67F29D84A0F','0C107B532C1207A74F0D8609B9E092FF','801C69A08B51470871A8110F8B0505EE','A18CC3E9191D21F604DFC2423916E6A2','B672154F0FD3D6B5277580C3B7CBBF8E','42D815590CE3A33F3A23DBF145EE66E3','F0852E778861F64576FA9A90EAB1091A','056B0F26941A1044B62F2D3869FAEE9E','4FDF7467A2038D09DC1089EA72CFBAD2','FAE8321D1D3EFE36F973B7F573BCABDC','E60D24AFA95CCA6753A64E9E0B6A2D71','B4C414BA3488EBECF8A633B313A41B00','F1711701C1CA0E36C3416C01621E781B','2BDC2F7390D611621FB592507A405B34','5AB4DE718E958FC082557F03BF439493','08099C118772D01BBA1C3F486BD8AF1E','8F0A1E787329C1C08F8F9120EE68E056','A0B5AF0F9B31690CEBB51ECD27D2BE71'],
                        "hack_license": ['82F90D5EFE52FDFD2FDEC3EAD6D5771D','177B80B867CEC990DA166BA1D0FCAF82','-73.972794','66C2CECD93E395CB9B875E9B382DB5D9','91A07EEF642E8590C2EFD631C3DF89C9','A69D2180076DCD9954F5EB66E2A747F7','FC703B19EB1901490D61D555C3DF104D','2D33FF8C0EBAEF5FC1DE86F1035306BE','-73.987671','BFAD5C863E53ED7E3234CE52DF9602D2','BAFAF99E399191ECAE3619F5A034C2BB','3FEE646E54EAF58B981C4409C795DB45','E0D514C8C7963351DE1C6FABFC333A56','B047B68FE6A9001656F9007A957A05C8','A11DF63CF1ED5DD1A69F7A6CF567EE50','-73.967812','FD80C29EBF2D97FAC33EB3A49055CA67','5CAED872BAAFDB2885077DC6667D5086','BE1E0759CF7A2356F0CA7D0A710CECE6','-73.976189'],
                        "vendor_id": ['VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS','VTS'],
                        "rate_code": [1,1,1,1,1,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
                        "store_and_fwd_flag":[0,0,-73.995262,0,0,0,0,0,0,0,0,0,0,0,0,-73.991043,0,0,0,0],
                        "pickup_datetime":['2013/12/01 00:13:00','2013/12/01 00:40:00','2013/12/31 07:39:00','2013/12/01 02:14:00','2013/12/01 04:45:00','2013/12/30 23:13:00','2013/12/01 08:35:00','2013/12/01 09:28:00','2013/12/31 12:14:00','2013/12/30 18:31:00','2013/12/03 18:04:00','2013/12/03 17:54:00','2013/12/03 18:20:00','2013/12/06 07:57:00','2013/12/03 17:34:00','2013/12/03 18:16:00','2013/12/03 18:40:00','2013/12/06 07:24:00','2013/12/06 07:58:00','2013/12/06 08:22:00'],
                        "dropoff_datetime":['2013/12/01 00:31:00','2013/12/01 00:48:00','2013/12/31 07:46:00','2013/12/01 02:22:00','2013/12/01 04:50:00','2013/12/30 23:25:00','2013/12/01 08:45:00','2013/12/01 09:38:00','2013/12/31 12:29:00','2013/12/01 10:15:00','2013/12/03 18:21:00','2013/12/03 18:21:00','2013/12/03 18:41:00','2013/12/06 08:01:00','2013/12/03 18:23:00','2013/12/03 18:32:00','2013/12/06 08:01:00','2013/12/06 07:57:00','2013/12/06 08:02:00','2013/12/06 08:33:00'],
                        "passenger_count":[1,6,5,1,1,1,6,6,5,4,1,1,1,1,6,2,1,2,2,3],
                        "trip_time_in_secs":[1080,480,420,120,300,720,600,600,900,840,1500,1620,1260,240,2940,960,180,1980,240,660],
                        "trip_distance":[3.79,3.20,2.29,0.72,1.02,1.23,5.73,2.18,3.25,5.84,0.11,6.22,1.86,3.06,11.75,1.53,0.44,10.07,0.52,1.37],
                        "pickup_longitude":[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None],
                        "pickup_latitude":[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None],
                        "dropoff_longitude":[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None],
                        "dropoff_latitude":[None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]
                    })
dfWeather = pd.read_csv("./weather.csv")

class ArgumentMocker(object):
    def __init__(self, _medallion, _start_date, _end_date):
        self.medallion = _medallion
        self.pickup_start_date = _start_date
        self.pickup_end_date = _end_date

def test_preprocessing_taxi_data():
    df = preprocess_taxi_data(dfTaxi.copy())
    assert df.shape[0] == 20
    assert df.shape[1] == 8


def test_preprocessing_weather_data():
    df = preprocess_weather_data(dfWeather.copy())
    assert df.shape[0] == 31
    assert df.shape[1] == 2


def test_apply_filter_invalid_start_dates():
    args = ArgumentMocker([],"2021-14-14","2021-07-14")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    df = apply_filter(dfTaxiData, args)
    assert SystemExit

def test_apply_filter_invalid_end_dates():
    args = ArgumentMocker([],"2021-07-14","2021-17-14")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    df = apply_filter(dfTaxiData, args)
    assert SystemExit

def test_apply_filter_valid_date_not_found():
    args = ArgumentMocker([],"2013-12-07","2013-12-29")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    df = apply_filter(dfTaxiData, args)
    assert df.shape[0] == 0
    assert df.shape[1] == 8

def test_apply_filter_valid_date_found():
    args = ArgumentMocker([],"2013-12-01","2013-12-05")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    df = apply_filter(dfTaxiData, args)
    assert df.shape[0] == 12
    assert df.shape[1] == 8

def test_apply_filter_invalid_date_format():
    args = ArgumentMocker([],"12-01-2013","12-05-2013")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    df = apply_filter(dfTaxiData, args)
    assert df.shape[0] == 0
    assert df.shape[1] == 8

def test_apply_filter_medallion_not_found():
    args = ArgumentMocker(["xxxxxxxxxxxxxxxxx"],"2013-12-01","2013-12-31")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    df = apply_filter(dfTaxiData, args)
    assert df.shape[0] == 0
    assert df.shape[1] == 8

def test_apply_filter_1_medallion():
    args = ArgumentMocker(["5455D5FF2BD94D10B304A15D4B7F2735"],"2013-12-01","2013-12-31")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    df = apply_filter(dfTaxiData, args)
    assert df.shape[0] == 1
    assert df.shape[1] == 8

def test_apply_filter_2_medallion():
    args = ArgumentMocker(["5455D5FF2BD94D10B304A15D4B7F2735,B672154F0FD3D6B5277580C3B7CBBF8E"],"2013-12-01","2021-12-31")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    df = apply_filter(dfTaxiData, args)
    assert df.shape[0] == 2
    assert df.shape[1] == 8

def test_process_data_all_medallions():
    args = ArgumentMocker([],"2013-12-01","2013-12-31")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    dfWeatherData = preprocess_weather_data(dfWeather.copy())
    df = apply_filter(dfTaxiData, args)
    df = process_data(df, dfWeatherData, args)
    expected_res = pd.DataFrame({
        "Pickup Date": [datetime(2013, 12, 1).date(), datetime(2013, 12, 3).date(), datetime(2013, 12, 6).date(), datetime(2013, 12, 30).date(),datetime(2013, 12, 31).date()],
        "Passengers": [21,12,8,5,10],
        "Travel Time(s)": [3180,8460,3120,1560,1320],
        "Travel Distance(Km)": [16.64,21.91,15.02,7.07,5.54],
        "Temperature(F)": [42.6,45.4,48.7,36.5,26.9]
    })
    pd.testing.assert_frame_equal(df, expected_res)
    assert df.shape[0] == 5
    assert df.shape[1] == 5

def test_process_data_1_medallion():
    args = ArgumentMocker(["D7D598CD99978BD012A87A76A7C891B7"],"2013-12-01","2013-12-31")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    dfWeatherData = preprocess_weather_data(dfWeather.copy())
    df = apply_filter(dfTaxiData, args)
    df = process_data(df, dfWeatherData, args)
    expected_res = pd.DataFrame({
        "Pickup Date": [datetime(2013, 12, 1).date()],  # datetime(2013,12,1) "2013-12-01"
        "Passengers": [1],
        "Travel Time(s)": [1080],
        "Travel Distance(Km)": [3.79],
        "Temperature(F)": [42.6]
    })
    pd.testing.assert_frame_equal(df, expected_res)
    assert df.shape[0] == 1
    assert df.shape[1] == 5

def test_process_data_2_medallions():
    args = ArgumentMocker(["5455D5FF2BD94D10B304A15D4B7F2735,B672154F0FD3D6B5277580C3B7CBBF8E"],"2013-12-01","2013-12-31")
    dfTaxiData = preprocess_taxi_data(dfTaxi.copy())
    dfWeatherData = preprocess_weather_data(dfWeather.copy())
    df = apply_filter(dfTaxiData, args)
    df = process_data(df, dfWeatherData, args)
    expected_res = pd.DataFrame({
                "Pickup Date": [datetime(2013,12,1).date()], #datetime(2013,12,1) "2013-12-01"
                "Passengers": [12],
                "Travel Time(s)": [1080],
                "Travel Distance(Km)": [8.93],
                "Temperature(F)": [42.6]
    })
    pd.testing.assert_frame_equal(df, expected_res)
    assert df.shape[0] == 1
    assert df.shape[1] == 5
