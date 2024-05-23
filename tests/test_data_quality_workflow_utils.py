import pytest
import datetime
import sys
sys.path.append('../')
from data_quality_notebooks.data_quality_workflow_utils import *
import logging

@pytest.fixture(scope="session")
def logger():
    logger_test = logging.getLogger('test')
    logger_test.setLevel(logging.getLevelName('CRITICAL'))
    yield logger_test


def test_get_workflow_schedule_obj_daily():
    ws = get_workflow_schedule_obj('Daily')
    assert(isinstance(ws, Daily) == True)

def test_get_workflow_schedule_obj_wrong_schedule():
    with pytest.raises(Exception):
        ws = get_workflow_schedule_obj('safiohebc')

def test_daily_is_any_run_ts_without_run(logger):
    ws = Daily()
    ws.set_params(None, False, logger)
    result = ws._is_any_run_ts()
    assert(result == False)

def test_daily_is_any_run_ts_with_run(logger):
    ws = Daily()
    ws.set_params(datetime.datetime.now(), False, logger)
    result = ws._is_any_run_ts()
    assert(result == True)

def test_daily_is_run_required_no_ts_failed_last_run(logger):
    ws = Daily()
    ws.set_params(None, False, logger)
    result = ws.is_run_required()
    assert(result == True)

def test_daily_is_run_required_no_ts_pass_last_run(logger):
    ws = Daily()
    ws.set_params(None, True, logger)
    result = ws.is_run_required()
    assert(result == True)

def test_daily_is_run_required_today_pass_last_run(logger):
    ws = Daily()
    ws.set_params(datetime.datetime.now(), True, logger)
    result = ws.is_run_required()
    assert(result == False)

def test_daily_is_run_required_today_fail_last_run(logger):
    ws = Daily()
    ws.set_params(datetime.datetime.now(), False, logger)
    result = ws.is_run_required()
    assert(result == True)

def test_daily_is_run_required_yesterday_fail_last_run(logger):
    ws = Daily()
    ws.set_params(datetime.datetime.now() - datetime.timedelta(days = 1), False, logger)
    result = ws.is_run_required()
    assert(result == True)

def test_daily_is_run_required_yesterday_pass_last_run(logger):
    ws = Daily()
    ws.set_params(datetime.datetime.now() - datetime.timedelta(days = 1), True, logger)
    result = ws.is_run_required()
    assert(result == True)

# No Schedule ----------------------------------------------------

def test_get_workflow_schedule_obj_no_schedule():
    ws = get_workflow_schedule_obj('')
    assert(isinstance(ws, NoSchedule) == True)

def test_no_schedule_is_run_required(logger):
    """Should always return True"""
    ws = NoSchedule()
    ws.set_params(datetime.datetime.now() - datetime.timedelta(days = 1), True, logger)
    result_1 = ws.is_run_required()
    ws.set_params(datetime.datetime.now() - datetime.timedelta(days = 1), False, logger)
    result_2 = ws.is_run_required()
    ws.set_params(datetime.datetime.now() - datetime.timedelta(days = 0), True, logger)
    result_3 = ws.is_run_required()
    ws.set_params(datetime.datetime.now() - datetime.timedelta(days = 0), False, logger)
    result_4 = ws.is_run_required()
    ws.set_params(None, True, logger)
    result_5 = ws.is_run_required()
    ws.set_params(None, False, logger)
    result_6 = ws.is_run_required()
    overall_result = result_1 & result_2 & result_3 & result_4 & result_5 & result_6
    assert (overall_result == True)

# Twice Daily -------------------------------------------------------

def test_twice_daily_is_any_run_ts_with_run(logger):
    ws = TwiceDaily()
    ws.set_params(datetime.datetime.now(), False, logger)
    result = ws._is_any_run_ts()
    assert(result == True)

def test_twice_daily_is_any_run_ts_without_run(logger):
    ws = TwiceDaily()
    ws.set_params(None, False, logger)
    result = ws._is_any_run_ts()
    assert(result == False)

def test_twice_daily_is_run_ts_today_pass(logger):
    ws = TwiceDaily()
    ws.set_params(datetime.datetime.now(), False, logger)
    result = ws._is_run_ts_today()
    assert(result == True)

def test_twice_daily_is_run_ts_today_fail(logger):
    ws = TwiceDaily()
    ws.set_params(datetime.datetime.now() - datetime.timedelta(days = 1), False, logger)
    result = ws._is_run_ts_today()
    assert(result == False)

def test_twice_daily_is_run_ts_hour_before_cutoff_5_true(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    ws.set_params(datetime.datetime(2024,11,15,5,10,22), False, logger)
    result = ws._is_run_ts_hour_before_cutoff()
    assert(result == True)

def test_twice_daily_is_run_ts_hour_before_cutoff_12_true(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    ws.set_params(datetime.datetime(2024,4,12,12,10,22), False, logger)
    result = ws._is_run_ts_hour_before_cutoff()
    assert(result == True)

def test_twice_daily_is_run_ts_hour_before_cutoff_13_false(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    ws.set_params(datetime.datetime(2024,2,19,13,10,22), False, logger)
    result = ws._is_run_ts_hour_before_cutoff()
    assert(result == False)

def test_twice_daily_is_run_ts_hour_before_cutoff_18_false(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    ws.set_params(datetime.datetime(2024,1,21,18,10,22), False, logger)
    result = ws._is_run_ts_hour_before_cutoff()
    assert(result == False)

#Test current time using dummy value for time for reliable results:
def test_twice_daily_is_current_time_before_cutoff_5_true(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    ws.set_params(None, False, logger)
    result = ws._is_current_time_before_cutoff(datetime.datetime(2024,2,19,5,10,22))
    assert(result == True)

def test_twice_daily_is_current_time_before_cutoff_12_true(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    ws.set_params(None, False, logger)
    result = ws._is_current_time_before_cutoff(datetime.datetime(2024,9,3,12,10,22))
    assert(result == True)

def test_twice_daily_is_current_time_before_cutoff_13_false(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    ws.set_params(None, False, logger)
    result = ws._is_current_time_before_cutoff(datetime.datetime(2024,5,12,13,10,22))
    assert(result == False)

def test_twice_daily_is_current_time_before_cutoff_18_false(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    ws.set_params(None, False, logger)
    result = ws._is_current_time_before_cutoff(datetime.datetime(2024,1,1,18,10,22))
    assert(result == False)

def test_twice_daily_is_run_required_both_morning_passed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,10,00,00)
    ws.set_params(dt - datetime.timedelta(hours=5), True, logger)

    result = ws.is_run_required(current_ts = dt)
    assert(result == False)

def test_twice_daily_is_run_required_both_afternoon_passed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,15,00,00)
    ws.set_params(dt - datetime.timedelta(hours=1), True, logger)

    result = ws.is_run_required(current_ts = dt)
    assert(result == False)

def test_twice_daily_is_run_required_morning_afternoon_passed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,10,00,00)
    ws.set_params(dt + datetime.timedelta(hours=5), True, logger)

    result = ws.is_run_required(current_ts = dt)
    assert(result == True)

def test_twice_daily_is_run_required_afternoon_morning_prev_day_passed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,15,00,00)
    ws.set_params(dt - datetime.timedelta(days=1, hours=5), True, logger)
    result = ws.is_run_required(current_ts = dt)
    assert(result == True)

def test_twice_daily_is_run_required_both_morning_different_day_passed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,10,00,00)
    ws.set_params(dt - datetime.timedelta(days=1, hours=5), True, logger)
    result = ws.is_run_required(current_ts = dt)
    assert(result == True)

def test_twice_daily_is_run_required_both_afternoon_different_day_passed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,16,00,00)
    ws.set_params(dt - datetime.timedelta(days=1, hours=2), True, logger)

    result = ws.is_run_required(current_ts = dt)
    assert(result == True)

def test_twice_daily_is_run_required_both_morning_failed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,10,00,00)
    ws.set_params(dt - datetime.timedelta(hours=5), False, logger)

    result = ws.is_run_required(current_ts = dt)
    assert(result == True)

def test_twice_daily_is_run_required_both_afternoon_failed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,15,00,00)
    ws.set_params(dt - datetime.timedelta(hours=1), False, logger)

    result = ws.is_run_required(current_ts = dt)
    assert(result == True)

def test_twice_daily_is_run_required_morning_afternoon_failed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,10,00,00)
    ws.set_params(dt + datetime.timedelta(hours=5), False, logger)

    result = ws.is_run_required(current_ts = dt)
    assert(result == True)

def test_twice_daily_is_run_required_afternoon_morning_prev_day_failed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,15,00,00)
    ws.set_params(dt - datetime.timedelta(days=1, hours=5), False, logger)

    result = ws.is_run_required(current_ts = dt)
    assert(result == True)

def test_twice_daily_is_run_required_both_morning_different_day_failed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,10,00,00)
    ws.set_params(dt - datetime.timedelta(days=1, hours=5), False, logger)

    result = ws.is_run_required(current_ts = dt)
    assert(result == True)

def test_twice_daily_is_run_required_both_afternoon_different_day_failed(logger):
    ws = TwiceDaily()
    #Cutoff is currently 13:00
    dt = datetime.datetime(2024,3,8,16,00,00)
    ws.set_params(dt - datetime.timedelta(days=1, hours=2), False, logger)

    result = ws.is_run_required(current_ts = dt)
    assert(result == True)

# ----------------------------










