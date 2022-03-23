




'''
Schedule details

start_date =
end_date = 
max_tries = 
schedule_interval = 

'''

# Airflow Scheduling => cron syntax 


# Cron syntax 

# minute(0~59) hour(0~23) day_of_month(1~31) month(1~12) day_of_the_week(0~6)
cron_syntax = "* * * * *"

# every day at 12:00
run_daily = "0 12 * * * " 

# Run once per minute on Febuary 25
cron2 = "* * 25 2 *"

# run every 15min 
cron3 = "0,15,30,45 * * * * "

# Airflow scheduler presets 

# @hourly
hour_cron = '0 * * * *'
# @daily 
daily_cron = '0 0 * * *'
# @weekly
weekly_cron = '0 0 * * 0'

# schedule task : start_date + schedule_interval
'start_date': datetime(2020,2,25),
'schedule_interval': @daily

# exercise 

default_args = {
    'owner':'Engineering',
    'start_date':datetime(2019, 11,1),
    'email':['airflowresults@datacamp.com'],
    'email_on_failure': False, 
    'email_on_retry':False,
    'retries':3,
    'retry_delay':timedelta(minutes=20)
}

dag = DAG(
    'update_dataflows',
    default_args=default_args,
    schedule_interval = '30 12 * * 3'
)