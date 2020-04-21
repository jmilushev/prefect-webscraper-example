#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 13:01:33 2020

@author: jmilushe
"""

import typing as T
import datetime
import tempfile
import re
import sqlalchemy as sa

from prefect import task, Flow, Parameter, unmapped
from prefect.engine.result import Result
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.engine import cache_validators
from prefect.environments import KubernetesJobEnvironment
from prefect.engine.result_handlers import LocalResultHandler
from prefect.environments.storage import Docker
from prefect.utilities.logging import get_logger

from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver as RemoteWebDriver
from selenium.common.exceptions import TimeoutException, InvalidSelectorException, NoSuchElementException, ElementNotVisibleException, InvalidElementStateException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


import pandas as pd
import numpy as np 

def parse_date (s: str):
   
    val_pattern = re.compile('(\d{1,4}([.\-\/])\d{1,2}([.\-\/])\d{1,4})')

    val_match = val_pattern.search(s)
    if val_match:
        val = val_match.group()
        dt_array = val.split('/')
        
        return datetime.date(int(dt_array[2]), int(dt_array[0]), int(dt_array[1]))
    else:
        return np.NaN

def rename_stat_names(str):
    if str == 'Total COVID-19 Positive':
        return 'total_positive'
    elif str == 'COVID-19 Fatalities':
        return 'total_fatalities'
    elif str == 'Newly Reported Fatalities':
        return 'new_fatalities'
    elif str == 'Total Covid-19 Tests':
        return 'total_tests'
    elif str == 'Covid-19 Negative':
        return 'negative_tests'
    elif str == 'Newly Reported Positive Tests':
        return 'new_positive_tests'
    elif str == 'Currently Hospitalized':
        return 'in_hospital'
    elif str == 'Currently in ICU':
        return 'in_icu'
    elif str == 'Currently on Vent':
        return 'on_vent'
    else:
        return str
    
        
@task(
      name="Iitialize Browser"
      )
def initialize_browser( path_to_chromedriver: str, url: str):
    options = webdriver.ChromeOptions()
    # run in 'headless' mode
    options.add_argument('--headless')
    # allow to run as 'root' user
    options.add_argument("--no-sandbox")
    # disabling extensions
    options.add_argument("--disable-extensions")
    # applicable to windows os only
    options.add_argument("--disable-gpu")
    # overcome limited resource problems
    options.add_argument("--disable-dev-shm-usage")
    # specify download directory
    options.add_experimental_option(
        'prefs',
        {
            'download.default_directory': tempfile.gettempdir()
        }
    )
    options.add_argument(" — incognito")
    
    driver = webdriver.Chrome(
        executable_path=path_to_chromedriver,
        options=options
    )
    assert isinstance(driver, RemoteWebDriver)

    driver.get(url)
    return driver
    options.add_argument("--disable-extensions")
    # applicable to windows os only
    options.add_argument("--disable-gpu")
    # overcome limited resource problems
    options.add_argument("--disable-dev-shm-usage")
    # specify download directory
    options.add_experimental_option(
        'prefs',
        {
            'download.default_directory': tempfile.gettempdir()
        }
    )
    driver = webdriver.Chrome(
        executable_path=path_to_chromedriver,
        options=options
    )
    assert isinstance(driver, RemoteWebDriver)
    get_logger().info(f"Selenium service_url: {svc.service_url}")
    return driver

@task(
      name="wait until html element is loaded in browser"
      )
def wait_for_page(driver, xpath):
    try:
        timeout = 20
        WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((By.XPATH, xpath)))
    except (TimeoutException, ) as ex:
        get_logger().error(f'Unable to locate element: {xpath} within {timeout} seconds')
        raise ex 
    except (InvalidSelectorException, ) as ex:
        raise ex
    except (NoSuchElementException, ElementNotVisibleException, InvalidElementStateException, ) as ex:
        raise ex

    
    
@task(
      name="parse the cummulitive data on state level"
      )
def task_parse_state_data(browser, xpath):
    try:
         state_data = browser.find_element_by_xpath(xpath).text.replace(',', '')
         d = np.array(state_data.split('\n'))
         get_logger().debug(f'raw data: {d}')
         d2 = np.reshape(d[0:27], (-1, 3))
         get_logger().debug(f'reshaped data: {d2}')
         df = pd.DataFrame(data = d2, columns=['name', 'value', 'source'])
         get_logger().debug(f'created dataframe from data. Converting stats to numeric')
         df.value = pd.to_numeric(df.value, errors='coerce')
         get_logger().debug(f'parsing update date from text')
         df['updated_on'] = df.apply(lambda x: parse_date(x['source']), axis=1)
         get_logger().debug(f'normalizing stat names')
         df['name'] = df.apply(lambda x: rename_stat_names(x['name']), axis=1)
        
         get_logger().info(f'dropping column and pivoting results')
         df = df.drop(['source'], axis=1)
         df = df.pivot(index='updated_on', columns='name', values='value')
         df['scraped_on'] = df['scraped_on'] = datetime.date.today()


         return df

    except (RuntimeError, TypeError, NameError) as ex:
          get_logger().error(f'Unable to parse state data: {ex}')
          raise ex 
 
@task(
        name='insert data to database'
     )
def insert_data(df, _db):
    df.to_sql(name='COVID19_STATS', con = _db, if_exists = 'replace',
              dtype={"updated_on": sa.DateTime,
                      "in_hospital": sa.Integer,
                      "in_icu": sa.Integer,
                      "negative_tests":  sa.Integer,
                      "new_fatalities": sa.Integer,
                      "new_positive_tests": sa.Integer,
                      "on_vent": sa.Integer,
                      "total_fatalities": sa.Integer,
                      "total_positive": sa.Integer,
                      "total_tests": sa.Integer,
                      "scraped_on": sa.DateTime})
    
    

@task(
    name="Create DB",
    tags=['db']
)
def create_db(filename: T.Union[str, Parameter]) -> sa.Table:
    """
    Specify the Schema of the output table
    """
    _db = sa.create_engine(f"sqlite:///{filename}")
    return _db

with Flow(
        name="scrape COVID-19 data for RI",
        schedule=Schedule(
            clocks=[
                # TODO: specify the schedule you want this to run, and with what parameters
                #  https://docs.prefect.io/core/concepts/schedules.html
                CronClock(
                    cron='0 0 * * *',
                    parameter_defaults=dict(
                        home_page='https://www.metacritic.com/',
                        gaming_platform='Switch'
                    )
                )
            ]
        )
        # TODO: specify how you want to handle results
        #  https://docs.prefect.io/core/concepts/results.html#results-and-result-handlers
) as flow:
    # specify the DAG input parameters
    _path_to_chromedriver = Parameter('path_to_chromedriver', default='/Users/jmilushe/Downloads/chromedriver')
    _home_page_url = Parameter('home_page', default='https://health.ri.gov/data/covid-19/')
    _db_file = Parameter("db_file", default='covid19_us_ri.sqlite', required=False)
    _data_xpath = Parameter("data_xpath", default="//div[@id='ember38']", required=True)
    _state_data_element=Parameter("state_data_element", default="//div[@id='ember79']/div[@class='text-left']/span[@class='ss-value']", required=True)


    _driver = initialize_browser( _path_to_chromedriver, _home_page_url)
    wait_for_page(_driver, _state_data_element)
    _state_df = task_parse_state_data(_driver, _data_xpath)
    # _driver.quit()
 
    _db = create_db(_db_file)
    #insert into SQLite table
    insert_data(_state_df, _db)
   
    
 
    
if __name__ == '__main__':

    # debug the local execution of the flow
    import sys
    import argparse
    from prefect.utilities.debug import raise_on_exception

    # get any CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--visualize', required=False, default=False)
    parser.add_argument('--deploy', required=False, default=False)
    p = parser.parse_args(sys.argv[1:])

    if p.visualize:
        # view the DAG
        flow.visualize()

    # execute the Flow manually, not on the schedule
    with raise_on_exception():
        if p.deploy:
            # TODO: hack for https://github.com/PrefectHQ/prefect/issues/2165
            flow.result_handler.dir = '/root/.prefect/results'
            flow.register(
                # TODO: specify the project_name on Prefect Cloud you're authenticated to
                project_name="Cisco",
                build=True,
                # TODO: specify any labels for Agents
                labels=[
                    'lab'
                ]
            )
        else:
            flow.run(
                run_on_schedule=False
            )    