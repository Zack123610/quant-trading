from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def initialize_web_driver():
    # options = webdriver.ChromeOptions()
    # options.add_argument('--verbose')
    # options.add_argument('--no-sandbox')
    # options.add_argument('--headless')
    # options.add_argument('--disable-gpu')
    # options.add_argument('--window-size=1920,1200')
    # options.add_argument('--disable-dev-shm-usage')
    # driver = webdriver.Chrome(options=options)
    
    # use firefox
    driver = webdriver.Firefox()
    # driver = webdriver.Chrome()
    return driver

# instantiate a selenium webdriver
driver = initialize_web_driver()

try:
    driver.get("https://www.tradingview.com/symbols/EURUSD/")

    chart_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "chart-nORFfEfo")))

    chart_element.screenshot("eurusd_chart.png")
    print("EURUSD chart saved as eurusd_chart.png")
except Exception as e:
    print(e)

finally:
    driver.quit()