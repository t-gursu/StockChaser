import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options as ChromeOptions
from telegram import Bot
from datetime import datetime
import asyncio
from decouple import config

TOKEN = config('TOKEN')
CHAT_ID = config('CHAT_ID')
LOGS_Sheet = config('LOGS_Sheet')
STOCKS_Sheet = config('STOCKS_Sheet')
EXCEL_PATH =  config('EXCEL_PATH')

def check_columns(df):
    default_columns = [
        "Ticker",
        "Last",
        "Cross",
        "Goal",
        "Status",
        "Last Update",
        "Notify",
    ]

    missing_columns = [column for column in default_columns if column not in df.columns]

    for column in missing_columns:
        df[column] = ""

    return missing_columns


def log_to_excel(writer, message, excel_file_path):
    try:
        log_df = pd.read_excel(excel_file_path, sheet_name="Logs")
    except Exception as e:
        log_df = pd.DataFrame(columns=["Time", "Log"])

    current_time = datetime.now().strftime(f"%Y-%m-%d %H:%M:%S")
    new_log = {"Time": current_time, "Log": message}
    print(new_log)
    log_df = log_df.append(new_log, ignore_index=True)
    log_df.to_excel(writer, sheet_name="Logs", index=False)


def setDriverOptions():
    options = ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--log-level=3")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors")
    options.add_argument("--disable-extensions")
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )


def is_excel_file_open(file_path):
    try:
        with open(file_path, "rb") as file:
            pass
    except PermissionError:
        return True
    return False


async def send_telegram_message(message):
    bot = Bot(TOKEN)
    await bot.sendMessage(CHAT_ID, text=message)


def get_stock_price(symbol, driver):
    # Sayfayı yükleyin.
    url = "https://finance.yahoo.com/quote/" + symbol
    driver.get(url)
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (
                    By.CSS_SELECTOR,
                    'fin-streamer[data-symbol="' + symbol + '"][data-test="qsp-price"]',
                )
            )
        )
        value = element.text
        return value
    except Exception as e:
        print(f"Bir hata oluştu: {e}")
        return None


async def main():
    ### Check if Excel file is open
    if is_excel_file_open(EXCEL_PATH):
        print("Excel file is open! StockChaser couldn't start.")
        send_telegram_message("Excel file is open! StockChaser couldn't start.")

    # Excel dosyasını okuma ve sütunları kontrol etme
    with pd.ExcelWriter(
        EXCEL_PATH, engine="openpyxl", mode="a", if_sheet_exists="replace"
    ) as writer:
        df = pd.read_excel(EXCEL_PATH, sheet_name=STOCKS_Sheet)
        column_status = check_columns(df)
        if column_status:
            print("Missing columns:", ", ".join(column_status))
            send_telegram_message(
                "Missing columns:",
                ", ".join(column_status) + ". StockChaser couldn't start.",
            )

        driver = setDriverOptions()

        for index, row in df.iterrows():
            ### Set variables
            current_time = datetime.now().strftime(f"%Y-%m-%d %H:%M:%S")
            symbol = row["Ticker"]
            cross = row["Cross"]
            goal = row["Goal"]
            last = row["Last"]
            status = row["Status"]
            notify = row["Notify"]

            # Check if any of the required columns are empty
            if pd.isna(symbol) or pd.isna(cross) or pd.isna(goal):
                print(
                    f"Error for index {index}: 'Ticker', 'Cross', or 'Goal' is empty."
                )
                continue

            ## Fetch the price
            price = get_stock_price(symbol, driver)
            price = price.replace(",", "")
            print(price)
            price = float(price)

            ## Update df values
            if price is not None:
                df.at[
                    index, "Last"
                ] = price  # 'Last' sütunu, alınan hisse fiyatıyla güncellenir.
            df.at[index, "Last Update"] = current_time

            if cross.lower() == "down":
                if last <= goal:
                    df.at[index, "Status"] = "OK"
                else:
                    df.at[index, "Status"] = "Not Yet"
            elif cross.lower() == "up":
                if last >= goal:
                    df.at[index, "Status"] = "OK"
                else:
                    df.at[index, "Status"] = "Not Yet"

            ### Call the last status to check if mail gonna sent or not
            status = row["Status"]
            notify = row["Notify"]

            if status == "OK" and pd.isna(notify):
                print("Send message")
                message = f"#{symbol} reached the goal by crossing {goal} to {cross} as last price of {last}"
                print(message)
                try:
                    await send_telegram_message(message)
                    df.at[index, "Notify"] = "Sent"
                except Exception as e:
                    await send_telegram_message(e)
                    print(e)

        driver.quit()

        # Update the Excel with last values
        df.to_excel(writer, sheet_name=STOCKS_Sheet, index=False)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(e)
