import boto3
from selenium import webdriver
from selenium.webdriver.common.by import By

NEXTSTRAIN_MPXV = "https://nextstrain.org/monkeypox/hmpxv1"


def fetch_metadata(link):
    # op = webdriver.ChromeOptions()
    # op.add_experimental_option("prefs", {"download.default_directory": ""})
    # driver = webdriver.Chrome(options=op)
    driver = webdriver.Chrome()
    driver.get(link)

    def find_button(text):
        if not (
            elems := [
                e for e in driver.find_elements(By.TAG_NAME, "button") if e.text == text
            ]
        ):
            raise ValueError(f"No button found with {text}")
        else:
            print(text)
        return elems[0]

    find_button("DOWNLOAD DATA").click()
    find_button("METADATA (TSV)").click()


if __name__ == "__main__":
    fetch_metadata(NEXTSTRAIN_MPXV)
