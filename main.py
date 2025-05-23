from src.utils.custom_logger import Printer, BrightYellowPrint
from src.ingest.create_kaggle_key import CreateKaggleKey
from src.ingest.get_files_from_kaggle import GetCsvFileFromKaggle

def main():
    CreateKaggleKey.create_kaggle_key()
    printer = Printer(BrightYellowPrint())
    downloader = GetCsvFileFromKaggle(printer)
    downloader.get_csv()

if __name__ == "__main__":
    main()

