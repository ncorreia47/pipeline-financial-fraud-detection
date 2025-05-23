from abc import ABC, abstractmethod
from colorama import Fore, Style, init

init(autoreset=True)


class PrintStrategy(ABC):
    @abstractmethod
    def print(self, message: str) -> None:
        pass


class BrightYellowPrint(PrintStrategy):
    def print(self, message: str) -> None:
        print(Style.BRIGHT + Fore.YELLOW + f'--> {message}')


class RedBoldPrint(PrintStrategy):
    def print(self, message: str) -> None:
        print(Style.BRIGHT + Fore.RED + f'# {message}')


class GreenNormalPrint(PrintStrategy):
    def print(self, message: str) -> None:
        print(Fore.GREEN + f'[{message}]')


class Printer:
    def __init__(self, strategy: PrintStrategy):
        self._strategy = strategy

    def set_strategy(self, strategy: PrintStrategy):
        self._strategy = strategy

    def display(self, message: str):
        self._strategy.print(message)
