from abc import ABC, abstractmethod

from jobs.context import JobContext


class Job(ABC):
    def __init__(self, jc: JobContext) -> None:
        self.jc = jc

    @abstractmethod
    def compute(self):
        pass
