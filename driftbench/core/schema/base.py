from abc import ABC, abstractmethod

class BaseSchemaExtractor(ABC):
    @abstractmethod
    def extract_schema(self):
        pass