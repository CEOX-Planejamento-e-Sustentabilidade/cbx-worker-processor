from abc import ABC, abstractmethod
from typing import List, TypeVar

from domain.chaves_expression import ChavesExpression
from domain.file_process_log import FileProcessLog
    
T = TypeVar('T')

class IChavesExpressionRepository(ABC):
    @abstractmethod
    def get_all_active(self) -> List[ChavesExpression]:
        pass
