from abc import ABC, abstractmethod
from typing import List, TypeVar

from domain.regex_expression import RegexExpression
    
T = TypeVar('T')

class IRegexExpressionRepository(ABC):
    @abstractmethod
    def get_all_active(self) -> List[RegexExpression]:
        pass
