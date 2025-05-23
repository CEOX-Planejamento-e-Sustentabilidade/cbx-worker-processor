from abc import ABC, abstractmethod
from typing import List, TypeVar

from domain.ie_interesse import IeInteresse
   
T = TypeVar('T')

class IIeInteresseRepository(ABC):
    @abstractmethod
    def get_all_active(self) -> List[IeInteresse]:
        pass
    
    @abstractmethod
    def get_all_not_elegible(self, tipo: int, ies: list) -> List[IeInteresse]:
        pass
