from abc import ABC, abstractmethod
from typing import List, TypeVar

from domain.ie import Ie
    
T = TypeVar('T')

class IIeRepository(ABC):       
    @abstractmethod
    def get_municipio(self, ie_value: int) -> str:
        pass