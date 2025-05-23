from abc import ABC, abstractmethod
from typing import TypeVar
from domain.users import Users
    
T = TypeVar('T')

class IUsersRepository(ABC):
    @abstractmethod
    def get_user_by_email(self, email: str) -> Users:
        pass