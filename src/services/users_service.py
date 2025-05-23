from typing import List
from pandas import DataFrame

from domain.users import Users
from repositories.users_repository import UsersRepository

class UsersService:
    def __init__(self):
        self.repository = UsersRepository()
        
    def get_user_by_email(self, email: str) -> Users:
        return self.repository.get_user_by_email(email)