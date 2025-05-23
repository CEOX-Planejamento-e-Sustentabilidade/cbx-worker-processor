from typing import List, TypeVar
from sqlalchemy import delete, select
from domain.users import Users
from interfaces.users_repository_interface import IUsersRepository
from repositories.base_repository import BaseRepository

T = TypeVar('T')

class UsersRepository(BaseRepository, IUsersRepository):
    def __init__(self):        
        super().__init__(Users)
                    
    def get_user_by_email(self, email: str) -> Users:
        with self.SessionSync() as session:
            try:            
                result = session.query(Users).filter(Users.email == email).first()
                return result
            except Exception as ex:
                session.rollback()
                raise ex