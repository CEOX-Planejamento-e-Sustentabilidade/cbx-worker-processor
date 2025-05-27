from typing import List, TypeVar
from domain.ie import Ie
from interfaces.ie_repository_interface import IIeRepository
from repositories.base_repository import BaseRepository
from sqlalchemy import and_, func, select, update, text, exc

T = TypeVar('T')

class IeRepository(BaseRepository, IIeRepository):
    def __init__(self):        
        super().__init__(Ie)
            
    def get_municipio(self, ie_value: int) -> str:
        with self.SessionSync() as session:
            try:                    
                stmt = select(Ie).where(Ie.ie_value == ie_value)
                result = session.execute(stmt).scalars().first()
                return result.properties.get('municipio', '') if result else ''
            except exc.SQLAlchemyError as ex:
                session.rollback()
                raise ex
            except Exception as ex:
                session.rollback()
                raise ex            