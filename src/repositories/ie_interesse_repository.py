from typing import List, TypeVar
from domain.ie_interesse import IeInteresse
from interfaces.ie_interesse_repository_interface import IIeInteresseRepository
from repositories.base_repository import BaseRepository
from sqlalchemy import and_, select, update, text, exc

T = TypeVar('T')

class IeInteresseRepository(BaseRepository, IIeInteresseRepository):
    def __init__(self):        
        super().__init__(IeInteresse)
            
    def get_all_active(self) -> List[IeInteresse]:
        with self.SessionSync() as session:
            try:
                stmt = select(IeInteresse).where(IeInteresse.ativo == True)
                result = session.execute(stmt).scalars().all()
                return result
            except exc.SQLAlchemyError as ex:
                session.rollback()
                raise ex
            except Exception as ex:
                session.rollback()
                raise ex
            
    def get_all_not_elegible(self, tipo: int, ies: list) -> List[IeInteresse]:
        with self.SessionSync() as session:
            try:
                stmt = select(IeInteresse).where(
                    and_(IeInteresse.ativo == True,
                         IeInteresse.tipo == tipo,
                         IeInteresse.ie_status == 'NAO INTERESSE'),
                         IeInteresse.ie_value.in_(ies))
                result = session.execute(stmt).scalars().all()
                return result
            except exc.SQLAlchemyError as ex:
                session.rollback()
                raise ex
            except Exception as ex:
                session.rollback()
                raise ex            