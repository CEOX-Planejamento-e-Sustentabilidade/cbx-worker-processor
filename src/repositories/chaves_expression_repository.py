from typing import List, TypeVar
from domain.chaves_expression import ChavesExpression
from interfaces.chaves_expression_repository_interface import IChavesExpressionRepository
from repositories.base_repository import BaseRepository
from sqlalchemy import select, update, text, exc

T = TypeVar('T')

class ChavesExpressionRepository(BaseRepository, IChavesExpressionRepository):
    def __init__(self):        
        super().__init__(ChavesExpression)
            
    def get_all_active(self) -> List[ChavesExpression]:
        with self.SessionSync() as session:
            try:
                stmt = select(ChavesExpression).where(ChavesExpression.ativo == True)
                result = session.execute(stmt).scalars().all()
                return result
            except exc.SQLAlchemyError as ex:
                session.rollback()
                raise ex
            except Exception as ex:
                session.rollback()
                raise ex