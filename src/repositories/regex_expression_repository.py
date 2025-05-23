from typing import List, TypeVar
from domain.regex_expression import RegexExpression
from interfaces.regex_expression_repository_interface import IRegexExpressionRepository
from repositories.base_repository import BaseRepository
from sqlalchemy import select, update, text, exc

T = TypeVar('T')

class RegexExpressionRepository(BaseRepository, IRegexExpressionRepository):
    def __init__(self):        
        super().__init__(RegexExpression)
            
    def get_all_active(self) -> List[RegexExpression]:
        with self.SessionSync() as session:
            try:
                stmt = select(RegexExpression).where(RegexExpression.ativo == True)
                result = session.execute(stmt).scalars().all()
                return result
            except exc.SQLAlchemyError as ex:
                session.rollback()
                raise ex
            except Exception as ex:
                session.rollback()
                raise ex