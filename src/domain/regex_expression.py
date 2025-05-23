from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class RegexExpression(Base):
    __table_args__ = {"schema": "cbx"}
    __tablename__ = 'regex_expression'

    id = Column(Integer, primary_key=True, autoincrement=True)
    expressao = Column(String, nullable=False)
    amostra = Column(String, nullable=False)
    descricao = Column(String, nullable=False)
    alvo = Column(String, nullable=False)
    ativo = Column(Boolean, nullable=False)
    created_at = Column(DateTime(timezone=False), nullable=False)
    updated_at = Column(DateTime(timezone=False), nullable=False)
    created_by = Column(Integer, nullable=True)
    updated_by = Column(Integer, nullable=True)
    