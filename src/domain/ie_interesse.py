from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class IeInteresse(Base):
    __table_args__ = {"schema": "cbx"}
    __tablename__ = 'ie_interesse'

    id = Column(Integer, primary_key=True, autoincrement=True)    
    ie_value = Column(Integer, nullable=False)
    razao_social = Column(String, nullable=False)
    cpf_cnpj = Column(String, nullable=False)
    municipio = Column(String, nullable=False)
    uf = Column(String, nullable=False)
    ie_status = Column(String, nullable=False) # 'INTERESSE' 'NAO INTERESSE' 'AVALIAR'
    ativo = Column(Boolean, nullable=False)
    clients = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=False), nullable=True)
    updated_at = Column(DateTime(timezone=False), nullable=True)
    created_by = Column(Integer, nullable=True)
    updated_by = Column(Integer, nullable=True)