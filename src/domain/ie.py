from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Ie(Base):
    __table_args__ = {"schema": "cbx"}
    __tablename__ = 'ie'

    id = Column(Integer, primary_key=True)
    properties = Column(JSON)
    ie_value = Column(String())    
    cpf = Column(String())
    cnpj = Column(String())
    status = Column(Boolean())
    ie_status_text = Column(String())
    uf_ie = Column(String())
    
    # chave grupo
    cpf_main = Column(String())
    group_cbx = Column(String())
    cbx_cod = Column(Integer())
    
    group_id = Column(Integer())
    codigo_produtor_sap = Column(Integer())
    classificacao = Column(String()) # 1 - ARMAZEN | 2 - MISTO |3 - PRODUTOR RURAL

    clients = Column(JSON) 
    s3_url = Column(String())    
    sources = Column(JSON)     
    created_at = Column(DateTime(timezone=False), nullable=True)
    updated_at = Column(DateTime(timezone=False), nullable=True)
    created_by = Column(Integer(), nullable=True)
    updated_by = Column(Integer(), nullable=True)
