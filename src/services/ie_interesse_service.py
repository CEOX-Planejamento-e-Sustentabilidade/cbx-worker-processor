import json
import re
import pandas as pd

from datetime import datetime
from typing import List, Optional
from pandas import DataFrame
from sqlalchemy import bindparam, text
from configs import EMAIL_FROM
from domain.ie_interesse import IeInteresse
from repositories.ie_interesse_repository import IeInteresseRepository
from services.users_service import UsersService

class IeInteresseService:
    def __init__(self):
        self.repository = IeInteresseRepository()

    def get_all_active(self) -> List[IeInteresse]:
        return self.repository.get_all_active()
    
    def insert_ie_interesse(self, df: DataFrame,
                            tipo_nota: int = 0,
                            column_ie: str = 'ie_value',
                            column_razao_social: str = 'razao_social',
                            column_cpf_cnpj: str = 'cpf_cnpj',
                            column_municipio: str = 'municipio',
                            column_uf: str = 'uf',
                            client_id: int = 1) -> None:
        if df.empty:
            return
        
        df.rename(columns={column_ie: 'ie_value'}, inplace=True)
        df.rename(columns={column_razao_social: 'razao_social'}, inplace=True)
        df.rename(columns={column_cpf_cnpj: 'cpf_cnpj'}, inplace=True)
        df.rename(columns={column_municipio: 'municipio'}, inplace=True)
        df.rename(columns={column_uf: 'uf'}, inplace=True)
        
        user_service = UsersService()
        user = user_service.get_user_by_email(EMAIL_FROM)
        
        # adicionar colunas
        df['tipo'] = tipo_nota
        df['ie_status'] = 'INTERESSE'
        df['ativo'] = True
        df['clients'] = df.apply(lambda row: json.dumps([{"id_client": client_id}]), axis=1)
        df['created_at'] = datetime.now().isoformat()
        df['updated_at'] = datetime.now().isoformat()
        df['created_by'] = user.id if user else None
        df['updated_by'] = user.id if user else None
        
        # deixar somente as ies que não estao no banco de dados
        ies_str = ", ".join(f"{ie}" for ie in df[column_ie].to_list())
        
        # pega as ies do banco de dados que estão no df
        ies = self.repository.query_by_where('ie_interesse', f"tipo = {tipo_nota} and ie_value in ({ies_str})", fields='ie_value')
        
        # garante que não há valores duplicados
        ies_set = set(row[0] for row in ies) if len(ies) > 0 else set()
        
        # elimina as chaves existentes no BD do df
        df = df[~df[column_ie].isin(ies_set)]
            
        # remover possíveis duplicatas dentro do próprio df
        df = df.drop_duplicates(subset=[column_ie], keep='first')        
        
        if not df.empty:
            # insert
            self.insert_chunk('ie_interesse', df)
        
    def sync_ie_interesse(self, df: DataFrame, 
            tipo_nota: int = 0,
            column_ie: str = 'ie_value', 
            column_razao_social: str = 'razao_social',
            column_cpf_cnpj: str = 'cpf_cnpj',
            column_municipio: str = 'municipio',
            column_uf: str = 'uf',
            client_id: int = 1) -> DataFrame:
        error: str = ''
        try:
            # NF 1 - Entrada
            # comprei: tipo: 0 | emissor: xpto -> ie 0001 | destina: edmar -> ie 0002
            # NF 2 - Entrada
            # comprei: tipo: 0 | emissor: dfgh -> ie 1234 | destina: edmar -> ie 0002
            # NF 3 - Saida
            # vendi: tipo: 1 | emissor: edmar -> ie 0002 | destina: wwqq -> ie 4567
            # NF 4 - Saida
            # vendi: tipo: 1 | emissor: edmar -> ie 0002 | destina: zzaa -> ie 0102

            if df.empty:
                return df, error

            # filtra ies de interesse
            #ies = set(df[df[column_tipo_nota] == '0'][column_ie].drop_duplicates())
            ies = set(df[column_ie].drop_duplicates())
            ies_not_eligibles = self.repository.get_all_not_elegible(tipo_nota, ies)
            #df_ies_elegible = df[(~df[column_ie].isin(ies_not_eligibles)) & (df[column_tipo_nota] == 0)].copy()
            df_ies_elegible = df[~df[column_ie].isin(ies_not_eligibles)].copy()
                        
            # insere banco de dados
            self.insert_ie_interesse(df_ies_elegible, 
                                    tipo_nota, 
                                    column_ie=column_ie, 
                                    column_razao_social=column_razao_social,
                                    column_cpf_cnpj=column_cpf_cnpj,
                                    column_municipio=column_municipio,
                                    column_uf=column_uf,
                                    client_id=client_id)      
            return df_ies_elegible, error
        except Exception as ex:
            error = f"Erro ao sincronizar IEs de Interesse no banco de dados. Erro: {str(ex)}"
        return df, error              

    def insert_chunk(self, table: str, chunk: DataFrame) -> str:
        return self.repository.insert_chunk(table, chunk)    
