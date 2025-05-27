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
from services.ie_service import IeService
from services.users_service import UsersService

class IeInteresseService:
    def __init__(self):
        self.repository = IeInteresseRepository()

    def get_all_active(self) -> List[IeInteresse]:
        return self.repository.get_all_active()
        
    def insert_ie_interesse(self, df: DataFrame,
                            column_ie: str = 'ie_value',
                            column_razao_social: str = 'razao_social',
                            column_cpf_cnpj: str = 'cpf_cnpj',
                            column_municipio: str = 'municipio',
                            column_uf: str = 'uf',
                            client_id: int = 1) -> int:
        if df.empty:
            return 0
        
        dfx = df.copy()
        
        dfx.rename(columns={column_ie: 'ie_value'}, inplace=True)
        dfx.rename(columns={column_razao_social: 'razao_social'}, inplace=True)
        dfx.rename(columns={column_cpf_cnpj: 'cpf_cnpj'}, inplace=True)
        dfx.rename(columns={column_uf: 'uf'}, inplace=True)
        
        column_ie = 'ie_value'
        
        # usuario
        user_service = UsersService()        
        user = user_service.get_user_by_email(EMAIL_FROM)
                        
        # adicionar colunas
        dfx['ie_status'] = 'AVALIAR'
        dfx['ativo'] = True
        dfx['clients'] = dfx.apply(lambda row: json.dumps([{"id_client": client_id}]), axis=1)
        dfx['created_at'] = datetime.now().isoformat()
        dfx['updated_at'] = datetime.now().isoformat()
        dfx['created_by'] = user.id if user else None
        dfx['updated_by'] = user.id if user else None                
        
        # deixar somente as ies que não estao no banco de dados
        ies_str = ", ".join(f"{str(ie)}" for ie in set(dfx[column_ie].to_list()))
        
        # pega as ies do banco de dados que estão no dfx
        ies = self.repository.query_by_where('ie_interesse', f"ie_value in ({ies_str})", fields='ie_value')
        
        # garante que não há valores duplicados
        ies_set = set(row[0] for row in ies) if len(ies) > 0 else set()
        
        # elimina as ies existentes no BD do dfx
        dfx = dfx[~dfx[column_ie].isin(ies_set)]
            
        # remover possíveis duplicatas dentro do próprio dfx
        dfx = dfx.drop_duplicates(subset=[column_ie], keep='first')        
        
        if not dfx.empty:
            ie_service = IeService()
            ie_service.get_municipio_cached.cache_clear()            
            dfx['municipio'] = dfx[column_ie].apply(ie_service.get_municipio_cached)
            
            columns_to_keep = ['ie_value', 'razao_social', 'cpf_cnpj', 'municipio', 'uf',
                               'ie_status', 'ativo', 'clients', 'created_at', 'updated_at',
                               'created_by', 'updated_by']
            dfx = dfx[columns_to_keep]
            self.insert_chunk('ie_interesse', dfx)
        return len(dfx)
        
    def sync_ie_interesse(self, df: DataFrame, 
            column_ie: str = 'ie_value', 
            column_razao_social: str = 'razao_social',
            column_cpf_cnpj: str = 'cpf_cnpj',
            column_municipio: str = 'municipio',
            column_uf: str = 'uf',
            client_id: int = 1) -> tuple[DataFrame, int, str]:
        error: str = ''
        try:
            if df.empty:
                return df, error

            # filtra ies de interesse
            ies = set(
                df[column_ie]
                .dropna()
                .astype(str)
                .str.replace(r'\D', '', regex=True)
                .astype(int)
                .tolist()
            )
            ies_not_eligibles = self.repository.get_all_not_elegible(ies)
            # remove as não elegíveis do df
            df_ies_elegible = df[~df[column_ie].isin(ies_not_eligibles)].copy()
                        
            # insere banco de dados
            total_novas_ies = self.insert_ie_interesse(df_ies_elegible,
                                    column_ie=column_ie,
                                    column_razao_social=column_razao_social,
                                    column_cpf_cnpj=column_cpf_cnpj,
                                    column_municipio=column_municipio,
                                    column_uf=column_uf,
                                    client_id=client_id)
            return df_ies_elegible, total_novas_ies, error
        except Exception as ex:
            error = f"Erro ao sincronizar IEs de Interesse no banco de dados. Erro: {str(ex)}"
            return df, 0, error              

    def insert_chunk(self, table: str, chunk: DataFrame) -> str:
        return self.repository.insert_chunk(table, chunk)    
