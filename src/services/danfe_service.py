import pdfplumber
import traceback
import pandas as pd
import re
from pathlib import Path
from configs import *
from services.regex_expression_service import RegexExpressionService

class DanfeService:
    def __init__(self):
        self.regex_expr_service = RegexExpressionService()
        expressions = self.regex_expr_service.get_all_active()
        self.ie_tipo_patterns = self.regex_expr_service.get_pattern_by_alvo(expressions, self.regex_expr_service.alvo_nf_tipo)
        self.chaves_patterns = self.regex_expr_service.get_pattern_by_alvo(expressions, self.regex_expr_service.alvo_nf_chave)
        self.ie_emissor_patterns = self.regex_expr_service.get_pattern_by_alvo(expressions, self.regex_expr_service.alvo_nf_ie_emissor)
        self.ie_destinatario_patterns = self.regex_expr_service.get_pattern_by_alvo(expressions, self.regex_expr_service.alvo_nf_ie_destinatario)

    def processar_danfes(self, folder_zip_extracted, full_path_zip_filename):
        try:
            erros = []
            dados = []
            pdf_folder = Path(folder_zip_extracted).rglob('*.pdf')
            files = [x for x in pdf_folder]
            i = 0
            total = len(files) 
            while i < total:
                if DEBUG:
                    print(f'{i} de {total}')
                    
                f = files[i]
                #for f in files:
                if "__MACOSX" not in str(f):
                    try:
                        _chave, _cpf_cnpj, _message, _total_pages, _ie_destinatario, _ie_emissor, _cpf_cnpj_emissor, \
                        tipo, numeronf, destinatario_data_emissao = self.process_danfe_file_string(f)

                        _ano_mes_emissao = ""

                        if _chave is not None:
                            _chave = _chave.replace(" ", "").strip()
                            if len(_chave) == 44:                                            
                                _ano_mes_emissao = _chave[2:6]
                                _cpf_cnpj_emissor = _chave[6:20]
                                numeronf = _chave[25:34]
                                
                            dados.append([_cpf_cnpj, _chave, _ie_destinatario, _ie_emissor, _cpf_cnpj_emissor,
                                        _ano_mes_emissao, tipo, numeronf, destinatario_data_emissao, str(f), _message, _total_pages])
                        else:
                            erros.append(f'Chave nula para o arquivo {str(f)}: {_message}')

                    except Exception as ex:
                        erros.append(f'Erro no arquivo {str(f)}: {str(ex)}')
                i += 1
            
            if not dados:
                erros.append(f"Sem xmls válidos para processar. Arquivo: {str(full_path_zip_filename)}")
                return {
                    "status": False, 
                    "erros": erros, 
                    "total_files": len(files) if files else 0,
                    "df": None
                }            
                        
            df = pd.DataFrame(dados)
            df.columns = ['CPF/CNPJ', 'CHAVE', "IE_DESTINATARIO", "IE_EMISSOR", "CPF/CNPJ EMISSOR", 
                        "ANO/MES EMISSAO", "TIPO PROCESSAMENTO", "NR. NOTA FISCAL", "DATA EMISSÃO",
                        "ARQUIVO", "STATUS", "Nr de Páginas"]
            
            return {
                "status": True, 
                "erros": erros, 
                "total_files": len(files) if files else 0,
                "df": df
            }            
        except Exception:
            erros.append({f"Erro ao processar xmls. Pasta de origem: {str(folder_zip_extracted)}", f"exception: {str(ex)}"})
            return {
                "status": False, 
                "erros": erros, 
                "total_files": len(files) if files else 0
            }                                    

    def process_danfe_file_string(self, filename):
        _chave = None
        _cpf_cnpj = None
        _message = None
        _total_pages = 0
        _ie_destinatario = None
        _ie_emissor = None
        ie_destinatario = None
        ie_emissor = None
        _cpf_cnpj_emissor = None
        tipo = None

        try:
            with pdfplumber.open(filename) as pdf:
                if DEBUG:
                    print("\n\nprocessando ", filename)
                _total_pages = len(pdf.pages)
                page = pdf.pages[0]  # Pegar a primeira página

                #print(page.width, page.height)

                text = page.extract_text()

                # para debug, imprimir o TXT do PDF
                if DEBUG:
                    with open(str(filename)+".txt", 'w', encoding='utf-8') as f:
                        f.write(text)

                # Se não houver texto ou o texto extraído for muito curto
                if not text or len(text.strip()) < 5:
                    return None, None, "o arquivo é uma imagem", None, None, None, None, None, None, None

                # Tente extrair Nº. 000.001.823
                numero_pattern = re.compile(r'Nº\.\s*([\d\.]+)')
                numero_match = numero_pattern.search(text)
                numero = numero_match.group(1) if numero_match else None

                tipo = self.regex_expr_service.get_after_group(text, self.ie_tipo_patterns)                
                chave_acesso = self.regex_expr_service.get_group(text, self.chaves_patterns)
                ie_emissor = self.regex_expr_service.get_after_group(text, self.ie_emissor_patterns)
                ie_destinatario = self.regex_expr_service.get_after_group_last_value(text, self.ie_destinatario_patterns)

                destinatario_nome = destinatario_cpf = destinatario_data_emissao = ''
                
                if tipo:
                    tipo = tipo.strip()
                
                # Remove all non-numeric characters
                if chave_acesso:
                    chave_acesso = re.sub(r'\D', '', str(chave_acesso))

                if DEBUG:
                    print("Número:", numero)
                    print("Inscrições Dest.:", ie_destinatario)
                    print("Destinatário Nome:", destinatario_nome)
                    print("Destinatário CPF:", destinatario_cpf)
                    print("Destinatário Data de Emissão:", destinatario_data_emissao)
                    print("IE Emissor", ie_emissor)
                    print("Chave de acesso", chave_acesso)

                return chave_acesso, destinatario_cpf, _message, _total_pages, ie_destinatario, ie_emissor, _cpf_cnpj_emissor, tipo, numero, destinatario_data_emissao
        except Exception as ex:
            raise ex