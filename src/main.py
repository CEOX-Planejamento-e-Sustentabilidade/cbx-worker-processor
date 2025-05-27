import shutil
import uuid

from pathlib import Path
from configs import *
from services.aws_service import AwsService
from services.email_service import EmailService
from services.file_service import FileService
from services.logger_service import LoggerService
from services.nf_service import NotaFiscalService
from os.path import join

from services.utils import format_title

class WorkerProcessor:
    def __init__(self):
        self.aws_service = AwsService()
        self.file_service = FileService()
        self.logger_service = LoggerService()
                    
    def run(self):
        try:
            # baixa arquivo zip do S3
            # descompacta o arquivo zip
            # processa os arquivos do zip
            # envia os arquivos processados para o S3
            # envia email com os links dos arquivos processados
            
            folder = ''
            download_path = ''            
            
            if DEBUG:
                #json_user = json.loads('{"auth": true, "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImVkemF0YXJpbkBnbWFpbC5jb20iLCJ1c2VyIjpbMTMzLCJoZWxkZXJAY2J4c3VzdGVudGFiaWxpZGFkZS5jb20uYnIiLCIwMzUxYTMyYzc0MTU1ZDRkZTcxOGYyYWYwYTU5ZmY3N2M1MGExODIwYjk4OTZiYmQ0ZjMyYzMxYWQ1YTkwODQyIix7Im5hbWUiOiJIZWxkZXIgQ2FzdHJvIiwiY2xpZW50cyI6WzMsMiwxLDcsMTJdLCJzZW5kX3F1ZXVlIjp0cnVlLCJtZXNzYWdlX2dyb3VwIjoiQ0JYIn0sdHJ1ZSwiYW5hbGlzdGEiXX0.VyaUdL6cIJWOlEYdaR2n8c9VhIpu4wmAkij3_KDuocI", "role": "analista", "clients": [3, 2, 1, 7, 12]}')
                #token = f'Bearer {json_user["token"]}'
                #userdata = jwt.decode(token.replace('Bearer ', ''), JWT_SECRET, algorithms=['HS256'])                                
                #send_queue = userdata['user'][3]['send_queue'] if 'send_queue' in userdata['user'][3] else False
                #message_group = userdata['user'][3]['message_group'] if 'message_group' in userdata['user'][3] else 'NO_MSG_GROUP'
                
                transaction_id = '4f3f8a5b-a8ec-493f-87ff-144205ee12r4'                
                tipo = 22 # 1: insumos, 2: milho, 5: cbios, 21: danfe, 22: sefaz, 23: chaves
                email = 'edzatarin@gmail.com'
                email_request = 'edzatarin@gmail.com'
                zip_name = 'NF 3498'
                #s3_path = 'input/Relatórios e NF-e Fertilizantes (2)-19e83c659.zip'
                #s3_path = 'input/NF 3498-b927eedbc.zip'
                s3_path = 'input/sefaz - 13.243.389-3 - Saida.zip'
                client_id = 1
                message_group = 'CBX'
                user_id = 139
                raw_send_queue = 'True'
                send_queue = True
                request_origin = 'WEB'
            else:
                # container docker - prod
                s3_path = os.getenv("S3_PATH")        
                transaction_id = os.getenv("TRANSACTION_ID")
                zip_name = os.getenv("FILE_NAME")
                
                tipo = os.getenv("TIPO")
                if tipo:
                    tipo = int(tipo)
                else:
                    return False, "Tipo não informado"
                    
                client_id = os.getenv("CLIENT_ID")
                if client_id:
                    client_id = int(client_id)
                else:
                    return False, "Client ID não informado"
                    
                request_origin = os.getenv("REQUEST_ORIGIN")
                
                # dados do usuário
                message_group = os.getenv("MESSAGE_GROUP")
                email_request = os.getenv("EMAIL_REQUEST")
                email = os.getenv("EMAIL")
                user_id = os.getenv("USER_ID")
                if user_id:
                    user_id = int(user_id)
                else:
                    return False, "User ID não informado"                   

                raw_send_queue = os.getenv("SEND_QUEUE")
                send_queue = str(raw_send_queue).strip().lower() in ("true")
            
            self.logger_service.info('-------------------')
            self.logger_service.info('VALORES RECEBIDOS:')
            self.logger_service.info('-------------------')
            self.logger_service.info(format_title('transaction id', transaction_id))
            self.logger_service.info(format_title('file name', zip_name))                
            self.logger_service.info(format_title('tipo', tipo))
            if email_request:
                self.logger_service.info(format_title('email robo', email_request))
            self.logger_service.info(format_title('email', email))
            self.logger_service.info(format_title('s3 path', s3_path))
            self.logger_service.info(format_title('client id', client_id))
            self.logger_service.info(format_title('message group', message_group))
            self.logger_service.info(format_title('user id', user_id))
            self.logger_service.info(format_title('send queue', send_queue) +" - "+ format_title('raw send queue', raw_send_queue) )
            self.logger_service.info(format_title('request origin', request_origin))
            self.logger_service.info('-------------------')
                                               
            # cria pasta para o arquivo zip        
            hash_id = str(uuid.uuid4()).replace('-', '')[:9]
            folder = join(ROOT_DOWNLOAD_FOLDER, zip_name, hash_id)
            sucesso, msg = self.file_service.create_folder(folder)
            self.logger_service.info(msg)
            if not sucesso:
                self.logger_service.error(msg)
                return False, msg
                
            # baixa arquivo zip do S3
            only_file_name = Path(s3_path).name
            download_path = join(folder, only_file_name)
            sucesso, msg =self.aws_service.download(s3_path, download_path)
            self.logger_service.info(msg)
            if not sucesso:
                self.logger_service.error(msg)
                return False, msg
            
            # processa o arquivo zip
            nf_service = NotaFiscalService()                
            email_send = email_request if request_origin == 'ROBO' else email
            result = nf_service.unzip_file_and_process(s3_path, zip_name, download_path, tipo,
                send_queue, user_id, message_group, email_send, transaction_id, request_origin, client_id)
            
            return True, f'Arquivo {zip_name} processado com sucesso!'
            
            # verifica erros
            # status = result["status"]
            # erros = result["erros"]        
            # if not status and erros:
            #     email_service = EmailService()
            #     error_str = email_service.get_flat_html_from_list(erros)
            #     if not DEBUG:
            #         #email_send = email_request if request_origin == 'ROBO' else email
            #         email_send = EMAIL_FROM
            #         sucesso, code, msg = email_service.send_error(email_send, error_str, zip_name, transaction_id)
            #         if not sucesso:
            #             error_str = f"{msg}\n{code}"
            #             return False, error_str                        
            #     error_str = f'{email_service.get_flat_str_from_list(erros)}'
            #     return False, error_str
            # else:            
            #     return True, 'Sucesso! Não há erros'
        except Exception as ex:
            return False, str(ex)
        finally:
            if folder and os.path.exists(folder):
                shutil.rmtree(folder) 

            if download_path and os.path.exists(download_path):
                os.remove(download_path) 
    
    def iniciar_worker(self):
        try:
            self.logger_service.info("<<<--- INÍCIO PROCESSOR --->>>")
            self.logger_service.info("Modo DEBUG: " + str(DEBUG))
            self.logger_service.info("ENVIRONMENT: " + ENVIRONMENT)            
            sucesso, msg = self.run()
            self.logger_service.info(msg)
            return sucesso, msg        
        except Exception as ex:
            msg = f"ERROR: {str(ex)}"            
            self.logger_service.error(msg)
            return False, msg
        finally:
            self.logger_service.info("<<<--- PROCESSOR FINALIZADO --->>>")

if __name__ == "__main__":
    worker = WorkerProcessor()
    worker.iniciar_worker()