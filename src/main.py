import json
import shutil
import uuid

from pathlib import Path

import jwt
from configs import *
from services.aws_service import AwsService
from services.email_service import EmailService
from services.file_service import FileService
from services.logger_service import LoggerService
from services.nf_service import NotaFiscalService
from os.path import join

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
            
            if DEBUG:
                #json_user = json.loads('{"auth": true, "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImVkemF0YXJpbkBnbWFpbC5jb20iLCJ1c2VyIjpbMTMzLCJoZWxkZXJAY2J4c3VzdGVudGFiaWxpZGFkZS5jb20uYnIiLCIwMzUxYTMyYzc0MTU1ZDRkZTcxOGYyYWYwYTU5ZmY3N2M1MGExODIwYjk4OTZiYmQ0ZjMyYzMxYWQ1YTkwODQyIix7Im5hbWUiOiJIZWxkZXIgQ2FzdHJvIiwiY2xpZW50cyI6WzMsMiwxLDcsMTJdLCJzZW5kX3F1ZXVlIjp0cnVlLCJtZXNzYWdlX2dyb3VwIjoiQ0JYIn0sdHJ1ZSwiYW5hbGlzdGEiXX0.VyaUdL6cIJWOlEYdaR2n8c9VhIpu4wmAkij3_KDuocI", "role": "analista", "clients": [3, 2, 1, 7, 12]}')
                #token = f'Bearer {json_user["token"]}'
                #userdata = jwt.decode(token.replace('Bearer ', ''), JWT_SECRET, algorithms=['HS256'])                                
                #send_queue = userdata['user'][3]['send_queue'] if 'send_queue' in userdata['user'][3] else False
                #message_group = userdata['user'][3]['message_group'] if 'message_group' in userdata['user'][3] else 'NO_MSG_GROUP'
                
                transaction_id = '4f3f8a5b-a8ec-493f-87ff-144205ee12r4'
                zip_name = 'XMLs-ba40b6951'
                tipo = 1
                email = 'edzatarin@gmail.com'
                email_request = 'edzatarin@gmail.com'
                s3_path = 'input/XMLs-ba40b6951.zip'
                client_id = 1
                message_group = 'CBX'
                user_id = 139
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
                    
                send_queue = os.getenv("SEND_QUEUE")
                
            #userdata = self.get_userdata(user_id, email, send_queue, message_group)
                
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
            
            # verifica erros
            status = result["status"]
            erros = result["erros"]        
            if not status and erros:
                email_service = EmailService()
                error_str = email_service.get_flat_html_from_list(erros)
                if not DEBUG:
                    email_send = email_request if request_origin == 'ROBO' else email
                    sucesso, code, msg = email_service.send_error(email_send, error_str, zip_name, transaction_id)
                    if not sucesso:
                        error_str = f"{msg}\n{code}"
                        return False, error_str                        
                error_str = f'{email_service.get_flat_str_from_list(erros)}'
                return False, error_str
            else:            
                return True, 'Sucesso! Não há erros'
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

    # def get_userdata(self, user_id: int, email: str, send_queue: bool, message_group: str):
    #     # mesmo padrao de estrutura da api
    #     userdata = {
    #         "email": email,
    #         "user": [
    #             user_id,
    #             email,
    #             "password",
    #             {
    #                 "name": "User name",
    #                 "clients": [],
    #                 "send_queue": send_queue,
    #                 "message_group": message_group
    #             },
    #             True,
    #             "admin"
    #         ]
    #     }
    #     return userdata            

if __name__ == "__main__":
    worker = WorkerProcessor()
    worker.iniciar_worker()
    worker.logger_service.info("CONTAINER FINALIZADO")