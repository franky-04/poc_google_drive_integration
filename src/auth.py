"""
Modulo per l'autenticazione con Google Drive API.
Supporta solo client di tipo 'web'.
"""

import os
import pickle
import logging
import socket
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

logger = logging.getLogger('drive_sync.auth')

# Scope richiesti per l'accesso a Google Drive
SCOPES = ['https://www.googleapis.com/auth/drive']

def check_port_available(port):
    """
    Verifica se una porta è disponibile.
    
    Args:
        port (int): Numero di porta da verificare
        
    Returns:
        bool: True se la porta è disponibile, False altrimenti
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) != 0

def authenticate(auth_config):
    """
    Gestisce l'autenticazione con Google Drive API, supportando solo client web.
    
    Args:
        auth_config (dict): Configurazione per l'autenticazione.
        
    Returns:
        googleapiclient.discovery.Resource: Servizio Google Drive autenticato.
    """
    # Imposta la variabile di ambiente per permettere HTTP su localhost
    # IMPORTANTE: Questo bypassa un controllo di sicurezza, ma è accettabile solo per localhost
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    
    creds = None
    token_path = auth_config.get('token_path', 'credentials/token.pickle')
    credentials_path = auth_config.get('credentials_path', 'credentials/web_credentials.json')
    headless = auth_config.get('headless', True)
    port = auth_config.get('port', 8080)
    
    # Verifica se la porta è disponibile, altrimenti usa un'alternativa
    if not check_port_available(port):
        logger.warning(f"Porta {port} non disponibile. Tentativo con porta alternativa.")
        port = 8090  # Porta alternativa
        if not check_port_available(port):
            logger.error("Anche la porta alternativa non è disponibile.")
            raise RuntimeError("Nessuna porta disponibile per il server locale di reindirizzamento.")
    
    # Carica il token salvato, se presente
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            logger.debug(f"Caricamento token da {token_path}")
            creds = pickle.load(token)
    
    # Se non ci sono credenziali valide, esegui il flusso di autenticazione
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.debug("Token scaduto, aggiornamento...")
            creds.refresh(Request())
        else:
            logger.info("Avvio flusso di autenticazione OAuth2")
            if not os.path.exists(credentials_path):
                logger.error(f"File delle credenziali non trovato: {credentials_path}")
                raise FileNotFoundError(
                    f"File delle credenziali non trovato: {credentials_path}. "
                    "Scaricalo dalla Google Cloud Console."
                )
            
            # Crea il flow per applicazione web
            logger.info("Utilizzo di credenziali per applicazione web")
            flow = Flow.from_client_secrets_file(
                credentials_path, 
                SCOPES,
                redirect_uri=f'http://localhost:{port}'
            )
            
            if headless:
                logger.info("Esecuzione in modalità headless")
                
                # Per applicazioni web in modalità headless
                auth_url, _ = flow.authorization_url(
                    access_type='offline',
                    prompt='consent',
                    include_granted_scopes='true'
                )
                
                print("\n" + "="*60)
                print("Autenticazione Google Drive (Web App)")
                print("="*60)
                print("\nVisita il seguente URL nel tuo browser per autorizzare l'applicazione:")
                print(f"\n{auth_url}\n")
                print("Dopo l'autorizzazione, verrai reindirizzato a un URL localhost.")
                print("Copia l'intero URL di reindirizzamento e incollalo qui sotto.")
                print("="*60 + "\n")
                
                # Richiedi all'utente l'URL di reindirizzamento completo
                redirect_url = input("Incolla l'URL di reindirizzamento completo: ").strip()
                
                # Estrai il codice dall'URL di reindirizzamento
                flow.fetch_token(authorization_response=redirect_url)
            else:
                # Se è disponibile un browser, usa il server locale
                logger.info(f"Avvio del server locale sulla porta {port}")
                
                # Per le applicazioni web, devi gestire manualmente il server
                from wsgiref.simple_server import make_server
                import threading
                import time
                from urllib.parse import urlparse, parse_qs
                
                # Flag e variabili condivise per la comunicazione tra thread
                authorization_code = [None]
                server_stopped = [False]
                
                def handle_redirect(environ, start_response):
                    """Gestore WSGI per catturare il codice di autorizzazione."""
                    query = parse_qs(urlparse(environ['REQUEST_URI']).query)
                    if 'code' in query:
                        authorization_code[0] = query['code'][0]
                    
                    start_response('200 OK', [('Content-type', 'text/html')])
                    return [b"Autenticazione completata! Puoi chiudere questa finestra."]
                
                # Avvia il server in un thread separato
                httpd = make_server('localhost', port, handle_redirect)
                server_thread = threading.Thread(target=httpd.serve_forever)
                server_thread.daemon = True
                server_thread.start()
                
                # Genera l'URL di autorizzazione e apri il browser
                auth_url, _ = flow.authorization_url(
                    access_type='offline',
                    prompt='consent',
                    include_granted_scopes='true'
                )
                
                import webbrowser
                webbrowser.open(auth_url)
                
                # Attendi il codice di autorizzazione
                print("Attesa dell'autorizzazione nel browser...")
                timeout = 300  # 5 minuti di timeout
                start_time = time.time()
                
                while authorization_code[0] is None and time.time() - start_time < timeout:
                    time.sleep(1)
                
                # Ferma il server
                httpd.shutdown()
                server_stopped[0] = True
                
                if authorization_code[0]:
                    # Usa il codice per ottenere il token
                    flow.fetch_token(code=authorization_code[0])
                    creds = flow.credentials
                else:
                    raise TimeoutError("Timeout durante l'attesa dell'autorizzazione.")
            
            creds = flow.credentials
        
        # Salva le credenziali per il prossimo utilizzo
        os.makedirs(os.path.dirname(token_path), exist_ok=True)
        with open(token_path, 'wb') as token:
            logger.debug(f"Salvataggio token in {token_path}")
            pickle.dump(creds, token)
    
    logger.info("Costruzione servizio Google Drive API")
    service = build('drive', 'v3', credentials=creds)
    return service