"""
Script di sincronizzazione bidirezionale Google Drive.
Punto di ingresso principale dell'applicazione.
"""

import argparse
import logging
import sys
import os
from src import config, auth
from src.drive_client import DriveClient

def setup_logging():
    """Configura il sistema di logging."""
    # Assicurati che la directory dei log esista
    os.makedirs('logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/drive_sync.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('drive_sync')

def parse_arguments():
    """Analizza gli argomenti da linea di comando."""
    parser = argparse.ArgumentParser(description='Sincronizzazione bidirezionale Google Drive')
    parser.add_argument('--config', default='config/config.yaml', 
                        help='Percorso del file di configurazione')
    parser.add_argument('--auth', action='store_true',
                        help='Esegui solo il processo di autenticazione')
    parser.add_argument('--test', action='store_true',
                        help='Esegui i test di connessione e lista file')
    return parser.parse_args()

def format_size(size_bytes):
    """Formatta una dimensione in bytes in formato leggibile."""
    if size_bytes >= 1024**3:  # GB
        return f"{round(size_bytes/(1024**3), 2)} GB"
    elif size_bytes >= 1024**2:  # MB
        return f"{round(size_bytes/(1024**2), 2)} MB"
    elif size_bytes >= 1024:  # KB
        return f"{round(size_bytes/1024), 2} KB"
    else:
        return f"{size_bytes} B"

def test_drive_connection(drive_client, logger):
    """Testa la connessione a Google Drive e mostra informazioni di base."""
    logger.info("Esecuzione del test di connessione a Google Drive")
    
    try:
        # Test della connessione
        about = drive_client.test_connection()
        
        # Visualizza informazioni sull'account
        print("\n" + "="*60)
        print("INFORMAZIONI ACCOUNT GOOGLE DRIVE")
        print("="*60)
        print(f"Nome utente: {about['user']['displayName']}")
        print(f"Email: {about['user']['emailAddress']}")
        
        # Informazioni sullo spazio di archiviazione
        storage = about['storageQuota']
        usage = int(storage.get('usage', 0))
        limit = int(storage.get('limit', 0))
        
        print("\nSPAZIO DI ARCHIVIAZIONE:")
        print(f"Spazio utilizzato: {format_size(usage)}")
        
        if limit > 0:
            percent = round((usage / limit) * 100, 2)
            print(f"Spazio totale: {format_size(limit)}")
            print(f"Percentuale utilizzata: {percent}%")
        
        # Elenca i file nella cartella root
        print("\n" + "="*60)
        print("FILE NELLA CARTELLA ROOT")
        print("="*60)
        
        files = drive_client.list_files()
        
        if not files:
            print("Nessun file trovato nella cartella root.")
        else:
            print(f"Trovati {len(files)} elementi")
            print("\n{:<40} {:<15} {:<20}".format("Nome", "Tipo", "Ultima modifica"))
            print("-" * 80)
            
            for file in files[:10]:  # Mostra solo i primi 10
                name = file.get('name', '')
                if len(name) > 37:
                    name = name[:34] + "..."
                
                mime_type = file.get('mimeType', '')
                if mime_type == 'application/vnd.google-apps.folder':
                    type_label = "Cartella"
                elif drive_client.is_google_doc(mime_type):
                    type_label = "Google Doc"
                else:
                    type_label = "File"
                
                modified = file.get('modifiedTime', '').replace('T', ' ').replace('Z', '')
                if modified:
                    modified = modified[:16]  # Tronca i secondi
                
                print("{:<40} {:<15} {:<20}".format(name, type_label, modified))
            
            if len(files) > 10:
                print("\n... e altri elementi non mostrati")
        
        return True
    except Exception as e:
        logger.error(f"Errore durante il test di connessione: {str(e)}")
        return False

def main():
    """Funzione principale."""
    args = parse_arguments()
    logger = setup_logging()
    
    logger.info("Avvio applicazione di sincronizzazione Google Drive")
    
    try:
        # Carica configurazione
        cfg = config.load_config(args.config)
        logger.info(f"Configurazione caricata da {args.config}")
        
        # Gestisci autenticazione
        drive_service = auth.authenticate(cfg['auth'])
        logger.info("Autenticazione con Google Drive completata")
        
        if args.auth:
            logger.info("Processo di autenticazione completato con successo")
            return 0
        
        # Crea un'istanza del client Drive
        drive_client = DriveClient(drive_service)
        logger.info("Client Drive inizializzato")
        
        if args.test:
            # Esegui i test di connessione
            success = test_drive_connection(drive_client, logger)
            if success:
                logger.info("Test di connessione completato con successo")
            else:
                logger.error("Test di connessione fallito")
                return 1
            return 0
            
        # TODO: Implementare la logica di sincronizzazione
        logger.info("La sincronizzazione verrà implementata nelle fasi successive")
        
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione: {str(e)}")
        return 1
    
    logger.info("Programma terminato con successo")
    return 0

if __name__ == "__main__":
    sys.exit(main())