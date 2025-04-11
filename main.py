
"""
Script di sincronizzazione bidirezionale Google Drive.
Punto di ingresso principale dell'applicazione.
"""

import argparse
import logging
import sys
from src import config, auth

def setup_logging():
    """Configura il sistema di logging."""
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
    return parser.parse_args()

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
            return
        
        # TODO: Implementare la logica di sincronizzazione
        logger.info("La sincronizzazione verrà implementata nelle fasi successive")
        
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione: {str(e)}")
        return 1
    
    logger.info("Programma terminato con successo")
    return 0

if __name__ == "__main__":
    sys.exit(main())