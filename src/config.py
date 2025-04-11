
"""
Modulo per la gestione della configurazione.
"""

import os
import yaml
import logging

logger = logging.getLogger('drive_sync.config')

def load_config(config_path):
    """
    Carica il file di configurazione, riceve come parametro il path al file di conf.
    
    """
    logger.info(f"Caricamento configurazione da {config_path}")
    
    if not os.path.exists(config_path):
        logger.error(f"File di configurazione non trovato: {config_path}")
        raise FileNotFoundError(f"File di configurazione non trovato: {config_path}")
    
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
            logger.debug("Configurazione caricata con successo")
            return config
    except Exception as e:
        logger.error(f"Errore durante il caricamento della configurazione: {str(e)}")
        raise