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
from src.sync_engine import SyncEngine
from src.sync_executor import SyncExecutor

def setup_logging():
    """Configura il sistema di logging."""
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
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
    parser.add_argument('--analyze', action='store_true',
                        help='Analizza le cartelle e mostra le differenze')
    parser.add_argument('--sync-down', action='store_true',
                        help='Esegui la sincronizzazione da Drive a locale')
    parser.add_argument('--sync-up', action='store_true',
                        help='Esegui la sincronizzazione da locale a Drive')
    parser.add_argument('--sync-both', action='store_true',
                        help='Esegui la sincronizzazione bidirezionale')
    parser.add_argument('--dry-run', action='store_true',
                        help='Simula la sincronizzazione senza eseguire modifiche')
    return parser.parse_args()

def format_size(size_bytes):
    """Formatta una dimensione in bytes in formato leggibile."""
    if size_bytes >= 1024**3:  # GB
        return f"{round(size_bytes/(1024**3), 2)} GB"
    elif size_bytes >= 1024**2:  # MB
        return f"{round(size_bytes/(1024**2), 2)} MB"
    elif size_bytes >= 1024:  # KB
        return f"{round(size_bytes/1024, 2)} KB"
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

def analyze_folders(sync_engine, logger):
    """Analizza le cartelle configurate e mostra le differenze."""
    logger.info("Analisi delle cartelle configurate")
    
    sync_plan = sync_engine.create_sync_plan()
    
    # Mostra statistiche di sincronizzazione
    print("\n" + "="*60)
    print("ANALISI DELLE CARTELLE CONFIGURATE")
    print("="*60)
    
    print(f"\nFile da scaricare da Drive: {len(sync_plan['to_download'])}")
    print(f"File da caricare su Drive: {len(sync_plan['to_upload'])}")
    print(f"File in conflitto: {len(sync_plan['conflicts'])}")
    print(f"Cartelle presenti solo su Drive: {len(sync_plan['drive_only_folders'])}")
    print(f"Cartelle presenti solo in locale: {len(sync_plan['local_only_folders'])}")
    
    # Mostra dettagli sui file da scaricare
    if sync_plan['to_download']:
        print("\n" + "-"*60)
        print("FILE DA SCARICARE:")
        print("-"*60)
        for file in sync_plan['to_download'][:10]:  # Limita a 10 per brevità
            print(f"  • {file['full_path']} ({format_size(int(file.get('size', 0)))})")
        if len(sync_plan['to_download']) > 10:
            print(f"  ... e altri {len(sync_plan['to_download']) - 10} file")
    
    # Mostra dettagli sui file da caricare
    if sync_plan['to_upload']:
        print("\n" + "-"*60)
        print("FILE DA CARICARE:")
        print("-"*60)
        for file in sync_plan['to_upload'][:10]:  # Limita a 10 per brevità
            print(f"  • {file['full_path']} ({format_size(int(file.get('size', 0)))})")
        if len(sync_plan['to_upload']) > 10:
            print(f"  ... e altri {len(sync_plan['to_upload']) - 10} file")
    
    # Mostra dettagli sui file in conflitto
    if sync_plan['conflicts']:
        print("\n" + "-"*60)
        print("FILE IN CONFLITTO:")
        print("-"*60)
        for drive_file, local_file in sync_plan['conflicts'][:10]:  # Limita a 10 per brevità
            print(f"  • {drive_file['full_path']}")
            print(f"    - Drive: modificato il {drive_file.get('modifiedTime', '').replace('T', ' ').replace('Z', '')[:16]}")
            print(f"    - Locale: modificato il {local_file.get('modifiedTime', '').replace('T', ' ').replace('Z', '')[:16]}")
        if len(sync_plan['conflicts']) > 10:
            print(f"  ... e altri {len(sync_plan['conflicts']) - 10} conflitti")
    
    return sync_plan

def sync_drive_to_local(sync_engine, sync_executor, logger, dry_run=False):
    """
    Esegue la sincronizzazione da Google Drive al filesystem locale.
    
    Args:
        sync_engine: Istanza di SyncEngine
        sync_executor: Istanza di SyncExecutor
        logger: Logger per messaggi informativi
        dry_run: Se True, simula la sincronizzazione senza eseguire modifiche
        
    Returns:
        bool: True se la sincronizzazione è stata completata con successo
    """
    logger.info("Avvio sincronizzazione da Google Drive a locale")
    
    try:
        # Genera il piano di sincronizzazione
        sync_plan = sync_engine.create_sync_plan()
        
        # Mostra il piano
        print("\n" + "="*60)
        print("PIANO DI SINCRONIZZAZIONE (DRIVE → LOCALE)")
        print("="*60)
        
        print(f"\nFile da scaricare: {len(sync_plan['to_download'])}")
        print(f"Cartelle da creare: {len(sync_plan['drive_only_folders'])}")
        print(f"Conflitti da risolvere: {len(sync_plan['conflicts'])}")
        
        # Calcola la dimensione totale da scaricare
        total_size = sum(int(file.get('size', 0)) for file in sync_plan['to_download'])
        for drive_file, _ in sync_plan['conflicts']:
            if sync_engine.preferences.get('conflict_resolution', 'drive') == 'drive':
                total_size += int(drive_file.get('size', 0))
        
        print(f"Dimensione totale da scaricare: {format_size(total_size)}")
        
        if dry_run:
            print("\nModalità DRY-RUN: Nessuna modifica verrà eseguita")
            return True
        
        # Chiedi conferma all'utente
        confirm = input("\nVuoi procedere con la sincronizzazione? (s/n): ").lower()
        if confirm != 's' and confirm != 'si' and confirm != 'sì':
            print("Sincronizzazione annullata.")
            return False
        
        # Esegui il piano di sincronizzazione
        stats = sync_executor.execute_plan(sync_plan, direction='down')
        
        # Mostra le statistiche finali
        print("\n" + "="*60)
        print("SINCRONIZZAZIONE COMPLETATA")
        print("="*60)
        
        print(f"\nFile scaricati: {stats['downloaded_files']}")
        print(f"Dati scaricati: {format_size(stats['downloaded_bytes'])}")
        print(f"Cartelle create: {stats['created_folders']}")
        print(f"Errori: {stats['errors']}")
        print(f"Tempo di esecuzione: {stats['execution_time']:.2f} secondi")
        
        return stats['errors'] == 0
        
    except Exception as e:
        logger.error(f"Errore durante la sincronizzazione: {str(e)}")
        return False

def sync_local_to_drive(sync_engine, sync_executor, logger, dry_run=False):
    """
    Esegue la sincronizzazione dal filesystem locale a Google Drive.
    
    Args:
        sync_engine: Istanza di SyncEngine
        sync_executor: Istanza di SyncExecutor
        logger: Logger per messaggi informativi
        dry_run: Se True, simula la sincronizzazione senza eseguire modifiche
        
    Returns:
        bool: True se la sincronizzazione è stata completata con successo
    """
    logger.info("Avvio sincronizzazione da locale a Google Drive")
    
    try:
        # Genera il piano di sincronizzazione
        sync_plan = sync_engine.create_sync_plan()
        
        # Mostra il piano
        print("\n" + "="*60)
        print("PIANO DI SINCRONIZZAZIONE (LOCALE → DRIVE)")
        print("="*60)
        
        print(f"\nFile da caricare: {len(sync_plan['to_upload'])}")
        print(f"Cartelle da creare su Drive: {len(sync_plan['local_only_folders'])}")
        print(f"Conflitti da risolvere: {len(sync_plan['conflicts'])}")
        
        # Calcola la dimensione totale da caricare
        total_size = sum(int(file.get('size', 0)) for file in sync_plan['to_upload'])
        for _, local_file in sync_plan['conflicts']:
            if sync_engine.preferences.get('conflict_resolution', 'drive') == 'local':
                total_size += int(local_file.get('size', 0))
        
        print(f"Dimensione totale da caricare: {format_size(total_size)}")
        
        if dry_run:
            print("\nModalità DRY-RUN: Nessuna modifica verrà eseguita")
            return True
        
        # Chiedi conferma all'utente
        confirm = input("\nVuoi procedere con la sincronizzazione? (s/n): ").lower()
        if confirm != 's' and confirm != 'si' and confirm != 'sì':
            print("Sincronizzazione annullata.")
            return False
        
        # Esegui il piano di sincronizzazione
        stats = sync_executor.execute_plan(sync_plan, direction='up')
        
        # Mostra le statistiche finali
        print("\n" + "="*60)
        print("SINCRONIZZAZIONE COMPLETATA")
        print("="*60)
        
        print(f"\nFile caricati: {stats['uploaded_files']}")
        print(f"Dati caricati: {format_size(stats['uploaded_bytes'])}")
        print(f"Cartelle remote create: {stats['created_remote_folders']}")
        print(f"Errori: {stats['errors']}")
        print(f"Tempo di esecuzione: {stats['execution_time']:.2f} secondi")
        
        return stats['errors'] == 0
        
    except Exception as e:
        logger.error(f"Errore durante la sincronizzazione: {str(e)}")
        return False

def sync_bidirectional(sync_engine, sync_executor, logger, dry_run=False):
    """
    Esegue la sincronizzazione bidirezionale tra Google Drive e il filesystem locale.
    
    Args:
        sync_engine: Istanza di SyncEngine
        sync_executor: Istanza di SyncExecutor
        logger: Logger per messaggi informativi
        dry_run: Se True, simula la sincronizzazione senza eseguire modifiche
        
    Returns:
        bool: True se la sincronizzazione è stata completata con successo
    """
    logger.info("Avvio sincronizzazione bidirezionale")
    
    try:
        # Genera il piano di sincronizzazione
        sync_plan = sync_engine.create_sync_plan()
        
        # Mostra il piano
        print("\n" + "="*60)
        print("PIANO DI SINCRONIZZAZIONE BIDIREZIONALE")
        print("="*60)
        
        print("\nOperazioni da Drive a locale:")
        print(f"- File da scaricare: {len(sync_plan['to_download'])}")
        print(f"- Cartelle da creare in locale: {len(sync_plan['drive_only_folders'])}")
        
        print("\nOperazioni da locale a Drive:")
        print(f"- File da caricare: {len(sync_plan['to_upload'])}")
        print(f"- Cartelle da creare su Drive: {len(sync_plan['local_only_folders'])}")
        
        print(f"\nConflitti da risolvere: {len(sync_plan['conflicts'])}")
        conflict_resolution = sync_engine.preferences.get('conflict_resolution', 'drive')
        print(f"Risoluzione conflitti: priorità a {'Drive' if conflict_resolution == 'drive' else 'Locale'}")
        
        # Calcola le dimensioni totali
        download_size = sum(int(file.get('size', 0)) for file in sync_plan['to_download'])
        upload_size = sum(int(file.get('size', 0)) for file in sync_plan['to_upload'])
        
        # Aggiungi dimensioni dei file in conflitto
        for drive_file, local_file in sync_plan['conflicts']:
            if conflict_resolution == 'drive':
                download_size += int(drive_file.get('size', 0))
            else:
                upload_size += int(local_file.get('size', 0))
        
        print(f"\nDimensione totale da scaricare: {format_size(download_size)}")
        print(f"Dimensione totale da caricare: {format_size(upload_size)}")
        
        if dry_run:
            print("\nModalità DRY-RUN: Nessuna modifica verrà eseguita")
            return True
        
        # Chiedi conferma all'utente
        confirm = input("\nVuoi procedere con la sincronizzazione bidirezionale? (s/n): ").lower()
        if confirm != 's' and confirm != 'si' and confirm != 'sì':
            print("Sincronizzazione annullata.")
            return False
        
        # Esegui il piano di sincronizzazione bidirezionale
        stats = sync_executor.execute_plan(sync_plan, direction='both')
        
        # Mostra le statistiche finali
        print("\n" + "="*60)
        print("SINCRONIZZAZIONE BIDIREZIONALE COMPLETATA")
        print("="*60)
        
        print(f"\nFile scaricati: {stats['downloaded_files']}")
        print(f"Dati scaricati: {format_size(stats['downloaded_bytes'])}")
        print(f"Cartelle locali create: {stats['created_folders']}")
        
        print(f"\nFile caricati: {stats['uploaded_files']}")
        print(f"Dati caricati: {format_size(stats['uploaded_bytes'])}")
        print(f"Cartelle remote create: {stats['created_remote_folders']}")
        
        print(f"\nErrori totali: {stats['errors']}")
        print(f"Tempo di esecuzione: {stats['execution_time']:.2f} secondi")
        
        return stats['errors'] == 0
        
    except Exception as e:
        logger.error(f"Errore durante la sincronizzazione: {str(e)}")
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
        
        # Crea un'istanza del motore di sincronizzazione
        sync_engine = SyncEngine(drive_client, cfg)
        logger.info("Motore di sincronizzazione inizializzato")
        
        # Crea un'istanza dell'esecutore di sincronizzazione
        sync_executor = SyncExecutor(drive_client, cfg)
        logger.info("Esecutore di sincronizzazione inizializzato")
        
        if args.analyze:
            # Analizza le cartelle configurate
            analyze_folders(sync_engine, logger)
            return 0
        
        if args.test:
            # Esegui i test di connessione
            success = test_drive_connection(drive_client, logger)
            if success:
                logger.info("Test di connessione completato con successo")
            else:
                logger.error("Test di connessione fallito")
                return 1
            return 0
        
        if args.sync_down:
            # Esegui la sincronizzazione da Drive a locale
            success = sync_drive_to_local(sync_engine, sync_executor, logger, args.dry_run)
            if success:
                logger.info("Sincronizzazione Drive → Locale completata con successo")
            else:
                logger.error("Sincronizzazione Drive → Locale fallita")
                return 1
            return 0
            
        if args.sync_up:
            # Esegui la sincronizzazione da locale a Drive
            success = sync_local_to_drive(sync_engine, sync_executor, logger, args.dry_run)
            if success:
                logger.info("Sincronizzazione Locale → Drive completata con successo")
            else:
                logger.error("Sincronizzazione Locale → Drive fallita")
                return 1
            return 0
            
        if args.sync_both:
            # Esegui la sincronizzazione bidirezionale
            success = sync_bidirectional(sync_engine, sync_executor, logger, args.dry_run)
            if success:
                logger.info("Sincronizzazione bidirezionale completata con successo")
            else:
                logger.error("Sincronizzazione bidirezionale fallita")
                return 1
            return 0
            
        # Se non è specificata alcuna azione, mostra il menu interattivo
        print("\n" + "="*60)
        print("SINCRONIZZAZIONE GOOGLE DRIVE")
        print("="*60)
        print("\nOperazioni disponibili:")
        print("1. Test connessione Google Drive")
        print("2. Analizza cartelle configurate")
        print("3. Sincronizza da Drive a locale")
        print("4. Sincronizza da locale a Drive")
        print("5. Sincronizzazione bidirezionale")
        print("0. Esci")
        
        choice = input("\nScegli un'operazione (0-5): ")
        
        if choice == '1':
            test_drive_connection(drive_client, logger)
        elif choice == '2':
            analyze_folders(sync_engine, logger)
        elif choice == '3':
            sync_drive_to_local(sync_engine, sync_executor, logger)
        elif choice == '4':
            sync_local_to_drive(sync_engine, sync_executor, logger)
        elif choice == '5':
            sync_bidirectional(sync_engine, sync_executor, logger)
        elif choice == '0':
            print("Uscita dal programma.")
        else:
            print("Scelta non valida.")
            
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione: {str(e)}")
        return 1
    
    logger.info("Programma terminato con successo")
    return 0

if __name__ == "__main__":
    sys.exit(main())