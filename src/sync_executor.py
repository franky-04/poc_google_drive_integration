"""
Modulo per l'esecuzione dei piani di sincronizzazione.
Si occupa di applicare le azioni definite nel piano di sincronizzazione.
"""

import os
import logging
import time
from typing import Dict, List, Any

logger = logging.getLogger('drive_sync.sync_executor')

class SyncExecutor:
    """
    Esecutore di piani di sincronizzazione. 
    Applica le azioni definite nel piano per sincronizzare file e cartelle.
    """
    
    def __init__(self, drive_client, config: Dict[str, Any]):
        """
        Inizializza l'esecutore di sincronizzazione.
        
        Args:
            drive_client: Client per interagire con Google Drive
            config: Configurazione della sincronizzazione
        """
        self.drive_client = drive_client
        self.config = config
        self.preferences = config.get('preferences', {})
        self.sync_folders = config.get('sync_folders', {})
        self.stats = {
            'downloaded_files': 0,
            'downloaded_bytes': 0,
            'uploaded_files': 0,
            'uploaded_bytes': 0,
            'created_folders': 0,
            'created_remote_folders': 0,
            'errors': 0
        }
        logger.debug("SyncExecutor inizializzato con successo")
    
    def execute_plan(self, sync_plan, direction='down'):
        """
        Esegue il piano di sincronizzazione.
        
        Args:
            sync_plan: Piano di sincronizzazione generato da SyncEngine
            direction: Direzione della sincronizzazione ('down' per Drive→Locale, 'up' per Locale→Drive, 'both' per bidirezionale)
            
        Returns:
            dict: Statistiche dell'esecuzione
        """
        logger.info(f"Inizio esecuzione del piano di sincronizzazione (direzione: {direction})")
        start_time = time.time()
        
        # Reset delle statistiche
        self.stats = {
            'downloaded_files': 0,
            'downloaded_bytes': 0,
            'uploaded_files': 0,
            'uploaded_bytes': 0,
            'created_folders': 0,
            'created_remote_folders': 0,
            'errors': 0
        }
        
        # Sincronizzazione da Drive a locale (DOWN)
        if direction in ['down', 'both']:
            # Fase 1: Crea cartelle mancanti localmente
            self._create_missing_folders(sync_plan.get('drive_only_folders', []))
            
            # Fase 2: Scarica i file da Drive
            self._download_files(sync_plan.get('to_download', []))
            
            # Fase 3: Gestisci i conflitti (Drive → Locale)
            if self.preferences.get('conflict_resolution', 'drive') == 'drive':
                self._resolve_conflicts_from_drive(sync_plan.get('conflicts', []))
                
        # Sincronizzazione da locale a Drive (UP)
        if direction in ['up', 'both']:
            # Fase 1: Crea cartelle mancanti su Drive
            self._create_remote_folders(sync_plan.get('local_only_folders', []))
            
            # Fase 2: Carica i file su Drive
            self._upload_files(sync_plan.get('to_upload', []))
            
            # Fase 3: Gestisci i conflitti (Locale → Drive)
            if self.preferences.get('conflict_resolution', 'local') == 'local':
                self._resolve_conflicts_from_local(sync_plan.get('conflicts', []))
        
        # Calcola il tempo impiegato
        execution_time = time.time() - start_time
        self.stats['execution_time'] = execution_time
        
        # Log delle statistiche
        logger.info(f"Esecuzione completata in {execution_time:.2f} secondi")
        if direction in ['down', 'both']:
            logger.info(f"File scaricati: {self.stats['downloaded_files']}")
            logger.info(f"Byte scaricati: {self.stats['downloaded_bytes']}")
            logger.info(f"Cartelle locali create: {self.stats['created_folders']}")
        if direction in ['up', 'both']:
            logger.info(f"File caricati: {self.stats['uploaded_files']}")
            logger.info(f"Byte caricati: {self.stats['uploaded_bytes']}")
            logger.info(f"Cartelle remote create: {self.stats['created_remote_folders']}")
        logger.info(f"Errori: {self.stats['errors']}")
        
        return self.stats
    
    def _create_missing_folders(self, folders):
        """
        Crea le cartelle mancanti in locale.
        
        Args:
            folders: Lista di cartelle da creare
        """
        logger.info(f"Creazione di {len(folders)} cartelle mancanti in locale")
        
        for folder in folders:
            try:
                # Estrai il percorso locale basato sul percorso drive
                local_base_path = self._get_local_path_for_drive_path(folder['path'])
                
                if not local_base_path:
                    logger.warning(f"Impossibile determinare il percorso locale per '{folder['path']}', cartella ignorata")
                    continue
                
                logger.debug(f"Creazione cartella locale: {local_base_path}")
                
                # Crea la cartella se non esiste
                if not os.path.exists(local_base_path):
                    os.makedirs(local_base_path, exist_ok=True)
                    self.stats['created_folders'] += 1
                    logger.debug(f"Cartella creata: {local_base_path}")
                else:
                    logger.debug(f"Cartella già esistente: {local_base_path}")
                
                # Crea ricorsivamente tutte le cartelle figlie
                if 'children' in folder:
                    self._create_missing_folders(folder['children'])
                
            except Exception as e:
                logger.error(f"Errore durante la creazione della cartella '{folder.get('path', '')}': {str(e)}")
                self.stats['errors'] += 1
    
    def _create_remote_folders(self, folders):
        """
        Crea le cartelle mancanti su Google Drive.
        
        Args:
            folders: Lista di cartelle locali da creare su Drive
        """
        logger.info(f"Creazione di {len(folders)} cartelle mancanti su Drive")
        
        for folder in folders:
            try:
                # Ottieni il percorso Drive corrispondente al percorso locale
                drive_path = self._get_drive_path_for_local_path(folder['path'])
                
                if not drive_path:
                    logger.warning(f"Impossibile determinare il percorso Drive per '{folder['path']}', cartella ignorata")
                    continue
                
                logger.debug(f"Creazione cartella remota: {drive_path}")
                
                # Crea la cartella su Drive
                created_folder = self.drive_client.create_folder_path(drive_path)
                
                if created_folder:
                    self.stats['created_remote_folders'] += 1
                    logger.debug(f"Cartella Drive creata: {drive_path}")
                else:
                    logger.warning(f"Impossibile creare la cartella Drive: {drive_path}")
                    self.stats['errors'] += 1
                
                # Crea ricorsivamente tutte le cartelle figlie
                if 'children' in folder:
                    self._create_remote_folders(folder['children'])
                
            except Exception as e:
                logger.error(f"Errore durante la creazione della cartella Drive '{folder.get('path', '')}': {str(e)}")
                self.stats['errors'] += 1
    
    def _download_files(self, files):
        """
        Scarica i file da Google Drive in locale.
        
        Args:
            files: Lista di file da scaricare
        """
        logger.info(f"Download di {len(files)} file")
        
        total_size = sum(int(file.get('size', 0)) for file in files)
        logger.info(f"Dimensione totale da scaricare: {self._format_size(total_size)}")
        
        for i, file in enumerate(files):
            try:
                # Verifica se il file è un documento Google nativo
                if file.get('is_google_doc', False):
                    logger.warning(f"Documento Google nativo ignorato: {file.get('name', 'Sconosciuto')}")
                    continue
                
                file_id = file.get('id')
                file_name = file.get('name', 'Sconosciuto')
                file_size = int(file.get('size', 0))
                
                # Determina il percorso locale
                local_path = file.get('local_path')
                
                if not local_path:
                    # Se local_path non è stato pre-calcolato, lo determiniamo
                    local_path = self._get_local_path_for_drive_path(file.get('full_path', ''))
                
                if not local_path:
                    logger.warning(f"Impossibile determinare il percorso locale per '{file_name}', file ignorato")
                    continue
                
                # Assicurati che la directory esista
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                # Log informativo
                progress = f"{i+1}/{len(files)}"
                logger.info(f"Download [{progress}]: {file_name} ({self._format_size(file_size)}) -> {local_path}")
                
                # Esegui il download
                success = self.drive_client.download_file(file_id, local_path)
                
                if success:
                    self.stats['downloaded_files'] += 1
                    self.stats['downloaded_bytes'] += file_size
                    logger.debug(f"Download completato: {file_name}")
                else:
                    logger.error(f"Errore durante il download di '{file_name}'")
                    self.stats['errors'] += 1
                
            except Exception as e:
                logger.error(f"Errore durante il download del file '{file.get('name', 'Sconosciuto')}': {str(e)}")
                self.stats['errors'] += 1
    
    def _upload_files(self, files):
        """
        Carica i file locali su Google Drive.
        
        Args:
            files: Lista di file locali da caricare
        """
        logger.info(f"Upload di {len(files)} file")
        
        total_size = sum(int(file.get('size', 0)) for file in files)
        logger.info(f"Dimensione totale da caricare: {self._format_size(total_size)}")
        
        for i, file in enumerate(files):
            try:
                file_name = file.get('name', 'Sconosciuto')
                file_size = int(file.get('size', 0))
                local_path = self._get_absolute_local_path(file.get('full_path', ''))
                
                if not local_path or not os.path.exists(local_path):
                    logger.warning(f"File locale non trovato: {local_path}")
                    continue
                
                # Determina la cartella padre su Drive
                parent_path = file.get('parent_path', '')
                drive_parent_path = self._get_drive_path_for_local_path(parent_path)
                
                if not drive_parent_path:
                    logger.warning(f"Impossibile determinare la cartella padre su Drive per '{parent_path}'")
                    continue
                
                # Trova o crea la cartella padre su Drive
                parent_id = self.drive_client.find_folder_by_path(drive_parent_path)
                if not parent_id:
                    logger.debug(f"Cartella padre '{drive_parent_path}' non trovata, tentativo di creazione")
                    parent_id = self.drive_client.create_folder_path(drive_parent_path)
                    if not parent_id:
                        logger.error(f"Impossibile creare la cartella padre '{drive_parent_path}'")
                        self.stats['errors'] += 1
                        continue
                
                # Log informativo
                progress = f"{i+1}/{len(files)}"
                logger.info(f"Upload [{progress}]: {file_name} ({self._format_size(file_size)}) -> {drive_parent_path}")
                
                # Esegui l'upload
                uploaded_file = self.drive_client.upload_file(local_path, parent_id, file_name)
                
                if uploaded_file:
                    self.stats['uploaded_files'] += 1
                    self.stats['uploaded_bytes'] += file_size
                    logger.debug(f"Upload completato: {file_name}")
                else:
                    logger.error(f"Errore durante l'upload di '{file_name}'")
                    self.stats['errors'] += 1
                
            except Exception as e:
                logger.error(f"Errore durante l'upload del file '{file.get('name', 'Sconosciuto')}': {str(e)}")
                self.stats['errors'] += 1
    
    def _resolve_conflicts_from_drive(self, conflicts):
        """
        Risolve i conflitti dando priorità alla versione su Drive.
        
        Args:
            conflicts: Lista di tuple (drive_file, local_file) in conflitto
        """
        drive_files = [drive_file for drive_file, _ in conflicts]
        logger.info(f"Risoluzione di {len(conflicts)} conflitti (priorità a Drive)")
        
        # Scarica i file da Drive
        self._download_files(drive_files)
    
    def _resolve_conflicts_from_local(self, conflicts):
        """
        Risolve i conflitti dando priorità alla versione locale.
        
        Args:
            conflicts: Lista di tuple (drive_file, local_file) in conflitto
        """
        local_files = [local_file for _, local_file in conflicts]
        logger.info(f"Risoluzione di {len(conflicts)} conflitti (priorità a Locale)")
        
        # Carica i file locali su Drive
        self._upload_files(local_files)
        
        # Per ogni file in conflitto, aggiorna il file esistente su Drive invece di crearne uno nuovo
        for drive_file, local_file in conflicts:
            try:
                drive_id = drive_file.get('id')
                local_path = self._get_absolute_local_path(local_file.get('full_path', ''))
                
                if drive_id and local_path and os.path.exists(local_path):
                    logger.debug(f"Aggiornamento file Drive: {drive_file.get('name')}")
                    self.drive_client.update_file(drive_id, local_path)
            except Exception as e:
                logger.error(f"Errore durante l'aggiornamento del file: {str(e)}")
                self.stats['errors'] += 1
    
    def _get_local_path_for_drive_path(self, drive_path):
        """
        Calcola il percorso locale corrispondente a un percorso su Drive.
        
        Args:
            drive_path: Percorso completo su Drive
            
        Returns:
            str or None: Percorso locale corrispondente o None se non trovato
        """
        # Gestisci il caso di file senza percorso (solo nome file)
        if '/' not in drive_path:
            # Se è solo un nome file, potrebbe essere nella root di Drive
            # Prendi la prima cartella configurata come destinazione
            for drive_folder, local_folder in self.sync_folders.items():
                if drive_folder == 'root' or drive_folder == '/':
                    # È configurato per sincronizzare la root di Drive
                    local_path = os.path.join(local_folder, drive_path)
                    logger.debug(f"Mappando file di root '{drive_path}' a '{local_path}'")
                    return os.path.normpath(local_path)
                
                # Se non c'è una mappatura esplicita per la root, usa la prima cartella come default
                local_path = os.path.join(local_folder, drive_path)
                logger.debug(f"Usando la prima cartella configurata per mappare '{drive_path}' a '{local_path}'")
                return os.path.normpath(local_path)
        
        # Logica originale per i percorsi completi
        for drive_folder, local_folder in self.sync_folders.items():
            # Se il drive_folder è 'root' o '/', gestisci in modo speciale
            if (drive_folder == 'root' or drive_folder == '/') and not drive_path.startswith('/'):
                local_path = os.path.join(local_folder, drive_path)
                return os.path.normpath(local_path)
                
            if drive_path.startswith(drive_folder) or drive_path == drive_folder:
                # Sostituisci il prefisso del percorso Drive con il percorso locale
                relative_path = drive_path[len(drive_folder):] if drive_path != drive_folder else ""
                relative_path = relative_path.lstrip('/')
                
                # Costruisci il percorso locale
                local_path = os.path.join(local_folder, relative_path)
                
                # Normalizza il percorso per il sistema operativo corrente
                local_path = os.path.normpath(local_path)
                
                return local_path
        
        # Tentativo di mappatura flessibile se non è stata trovata una corrispondenza esatta
        file_name = os.path.basename(drive_path)
        for _, local_folder in self.sync_folders.items():
            # Prova semplicemente ad unire il nome file al percorso locale
            possible_path = os.path.join(local_folder, file_name)
            logger.debug(f"Tentativo di mappatura flessibile: '{drive_path}' a '{possible_path}'")
            return os.path.normpath(possible_path)
        
        # Nessuna corrispondenza trovata
        return None
    
    def _get_drive_path_for_local_path(self, local_path):
        """
        Calcola il percorso su Drive corrispondente a un percorso locale.
        
        Args:
            local_path: Percorso locale (relativo alla struttura)
            
        Returns:
            str or None: Percorso Drive corrispondente o None se non trovato
        """
        # Questo metodo assume che local_path sia un percorso relativo alla struttura,
        # come quelli generati durante la scansione, non un percorso assoluto
        
        # Trova la cartella principale locale a cui appartiene il percorso
        for drive_folder, local_base in self.sync_folders.items():
            local_base_name = os.path.basename(os.path.normpath(local_base))
            
            # Verifica se il percorso inizia con il nome della cartella locale base
            if local_path.startswith(local_base_name):
                # Estrai il percorso relativo
                relative_path = local_path[len(local_base_name):].lstrip('/')
                
                # Costruisci il percorso Drive
                drive_path = f"{drive_folder.rstrip('/')}/{relative_path}" if relative_path else drive_folder
                
                return drive_path
                
        # Nessuna corrispondenza trovata
        return None
    
    def _get_absolute_local_path(self, relative_path):
        """
        Converte un percorso relativo della struttura in un percorso locale assoluto.
        
        Args:
            relative_path: Percorso relativo alla struttura
            
        Returns:
            str or None: Percorso locale assoluto o None se non trovato
        """
        for drive_folder, local_folder in self.sync_folders.items():
            local_base_name = os.path.basename(os.path.normpath(local_folder))
            
            # Se il percorso inizia con il nome della cartella locale base
            if relative_path.startswith(local_base_name):
                # Sostituisci il nome della cartella base con il percorso completo
                local_path = relative_path.replace(local_base_name, local_folder, 1)
                return os.path.normpath(local_path)
        
        # Se nessuna corrispondenza diretta, prova a costruire il percorso
        first_part = relative_path.split('/')[0]
        for drive_folder, local_folder in self.sync_folders.items():
            if os.path.basename(local_folder) == first_part:
                rest_path = '/'.join(relative_path.split('/')[1:])
                return os.path.normpath(os.path.join(os.path.dirname(local_folder), relative_path))
        
        # Se siamo qui, proviamo un approccio diverso per i percorsi che 
        # non iniziano con il nome della cartella base
        for drive_folder, local_folder in self.sync_folders.items():
            # Prova a vedere se il percorso è dentro la cartella locale base
            if '/'+first_part+'/' in local_folder+'/' or local_folder.endswith('/'+first_part):
                return os.path.normpath(os.path.join(local_folder, relative_path))
        
        return None
    
    def _format_size(self, size_bytes):
        """
        Formatta una dimensione in bytes in formato leggibile.
        
        Args:
            size_bytes: Dimensione in bytes
            
        Returns:
            str: Dimensione formattata
        """
        if size_bytes >= 1024**3:  # GB
            return f"{round(size_bytes/(1024**3), 2)} GB"
        elif size_bytes >= 1024**2:  # MB
            return f"{round(size_bytes/(1024**2), 2)} MB"
        elif size_bytes >= 1024:  # KB
            return f"{round(size_bytes/1024, 2)} KB"
        else:
            return f"{size_bytes} B"