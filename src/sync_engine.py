"""
Motore di sincronizzazione che confronta strutture di file locali e remote,
e determina le azioni da intraprendere per la sincronizzazione.
"""

import os
import logging
import datetime
import hashlib
import mimetypes
from typing import Dict, List, Tuple, Any

logger = logging.getLogger('drive_sync.sync_engine')

class SyncEngine:
    """
    Motore di sincronizzazione per confrontare e sincronizzare strutture di file 
    tra Google Drive e il file system locale.
    """
    
    def __init__(self, drive_client, config: Dict[str, Any]):
        """
        Inizializza il motore di sincronizzazione.
        
        Args:
            drive_client: Client per interagire con Google Drive
            config: Configurazione della sincronizzazione
        """
        self.drive_client = drive_client
        self.config = config
        self.preferences = config.get('preferences', {})
        self.sync_folders = config.get('sync_folders', {})
        logger.debug("SyncEngine inizializzato con successo")
    
    def scan_drive_structure(self, folder_path: str):
        """
        Scansiona una cartella su Google Drive e restituisce la sua struttura completa.
        Supporta sia nomi di cartelle singole che percorsi con slash.
        
        Args:
            folder_path: Nome o percorso della cartella su Google Drive da scansionare
                
        Returns:
            dict: Struttura della cartella con metadati completi
        """
        # Gestisci percorsi con slash
        if '/' in folder_path:
            # Utilizza la funzione find_folder_by_path per trovare cartelle per percorso
            folder_id = self.drive_client.find_folder_by_path(folder_path)
            if not folder_id:
                logger.warning(f"Cartella '{folder_path}' non trovata su Google Drive")
                return None
                
            logger.info(f"Cartella '{folder_path}' trovata su Google Drive (ID: {folder_id})")
        else:
            # Comportamento originale per cartelle nella root
            query = f"name='{folder_path}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            folders = self.drive_client.search_files(query)
            
            if not folders:
                logger.warning(f"Cartella '{folder_path}' non trovata su Google Drive")
                return None
            
            # Utilizza la prima cartella trovata con quel nome
            folder_id = folders[0]['id']
            logger.info(f"Cartella '{folder_path}' trovata su Google Drive (ID: {folder_id})")
        
        # Scansione ricorsiva della cartella
        structure = self.drive_client.get_folder_hierarchy(folder_id)
        
        # Aggiungi metadati aggiuntivi e struttura
        self._enhance_drive_metadata(structure)
        
        return structure
    
    def _enhance_drive_metadata(self, item):
        """
        Aggiunge metadati aggiuntivi a un elemento di Drive.
        Modifica l'elemento in-place.
        
        Args:
            item: Elemento da arricchire con metadati
        """
        # Aggiungi informazioni path
        if 'path' not in item:
            item['path'] = item.get('name', '')
        
        # Processa i file all'interno
        if 'files' in item:
            for file in item['files']:
                file['parent_path'] = item['path']
                file['full_path'] = f"{item['path']}/{file['name']}"
                
                # Aggiungi flag per tipi speciali di file
                file['is_google_doc'] = self.drive_client.is_google_doc(file.get('mimeType', ''))
                
                # Calcola MD5 se disponibile
                file['md5'] = file.get('md5Checksum', None)
        
        # Procedi ricorsivamente per tutte le sottocartelle
        if 'children' in item:
            for child in item['children']:
                child['path'] = f"{item['path']}/{child['name']}"
                self._enhance_drive_metadata(child)
    
    def scan_local_structure(self, local_path: str):
        """
        Scansiona una cartella locale e restituisce la sua struttura completa.
        
        Args:
            local_path: Percorso della cartella locale da scansionare
            
        Returns:
            dict: Struttura della cartella con metadati completi
        """
        if not os.path.exists(local_path):
            logger.warning(f"Il percorso locale '{local_path}' non esiste")
            return None
        
        if not os.path.isdir(local_path):
            logger.warning(f"'{local_path}' non è una cartella")
            return None
        
        # Ottieni il nome della cartella base
        base_name = os.path.basename(local_path)
        
        # Crea la struttura base
        structure = {
            'id': 'local',
            'name': base_name,
            'path': base_name,
            'children': [],
            'files': []
        }
        
        # Scansiona la cartella ricorsivamente
        self._scan_local_folder(local_path, structure)
        
        return structure
    
    def _scan_local_folder(self, folder_path: str, folder_structure: Dict):
        """
        Scansiona ricorsivamente una cartella locale.
        Aggiunge gli elementi in-place alla struttura della cartella.
        
        Args:
            folder_path: Percorso della cartella da scansionare
            folder_structure: Struttura della cartella da aggiornare
        """
        try:
            items = os.listdir(folder_path)
            
            for item_name in items:
                item_path = os.path.join(folder_path, item_name)
                
                # Controlla se l'elemento deve essere escluso
                if self._should_exclude(item_name):
                    logger.debug(f"Elemento escluso: {item_path}")
                    continue
                
                if os.path.isdir(item_path):
                    # È una cartella
                    child_structure = {
                        'id': 'local',
                        'name': item_name,
                        'path': f"{folder_structure['path']}/{item_name}",
                        'children': [],
                        'files': []
                    }
                    
                    # Scansiona ricorsivamente la sottocartella
                    self._scan_local_folder(item_path, child_structure)
                    
                    # Aggiungi alla struttura parent
                    folder_structure['children'].append(child_structure)
                    
                else:
                    # È un file
                    file_stats = os.stat(item_path)
                    file_structure = {
                        'id': 'local',
                        'name': item_name,
                        'parent_path': folder_structure['path'],
                        'full_path': f"{folder_structure['path']}/{item_name}",
                        'mimeType': self._guess_mime_type(item_name),
                        'size': file_stats.st_size,
                        'modifiedTime': datetime.datetime.fromtimestamp(file_stats.st_mtime).isoformat() + 'Z',
                        'md5': self._calculate_file_md5(item_path)
                    }
                    
                    # Aggiungi alla struttura parent
                    folder_structure['files'].append(file_structure)
        
        except Exception as e:
            logger.error(f"Errore durante la scansione della cartella locale '{folder_path}': {str(e)}")
    
    def _should_exclude(self, item_name: str) -> bool:
        """
        Verifica se un elemento deve essere escluso dalla sincronizzazione
        in base alle regole di esclusione configurate.
        
        Args:
            item_name: Nome dell'elemento da verificare
            
        Returns:
            bool: True se l'elemento deve essere escluso, False altrimenti
        """
        # Ottieni le regole di esclusione dalla configurazione
        exclusions = self.preferences.get('exclusions', [])
        
        for pattern in exclusions:
            import fnmatch
            if fnmatch.fnmatch(item_name, pattern):
                return True
        
        return False
    
    def _guess_mime_type(self, file_name: str) -> str:
        """
        Cerca di indovinare il tipo MIME di un file in base all'estensione.
        
        Args:
            file_name: Nome del file
            
        Returns:
            str: Tipo MIME del file
        """
        mime_type, _ = mimetypes.guess_type(file_name)
        return mime_type if mime_type else 'application/octet-stream'
    
    def _calculate_file_md5(self, file_path: str) -> str:
        """
        Calcola l'MD5 di un file.
        
        Args:
            file_path: Percorso del file
            
        Returns:
            str: Hash MD5 del file o None in caso di errore
        """
        try:
            md5_hash = hashlib.md5()
            with open(file_path, "rb") as f:
                # Leggi il file a blocchi per evitare di caricare file enormi in memoria
                for chunk in iter(lambda: f.read(4096), b""):
                    md5_hash.update(chunk)
            return md5_hash.hexdigest()
        except Exception as e:
            logger.error(f"Errore durante il calcolo dell'MD5 per '{file_path}': {str(e)}")
            return None
    
    def compare_structures(self, drive_structure, local_structure):
        """
        Confronta le strutture di Google Drive e locali per identificare differenze.
        
        Args:
            drive_structure: Struttura delle cartelle di Google Drive
            local_structure: Struttura delle cartelle locali
            
        Returns:
            dict: Piano di sincronizzazione con azioni da intraprendere
        """
        if not drive_structure or not local_structure:
            logger.warning("Impossibile confrontare le strutture: una o entrambe sono mancanti")
            return None
        
        # Inizializza il piano di sincronizzazione
        sync_plan = {
            'to_download': [],  # File da scaricare da Drive
            'to_upload': [],    # File da caricare su Drive
            'to_delete_local': [],  # File da eliminare localmente
            'to_delete_remote': [],  # File da eliminare su Drive
            'conflicts': [],    # File con conflitti di modifica
            'drive_only_folders': [],  # Cartelle presenti solo su Drive
            'local_only_folders': []   # Cartelle presenti solo in locale
        }
        
        # Esegui confronto ricorsivo
        self._compare_folders(drive_structure, local_structure, sync_plan)
        
        return sync_plan
    
    def _compare_folders(self, drive_folder, local_folder, sync_plan):
        """
        Confronta ricorsivamente due cartelle (Drive e locale).
        Aggiorna il piano di sincronizzazione in-place.
        
        Args:
            drive_folder: Struttura della cartella di Drive
            local_folder: Struttura della cartella locale
            sync_plan: Piano di sincronizzazione da aggiornare
        """
        # Confronta i file nelle cartelle
        self._compare_files(drive_folder, local_folder, sync_plan)
        
        # Mappa le sottocartelle per nome
        drive_children = {child['name']: child for child in drive_folder.get('children', [])}
        local_children = {child['name']: child for child in local_folder.get('children', [])}
        
        # Trova cartelle presenti solo su Drive
        for name, folder in drive_children.items():
            if name not in local_children:
                sync_plan['drive_only_folders'].append(folder)
        
        # Trova cartelle presenti solo in locale
        for name, folder in local_children.items():
            if name not in drive_children:
                sync_plan['local_only_folders'].append(folder)
        
        # Confronta ricorsivamente le cartelle comuni
        for name in set(drive_children.keys()) & set(local_children.keys()):
            self._compare_folders(drive_children[name], local_children[name], sync_plan)
    
    def _compare_files(self, drive_folder, local_folder, sync_plan):
        """
        Confronta i file in due cartelle (Drive e locale).
        Aggiorna il piano di sincronizzazione in-place.
        
        Args:
            drive_folder: Struttura della cartella di Drive
            local_folder: Struttura della cartella locale
            sync_plan: Piano di sincronizzazione da aggiornare
        """
        # Mappa i file per nome
        drive_files = {file['name']: file for file in drive_folder.get('files', [])}
        local_files = {file['name']: file for file in local_folder.get('files', [])}
        
        # Ottieni la priorità per risolvere i conflitti
        conflict_resolution = self.preferences.get('conflict_resolution', 'drive')
        
        # Trova file presenti solo su Drive
        for name, file in drive_files.items():
            if name not in local_files:
                # Ignora Google Docs che non possono essere scaricati come file
                if not file.get('is_google_doc', False):
                    sync_plan['to_download'].append(file)
        
        # Trova file presenti solo in locale
        for name, file in local_files.items():
            if name not in drive_files:
                sync_plan['to_upload'].append(file)
        
        # Confronta i file comuni
        for name in set(drive_files.keys()) & set(local_files.keys()):
            drive_file = drive_files[name]
            local_file = local_files[name]
            
            # Ignora Google Docs
            if drive_file.get('is_google_doc', False):
                continue
            
            # Confronta i file per determinare quale è più recente
            is_different = self._are_files_different(drive_file, local_file)
            
            if is_different:
                # Determina quale file è più recente
                drive_time = datetime.datetime.fromisoformat(drive_file.get('modifiedTime', '').replace('Z', '+00:00'))
                local_time = datetime.datetime.fromisoformat(local_file.get('modifiedTime', '').replace('Z', '+00:00'))
                
                if drive_time > local_time:
                    # Drive è più recente
                    if conflict_resolution == 'drive':
                        sync_plan['to_download'].append(drive_file)
                    else:
                        sync_plan['conflicts'].append((drive_file, local_file))
                else:
                    # Locale è più recente
                    if conflict_resolution == 'local':
                        sync_plan['to_upload'].append(local_file)
                    else:
                        sync_plan['conflicts'].append((drive_file, local_file))
    
    def _are_files_different(self, drive_file, local_file):
        """
        Verifica se due file sono diversi in base a vari criteri.
        
        Args:
            drive_file: Metadati del file su Drive
            local_file: Metadati del file locale
            
        Returns:
            bool: True se i file sono diversi, False altrimenti
        """
        # Se entrambi hanno MD5, confrontali (metodo più affidabile)
        if drive_file.get('md5') and local_file.get('md5'):
            return drive_file['md5'] != local_file['md5']
        
        # Se le dimensioni sono diverse, i file sono diversi
        if drive_file.get('size') != local_file.get('size'):
            return True
        
        # Confronta le date di modifica
        try:
            drive_time = datetime.datetime.fromisoformat(drive_file.get('modifiedTime', '').replace('Z', '+00:00'))
            local_time = datetime.datetime.fromisoformat(local_file.get('modifiedTime', '').replace('Z', '+00:00'))
            
            # Tollera piccole differenze (meno di 1 minuto)
            time_diff = abs((drive_time - local_time).total_seconds())
            return time_diff > 60
        except Exception:
            # In caso di errore, considera i file diversi per sicurezza
            return True

    def create_sync_plan(self):
        """
        Crea un piano di sincronizzazione completo per tutte le cartelle configurate.
        
        Returns:
            dict: Piano di sincronizzazione completo
        """
        overall_plan = {
            'to_download': [],
            'to_upload': [],
            'to_delete_local': [],
            'to_delete_remote': [],
            'conflicts': [],
            'drive_only_folders': [],
            'local_only_folders': []
        }
        
        # Per ogni cartella configurata
        for drive_folder_name, local_path in self.sync_folders.items():
            logger.info(f"Analisi della cartella '{drive_folder_name}' -> '{local_path}'")
            
            # Scansiona le strutture
            drive_structure = self.scan_drive_structure(drive_folder_name)
            local_structure = self.scan_local_structure(local_path)
            
            if not drive_structure:
                logger.warning(f"Impossibile trovare la cartella '{drive_folder_name}' su Drive")
                continue
                
            if not local_structure:
                # Se la cartella locale non esiste, sarà creata durante la sincronizzazione
                # Tutti i file di Drive saranno scaricati
                logger.info(f"La cartella locale '{local_path}' non esiste, verrà creata")
                
                # Crea una struttura locale vuota
                local_structure = {
                    'id': 'local',
                    'name': os.path.basename(local_path),
                    'path': os.path.basename(local_path),
                    'children': [],
                    'files': []
                }
            
            # Confronta le strutture
            folder_plan = self.compare_structures(drive_structure, local_structure)
            
            # Aggiungi le informazioni locali
            for item in folder_plan.get('to_download', []):
                item['local_path'] = os.path.join(local_path, item['full_path'][len(drive_structure['path']):].lstrip('/'))
            
            for item in folder_plan.get('to_upload', []):
                item['drive_parent'] = drive_folder_name
            
            # Unisci al piano complessivo
            for key in overall_plan:
                overall_plan[key].extend(folder_plan.get(key, []))
        
        # Registra statistiche
        logger.info(f"Piano di sincronizzazione creato: "
                  f"{len(overall_plan['to_download'])} da scaricare, "
                  f"{len(overall_plan['to_upload'])} da caricare, "
                  f"{len(overall_plan['conflicts'])} conflitti")
        
        return overall_plan