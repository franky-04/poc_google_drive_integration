"""
Modulo client per interagire con l'API di Google Drive.
Fornisce un'interfaccia semplificata per operazioni comuni su Google Drive.
"""

import logging
import os
import mimetypes
from googleapiclient.discovery import Resource
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io

logger = logging.getLogger('drive_sync.drive_client')

class DriveClient:
    """
    Client per interagire con l'API di Google Drive.
    Fornisce metodi semplificati per operazioni comuni.
    """

    def __init__(self, service: Resource):
        """
        Inizializza il client Drive.
        
        Args:
            service (Resource): Servizio Google Drive autenticato.
        """
        self.service = service
        logger.debug("DriveClient inizializzato con successo")
    
    def test_connection(self):
        """
        Testa la connessione con Google Drive.
        
        Returns:
            dict: Informazioni di base sull'account Drive.
        """
        try:
            about = self.service.about().get(fields="user,storageQuota").execute()
            logger.info(f"Connessione riuscita: {about['user']['displayName']}")
            return about
        except Exception as e:
            logger.error(f"Errore durante il test della connessione: {str(e)}")
            raise

    def list_files(self, folder_id='root', page_size=100, query=None, fields="files(id,name,mimeType,modifiedTime,size,parents)"):
        """
        Elenca i file in una cartella.
        
        Args:
            folder_id (str): ID della cartella (default: 'root')
            page_size (int): Numero massimo di file da restituire
            query (str): Query personalizzata (se None, elenco tutti i file nella cartella)
            fields (str): Campi da includere nei risultati
            
        Returns:
            list: Lista di file nella cartella specificata
        """
        if query is None:
            query = f"'{folder_id}' in parents and trashed=false"
        
        try:
            results = self.service.files().list(
                q=query,
                pageSize=page_size,
                fields=f"nextPageToken, {fields}"
            ).execute()
            
            items = results.get('files', [])
            
            # Se c'è un nextPageToken, ci sono altri file da recuperare
            # In una versione più completa, potremmo implementare la paginazione
            next_page_token = results.get('nextPageToken')
            if next_page_token:
                logger.debug(f"Ci sono altri file oltre ai primi {page_size} risultati")
            
            logger.debug(f"Trovati {len(items)} file in '{folder_id}'")
            return items
        except Exception as e:
            logger.error(f"Errore durante l'elenco dei file: {str(e)}")
            raise

    def get_file_metadata(self, file_id, fields="id,name,mimeType,modifiedTime,size,parents"):
        """
        Ottiene i metadati di un file.
        
        Args:
            file_id (str): ID del file
            fields (str): Campi da includere nei risultati
            
        Returns:
            dict: Metadati del file
        """
        try:
            file_metadata = self.service.files().get(
                fileId=file_id,
                fields=fields
            ).execute()
            logger.debug(f"Recuperati metadati per file '{file_metadata.get('name')}'")
            return file_metadata
        except Exception as e:
            logger.error(f"Errore durante il recupero dei metadati del file: {str(e)}")
            raise
    
    def search_files(self, query, page_size=100, fields="files(id,name,mimeType,modifiedTime,size,parents)"):
        """
        Cerca file in base a criteri specifici.
        
        Args:
            query (str): Query di ricerca (sintassi delle query di Google Drive)
            page_size (int): Numero massimo di file da restituire
            fields (str): Campi da includere nei risultati
            
        Returns:
            list: Lista di file che corrispondono ai criteri di ricerca
        """
        try:
            results = self.service.files().list(
                q=query,
                pageSize=page_size,
                fields=f"nextPageToken, {fields}"
            ).execute()
            
            items = results.get('files', [])
            logger.debug(f"Trovati {len(items)} file per la query '{query}'")
            return items
        except Exception as e:
            logger.error(f"Errore durante la ricerca dei file: {str(e)}")
            raise
    
    def find_folder_by_path(self, path, parent_id='root'):
        """
        Trova una cartella tramite il suo percorso.
        
        Args:
            path (str): Percorso della cartella (es. 'Cartella1/Cartella2')
            parent_id (str): ID della cartella genitore da cui iniziare la ricerca
            
        Returns:
            str or None: ID della cartella trovata o None se non trovata
        """
        if not path or path == '/' or path == '':
            return parent_id
        
        # Dividi il percorso in componenti
        parts = path.strip('/').split('/')
        current_parent = parent_id
        
        for part in parts:
            # Cerca la cartella corrente tra i figli dell'attuale genitore
            query = f"'{current_parent}' in parents and name='{part}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.search_files(query, page_size=1)
            
            if not results:
                logger.warning(f"Cartella '{part}' non trovata nel percorso '{path}'")
                return None
            
            current_parent = results[0]['id']
        
        logger.debug(f"Trovata cartella per il percorso '{path}': {current_parent}")
        return current_parent
    
    def is_google_doc(self, mime_type):
        """
        Verifica se un file è un documento nativo di Google.
        
        Args:
            mime_type (str): Tipo MIME del file
            
        Returns:
            bool: True se è un documento nativo di Google, False altrimenti
        """
        google_mime_types = [
            'application/vnd.google-apps.document',
            'application/vnd.google-apps.spreadsheet',
            'application/vnd.google-apps.presentation',
            'application/vnd.google-apps.drawing',
            'application/vnd.google-apps.form',
            'application/vnd.google-apps.script'
        ]
        return mime_type in google_mime_types
    
    def get_folder_hierarchy(self, folder_id='root', max_depth=10):
        """
        Ottiene la gerarchia completa di una cartella.
        
        Args:
            folder_id (str): ID della cartella di partenza
            max_depth (int): Profondità massima di ricorsione
            
        Returns:
            dict: Struttura gerarchica della cartella
        """
        if max_depth <= 0:
            logger.warning("Raggiunta profondità massima di ricorsione")
            return {'id': folder_id, 'children': []}
        
        try:
            # Ottieni informazioni sulla cartella corrente
            folder_info = self.get_file_metadata(folder_id)
            
            # Ottieni tutti i file e le cartelle figlie
            children = self.list_files(folder_id)
            
            # Filtra solo le cartelle
            subfolders = [item for item in children if item['mimeType'] == 'application/vnd.google-apps.folder']
            
            # Costruisci la gerarchia ricorsivamente
            folder_struct = {
                'id': folder_id,
                'name': folder_info.get('name', 'root'),
                'children': []
            }
            
            # Aggiungi i file (non cartelle) direttamente
            folder_struct['files'] = [item for item in children if item['mimeType'] != 'application/vnd.google-apps.folder']
            
            # Aggiungi le sottocartelle ricorsivamente
            for subfolder in subfolders:
                subfolder_struct = self.get_folder_hierarchy(subfolder['id'], max_depth - 1)
                folder_struct['children'].append(subfolder_struct)
            
            return folder_struct
            
        except Exception as e:
            logger.error(f"Errore durante il recupero della gerarchia della cartella: {str(e)}")
            raise

    def download_file(self, file_id, local_path):
        """
        Scarica un file da Google Drive.
        
        Args:
            file_id (str): ID del file su Google Drive
            local_path (str): Percorso locale dove salvare il file
            
        Returns:
            bool: True se il download è riuscito, False altrimenti
        """
        try:
            # Crea le directory se necessario
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Crea una richiesta di download
            request = self.service.files().get_media(fileId=file_id)
            
            # Inizializza lo stream di file
            fh = io.FileIO(local_path, mode='wb')
            downloader = MediaIoBaseDownload(fh, request)
            
            # Esegui il download
            done = False
            while not done:
                status, done = downloader.next_chunk()
                logger.debug(f"Download {int(status.progress() * 100)}%")
            
            logger.info(f"File scaricato con successo: {local_path}")
            return True
        except Exception as e:
            logger.error(f"Errore durante il download del file: {str(e)}")
            return False

    def upload_file(self, local_path, parent_id, filename=None):
        """
        Carica un file su Google Drive.
        
        Args:
            local_path (str): Percorso locale del file da caricare
            parent_id (str): ID della cartella padre su Google Drive
            filename (str, optional): Nome del file su Drive. Se None, usa il nome del file locale.
            
        Returns:
            dict or None: Metadati del file caricato o None in caso di errore
        """
        try:
            if not os.path.exists(local_path):
                logger.error(f"Il file locale non esiste: {local_path}")
                return None
            
            # Determina il nome del file
            if filename is None:
                filename = os.path.basename(local_path)
            
            # Indovina il tipo MIME
            import mimetypes
            mime_type, _ = mimetypes.guess_type(local_path)
            if mime_type is None:
                mime_type = 'application/octet-stream'
            
            # Prepara i metadati del file
            file_metadata = {
                'name': filename,
                'parents': [parent_id]
            }
            
            # Prepara il media
            media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
            
            # Esegui l'upload
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,mimeType,modifiedTime,size,parents'
            ).execute()
            
            logger.info(f"File caricato con successo: {filename} (ID: {file.get('id')})")
            return file
        except Exception as e:
            logger.error(f"Errore durante l'upload del file: {str(e)}")
            return None

    def create_folder(self, name, parent_id='root'):
        """
        Crea una cartella su Google Drive.
        
        Args:
            name (str): Nome della cartella
            parent_id (str): ID della cartella padre (default: 'root')
            
        Returns:
            dict or None: Metadati della cartella creata o None in caso di errore
        """
        try:
            # Verifica se la cartella esiste già
            query = f"name='{name}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.search_files(query)
            
            if results:
                logger.debug(f"La cartella '{name}' esiste già in '{parent_id}'")
                return results[0]
            
            # Prepara i metadati della cartella
            folder_metadata = {
                'name': name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            
            # Crea la cartella
            folder = self.service.files().create(
                body=folder_metadata,
                fields='id,name,mimeType,modifiedTime'
            ).execute()
            
            logger.info(f"Cartella creata con successo: {name} (ID: {folder.get('id')})")
            return folder
        except Exception as e:
            logger.error(f"Errore durante la creazione della cartella: {str(e)}")
            return None

    def create_folder_path(self, path, parent_id='root'):
        """
        Crea un percorso di cartelle su Google Drive, creando tutte le cartelle intermedie necessarie.
        
        Args:
            path (str): Percorso della cartella (es. 'Cartella1/Cartella2/Cartella3')
            parent_id (str): ID della cartella padre da cui iniziare (default: 'root')
            
        Returns:
            str or None: ID dell'ultima cartella creata o None in caso di errore
        """
        if not path or path == '/' or path == '':
            return parent_id
        
        # Rimuovi eventuali slash iniziali e finali
        path = path.strip('/')
        
        # Dividi il percorso in componenti
        parts = path.split('/')
        current_parent = parent_id
        
        # Crea ogni parte del percorso
        for part in parts:
            if not part:  # Salta parti vuote
                continue
                
            # Cerca la cartella corrente tra i figli dell'attuale genitore
            query = f"'{current_parent}' in parents and name='{part}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.search_files(query, page_size=1)
            
            if results:
                # Cartella esistente
                current_parent = results[0]['id']
            else:
                # Crea una nuova cartella
                folder = self.create_folder(part, current_parent)
                if not folder:
                    logger.error(f"Impossibile creare la cartella '{part}' nel percorso '{path}'")
                    return None
                current_parent = folder['id']
        
        return current_parent

    def delete_file(self, file_id):
        """
        Elimina un file da Google Drive.
        
        Args:
            file_id (str): ID del file da eliminare
            
        Returns:
            bool: True se l'eliminazione è riuscita, False altrimenti
        """
        try:
            self.service.files().delete(fileId=file_id).execute()
            logger.info(f"File eliminato con successo (ID: {file_id})")
            return True
        except Exception as e:
            logger.error(f"Errore durante l'eliminazione del file: {str(e)}")
            return False

    def update_file(self, file_id, local_path):
        """
        Aggiorna un file esistente su Google Drive.
        
        Args:
            file_id (str): ID del file da aggiornare
            local_path (str): Percorso locale del file aggiornato
            
        Returns:
            dict or None: Metadati del file aggiornato o None in caso di errore
        """
        try:
            if not os.path.exists(local_path):
                logger.error(f"Il file locale non esiste: {local_path}")
                return None
            
            # Indovina il tipo MIME
            import mimetypes
            mime_type, _ = mimetypes.guess_type(local_path)
            if mime_type is None:
                mime_type = 'application/octet-stream'
            
            # Prepara il media
            media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
            
            # Aggiorna il file
            file = self.service.files().update(
                fileId=file_id,
                media_body=media,
                fields='id,name,mimeType,modifiedTime,size,parents'
            ).execute()
            
            logger.info(f"File aggiornato con successo (ID: {file_id})")
            return file
        except Exception as e:
            logger.error(f"Errore durante l'aggiornamento del file: {str(e)}")
            return None