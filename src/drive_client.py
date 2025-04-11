"""
Modulo client per interagire con l'API di Google Drive.
Fornisce un'interfaccia semplificata per operazioni comuni su Google Drive.
"""

import logging
import os
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